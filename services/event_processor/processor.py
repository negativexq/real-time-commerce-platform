"""Auditable single-record processing orchestration."""

import random
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from time import perf_counter, sleep
from typing import Protocol, cast
from uuid import UUID, uuid4

from services.event_processor.config import ProcessorConfig
from services.event_processor.dlq import DlqPublisher, build_dlq_envelope
from services.event_processor.errors import (
    FraudContextDependencyError,
    FraudEvaluationIntegrityError,
    MissingBusinessDependencyError,
    PermanentDatabaseIntegrityError,
    PermanentProcessingError,
    RetryableDatabaseError,
    RetryableProcessingError,
)
from services.event_processor.handler import EventHandler, resolve_handler
from services.event_processor.idempotency import (
    RedisIdempotencyStore,
    ReservationState,
)
from services.event_processor.logging import get_logger
from services.event_processor.models import (
    ConsumedMessage,
    ProcessingContext,
    ProcessingOutcome,
    ProcessingStatus,
    RunSummary,
    ValidationCategory,
    ValidationErrorInfo,
)
from services.event_processor.persistence.models import PersistenceResult
from services.event_processor.persistence.unit_of_work import (
    TransactionalHandler,
    UnitOfWorkFactory,
)
from services.event_processor.retry import RetryPolicy, run_with_retry
from services.event_processor.validation import validate_message
from shared.commerce_common.enums import EventType
from shared.observability.metrics import ApplicationMetrics
from shared.schemas import EventEnvelope
from shared.schemas.base import ContractModel


class OffsetCommitter(Protocol):
    def commit_terminal(self, message: ConsumedMessage) -> None: ...


class MessageProcessor:
    """Drive validation, Redis, handler, retry, DLQ, then offset commit."""

    def __init__(
        self,
        config: ProcessorConfig,
        idempotency: RedisIdempotencyStore,
        dlq: DlqPublisher,
        committer: OffsetCommitter,
        handlers: Mapping[EventType, EventHandler | TransactionalHandler],
        summary: RunSummary,
        *,
        processor_instance_id: str,
        persistence: UnitOfWorkFactory | None = None,
        wait: Callable[[float], object] = sleep,
        rng: random.Random | None = None,
        metrics: ApplicationMetrics | None = None,
    ) -> None:
        self._config = config
        self._idempotency = idempotency
        self._dlq = dlq
        self._committer = committer
        self._handlers = handlers
        self._summary = summary
        self._instance_id = processor_instance_id
        self._persistence = persistence
        self._wait = wait
        self._rng = rng or random.Random()
        self._logger = get_logger()
        self._metrics = metrics
        self._retry_policy = RetryPolicy(
            config.processor_max_processing_attempts,
            config.processor_retry_initial_backoff_ms,
            config.processor_retry_max_backoff_ms,
            config.processor_retry_multiplier,
            config.processor_retry_jitter_ratio,
        )

    def process(self, message: ConsumedMessage) -> ProcessingOutcome:
        started = perf_counter()
        if self._metrics is not None:
            self._metrics.processor_inflight.inc()
        self._summary.consumed_records += 1
        validation_started = perf_counter()
        validation = validate_message(message)
        if self._metrics is not None:
            self._metrics.processor_validation_duration.observe(
                perf_counter() - validation_started
            )
        if not validation.valid:
            error = validation.error
            assert error is not None
            self._summary.validation_failures[error.category] += 1
            if self._metrics is not None:
                self._metrics.processor_events_received.labels(
                    "unknown", message.topic
                ).inc()
                self._metrics.processor_validation_failures.labels(
                    error.category.value
                ).inc()
            outcome = self._dead_letter(message, error, 1)
            self._observe_terminal("unknown", outcome, started)
            return outcome

        event = validation.event
        assert event is not None
        if self._metrics is not None:
            self._metrics.processor_events_received.labels(
                event.event_type.value, message.topic
            ).inc()
        self._summary.valid_records += 1
        token = str(uuid4())
        try:
            reservation = self._idempotency.reserve(
                event.event_id, token, message, datetime.now(UTC)
            )
        except RetryableProcessingError:
            self._summary.redis_errors += 1
            self._summary.unresolved_records += 1
            return ProcessingOutcome(ProcessingStatus.UNRESOLVED)

        if reservation.state is ReservationState.COMPLETED:
            recovered_persisted = False
            if self._persistence is not None:
                recovery = self._persist_with_retry(event, message)
                if isinstance(recovery, ProcessingOutcome):
                    return recovery
                recovered_persisted = recovery.already_persisted
                if not recovery.already_persisted:
                    self._record_persistence(recovery)
            self._logger.info(
                "duplicate_event_skipped",
                event_id=str(event.event_id),
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
                existing_state="completed",
                action="skip_duplicate",
            )
            self._commit(message)
            self._summary.duplicate_records += 1
            if self._persistence is not None:
                self._summary.already_persisted_events += int(recovered_persisted)
            outcome = ProcessingOutcome(
                ProcessingStatus.DUPLICATE, event_type=event.event_type
            )
            self._observe_terminal(event.event_type.value, outcome, started)
            return outcome
        if reservation.state is ReservationState.PROCESSING:
            self._logger.warning(
                "active_processing_lease",
                event_id=str(event.event_id),
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
            )
            self._summary.unresolved_records += 1
            outcome = ProcessingOutcome(ProcessingStatus.UNRESOLVED)
            self._observe_terminal(event.event_type.value, outcome, started)
            return outcome

        attempts = 1
        persistence_result = PersistenceResult(False)
        try:
            handler = resolve_handler(self._handlers, event.event_type)
            persistence_result, attempts = run_with_retry(
                lambda attempt: self._handle(handler, event, message, attempt),
                self._retry_policy,
                self._wait,
                self._rng,
                self._on_retry,
            )
        except PermanentProcessingError as exc:
            self._summary.processing_failures += 1
            if isinstance(exc, FraudEvaluationIntegrityError):
                self._summary.fraud_integrity_failures += 1
            if isinstance(exc, PermanentDatabaseIntegrityError):
                self._summary.integrity_failures += 1
                self._summary.database_errors += 1
            self._safe_release(event.event_id, token)
            return self._dead_letter(
                message,
                self._processing_error(
                    event,
                    exc,
                    "database_integrity_error"
                    if isinstance(exc, PermanentDatabaseIntegrityError)
                    else "permanent_processing_error",
                ),
                attempts,
            )
        except RetryableProcessingError as exc:
            self._summary.processing_failures += 1
            self._summary.retry_exhausted += 1
            if isinstance(exc, MissingBusinessDependencyError):
                self._summary.missing_dependency_errors += 1
            if isinstance(exc, FraudContextDependencyError):
                self._summary.fraud_context_failures += 1
            if isinstance(exc, RetryableDatabaseError):
                self._summary.database_errors += 1
            self._safe_release(event.event_id, token)
            return self._dead_letter(
                message,
                self._processing_error(
                    event,
                    exc,
                    "missing_business_dependency"
                    if isinstance(exc, MissingBusinessDependencyError)
                    else "retry_exhausted",
                ),
                self._config.processor_max_processing_attempts,
            )
        self._record_persistence(persistence_result)
        try:
            completed = self._idempotency.complete(
                event.event_id, token, datetime.now(UTC)
            )
        except RetryableProcessingError:
            self._summary.redis_errors += 1
            self._summary.unresolved_records += 1
            return ProcessingOutcome(ProcessingStatus.UNRESOLVED, attempts)
        if not completed:
            self._summary.redis_errors += 1
            self._summary.unresolved_records += 1
            return ProcessingOutcome(ProcessingStatus.UNRESOLVED, attempts)

        self._commit(message)
        duration = (perf_counter() - started) * 1_000
        self._summary.processed_records += 1
        self._summary.processed_events[event.event_type] += 1
        self._summary.record_latency(duration)
        self._logger.debug(
            "event_processed",
            event_id=str(event.event_id),
            event_type=event.event_type.value,
            correlation_id=str(event.correlation_id),
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
            processing_attempt=attempts,
            processing_duration_ms=round(duration, 3),
            idempotency_status="completed",
            database_transaction_status="already_persisted"
            if persistence_result.already_persisted
            else "committed",
            affected_tables=list(persistence_result.affected_tables),
            already_persisted=persistence_result.already_persisted,
            database_duration_ms=round(persistence_result.duration_ms, 3),
        )
        outcome = ProcessingOutcome(
            ProcessingStatus.PROCESSED, attempts, event.event_type
        )
        self._observe_terminal(event.event_type.value, outcome, started)
        return outcome

    def _handle(
        self,
        handler: EventHandler | TransactionalHandler,
        event: EventEnvelope[ContractModel],
        message: ConsumedMessage,
        attempt: int,
    ) -> PersistenceResult:
        context = ProcessingContext(
            message.topic,
            message.partition,
            message.offset,
            self._config.processor_consumer_group,
            self._instance_id,
            attempt,
        )
        if self._persistence is not None:
            transactional = cast(TransactionalHandler, handler)
            self._summary.database_transactions_started += 1
            try:
                return self._persistence.persist(event, message, context, transactional)
            except Exception:
                self._summary.database_transactions_rolled_back += 1
                raise
        cast(EventHandler, handler).handle(
            event,
            context,
        )
        return PersistenceResult(False)

    def _on_retry(self, attempt: int, error: Exception) -> None:
        self._summary.retries += 1
        if isinstance(error, (RetryableDatabaseError, MissingBusinessDependencyError)):
            self._summary.database_retries += 1
        self._logger.warning(
            "processing_retry",
            processing_attempt=attempt,
            error_type=type(error).__name__,
        )
        if self._metrics is not None:
            category = (
                "missing_dependency"
                if isinstance(error, MissingBusinessDependencyError)
                else "database"
                if isinstance(error, RetryableDatabaseError)
                else "processing"
            )
            self._metrics.processor_retries.labels(category, "scheduled").inc()

    def _record_persistence(self, result: PersistenceResult) -> None:
        if self._persistence is None:
            return
        if result.already_persisted:
            self._summary.already_persisted_events += 1
            if self._metrics is not None:
                self._metrics.database_transactions.labels("already_persisted").inc()
        else:
            self._summary.database_transactions_committed += 1
            self._summary.rows_written_by_table.update(result.rows_written)
            self._summary.fraud_evaluations += result.rows_written.get(
                "fraud_evaluations", 0
            )
            self._summary.fraud_alerts_created += result.rows_written.get(
                "fraud_alerts", 0
            )
            self._summary.fraud_outbox_rows_created += result.rows_written.get(
                "fraud_outbox", 0
            )
            if result.fraud_decision == "APPROVE":
                self._summary.approve_decisions += 1
            elif result.fraud_decision == "REVIEW":
                self._summary.review_decisions += 1
            elif result.fraud_decision == "BLOCK":
                self._summary.block_decisions += 1
            self._summary.matched_rules.update(result.matched_rule_ids)
            if self._metrics is not None:
                self._metrics.database_transactions.labels("committed").inc()
                for table, count in result.rows_written.items():
                    self._metrics.database_rows_written.labels(table, "insert").inc(
                        count
                    )
                if result.fraud_decision is not None:
                    severity = result.fraud_severity or "UNKNOWN"
                    self._metrics.fraud_evaluations.labels(
                        result.fraud_event_type or "unknown",
                        result.fraud_decision,
                        severity,
                    ).inc()
                    self._metrics.fraud_score.observe(result.fraud_score or 0)
                    self._metrics.fraud_last_evaluation.set_to_current_time()
                    for rule_id in result.matched_rule_ids:
                        self._metrics.fraud_rule_matches.labels(rule_id, severity).inc()
                    if result.rows_written.get("fraud_alerts", 0):
                        self._metrics.fraud_alerts_created.labels(
                            result.fraud_decision, severity
                        ).inc()
        if result.duration_ms >= self._config.processor_db_log_slow_query_ms:
            self._summary.slow_database_operations += 1
            if self._metrics is not None:
                self._metrics.database_slow_operations.labels(
                    "source_event_transaction"
                ).inc()
        if self._metrics is not None:
            transaction_result = (
                "already_persisted" if result.already_persisted else "committed"
            )
            duration_seconds = result.duration_ms / 1_000
            self._metrics.database_transaction_duration.labels(
                transaction_result
            ).observe(duration_seconds)
            self._metrics.processor_database_duration.labels(
                "source_event_transaction", transaction_result
            ).observe(duration_seconds)

    def _persist_with_retry(
        self,
        event: EventEnvelope[ContractModel],
        message: ConsumedMessage,
    ) -> PersistenceResult | ProcessingOutcome:
        attempts = 1
        try:
            handler = resolve_handler(self._handlers, event.event_type)
            result, attempts = run_with_retry(
                lambda attempt: self._handle(handler, event, message, attempt),
                self._retry_policy,
                self._wait,
                self._rng,
                self._on_retry,
            )
            return result
        except PermanentProcessingError as exc:
            self._summary.integrity_failures += int(
                isinstance(exc, PermanentDatabaseIntegrityError)
            )
            return self._dead_letter(
                message,
                self._processing_error(event, exc, "database_integrity_error"),
                attempts,
            )
        except RetryableProcessingError as exc:
            self._summary.retry_exhausted += 1
            category = (
                "missing_business_dependency"
                if isinstance(exc, MissingBusinessDependencyError)
                else "retry_exhausted"
            )
            return self._dead_letter(
                message,
                self._processing_error(event, exc, category),
                self._config.processor_max_processing_attempts,
            )

    def _safe_release(self, event_id: UUID, token: str) -> None:
        try:
            self._idempotency.release(event_id, token)
        except RetryableProcessingError:
            self._summary.redis_errors += 1

    def _dead_letter(
        self,
        message: ConsumedMessage,
        error: ValidationErrorInfo,
        attempts: int,
    ) -> ProcessingOutcome:
        record = build_dlq_envelope(
            message,
            error,
            attempts=attempts,
            consumer_group=self._config.processor_consumer_group,
            processor_instance_id=self._instance_id,
            maximum_payload_bytes=self._config.processor_dlq_max_payload_bytes,
        )
        try:
            started = perf_counter()
            self._dlq.publish(record)
        except RetryableProcessingError:
            self._summary.unresolved_records += 1
            return ProcessingOutcome(ProcessingStatus.UNRESOLVED, attempts)
        self._commit(message)
        self._summary.dlq_records += 1
        if self._metrics is not None:
            self._metrics.processor_dlq_duration.observe(perf_counter() - started)
            self._metrics.processor_dlq_published.labels(error.category.value).inc()
        self._logger.warning(
            "event_dead_lettered",
            dlq_record_id=str(record.dlq_record_id),
            error_category=error.category.value,
            event_id=str(error.event_id) if error.event_id else None,
            event_type=error.event_type,
            anomaly_type=error.anomaly_type,
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
            processing_attempts=attempts,
        )
        return ProcessingOutcome(ProcessingStatus.DLQ, attempts)

    def _commit(self, message: ConsumedMessage) -> None:
        # commit_terminal() records this record's offset as safe and may or
        # may not trigger an actual batched Kafka commit call; the
        # processor_offset_commits/processor_offset_duration metrics are
        # recorded by the committer's own batching layer (OffsetCommitTracker)
        # against real Kafka commit calls, not per event.
        try:
            self._committer.commit_terminal(message)
        except Exception:
            self._summary.commit_failures += 1
            self._summary.unresolved_records += 1
            raise
        self._summary.commit_successes += 1

    def _observe_terminal(
        self, event_type: str, outcome: ProcessingOutcome, started: float
    ) -> None:
        if self._metrics is None:
            return
        result = outcome.status.value
        self._metrics.processor_events_terminal.labels(event_type, result).inc()
        self._metrics.processor_processing_duration.labels(event_type, result).observe(
            perf_counter() - started
        )
        self._metrics.processor_inflight.dec()
        if outcome.terminal:
            self._metrics.processor_last_success.set_to_current_time()
            self._metrics.success()

    @staticmethod
    def _processing_error(
        event: EventEnvelope[ContractModel], error: Exception, category: str
    ) -> ValidationErrorInfo:
        error_category = (
            ValidationCategory.RETRY_EXHAUSTED
            if category == "retry_exhausted"
            else (
                ValidationCategory.MISSING_BUSINESS_DEPENDENCY
                if category == "missing_business_dependency"
                else (
                    ValidationCategory.DATABASE_INTEGRITY_ERROR
                    if category == "database_integrity_error"
                    else ValidationCategory.PERMANENT_PROCESSING_ERROR
                )
            )
        )
        return ValidationErrorInfo(
            error_category,
            str(error)[:512] or category,
            type(error).__name__,
            event.event_id,
            event.event_type.value,
            event.correlation_id,
        )
