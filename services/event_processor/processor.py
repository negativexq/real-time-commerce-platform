"""Auditable single-record processing orchestration."""

import random
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from time import perf_counter, sleep
from typing import Protocol
from uuid import UUID, uuid4

from services.event_processor.config import ProcessorConfig
from services.event_processor.dlq import DlqPublisher, build_dlq_envelope
from services.event_processor.errors import (
    PermanentProcessingError,
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
from services.event_processor.retry import RetryPolicy, run_with_retry
from services.event_processor.validation import validate_message
from shared.commerce_common.enums import EventType
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
        handlers: Mapping[EventType, EventHandler],
        summary: RunSummary,
        *,
        processor_instance_id: str,
        wait: Callable[[float], object] = sleep,
        rng: random.Random | None = None,
    ) -> None:
        self._config = config
        self._idempotency = idempotency
        self._dlq = dlq
        self._committer = committer
        self._handlers = handlers
        self._summary = summary
        self._instance_id = processor_instance_id
        self._wait = wait
        self._rng = rng or random.Random()
        self._logger = get_logger()
        self._retry_policy = RetryPolicy(
            config.processor_max_processing_attempts,
            config.processor_retry_initial_backoff_ms,
            config.processor_retry_max_backoff_ms,
            config.processor_retry_multiplier,
            config.processor_retry_jitter_ratio,
        )

    def process(self, message: ConsumedMessage) -> ProcessingOutcome:
        started = perf_counter()
        self._summary.consumed_records += 1
        validation = validate_message(message)
        if not validation.valid:
            error = validation.error
            assert error is not None
            self._summary.validation_failures[error.category] += 1
            return self._dead_letter(message, error, 1)

        event = validation.event
        assert event is not None
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
            return ProcessingOutcome(
                ProcessingStatus.DUPLICATE, event_type=event.event_type
            )
        if reservation.state is ReservationState.PROCESSING:
            self._logger.warning(
                "active_processing_lease",
                event_id=str(event.event_id),
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
            )
            self._summary.unresolved_records += 1
            return ProcessingOutcome(ProcessingStatus.UNRESOLVED)

        attempts = 1
        try:
            handler = resolve_handler(self._handlers, event.event_type)
            _, attempts = run_with_retry(
                lambda attempt: self._handle(handler, event, message, attempt),
                self._retry_policy,
                self._wait,
                self._rng,
                self._on_retry,
            )
        except PermanentProcessingError as exc:
            self._summary.processing_failures += 1
            self._safe_release(event.event_id, token)
            return self._dead_letter(
                message,
                self._processing_error(event, exc, "permanent_processing_error"),
                attempts,
            )
        except RetryableProcessingError as exc:
            self._summary.processing_failures += 1
            self._summary.retry_exhausted += 1
            self._safe_release(event.event_id, token)
            return self._dead_letter(
                message,
                self._processing_error(event, exc, "retry_exhausted"),
                self._config.processor_max_processing_attempts,
            )
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
        self._logger.info(
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
        )
        return ProcessingOutcome(ProcessingStatus.PROCESSED, attempts, event.event_type)

    def _handle(
        self,
        handler: EventHandler,
        event: EventEnvelope[ContractModel],
        message: ConsumedMessage,
        attempt: int,
    ) -> None:
        handler.handle(
            event,
            ProcessingContext(
                message.topic,
                message.partition,
                message.offset,
                self._config.processor_consumer_group,
                self._instance_id,
                attempt,
            ),
        )

    def _on_retry(self, attempt: int, error: Exception) -> None:
        self._summary.retries += 1
        self._logger.warning(
            "processing_retry",
            processing_attempt=attempt,
            error_type=type(error).__name__,
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
            self._dlq.publish(record)
        except RetryableProcessingError:
            self._summary.unresolved_records += 1
            return ProcessingOutcome(ProcessingStatus.UNRESOLVED, attempts)
        self._commit(message)
        self._summary.dlq_records += 1
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
        try:
            self._committer.commit_terminal(message)
        except Exception:
            self._summary.commit_failures += 1
            self._summary.unresolved_records += 1
            raise
        self._summary.commit_successes += 1

    @staticmethod
    def _processing_error(
        event: EventEnvelope[ContractModel], error: Exception, category: str
    ) -> ValidationErrorInfo:
        error_category = (
            ValidationCategory.RETRY_EXHAUSTED
            if category == "retry_exhausted"
            else ValidationCategory.PERMANENT_PROCESSING_ERROR
        )
        return ValidationErrorInfo(
            error_category,
            str(error)[:512] or category,
            type(error).__name__,
            event.event_id,
            event.event_type.value,
            event.correlation_id,
        )
