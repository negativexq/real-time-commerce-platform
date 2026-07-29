"""Processor ordering, terminal commits, and failure-boundary tests."""

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from services.event_processor.config import ProcessorConfig
from services.event_processor.dlq import DlqPublisher
from services.event_processor.errors import (
    PermanentProcessingError,
    RetryableProcessingError,
)
from services.event_processor.handler import EventHandler
from services.event_processor.idempotency import (
    RedisIdempotencyStore,
    Reservation,
    ReservationState,
)
from services.event_processor.models import (
    ConsumedMessage,
    ProcessingContext,
    ProcessingStatus,
    RunSummary,
)
from services.event_processor.processor import MessageProcessor, OffsetCommitter
from shared.commerce_common.enums import EventType
from shared.schemas import EventEnvelope
from shared.schemas.base import ContractModel
from tests.unit.test_processor_validation import message


class StubStore:
    def __init__(self, state: ReservationState = ReservationState.RESERVED) -> None:
        self.state = state
        self.calls: list[str] = []
        self.complete_result = True
        self.complete_error = False

    def reserve(
        self,
        event_id: UUID,
        token: str,
        record: ConsumedMessage,
        first_seen_at: datetime,
    ) -> Reservation:
        del event_id, token, record, first_seen_at
        self.calls.append("reserve")
        return Reservation(self.state, "token")

    def complete(self, event_id: UUID, token: str, completed_at: datetime) -> bool:
        del event_id, token, completed_at
        self.calls.append("complete")
        if self.complete_error:
            raise RetryableProcessingError("Redis unavailable")
        return self.complete_result

    def release(self, event_id: UUID, token: str) -> bool:
        del event_id, token
        self.calls.append("release")
        return True


class StubDlq:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published = 0

    def publish(self, record: object) -> None:
        del record
        if self.fail:
            raise RetryableProcessingError("DLQ unavailable")
        self.published += 1


class StubCommitter:
    def __init__(self) -> None:
        self.commits: list[int] = []

    def commit_terminal(self, record: ConsumedMessage) -> None:
        self.commits.append(record.offset)


class StubHandler:
    def __init__(self, failures: int = 0, permanent: bool = False) -> None:
        self.failures = failures
        self.permanent = permanent
        self.calls = 0

    def handle(
        self, item: EventEnvelope[ContractModel], context: ProcessingContext
    ) -> None:
        del item, context
        self.calls += 1
        if self.permanent:
            raise PermanentProcessingError("rejected")
        if self.calls <= self.failures:
            raise RetryableProcessingError("temporary")


def processor(
    store: StubStore,
    dlq: StubDlq,
    committer: StubCommitter,
    handler: StubHandler,
    summary: RunSummary,
    *,
    attempts: int = 3,
) -> MessageProcessor:
    config = ProcessorConfig(
        processor_max_processing_attempts=attempts,
        processor_retry_initial_backoff_ms=0,
        processor_retry_max_backoff_ms=0,
    )
    handlers: Mapping[EventType, EventHandler] = {EventType.ORDER_CREATED: handler}
    return MessageProcessor(
        config,
        cast(RedisIdempotencyStore, store),
        cast(DlqPublisher, dlq),
        cast(OffsetCommitter, committer),
        handlers,
        summary,
        processor_instance_id="test",
        wait=lambda seconds: None,
    )


def test_success_completes_redis_before_commit() -> None:
    store, dlq, committer, handler = (
        StubStore(),
        StubDlq(),
        StubCommitter(),
        StubHandler(),
    )
    summary = RunSummary()
    outcome = processor(store, dlq, committer, handler, summary).process(message())
    assert outcome.status is ProcessingStatus.PROCESSED
    assert store.calls == ["reserve", "complete"]
    assert committer.commits == [10]
    assert handler.calls == 1


def test_completed_duplicate_skips_handler_and_commits() -> None:
    store = StubStore(ReservationState.COMPLETED)
    committer, handler = StubCommitter(), StubHandler()
    outcome = processor(store, StubDlq(), committer, handler, RunSummary()).process(
        message()
    )
    assert outcome.status is ProcessingStatus.DUPLICATE
    assert handler.calls == 0
    assert committer.commits == [10]


def test_active_lease_is_unresolved_and_not_committed() -> None:
    committer = StubCommitter()
    outcome = processor(
        StubStore(ReservationState.PROCESSING),
        StubDlq(),
        committer,
        StubHandler(),
        RunSummary(),
    ).process(message())
    assert outcome.status is ProcessingStatus.UNRESOLVED
    assert committer.commits == []


def test_redis_completion_failure_is_unresolved_and_not_committed() -> None:
    store, committer = StubStore(), StubCommitter()
    store.complete_error = True
    outcome = processor(
        store, StubDlq(), committer, StubHandler(), RunSummary()
    ).process(message())
    assert outcome.status is ProcessingStatus.UNRESOLVED
    assert committer.commits == []


def test_validation_failure_publishes_dlq_then_commits() -> None:
    dlq, committer = StubDlq(), StubCommitter()
    invalid = ConsumedMessage(
        "commerce.events", 0, 5, datetime.now(UTC), b"k", b"{", []
    )
    outcome = processor(
        StubStore(), dlq, committer, StubHandler(), RunSummary()
    ).process(invalid)
    assert outcome.status is ProcessingStatus.DLQ
    assert dlq.published == 1
    assert committer.commits == [5]


def test_dlq_failure_never_commits_source() -> None:
    committer = StubCommitter()
    invalid = ConsumedMessage(
        "commerce.events", 0, 5, datetime.now(UTC), b"k", b"{", []
    )
    outcome = processor(
        StubStore(), StubDlq(fail=True), committer, StubHandler(), RunSummary()
    ).process(invalid)
    assert outcome.status is ProcessingStatus.UNRESOLVED
    assert committer.commits == []


def test_retry_then_success_completes_and_commits() -> None:
    store, committer, handler = StubStore(), StubCommitter(), StubHandler(failures=2)
    summary = RunSummary()
    outcome = processor(store, StubDlq(), committer, handler, summary).process(
        message()
    )
    assert outcome.status is ProcessingStatus.PROCESSED
    assert outcome.attempts == 3
    assert summary.retries == 2
    assert store.calls[-1] == "complete"
    assert committer.commits == [10]


def test_exhausted_and_permanent_failures_release_dlq_then_commit() -> None:
    for handler in (StubHandler(failures=3), StubHandler(permanent=True)):
        store, dlq, committer = StubStore(), StubDlq(), StubCommitter()
        outcome = processor(store, dlq, committer, handler, RunSummary()).process(
            message()
        )
        assert outcome.status is ProcessingStatus.DLQ
        assert "release" in store.calls
        assert dlq.published == 1
        assert committer.commits == [10]


def test_missing_handler_is_dlq_configuration_failure() -> None:
    store, dlq, committer = StubStore(), StubDlq(), StubCommitter()
    service = processor(store, dlq, committer, StubHandler(), RunSummary())
    service._handlers = {}  # exercise explicit registry completeness guard
    outcome = service.process(message())
    assert outcome.status is ProcessingStatus.DLQ
    assert committer.commits == [10]


def test_redelivery_after_completed_before_commit_skips_handler() -> None:
    handler = StubHandler()
    committer = StubCommitter()
    outcome = processor(
        StubStore(ReservationState.COMPLETED),
        StubDlq(),
        committer,
        handler,
        RunSummary(),
    ).process(message())
    assert outcome.status is ProcessingStatus.DUPLICATE
    assert handler.calls == 0
    assert committer.commits == [message().offset]
