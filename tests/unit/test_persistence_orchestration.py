"""PostgreSQL, Redis, and Kafka ordering plus recovery tests."""

from collections.abc import Mapping
from datetime import datetime
from typing import cast
from uuid import UUID

from services.event_processor.config import ProcessorConfig
from services.event_processor.dlq import DlqPublisher
from services.event_processor.handler import EventHandler
from services.event_processor.idempotency import (
    RedisIdempotencyStore,
    Reservation,
    ReservationState,
)
from services.event_processor.models import (
    ConsumedMessage,
    ProcessingStatus,
    RunSummary,
)
from services.event_processor.persistence.models import PersistenceResult
from services.event_processor.persistence.unit_of_work import UnitOfWorkFactory
from services.event_processor.processor import MessageProcessor, OffsetCommitter
from shared.commerce_common.enums import EventType
from tests.unit.test_processor_orchestration import StubDlq, StubHandler
from tests.unit.test_processor_validation import message


class OrderedStore:
    def __init__(self, trace: list[str]) -> None:
        self.trace = trace

    def reserve(
        self,
        event_id: UUID,
        token: str,
        record: ConsumedMessage,
        first_seen_at: datetime,
    ) -> Reservation:
        del event_id, token, record, first_seen_at
        self.trace.append("redis_reserve")
        return Reservation(ReservationState.RESERVED, "token")

    def complete(self, event_id: UUID, token: str, completed_at: datetime) -> bool:
        del event_id, token, completed_at
        self.trace.append("redis_complete")
        return True

    def release(self, event_id: UUID, token: str) -> bool:
        del event_id, token
        self.trace.append("redis_release")
        return True


class OrderedCommitter:
    def __init__(self, trace: list[str]) -> None:
        self.trace = trace

    def commit_terminal(self, record: ConsumedMessage) -> None:
        del record
        self.trace.append("kafka_commit")


class FakePersistence:
    def __init__(self, trace: list[str], *, already_persisted: bool = False) -> None:
        self.trace = trace
        self.already_persisted = already_persisted

    def persist(self, *args: object, **kwargs: object) -> PersistenceResult:
        del args, kwargs
        self.trace.extend(["database_begin", "database_commit"])
        return PersistenceResult(self.already_persisted, ("processed_events",))


def service(trace: list[str], persistence: FakePersistence) -> MessageProcessor:
    config = ProcessorConfig(
        processor_retry_initial_backoff_ms=0,
        processor_retry_max_backoff_ms=0,
    )
    handlers: Mapping[EventType, EventHandler] = {
        EventType.ORDER_CREATED: StubHandler()
    }
    return MessageProcessor(
        config,
        cast(RedisIdempotencyStore, OrderedStore(trace)),
        cast(DlqPublisher, StubDlq()),
        cast(OffsetCommitter, OrderedCommitter(trace)),
        handlers,
        RunSummary(),
        processor_instance_id="test",
        persistence=cast(UnitOfWorkFactory, persistence),
        wait=lambda seconds: None,
    )


def test_database_commit_precedes_redis_and_kafka_commit() -> None:
    trace: list[str] = []
    outcome = service(trace, FakePersistence(trace)).process(message())
    assert outcome.status is ProcessingStatus.PROCESSED
    assert trace == [
        "redis_reserve",
        "database_begin",
        "database_commit",
        "redis_complete",
        "kafka_commit",
    ]


def test_already_persisted_recovery_still_repairs_redis_before_commit() -> None:
    trace: list[str] = []
    outcome = service(trace, FakePersistence(trace, already_persisted=True)).process(
        message()
    )
    assert outcome.status is ProcessingStatus.PROCESSED
    assert trace[-2:] == ["redis_complete", "kafka_commit"]
