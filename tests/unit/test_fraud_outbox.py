"""Fraud outbox publication, recovery, and claim safety tests."""

import inspect
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from uuid import UUID

import pytest
from pydantic import ValidationError

from services.fraud_outbox_publisher.config import OutboxConfig
from services.fraud_outbox_publisher.errors import FraudOutboxRetryableError
from services.fraud_outbox_publisher.publisher import (
    AlertPublisher,
    ProducerClient,
)
from services.fraud_outbox_publisher.repository import (
    OutboxRecord,
    OutboxRepository,
)
from shared.commerce_common.enums import EventType, FraudDecision
from shared.kafka_metadata import event_message_headers, event_message_key
from shared.schemas import (
    CURRENT_EVENT_VERSION,
    EventEnvelope,
    FraudAlertCreatedPayload,
    canonical_json,
)
from shared.schemas.base import ContractModel

OUTBOX_ID = UUID("00000000-0000-4000-8000-0000000000a1")
ALERT_EVENT_ID = UUID("00000000-0000-4000-8000-0000000000a2")


class Delivered:
    def partition(self) -> int:
        return 2


class FakeProducer:
    def __init__(self, error: object | None = None, remaining: int = 0) -> None:
        self.error = error
        self.remaining = remaining
        self.values: list[bytes] = []
        self.callback: Callable[[object | None, Delivered], None] | None = None

    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        headers: list[tuple[str, bytes]],
        on_delivery: Callable[[object | None, Delivered], None],
    ) -> None:
        del topic, key, headers
        self.values.append(value)
        self.callback = on_delivery

    def flush(self, timeout: float) -> int:
        del timeout
        assert self.callback is not None
        self.callback(self.error, Delivered())
        return self.remaining


def record() -> OutboxRecord:
    """A stored fraud_alert_created outbox row, shaped as the Go processor
    writes it (canonical payload, message key, and headers)."""
    created_at = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    event = EventEnvelope[ContractModel](
        event_id=ALERT_EVENT_ID,
        event_type=EventType.FRAUD_ALERT_CREATED,
        event_version=CURRENT_EVENT_VERSION,
        event_time=created_at,
        produced_at=created_at,
        source="fraud-engine",
        correlation_id=UUID("00000000-0000-4000-8000-0000000000a3"),
        payload=FraudAlertCreatedPayload(
            alert_id=UUID("00000000-0000-4000-8000-0000000000a4"),
            event_id=UUID("00000000-0000-4000-8000-0000000000a5"),
            customer_id=UUID("00000000-0000-4000-8000-0000000000a6"),
            order_id=None,
            fraud_score=Decimal(100),
            decision=FraudDecision.BLOCK,
            reasons=["PAYMENT_AMOUNT_MISMATCH"],
            created_at=created_at,
        ),
    )
    return OutboxRecord(
        OUTBOX_ID,
        ALERT_EVENT_ID,
        "commerce.fraud-alerts",
        event_message_key(event),
        tuple(event_message_headers(event)),
        canonical_json(event).encode(),
        1,
        OUTBOX_ID,
    )


def publisher(fake: FakeProducer) -> AlertPublisher:
    return AlertPublisher(
        OutboxConfig.from_environment(),
        cast(ProducerClient, fake),
    )


def test_confirmed_delivery_returns_partition() -> None:
    fake = FakeProducer()
    assert publisher(fake).publish(record()) == 2
    assert fake.values == [record().payload_bytes]


@pytest.mark.parametrize(
    "fake", [FakeProducer(error=RuntimeError("Kafka down")), FakeProducer(remaining=1)]
)
def test_delivery_failure_is_retryable(fake: FakeProducer) -> None:
    with pytest.raises(FraudOutboxRetryableError):
        publisher(fake).publish(record())


def test_invalid_stored_event_is_permanent_before_kafka() -> None:
    broken = record()
    broken = OutboxRecord(
        broken.outbox_id,
        broken.event_id,
        broken.topic,
        broken.message_key,
        broken.headers,
        b"{}",
        broken.attempts,
        broken.claim_token,
    )
    with pytest.raises(ValueError):
        publisher(FakeProducer()).publish(broken)


def test_claim_query_uses_skip_locked_and_recovers_expired_leases() -> None:
    source = inspect.getsource(OutboxRepository.claim)
    assert "FOR UPDATE SKIP LOCKED" in source
    assert "status = 'PUBLISHING'" in source
    assert "fraud_outbox_claim_ttl_seconds" in source


def test_deterministic_event_id_survives_republication_window() -> None:
    item = record()
    fake = FakeProducer()
    service = publisher(fake)
    service.publish(item)
    service.publish(item)
    assert len(fake.values) == 2
    assert fake.values[0] == fake.values[1]


def test_config_reads_the_same_environment_names_and_defaults() -> None:
    config = OutboxConfig.from_environment(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
            "POSTGRES_DSN": "postgresql://u:p@db:5432/x",
            "PROCESSOR_DLQ_DELIVERY_TIMEOUT_MS": "2500",
            "FRAUD_OUTBOX_BATCH_SIZE": "7",
            "FRAUD_ENGINE_VERSION": "ignored-by-the-publisher",
        }
    )
    assert config.connection.kafka_bootstrap_servers == "broker:9092"
    assert config.connection.postgres_dsn == "postgresql://u:p@db:5432/x"
    assert config.connection.delivery_timeout_ms == 2500
    assert config.outbox.fraud_outbox_batch_size == 7
    assert config.outbox.fraud_outbox_max_attempts == 10
    assert config.outbox.fraud_alert_topic == "commerce.fraud-alerts"


def test_config_rejects_initial_backoff_above_maximum() -> None:
    with pytest.raises(ValidationError, match="initial outbox backoff"):
        OutboxConfig.from_environment(
            {
                "FRAUD_OUTBOX_INITIAL_BACKOFF_MS": "5000",
                "FRAUD_OUTBOX_MAX_BACKOFF_MS": "1000",
            }
        )
