"""Fraud outbox publication, recovery, and claim safety tests."""

import inspect
from collections.abc import Callable
from typing import cast

import pytest

from services.event_processor.errors import FraudOutboxRetryableError
from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.engine import FraudEngine
from services.event_processor.fraud.publisher import build_alert_message
from services.fraud_outbox_publisher.config import OutboxConfig
from services.fraud_outbox_publisher.publisher import (
    AlertPublisher,
    ProducerClient,
)
from services.fraud_outbox_publisher.repository import (
    OutboxRecord,
    OutboxRepository,
)
from tests.unit.test_fraud_engine import context, source_event


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
    evaluation = FraudEngine(FraudConfig()).evaluate(
        context(payment_amount_matches_order=False)
    )
    message = build_alert_message(evaluation, source_event())
    return OutboxRecord(
        message.outbox_id,
        message.alert_event_id,
        "commerce.fraud-alerts",
        message.key,
        tuple(message.headers),
        message.payload_bytes,
        1,
        message.outbox_id,
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
