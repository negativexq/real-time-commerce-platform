"""Confirmed idempotent Kafka publication for claimed alert records."""

from collections.abc import Callable
from typing import Protocol, cast

from confluent_kafka import Producer  # type: ignore[import-untyped]

from services.event_processor.errors import FraudOutboxRetryableError
from services.fraud_outbox_publisher.config import OutboxConfig
from services.fraud_outbox_publisher.repository import OutboxRecord
from shared.schemas import parse_event


class Delivered(Protocol):
    def partition(self) -> int: ...


class ProducerClient(Protocol):
    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        headers: list[tuple[str, bytes]],
        on_delivery: Callable[[object | None, Delivered], None],
    ) -> None: ...

    def flush(self, timeout: float) -> int: ...


class AlertPublisher:
    def __init__(
        self, config: OutboxConfig, client: ProducerClient | None = None
    ) -> None:
        self.client = client or Producer(
            {
                "bootstrap.servers": config.processor.kafka_bootstrap_servers,
                "client.id": "fraud-outbox-publisher",
                "enable.idempotence": True,
                "acks": "all",
                "max.in.flight.requests.per.connection": 5,
            }
        )
        self.timeout = config.processor.processor_dlq_delivery_timeout_ms / 1_000

    def publish(self, record: OutboxRecord) -> int:
        event = parse_event(record.payload_bytes)
        if event.event_id != record.event_id:
            raise ValueError("stored outbox event identity mismatch")
        outcome: dict[str, object] = {}

        def delivered(error: object | None, message: Delivered) -> None:
            outcome["error"] = error
            outcome["partition"] = message.partition()

        self.client.produce(
            record.topic,
            key=record.message_key,
            value=record.payload_bytes,
            headers=list(record.headers),
            on_delivery=delivered,
        )
        remaining = self.client.flush(self.timeout)
        if remaining or outcome.get("error") is not None:
            raise FraudOutboxRetryableError("fraud alert Kafka delivery failed")
        return cast(int, outcome.get("partition", -1))
