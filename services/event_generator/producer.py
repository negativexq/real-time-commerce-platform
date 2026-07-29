"""Kafka producer boundary, metadata, callbacks, and bounded flushing."""

from collections.abc import Callable
from typing import Protocol
from uuid import UUID

from confluent_kafka import Producer  # type: ignore[import-untyped]

from services.event_generator.config import GeneratorConfig
from services.event_generator.logging import get_logger
from shared.schemas import EventEnvelope, canonical_json
from shared.schemas.base import ContractModel

KafkaHeaders = list[tuple[str, bytes]]


class DeliveryMessage(Protocol):
    """Subset of delivered-message metadata used by the service."""

    def topic(self) -> str: ...

    def partition(self) -> int: ...

    def offset(self) -> int: ...


DeliveryCallback = Callable[[object | None, DeliveryMessage], None]


class ProducerClient(Protocol):
    """Mockable boundary around the Confluent producer."""

    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        headers: KafkaHeaders,
        on_delivery: DeliveryCallback,
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


class ProducerDeliveryError(RuntimeError):
    """Raised when Kafka delivery fails or messages remain queued."""


def message_key(event: EventEnvelope[ContractModel]) -> bytes:
    """Select customer ID when present, otherwise correlation ID."""
    customer_id = getattr(event.payload, "customer_id", None)
    selected = customer_id if isinstance(customer_id, UUID) else event.correlation_id
    return str(selected).encode()


def message_headers(event: EventEnvelope[ContractModel]) -> KafkaHeaders:
    """Build compact UTF-8 Kafka metadata headers."""
    values = {
        "event_id": str(event.event_id),
        "event_type": event.event_type.value,
        "event_version": str(event.event_version),
        "correlation_id": str(event.correlation_id),
        "source": event.source,
        "content_type": "application/json",
    }
    return [(name, value.encode()) for name, value in values.items()]


class KafkaEventProducer:
    """Non-blocking idempotent producer with delivery accounting."""

    def __init__(
        self,
        config: GeneratorConfig,
        client: ProducerClient | None = None,
    ) -> None:
        self._config = config
        self._client = client or Producer(self.kafka_config(config))
        self._delivery_failures = 0
        self._published = 0
        self._logger = get_logger()

    @staticmethod
    def kafka_config(config: GeneratorConfig) -> dict[str, object]:
        """Return the production-safe Confluent producer configuration."""
        return {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "client.id": config.kafka_client_id,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 2_147_483_647,
            "max.in.flight.requests.per.connection": 5,
            "compression.type": config.kafka_compression_type,
            "linger.ms": config.kafka_linger_ms,
            "batch.size": config.kafka_batch_size,
            "delivery.timeout.ms": config.kafka_delivery_timeout_ms,
            "request.timeout.ms": config.kafka_request_timeout_ms,
        }

    @property
    def delivery_failures(self) -> int:
        """Return the number of failed callbacks."""
        return self._delivery_failures

    def publish(self, event: EventEnvelope[ContractModel]) -> None:
        """Queue one event, polling boundedly if the local queue is full."""
        callback = self._delivery_callback(event)
        for attempt in range(6):
            try:
                self._client.produce(
                    self._config.kafka_events_topic,
                    key=message_key(event),
                    value=canonical_json(event).encode(),
                    headers=message_headers(event),
                    on_delivery=callback,
                )
                self._published += 1
                self._client.poll(0)
                return
            except BufferError:
                if attempt == 5:
                    raise ProducerDeliveryError(
                        "Kafka producer queue remained full after bounded polling"
                    ) from None
                self._client.poll(0.1)

    def poll(self, timeout: float = 0) -> None:
        """Serve delivery callbacks."""
        self._client.poll(timeout)
        if self._delivery_failures:
            raise ProducerDeliveryError(
                f"{self._delivery_failures} Kafka delivery callback(s) failed"
            )

    def flush(self) -> None:
        """Flush for the configured bounded timeout and verify all deliveries."""
        remaining = self._client.flush(self._config.generator_flush_timeout_seconds)
        if remaining:
            raise ProducerDeliveryError(
                f"{remaining} Kafka message(s) remained undelivered after flush"
            )
        if self._delivery_failures:
            raise ProducerDeliveryError(
                f"{self._delivery_failures} Kafka delivery callback(s) failed"
            )

    def _delivery_callback(
        self,
        event: EventEnvelope[ContractModel],
    ) -> DeliveryCallback:
        def callback(error: object | None, message: DeliveryMessage) -> None:
            if error is not None:
                self._delivery_failures += 1
                self._logger.error(
                    "event_delivery_failed",
                    event_id=str(event.event_id),
                    event_type=event.event_type.value,
                    correlation_id=str(event.correlation_id),
                    topic=self._config.kafka_events_topic,
                    error=str(error),
                )
                return
            self._logger.info(
                "event_delivered",
                event_id=str(event.event_id),
                event_type=event.event_type.value,
                correlation_id=str(event.correlation_id),
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
            )

        return callback
