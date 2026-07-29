"""Kafka producer boundary, metadata, callbacks, and bounded flushing."""

from collections.abc import Callable
from time import perf_counter
from typing import Protocol

from confluent_kafka import Producer  # type: ignore[import-untyped]

from services.event_generator.config import GeneratorConfig
from services.event_generator.logging import get_logger
from services.event_generator.messages import KafkaHeaders, PublishableMessage
from shared.kafka_metadata import event_message_headers, event_message_key
from shared.observability.metrics import ApplicationMetrics
from shared.schemas import EventEnvelope, canonical_json
from shared.schemas.base import ContractModel


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
    return event_message_key(event)


def message_headers(event: EventEnvelope[ContractModel]) -> KafkaHeaders:
    """Build compact UTF-8 Kafka metadata headers."""
    return event_message_headers(event)


class KafkaEventProducer:
    """Non-blocking idempotent producer with delivery accounting."""

    def __init__(
        self,
        config: GeneratorConfig,
        client: ProducerClient | None = None,
        metrics: ApplicationMetrics | None = None,
    ) -> None:
        self._config = config
        self._client = client or Producer(self.kafka_config(config))
        self._delivery_failures = 0
        self._published = 0
        self._logger = get_logger()
        self._metrics = metrics

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
        self.publish_message(
            PublishableMessage(
                canonical_json(event).encode(),
                message_key(event),
                message_headers(event),
                event.event_id,
                event.event_type.value,
                event.correlation_id,
            )
        )

    def publish_message(self, message: PublishableMessage) -> None:
        """Queue a prepared valid or deliberately anomalous record."""
        callback = self._delivery_callback(message)
        started = perf_counter()
        for attempt in range(6):
            try:
                self._client.produce(
                    self._config.kafka_events_topic,
                    key=message.key,
                    value=message.value,
                    headers=message.headers,
                    on_delivery=callback,
                )
                self._published += 1
                if self._metrics is not None:
                    self._metrics.generator_publish_duration.labels("queued").observe(
                        perf_counter() - started
                    )
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
        event: PublishableMessage,
    ) -> DeliveryCallback:
        def callback(error: object | None, message: DeliveryMessage) -> None:
            if error is not None:
                self._delivery_failures += 1
                self._logger.error(
                    "event_delivery_failed",
                    event_id=str(event.event_id) if event.event_id else None,
                    event_type=event.event_type,
                    correlation_id=str(event.correlation_id),
                    anomaly_type=event.anomaly_type,
                    topic=self._config.kafka_events_topic,
                    error=str(error),
                )
                if self._metrics is not None:
                    self._metrics.generator_events_published.labels(
                        event.event_type or "unknown", "failed"
                    ).inc()
                return
            if self._metrics is not None:
                self._metrics.generator_events_published.labels(
                    event.event_type or "unknown", "published"
                ).inc()
                self._metrics.success()
            self._logger.info(
                "event_delivered",
                event_id=str(event.event_id) if event.event_id else None,
                event_type=event.event_type,
                correlation_id=str(event.correlation_id),
                anomaly_type=event.anomaly_type,
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
            )

        return callback
