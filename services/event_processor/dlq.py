"""Typed deterministic dead-letter records and confirmed Kafka publication."""

import base64
import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid5

from confluent_kafka import Producer  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field

from services.event_processor.config import ProcessorConfig
from services.event_processor.errors import RetryableProcessingError
from services.event_processor.models import ConsumedMessage, ValidationErrorInfo

DLQ_NAMESPACE = UUID("bb8709f1-40b8-4a8f-93ca-96775f4c7d7a")
DLQ_SCHEMA_VERSION = 1
MAX_ERROR_MESSAGE_LENGTH = 512
SAFE_HEADER_NAMES = frozenset(
    {
        "event_id",
        "event_type",
        "event_version",
        "correlation_id",
        "source",
        "content_type",
        "synthetic_anomaly",
    }
)


class DlqEnvelope(BaseModel):
    """Processor-owned schema, deliberately separate from commerce contracts."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    dlq_schema_version: int = DLQ_SCHEMA_VERSION
    dlq_record_id: UUID
    failed_at: datetime
    source_topic: str
    source_partition: int
    source_offset: int
    source_timestamp: datetime | None
    source_key_base64: str | None
    source_headers: dict[str, str]
    original_value_base64: str | None
    original_value_truncated: bool
    error_category: str
    error_message: str = Field(max_length=MAX_ERROR_MESSAGE_LENGTH)
    error_type: str
    processing_attempts: int = Field(gt=0)
    consumer_group: str
    processor_instance_id: str
    original_event_id: UUID | None
    original_event_type: str | None
    original_correlation_id: UUID | None
    anomaly_type: str | None

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()

    def kafka_key(self) -> bytes:
        if self.original_event_id is not None:
            return str(self.original_event_id).encode()
        return (
            f"{self.source_topic}:{self.source_partition}:{self.source_offset}".encode()
        )

    def kafka_headers(self) -> list[tuple[str, bytes]]:
        values = {
            "dlq_record_id": str(self.dlq_record_id),
            "error_category": self.error_category,
            "source_topic": self.source_topic,
            "source_partition": str(self.source_partition),
            "source_offset": str(self.source_offset),
            "content_type": "application/json",
            "dlq_schema_version": str(self.dlq_schema_version),
        }
        if self.original_event_id is not None:
            values["original_event_id"] = str(self.original_event_id)
        return [(name, value.encode()) for name, value in values.items()]


def deterministic_dlq_id(message: ConsumedMessage, error_category: str) -> UUID:
    identity = f"{message.topic}:{message.partition}:{message.offset}:{error_category}"
    return uuid5(DLQ_NAMESPACE, identity)


def build_dlq_envelope(
    message: ConsumedMessage,
    error: ValidationErrorInfo,
    *,
    attempts: int,
    consumer_group: str,
    processor_instance_id: str,
    maximum_payload_bytes: int,
    failed_at: datetime | None = None,
) -> DlqEnvelope:
    value = message.value
    included = value[:maximum_payload_bytes] if value is not None else None
    sanitized: dict[str, str] = {}
    for name, raw in message.headers:
        if name not in SAFE_HEADER_NAMES or raw is None or name in sanitized:
            continue
        try:
            sanitized[name] = raw.decode("utf-8")[:256]
        except UnicodeDecodeError:
            sanitized[name] = "<invalid-utf8>"
    return DlqEnvelope(
        dlq_record_id=deterministic_dlq_id(message, error.category.value),
        failed_at=(failed_at or datetime.now(UTC)),
        source_topic=message.topic,
        source_partition=message.partition,
        source_offset=message.offset,
        source_timestamp=message.timestamp,
        source_key_base64=(
            base64.b64encode(message.key).decode() if message.key is not None else None
        ),
        source_headers=sanitized,
        original_value_base64=(
            base64.b64encode(included).decode() if included is not None else None
        ),
        original_value_truncated=value is not None
        and len(value) > len(included or b""),
        error_category=error.category.value,
        error_message=error.message[:MAX_ERROR_MESSAGE_LENGTH],
        error_type=error.error_type,
        processing_attempts=attempts,
        consumer_group=consumer_group,
        processor_instance_id=processor_instance_id,
        original_event_id=error.event_id,
        original_event_type=error.event_type,
        original_correlation_id=error.correlation_id,
        anomaly_type=error.anomaly_type,
    )


class DeliveryMessage(Protocol):
    def topic(self) -> str: ...


DeliveryCallback = Callable[[object | None, DeliveryMessage], None]


class ProducerClient(Protocol):
    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        headers: list[tuple[str, bytes]],
        on_delivery: DeliveryCallback,
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


class DlqPublisher:
    """Idempotent producer that waits boundedly for the delivery result."""

    def __init__(
        self, config: ProcessorConfig, client: ProducerClient | None = None
    ) -> None:
        self._config = config
        self._client = client or Producer(
            {
                "bootstrap.servers": config.kafka_bootstrap_servers,
                "client.id": f"{config.processor_client_id}-dlq",
                "enable.idempotence": True,
                "acks": "all",
                "delivery.timeout.ms": config.processor_dlq_delivery_timeout_ms,
                "request.timeout.ms": min(
                    config.processor_dlq_delivery_timeout_ms, 10_000
                ),
            }
        )

    def publish(self, record: DlqEnvelope) -> None:
        delivered: list[object | None] = []

        def callback(error: object | None, message: DeliveryMessage) -> None:
            del message
            delivered.append(error)

        try:
            self._client.produce(
                self._config.processor_dlq_topic,
                key=record.kafka_key(),
                value=record.canonical_bytes(),
                headers=record.kafka_headers(),
                on_delivery=callback,
            )
            remaining = self._client.flush(
                self._config.processor_dlq_delivery_timeout_ms / 1_000
            )
        except (BufferError, RuntimeError) as exc:
            raise RetryableProcessingError("DLQ publication failed") from exc
        if remaining or not delivered or delivered[0] is not None:
            raise RetryableProcessingError("DLQ delivery was not confirmed")

    def close(self) -> None:
        self._client.flush(self._config.processor_shutdown_timeout_seconds)
