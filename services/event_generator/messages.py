"""Typed Kafka-bound message representation."""

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

KafkaHeaders = list[tuple[str, bytes]]


class AnomalyType(StrEnum):
    """Supported controlled raw-message anomalies."""

    DUPLICATE = "duplicate"
    MALFORMED_JSON = "malformed_json"
    MISSING_FIELD = "missing_field"
    UNKNOWN_EVENT_TYPE = "unknown_event_type"
    LATE_EVENT = "late_event"
    OUT_OF_ORDER = "out_of_order"
    PAYLOAD_MISMATCH = "payload_mismatch"


@dataclass(frozen=True, slots=True)
class PublishableMessage:
    """One fully prepared Kafka record."""

    value: bytes
    key: bytes
    headers: KafkaHeaders
    event_id: UUID | None
    event_type: str
    correlation_id: UUID | None
    anomaly_type: AnomalyType | None = None
