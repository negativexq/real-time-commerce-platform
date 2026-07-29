"""Internal typed models; commerce contracts remain in ``shared.schemas``."""

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from shared.commerce_common.enums import EventType

KafkaHeaders = Sequence[tuple[str, bytes | None]]


class ValidationCategory(StrEnum):
    MISSING_VALUE = "missing_value"
    MISSING_KEY = "missing_key"
    MALFORMED_JSON = "malformed_json"
    UNKNOWN_EVENT_TYPE = "unknown_event_type"
    CONTRACT_VALIDATION_FAILED = "contract_validation_failed"
    MISSING_HEADER = "missing_header"
    DUPLICATE_HEADER = "duplicate_header"
    INVALID_HEADER_ENCODING = "invalid_header_encoding"
    INVALID_CONTENT_TYPE = "invalid_content_type"
    HEADER_BODY_MISMATCH = "header_body_mismatch"
    KEY_BODY_MISMATCH = "key_body_mismatch"
    UNSUPPORTED_EVENT_VERSION = "unsupported_event_version"
    PERMANENT_PROCESSING_ERROR = "permanent_processing_error"
    RETRY_EXHAUSTED = "retry_exhausted"


class ProcessingStatus(StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    DLQ = "dlq"
    UNRESOLVED = "unresolved"


@dataclass(frozen=True, slots=True)
class ConsumedMessage:
    topic: str
    partition: int
    offset: int
    timestamp: datetime | None
    key: bytes | None
    value: bytes | None
    headers: KafkaHeaders


@dataclass(frozen=True, slots=True)
class ValidationErrorInfo:
    category: ValidationCategory
    message: str
    error_type: str
    event_id: UUID | None = None
    event_type: str | None = None
    correlation_id: UUID | None = None
    anomaly_type: str | None = None


@dataclass(frozen=True, slots=True)
class ProcessingContext:
    topic: str
    partition: int
    offset: int
    consumer_group: str
    processor_instance_id: str
    attempt: int


@dataclass(frozen=True, slots=True)
class ProcessingOutcome:
    status: ProcessingStatus
    attempts: int = 1
    event_type: EventType | None = None

    @property
    def terminal(self) -> bool:
        return self.status is not ProcessingStatus.UNRESOLVED


@dataclass(slots=True)
class RunSummary:
    consumed_records: int = 0
    valid_records: int = 0
    processed_records: int = 0
    duplicate_records: int = 0
    dlq_records: int = 0
    retries: int = 0
    retry_exhausted: int = 0
    kafka_errors: int = 0
    redis_errors: int = 0
    commit_successes: int = 0
    commit_failures: int = 0
    processing_failures: int = 0
    unresolved_records: int = 0
    latency_total_ms: float = 0
    latency_max_ms: float = 0
    validation_failures: Counter[ValidationCategory] = field(default_factory=Counter)
    processed_events: Counter[EventType] = field(default_factory=Counter)

    def record_latency(self, milliseconds: float) -> None:
        self.latency_total_ms += milliseconds
        self.latency_max_ms = max(self.latency_max_ms, milliseconds)

    def as_log(self) -> dict[str, object]:
        average = (
            self.latency_total_ms / self.processed_records
            if self.processed_records
            else 0
        )
        return {
            "consumed_records": self.consumed_records,
            "valid_records": self.valid_records,
            "processed_records": self.processed_records,
            "duplicate_records": self.duplicate_records,
            "dlq_records": self.dlq_records,
            "validation_failures": {
                item.value: self.validation_failures[item]
                for item in sorted(
                    self.validation_failures, key=lambda value: value.value
                )
            },
            "processed_events": {
                item.value: self.processed_events[item]
                for item in sorted(self.processed_events, key=lambda value: value.value)
            },
            "retry_attempts": self.retries,
            "retry_exhausted": self.retry_exhausted,
            "kafka_errors": self.kafka_errors,
            "redis_errors": self.redis_errors,
            "commit_successes": self.commit_successes,
            "commit_failures": self.commit_failures,
            "processing_failures": self.processing_failures,
            "processing_latency_average_ms": round(average, 3),
            "processing_latency_max_ms": round(self.latency_max_ms, 3),
            "unresolved_records": self.unresolved_records,
        }
