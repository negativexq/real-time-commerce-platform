"""Kafka metadata and shared-contract validation."""

import json
from dataclasses import dataclass
from json import JSONDecodeError
from uuid import UUID

from pydantic import ValidationError

from services.event_processor.models import (
    ConsumedMessage,
    ValidationCategory,
    ValidationErrorInfo,
)
from shared.kafka_metadata import REQUIRED_EVENT_HEADERS, event_message_key
from shared.schemas import CURRENT_EVENT_VERSION, EventEnvelope, parse_event
from shared.schemas.base import ContractModel


@dataclass(frozen=True, slots=True)
class ValidationResult:
    event: EventEnvelope[ContractModel] | None
    error: ValidationErrorInfo | None

    @property
    def valid(self) -> bool:
        return self.event is not None


def _error(
    category: ValidationCategory,
    message: str,
    *,
    error_type: str = "PermanentMessageError",
    event_id: UUID | None = None,
    event_type: str | None = None,
    correlation_id: UUID | None = None,
    anomaly_type: str | None = None,
) -> ValidationResult:
    return ValidationResult(
        None,
        ValidationErrorInfo(
            category,
            message[:512],
            error_type,
            event_id,
            event_type,
            correlation_id,
            anomaly_type,
        ),
    )


def _safe_uuid(value: object) -> UUID | None:
    try:
        return UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        return None


def _body_hints(value: bytes) -> tuple[UUID | None, str | None, UUID | None]:
    try:
        decoded = json.loads(value)
    except (JSONDecodeError, UnicodeDecodeError):
        return None, None, None
    if not isinstance(decoded, dict):
        return None, None, None
    raw_type = decoded.get("event_type")
    return (
        _safe_uuid(decoded.get("event_id")),
        raw_type if isinstance(raw_type, str) else None,
        _safe_uuid(decoded.get("correlation_id")),
    )


def validate_message(message: ConsumedMessage) -> ValidationResult:
    """Validate one record without logging or infrastructure side effects."""
    if message.value is None:
        return _error(ValidationCategory.MISSING_VALUE, "Kafka value is required")
    if message.key is None:
        return _error(ValidationCategory.MISSING_KEY, "Kafka key is required")

    grouped: dict[str, list[bytes | None]] = {}
    for name, value in message.headers:
        grouped.setdefault(name, []).append(value)
    anomaly = None
    raw_anomaly = grouped.get("synthetic_anomaly", [None])[0]
    if raw_anomaly is not None:
        try:
            anomaly = raw_anomaly.decode("utf-8")
        except UnicodeDecodeError:
            anomaly = None
    missing = sorted(REQUIRED_EVENT_HEADERS - grouped.keys())
    if missing:
        return _error(
            ValidationCategory.MISSING_HEADER,
            f"missing required header: {missing[0]}",
            anomaly_type=anomaly,
        )
    duplicate = sorted(
        name for name in REQUIRED_EVENT_HEADERS if len(grouped[name]) != 1
    )
    if duplicate:
        return _error(
            ValidationCategory.DUPLICATE_HEADER,
            f"duplicate required header: {duplicate[0]}",
            anomaly_type=anomaly,
        )
    decoded_headers: dict[str, str] = {}
    for name in REQUIRED_EVENT_HEADERS:
        raw = grouped[name][0]
        if raw is None:
            return _error(
                ValidationCategory.INVALID_HEADER_ENCODING,
                f"header {name} has no value",
                anomaly_type=anomaly,
            )
        try:
            decoded_headers[name] = raw.decode("utf-8")
        except UnicodeDecodeError:
            return _error(
                ValidationCategory.INVALID_HEADER_ENCODING,
                f"header {name} is not UTF-8",
                anomaly_type=anomaly,
            )
    if decoded_headers["content_type"] != "application/json":
        return _error(
            ValidationCategory.INVALID_CONTENT_TYPE,
            "content_type must be application/json",
            anomaly_type=anomaly,
        )

    event_id, event_type, correlation_id = _body_hints(message.value)
    try:
        decoded = json.loads(message.value)
    except (JSONDecodeError, UnicodeDecodeError):
        return _error(
            ValidationCategory.MALFORMED_JSON,
            "malformed event JSON",
            event_id=event_id,
            event_type=event_type,
            correlation_id=correlation_id,
            anomaly_type=anomaly,
        )
    if isinstance(decoded, dict):
        raw_type = decoded.get("event_type")
        from shared.commerce_common.enums import EventType

        if not isinstance(raw_type, str) or raw_type not in {
            item.value for item in EventType
        }:
            return _error(
                ValidationCategory.UNKNOWN_EVENT_TYPE,
                "unknown event type",
                event_id=event_id,
                event_type=event_type,
                correlation_id=correlation_id,
                anomaly_type=anomaly,
            )
        version = decoded.get("event_version")
        if isinstance(version, int) and version > CURRENT_EVENT_VERSION:
            return _error(
                ValidationCategory.UNSUPPORTED_EVENT_VERSION,
                f"event version {version} is unsupported",
                event_id=event_id,
                event_type=event_type,
                correlation_id=correlation_id,
                anomaly_type=anomaly,
            )
    try:
        event = parse_event(message.value)
    except (ValueError, ValidationError):
        return _error(
            ValidationCategory.CONTRACT_VALIDATION_FAILED,
            "event contract validation failed",
            event_id=event_id,
            event_type=event_type,
            correlation_id=correlation_id,
            anomaly_type=anomaly,
        )
    if event.event_version != CURRENT_EVENT_VERSION:
        return _error(
            ValidationCategory.UNSUPPORTED_EVENT_VERSION,
            f"event version {event.event_version} is unsupported",
            event_id=event.event_id,
            event_type=event.event_type.value,
            correlation_id=event.correlation_id,
            anomaly_type=anomaly,
        )
    expected_headers = {
        "event_id": str(event.event_id),
        "event_type": event.event_type.value,
        "event_version": str(event.event_version),
        "correlation_id": str(event.correlation_id),
        "source": event.source,
    }
    mismatch = next(
        (
            name
            for name, expected in expected_headers.items()
            if decoded_headers[name] != expected
        ),
        None,
    )
    if mismatch is not None:
        return _error(
            ValidationCategory.HEADER_BODY_MISMATCH,
            f"{mismatch} header does not match body",
            event_id=event.event_id,
            event_type=event.event_type.value,
            correlation_id=event.correlation_id,
            anomaly_type=anomaly,
        )
    if message.key != event_message_key(event):
        return _error(
            ValidationCategory.KEY_BODY_MISMATCH,
            "Kafka key does not match body ordering key",
            event_id=event.event_id,
            event_type=event.event_type.value,
            correlation_id=event.correlation_id,
            anomaly_type=anomaly,
        )
    return ValidationResult(event, None)
