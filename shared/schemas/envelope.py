"""Versioned event envelope, canonical serialization, and typed parsing."""

import json
from datetime import timedelta
from json import JSONDecodeError
from typing import Any, Self
from uuid import UUID

from pydantic import PositiveInt, SerializeAsAny, TypeAdapter, model_validator

from shared.commerce_common.clock import UtcDateTime
from shared.commerce_common.enums import EventType
from shared.schemas.base import ContractModel, NonEmptyString
from shared.schemas.registry import EVENT_PAYLOAD_REGISTRY

MAX_PRODUCER_CLOCK_SKEW = timedelta(minutes=5)
CURRENT_EVENT_VERSION = 1


class EventEnvelope[PayloadT: ContractModel](ContractModel):
    """Strict, versioned envelope around one registered payload type."""

    event_id: UUID
    event_type: EventType
    event_version: PositiveInt
    event_time: UtcDateTime
    produced_at: UtcDateTime
    source: NonEmptyString
    correlation_id: UUID
    payload: SerializeAsAny[PayloadT]

    @model_validator(mode="after")
    def validate_contract_consistency(self) -> Self:
        """Match payload type to event type and reject unreasonable clock skew."""
        expected_payload = EVENT_PAYLOAD_REGISTRY[self.event_type]
        if type(self.payload) is not expected_payload:
            raise ValueError(
                f"{self.event_type.value} requires {expected_payload.__name__}"
            )
        if self.produced_at < self.event_time - MAX_PRODUCER_CLOCK_SKEW:
            raise ValueError(
                "produced_at cannot precede event_time by more than five minutes"
            )
        return self


class _EnvelopeInput(ContractModel):
    """Validated envelope fields used before selecting a payload model."""

    event_id: UUID
    event_type: EventType
    event_version: PositiveInt
    event_time: UtcDateTime
    produced_at: UtcDateTime
    source: NonEmptyString
    correlation_id: UUID
    payload: dict[str, Any]


def canonical_json[PayloadT: ContractModel](event: EventEnvelope[PayloadT]) -> str:
    """Serialize an event deterministically with JSON-safe scalar values."""
    return json.dumps(
        event.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def parse_event(data: str | bytes) -> EventEnvelope[ContractModel]:
    """Parse JSON into an envelope containing the registered payload class."""
    try:
        decoded = json.loads(data)
    except (JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError("malformed event JSON") from exc

    if not isinstance(decoded, dict):
        raise ValueError("event JSON must be an object")

    raw_event_type = decoded.get("event_type")
    if not isinstance(raw_event_type, str):
        raise ValueError(f"unknown event type: {raw_event_type!r}")
    try:
        event_type = EventType(raw_event_type)
    except ValueError as exc:
        raise ValueError(f"unknown event type: {raw_event_type!r}") from exc

    envelope_input = _EnvelopeInput.model_validate_json(data)
    payload_model = EVENT_PAYLOAD_REGISTRY[event_type]
    payload_json = json.dumps(envelope_input.payload, separators=(",", ":"))
    payload = TypeAdapter(payload_model).validate_json(payload_json)

    envelope_values = envelope_input.model_dump(exclude={"payload"})
    return EventEnvelope[ContractModel](**envelope_values, payload=payload)


__all__ = [
    "MAX_PRODUCER_CLOCK_SKEW",
    "CURRENT_EVENT_VERSION",
    "EventEnvelope",
    "canonical_json",
    "parse_event",
]
