"""Shared Kafka key and header rules for commerce event producers and consumers."""

from uuid import UUID

from shared.schemas import EventEnvelope
from shared.schemas.base import ContractModel

KafkaHeaders = list[tuple[str, bytes]]
REQUIRED_EVENT_HEADERS = frozenset(
    {
        "event_id",
        "event_type",
        "event_version",
        "correlation_id",
        "source",
        "content_type",
    }
)


def event_message_key(event: EventEnvelope[ContractModel]) -> bytes:
    """Select customer ID when present, otherwise correlation ID."""
    customer_id = getattr(event.payload, "customer_id", None)
    selected = customer_id if isinstance(customer_id, UUID) else event.correlation_id
    return str(selected).encode()


def event_message_headers(event: EventEnvelope[ContractModel]) -> KafkaHeaders:
    """Build the required compact UTF-8 Kafka metadata headers."""
    values = {
        "event_id": str(event.event_id),
        "event_type": event.event_type.value,
        "event_version": str(event.event_version),
        "correlation_id": str(event.correlation_id),
        "source": event.source,
        "content_type": "application/json",
    }
    return [(name, value.encode()) for name, value in values.items()]


__all__ = [
    "KafkaHeaders",
    "REQUIRED_EVENT_HEADERS",
    "event_message_headers",
    "event_message_key",
]
