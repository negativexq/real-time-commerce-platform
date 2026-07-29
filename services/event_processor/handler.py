"""Typed event handler boundary and complete audit registry."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Protocol

from services.event_processor.errors import PermanentProcessingError
from services.event_processor.logging import get_logger
from services.event_processor.models import ProcessingContext
from shared.commerce_common.enums import EventType
from shared.schemas import EventEnvelope
from shared.schemas.base import ContractModel


class EventHandler(Protocol):
    def handle(
        self, event: EventEnvelope[ContractModel], context: ProcessingContext
    ) -> None: ...


class AuditEventHandler:
    """Deterministic no-op handler with structured audit logging."""

    def handle(
        self, event: EventEnvelope[ContractModel], context: ProcessingContext
    ) -> None:
        get_logger().info(
            "event_audited",
            event_id=str(event.event_id),
            event_type=event.event_type.value,
            correlation_id=str(event.correlation_id),
            topic=context.topic,
            partition=context.partition,
            offset=context.offset,
            processing_attempt=context.attempt,
        )


def default_handler_registry() -> Mapping[EventType, EventHandler]:
    """Return one explicit handler entry for every supported event type."""
    handler = AuditEventHandler()
    return MappingProxyType({event_type: handler for event_type in EventType})


def resolve_handler(
    registry: Mapping[EventType, EventHandler], event_type: EventType
) -> EventHandler:
    try:
        return registry[event_type]
    except KeyError as exc:
        raise PermanentProcessingError(
            f"no handler registered for {event_type.value}"
        ) from exc
