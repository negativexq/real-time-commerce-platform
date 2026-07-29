"""Canonical fraud-alert event construction; no scoring or Kafka I/O."""

import json
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID, uuid5

from services.event_processor.fraud.models import FraudEvaluation
from shared.commerce_common.enums import EventType, FraudDecision
from shared.kafka_metadata import event_message_headers, event_message_key
from shared.schemas import (
    CURRENT_EVENT_VERSION,
    EventEnvelope,
    FraudAlertCreatedPayload,
    canonical_json,
)
from shared.schemas.base import ContractModel

ALERT_NAMESPACE = UUID("8738db40-333e-491a-a20d-4658782dfc44")
ALERT_EVENT_NAMESPACE = UUID("8b3f028f-d1cb-465c-a662-ebfdcfb74d38")
OUTBOX_NAMESPACE = UUID("dc27cc58-f4b8-4849-9417-490923fc7ae2")


@dataclass(frozen=True, slots=True)
class FraudAlertMessage:
    alert_id: UUID
    alert_event_id: UUID
    outbox_id: UUID
    event: EventEnvelope[ContractModel]
    key: bytes
    headers: list[tuple[str, bytes]]
    payload_bytes: bytes


def build_alert_message(
    evaluation: FraudEvaluation,
    source_event: EventEnvelope[ContractModel],
) -> FraudAlertMessage:
    alert_id = uuid5(ALERT_NAMESPACE, str(evaluation.evaluation_id))
    event_id = uuid5(ALERT_EVENT_NAMESPACE, str(alert_id))
    reasons = [item.reason_code for item in evaluation.matched_rules]
    payload = FraudAlertCreatedPayload(
        alert_id=alert_id,
        event_id=evaluation.source_event_id,
        customer_id=evaluation.customer_id,
        order_id=evaluation.order_id,
        fraud_score=Decimal(evaluation.total_score),
        decision=FraudDecision(evaluation.decision.value),
        reasons=reasons or ["DECISION_THRESHOLD"],
        created_at=evaluation.evaluated_at,
    )
    event = EventEnvelope[ContractModel](
        event_id=event_id,
        event_type=EventType.FRAUD_ALERT_CREATED,
        event_version=CURRENT_EVENT_VERSION,
        event_time=evaluation.evaluated_at,
        produced_at=evaluation.evaluated_at,
        source="fraud-engine",
        correlation_id=source_event.correlation_id,
        payload=payload,
    )
    headers = [
        *event_message_headers(event),
        ("causation_id", str(source_event.event_id).encode()),
    ]
    body = canonical_json(event).encode()
    json.loads(body)
    return FraudAlertMessage(
        alert_id,
        event_id,
        uuid5(OUTBOX_NAMESPACE, str(event_id)),
        event,
        event_message_key(event),
        headers,
        body,
    )
