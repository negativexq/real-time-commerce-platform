"""Fraud event payloads."""

from typing import Annotated
from uuid import UUID

from pydantic import Field

from shared.commerce_common.clock import UtcDateTime
from shared.commerce_common.enums import FraudDecision
from shared.commerce_common.money import FraudScore
from shared.schemas.base import ContractModel, NonEmptyString


class FraudAlertCreatedPayload(ContractModel):
    """Payload for an explainable fraud alert."""

    alert_id: UUID
    event_id: UUID
    customer_id: UUID | None
    order_id: UUID | None
    fraud_score: FraudScore
    decision: FraudDecision
    reasons: Annotated[list[NonEmptyString], Field(min_length=1)]
    created_at: UtcDateTime
