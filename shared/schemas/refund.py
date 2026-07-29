"""Refund event payloads."""

from uuid import UUID

from shared.commerce_common.clock import UtcDateTime
from shared.commerce_common.enums import Currency
from shared.commerce_common.money import PositiveMoney
from shared.schemas.base import ContractModel, NonEmptyString


class RefundRequestedPayload(ContractModel):
    """Payload for a requested refund."""

    refund_id: UUID
    payment_id: UUID
    order_id: UUID
    customer_id: UUID
    amount: PositiveMoney
    currency: Currency
    reason: NonEmptyString
    requested_at: UtcDateTime
