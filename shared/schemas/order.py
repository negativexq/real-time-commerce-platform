"""Order event payloads."""

from decimal import Decimal
from typing import Self
from uuid import UUID

from pydantic import PositiveInt, model_validator

from shared.commerce_common.clock import UtcDateTime
from shared.commerce_common.enums import Currency
from shared.commerce_common.money import NonNegativeMoney
from shared.schemas.base import ContractModel, CountryCode


class OrderCreatedPayload(ContractModel):
    """Payload for a created order."""

    order_id: UUID
    customer_id: UUID
    session_id: UUID
    cart_id: UUID
    item_count: PositiveInt
    subtotal: NonNegativeMoney
    discount_amount: NonNegativeMoney
    total_amount: NonNegativeMoney
    currency: Currency
    shipping_country_code: CountryCode
    billing_country_code: CountryCode
    created_at: UtcDateTime

    @model_validator(mode="after")
    def validate_totals(self) -> Self:
        """Ensure the discount and total agree exactly in decimal arithmetic."""
        if self.discount_amount > self.subtotal:
            raise ValueError("discount_amount cannot exceed subtotal")
        expected_total = self.subtotal - self.discount_amount
        if self.total_amount != Decimal(expected_total):
            raise ValueError("total_amount must equal subtotal minus discount_amount")
        return self
