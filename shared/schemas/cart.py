"""Cart and checkout event payloads."""

from decimal import Decimal
from typing import Self
from uuid import UUID

from pydantic import PositiveInt, model_validator

from shared.commerce_common.enums import Currency
from shared.commerce_common.money import NonNegativeMoney
from shared.schemas.base import ContractModel


class AddedToCartPayload(ContractModel):
    """Payload for adding one product to a cart."""

    session_id: UUID
    customer_id: UUID
    cart_id: UUID
    product_id: UUID
    quantity: PositiveInt
    unit_price: NonNegativeMoney
    currency: Currency


class CheckoutStartedPayload(ContractModel):
    """Payload for checkout monetary totals."""

    session_id: UUID
    customer_id: UUID
    cart_id: UUID
    item_count: PositiveInt
    subtotal: NonNegativeMoney
    discount_amount: NonNegativeMoney
    total_amount: NonNegativeMoney
    currency: Currency

    @model_validator(mode="after")
    def validate_totals(self) -> Self:
        """Ensure the discount and total agree exactly in decimal arithmetic."""
        if self.discount_amount > self.subtotal:
            raise ValueError("discount_amount cannot exceed subtotal")
        expected_total = self.subtotal - self.discount_amount
        if self.total_amount != Decimal(expected_total):
            raise ValueError("total_amount must equal subtotal minus discount_amount")
        return self
