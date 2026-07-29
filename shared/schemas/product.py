"""Product event payloads."""

from uuid import UUID

from pydantic import NonNegativeInt

from shared.commerce_common.enums import Currency
from shared.commerce_common.money import NonNegativeMoney
from shared.schemas.base import ContractModel, NonEmptyString


class ProductViewedPayload(ContractModel):
    """Payload for a product detail view."""

    session_id: UUID
    customer_id: UUID
    product_id: UUID
    category: NonEmptyString
    unit_price: NonNegativeMoney
    currency: Currency
    quantity_available: NonNegativeInt
