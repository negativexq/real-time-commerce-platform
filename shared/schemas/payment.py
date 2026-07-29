"""Payment event payloads."""

from uuid import UUID

from pydantic import IPvAnyAddress

from shared.commerce_common.clock import UtcDateTime
from shared.commerce_common.enums import (
    Currency,
    PaymentFailureReason,
    PaymentMethod,
)
from shared.commerce_common.money import PositiveMoney
from shared.schemas.base import ContractModel, CountryCode


class PaymentCompletedPayload(ContractModel):
    """Payload for a successful payment."""

    payment_id: UUID
    order_id: UUID
    customer_id: UUID
    amount: PositiveMoney
    currency: Currency
    payment_method: PaymentMethod
    device_id: UUID
    ip_address: IPvAnyAddress
    country_code: CountryCode
    completed_at: UtcDateTime


class PaymentFailedPayload(ContractModel):
    """Payload for an unsuccessful payment."""

    payment_id: UUID
    order_id: UUID
    customer_id: UUID
    amount: PositiveMoney
    currency: Currency
    payment_method: PaymentMethod
    failure_reason: PaymentFailureReason
    device_id: UUID
    ip_address: IPvAnyAddress
    country_code: CountryCode
    failed_at: UtcDateTime
