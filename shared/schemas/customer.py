"""Customer event payloads."""

from uuid import UUID

from shared.commerce_common.clock import UtcDateTime
from shared.commerce_common.enums import CustomerPersona
from shared.schemas.base import ContractModel, CountryCode, NonEmptyString


class UserRegisteredPayload(ContractModel):
    """Payload for a newly registered customer."""

    customer_id: UUID
    email_hash: NonEmptyString
    country_code: CountryCode
    persona: CustomerPersona
    registered_at: UtcDateTime
