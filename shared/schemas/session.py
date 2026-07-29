"""Session event payloads."""

from uuid import UUID

from pydantic import IPvAnyAddress

from shared.commerce_common.clock import UtcDateTime
from shared.commerce_common.enums import DeviceType, SessionChannel
from shared.schemas.base import ContractModel, CountryCode


class SessionStartedPayload(ContractModel):
    """Payload for the start of a customer session."""

    session_id: UUID
    customer_id: UUID
    device_id: UUID
    device_type: DeviceType
    ip_address: IPvAnyAddress
    country_code: CountryCode
    channel: SessionChannel
    started_at: UtcDateTime
