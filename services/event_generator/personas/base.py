"""Shared persona strategy contract and bounded behavior profile."""

from dataclasses import dataclass
from datetime import timedelta

from services.event_generator.generator import Product
from shared.commerce_common.enums import CustomerPersona


@dataclass(frozen=True, slots=True)
class PersonaProfile:
    """Parameters consumed by the shared journey builder."""

    persona: CustomerPersona
    min_views: int
    max_views: int
    add_probability: float
    checkout_probability: float
    payment_success_probability: float
    refund_probability: float
    discount_probability: float
    discount_rate: str
    action_delay: timedelta
    return_delay: timedelta
    device_change_probability: float = 0
    country_change_probability: float = 0
    retry_probability: float = 0
    high_value: bool = False
    repeat_views: bool = False
    reuse_abandoned_cart: bool = False


class PersonaStrategy:
    """Strategy interface with optional catalogue ranking."""

    profile: PersonaProfile

    def rank_products(self, products: tuple[Product, ...]) -> tuple[Product, ...]:
        """Return products in persona-preferred order."""
        return products
