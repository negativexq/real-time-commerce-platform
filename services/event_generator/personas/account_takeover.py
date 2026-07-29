"""Synthetic account-takeover behavior."""

from datetime import timedelta

from services.event_generator.generator import Product
from services.event_generator.personas.base import PersonaProfile, PersonaStrategy
from shared.commerce_common.enums import CustomerPersona


class AccountTakeoverStrategy(PersonaStrategy):
    """Rapid high-value activity on a previously normal customer."""

    profile = PersonaProfile(
        CustomerPersona.ACCOUNT_TAKEOVER,
        1,
        2,
        1,
        1,
        0.55,
        0.20,
        0,
        "0",
        timedelta(milliseconds=500),
        timedelta(minutes=1),
        device_change_probability=1,
        country_change_probability=1,
        retry_probability=0.90,
        high_value=True,
    )

    def rank_products(self, products: tuple[Product, ...]) -> tuple[Product, ...]:
        return tuple(sorted(products, key=lambda product: product.price, reverse=True))
