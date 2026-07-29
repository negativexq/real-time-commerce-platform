"""Suspicious synthetic behavior (not fraud classification)."""

from datetime import timedelta

from services.event_generator.generator import Product
from services.event_generator.personas.base import PersonaProfile, PersonaStrategy
from shared.commerce_common.enums import CustomerPersona


class SuspiciousStrategy(PersonaStrategy):
    """Fast, high-value, device-changing behavior with payment retries."""

    profile = PersonaProfile(
        CustomerPersona.SUSPICIOUS,
        1,
        3,
        0.92,
        0.94,
        0.45,
        0.28,
        0.10,
        "0.05",
        timedelta(seconds=2),
        timedelta(minutes=5),
        device_change_probability=0.75,
        country_change_probability=0.45,
        retry_probability=0.80,
        high_value=True,
    )

    def rank_products(self, products: tuple[Product, ...]) -> tuple[Product, ...]:
        return tuple(sorted(products, key=lambda product: product.price, reverse=True))
