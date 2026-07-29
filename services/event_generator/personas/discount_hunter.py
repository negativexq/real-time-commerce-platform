"""Discount-hunter customer behavior."""

from datetime import timedelta

from services.event_generator.generator import Product
from services.event_generator.personas.base import PersonaProfile, PersonaStrategy
from shared.commerce_common.enums import CustomerPersona


class DiscountHunterStrategy(PersonaStrategy):
    """Price-sensitive behavior with larger deterministic discounts."""

    profile = PersonaProfile(
        CustomerPersona.DISCOUNT_HUNTER,
        2,
        5,
        0.82,
        0.72,
        0.90,
        0.10,
        0.75,
        "0.20",
        timedelta(minutes=3),
        timedelta(hours=18),
        reuse_abandoned_cart=True,
    )

    def rank_products(self, products: tuple[Product, ...]) -> tuple[Product, ...]:
        return tuple(sorted(products, key=lambda product: product.price))
