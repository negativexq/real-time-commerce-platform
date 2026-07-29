"""Indecisive customer behavior."""

from datetime import timedelta

from services.event_generator.personas.base import PersonaProfile, PersonaStrategy
from shared.commerce_common.enums import CustomerPersona


class IndecisiveStrategy(PersonaStrategy):
    """Repeated browsing and cart abandonment across sessions."""

    profile = PersonaProfile(
        CustomerPersona.INDECISIVE,
        4,
        8,
        0.62,
        0.32,
        0.88,
        0.06,
        0.25,
        "0.08",
        timedelta(minutes=12),
        timedelta(days=1),
        repeat_views=True,
        reuse_abandoned_cart=True,
    )
