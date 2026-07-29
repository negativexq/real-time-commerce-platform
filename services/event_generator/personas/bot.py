"""Bounded automation-like browsing behavior."""

from datetime import timedelta

from services.event_generator.personas.base import PersonaProfile, PersonaStrategy
from shared.commerce_common.enums import CustomerPersona


class BotStrategy(PersonaStrategy):
    """Catalogue-scanning bursts with almost no purchasing."""

    profile = PersonaProfile(
        CustomerPersona.BOT,
        8,
        20,
        0.03,
        0.05,
        0.50,
        0,
        0,
        "0",
        timedelta(milliseconds=50),
        timedelta(seconds=5),
        repeat_views=True,
    )
