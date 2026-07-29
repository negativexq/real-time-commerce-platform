"""Normal customer behavior."""

from datetime import timedelta

from services.event_generator.personas.base import PersonaProfile, PersonaStrategy
from shared.commerce_common.enums import CustomerPersona


class NormalStrategy(PersonaStrategy):
    """Moderate browsing with stable identity and high payment success."""

    profile = PersonaProfile(
        CustomerPersona.NORMAL,
        1,
        3,
        0.60,
        0.78,
        0.92,
        0.03,
        0.25,
        "0.05",
        timedelta(seconds=30),
        timedelta(hours=6),
    )
