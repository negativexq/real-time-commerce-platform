"""Single source of truth for customer persona strategies."""

from types import MappingProxyType

from services.event_generator.personas.account_takeover import (
    AccountTakeoverStrategy,
)
from services.event_generator.personas.base import PersonaStrategy
from services.event_generator.personas.bot import BotStrategy
from services.event_generator.personas.discount_hunter import (
    DiscountHunterStrategy,
)
from services.event_generator.personas.indecisive import IndecisiveStrategy
from services.event_generator.personas.normal import NormalStrategy
from services.event_generator.personas.suspicious import SuspiciousStrategy
from shared.commerce_common.enums import CustomerPersona

PERSONA_REGISTRY: MappingProxyType[CustomerPersona, PersonaStrategy] = MappingProxyType(
    {
        CustomerPersona.NORMAL: NormalStrategy(),
        CustomerPersona.INDECISIVE: IndecisiveStrategy(),
        CustomerPersona.DISCOUNT_HUNTER: DiscountHunterStrategy(),
        CustomerPersona.SUSPICIOUS: SuspiciousStrategy(),
        CustomerPersona.BOT: BotStrategy(),
        CustomerPersona.ACCOUNT_TAKEOVER: AccountTakeoverStrategy(),
    }
)


def strategy_for(persona: CustomerPersona) -> PersonaStrategy:
    """Return the registered strategy for one persona."""
    return PERSONA_REGISTRY[persona]


__all__ = ["PERSONA_REGISTRY", "strategy_for"]
