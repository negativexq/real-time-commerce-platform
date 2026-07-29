"""Automation-like browsing followed by a transaction."""

from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.context import FraudContext
from services.event_processor.fraud.models import FraudRuleResult, FraudSeverity
from services.event_processor.fraud.rules.helpers import result
from shared.commerce_common.enums import EventType


class BotCheckoutRule:
    rule_id = "bot_checkout"
    rule_version = "1.0"
    supported_event_types = frozenset(
        {
            EventType.CHECKOUT_STARTED,
            EventType.ORDER_CREATED,
            EventType.PAYMENT_FAILED,
            EventType.PAYMENT_COMPLETED,
        }
    )

    def __init__(self, config: FraudConfig) -> None:
        self.config = config
        self.maximum_score = config.fraud_bot_checkout_score

    def evaluate(self, context: FraudContext) -> FraudRuleResult:
        rapid = (
            context.checkout_seconds is not None
            and context.checkout_seconds <= self.config.fraud_rapid_checkout_seconds
        )
        matched = (
            context.product_view_count >= self.config.fraud_bot_view_threshold and rapid
        )
        return result(
            context,
            self.rule_id,
            matched,
            self.maximum_score,
            FraudSeverity.MEDIUM,
            "BOT_CHECKOUT_PATTERN",
            "A high browsing count was followed by a very rapid transaction.",
            {"view_count": min(context.product_view_count, 999), "rapid": rapid},
        )
