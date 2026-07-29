"""Payment velocity rule."""

from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.context import FraudContext
from services.event_processor.fraud.models import FraudRuleResult, FraudSeverity
from services.event_processor.fraud.rules.helpers import result
from shared.commerce_common.enums import EventType


class PaymentVelocityRule:
    rule_id = "payment_velocity"
    rule_version = "1.0"
    supported_event_types = frozenset(
        {EventType.PAYMENT_FAILED, EventType.PAYMENT_COMPLETED}
    )

    def __init__(self, config: FraudConfig) -> None:
        self.config = config
        self.maximum_score = config.fraud_velocity_score

    def evaluate(self, context: FraudContext) -> FraudRuleResult:
        matched = (
            context.recent_payment_attempts >= self.config.fraud_velocity_max_attempts
        )
        return result(
            context,
            self.rule_id,
            matched,
            self.maximum_score,
            FraudSeverity.MEDIUM,
            "PAYMENT_VELOCITY",
            "Several payment attempts occurred inside the configured window.",
            {"attempt_count": min(context.recent_payment_attempts, 999)},
        )
