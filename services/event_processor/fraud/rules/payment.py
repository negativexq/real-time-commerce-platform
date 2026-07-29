"""Failed-payment burst rule."""

from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.context import FraudContext
from services.event_processor.fraud.models import FraudRuleResult, FraudSeverity
from services.event_processor.fraud.rules.helpers import result
from shared.commerce_common.enums import EventType


class FailedPaymentBurstRule:
    rule_id = "failed_payment_burst"
    rule_version = "1.0"
    supported_event_types = frozenset(
        {EventType.PAYMENT_FAILED, EventType.PAYMENT_COMPLETED}
    )

    def __init__(self, config: FraudConfig) -> None:
        self.config = config
        self.maximum_score = config.fraud_failed_payment_score

    def evaluate(self, context: FraudContext) -> FraudRuleResult:
        matched = (
            context.recent_failed_payments >= self.config.fraud_failed_payment_threshold
        )
        return result(
            context,
            self.rule_id,
            matched,
            self.maximum_score,
            FraudSeverity.HIGH,
            "FAILED_PAYMENT_BURST",
            "Repeated normalized payment failures occurred recently.",
            {
                "failure_count": min(context.recent_failed_payments, 999),
                "distinct_reason_count": min(context.recent_failure_reasons, 20),
            },
        )
