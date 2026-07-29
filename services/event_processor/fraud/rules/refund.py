"""Refund-abuse rule."""

from decimal import Decimal

from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.context import FraudContext
from services.event_processor.fraud.models import FraudRuleResult, FraudSeverity
from services.event_processor.fraud.rules.helpers import result
from shared.commerce_common.enums import EventType


class RefundAbuseRule:
    rule_id = "refund_abuse"
    rule_version = "1.0"
    supported_event_types = frozenset({EventType.REFUND_REQUESTED})

    def __init__(self, config: FraudConfig) -> None:
        self.config = config
        self.maximum_score = config.fraud_refund_abuse_score

    def evaluate(self, context: FraudContext) -> FraudRuleResult:
        rapid = (
            context.seconds_since_payment is not None
            and context.seconds_since_payment <= self.config.fraud_rapid_refund_seconds
        )
        near_full = (
            context.refund_amount is not None
            and context.refundable_amount is not None
            and context.refundable_amount > 0
            and context.refund_amount / context.refundable_amount
            >= max(self.config.fraud_refund_rate_threshold, Decimal("0.9"))
        )
        repeated = context.recent_refunds >= 3
        matched = repeated or (rapid and near_full and context.recent_refunds >= 2)
        return result(
            context,
            self.rule_id,
            matched,
            self.maximum_score,
            FraudSeverity.HIGH,
            "REFUND_ABUSE_PATTERN",
            "Refund timing, frequency, and normalized amount form "
            "a suspicious pattern.",
            {
                "rapid": rapid,
                "near_full": near_full,
                "recent_refund_count": context.recent_refunds,
            },
        )
