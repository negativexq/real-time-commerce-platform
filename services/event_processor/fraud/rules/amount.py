"""Amount and monetary-integrity fraud signals."""

from decimal import Decimal

from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.context import FraudContext
from services.event_processor.fraud.models import FraudRuleResult, FraudSeverity
from services.event_processor.fraud.rules.helpers import result
from shared.commerce_common.enums import EventType

ELIGIBLE = frozenset(
    {
        EventType.CHECKOUT_STARTED,
        EventType.ORDER_CREATED,
        EventType.PAYMENT_FAILED,
        EventType.PAYMENT_COMPLETED,
        EventType.REFUND_REQUESTED,
    }
)


class HighAmountRule:
    rule_id = "high_amount"
    rule_version = "1.0"
    supported_event_types = ELIGIBLE

    def __init__(self, config: FraudConfig) -> None:
        self.config = config
        self.maximum_score = min(
            100,
            config.fraud_high_amount_score + config.fraud_amount_deviation_score,
        )

    def evaluate(self, context: FraudContext) -> FraudRuleResult:
        amount = context.amount
        absolute = (
            amount is not None and amount >= self.config.fraud_high_amount_threshold
        )
        average = context.historical_average_amount
        deviation = (
            amount is not None
            and average is not None
            and average > Decimal("0")
            and amount >= average * self.config.fraud_amount_multiplier_threshold
        )
        score = self.config.fraud_high_amount_score * int(
            absolute
        ) + self.config.fraud_amount_deviation_score * int(deviation)
        return result(
            context,
            self.rule_id,
            absolute or deviation,
            min(score, self.maximum_score),
            FraudSeverity.HIGH if deviation else FraudSeverity.MEDIUM,
            "HIGH_AMOUNT",
            "Transaction amount exceeds a configured behavioral threshold.",
            {"absolute_threshold": absolute, "historical_deviation": deviation},
        )


class PaymentAmountMismatchRule:
    rule_id = "payment_amount_mismatch"
    rule_version = "1.0"
    supported_event_types = ELIGIBLE

    def __init__(self, config: FraudConfig) -> None:
        self.maximum_score = config.fraud_amount_mismatch_score

    def evaluate(self, context: FraudContext) -> FraudRuleResult:
        matched = not context.payment_amount_matches_order
        return result(
            context,
            self.rule_id,
            matched,
            self.maximum_score,
            FraudSeverity.CRITICAL,
            "PAYMENT_AMOUNT_MISMATCH",
            "Persisted monetary integrity indicators do not reconcile.",
            {"integrity_match": not matched},
        )
