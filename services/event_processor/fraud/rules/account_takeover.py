"""Rapid checkout and account-takeover composite rules."""

from decimal import Decimal

from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.context import FraudContext
from services.event_processor.fraud.models import FraudRuleResult, FraudSeverity
from services.event_processor.fraud.rules.helpers import result
from shared.commerce_common.enums import EventType

SUPPORTED = frozenset(
    {
        EventType.CHECKOUT_STARTED,
        EventType.ORDER_CREATED,
        EventType.PAYMENT_FAILED,
        EventType.PAYMENT_COMPLETED,
    }
)


class RapidCheckoutRule:
    rule_id = "rapid_checkout"
    rule_version = "1.0"
    supported_event_types = SUPPORTED

    def __init__(self, config: FraudConfig) -> None:
        self.config = config
        self.maximum_score = config.fraud_rapid_checkout_score

    def evaluate(self, context: FraudContext) -> FraudRuleResult:
        seconds = context.checkout_seconds
        matched = (
            seconds is not None and seconds <= self.config.fraud_rapid_checkout_seconds
        )
        return result(
            context,
            self.rule_id,
            matched,
            self.maximum_score,
            FraudSeverity.MEDIUM,
            "RAPID_CHECKOUT",
            "The transaction followed session start unusually quickly.",
            {"elapsed_seconds": seconds},
        )


class AccountTakeoverCompositeRule:
    rule_id = "account_takeover_composite"
    rule_version = "1.0"
    supported_event_types = SUPPORTED

    def __init__(self, config: FraudConfig) -> None:
        self.config = config
        self.maximum_score = config.fraud_account_takeover_score

    def evaluate(self, context: FraudContext) -> FraudRuleResult:
        average = context.historical_average_amount
        high = (
            context.amount is not None
            and average is not None
            and average > Decimal("0")
            and context.amount
            >= average * self.config.fraud_amount_multiplier_threshold
        )
        signals = (
            context.established_history,
            context.is_new_device,
            context.country_mismatch,
            context.checkout_seconds is not None
            and context.checkout_seconds <= self.config.fraud_rapid_checkout_seconds,
            high,
        )
        count = sum(signals)
        matched = count >= self.config.fraud_account_takeover_min_signals
        return result(
            context,
            self.rule_id,
            matched,
            self.maximum_score,
            FraudSeverity.CRITICAL,
            "ACCOUNT_TAKEOVER_PATTERN",
            "Multiple established-account change indicators occurred together.",
            {
                "signal_count": count,
                "required_signal_count": self.config.fraud_account_takeover_min_signals,
            },
        )
