"""Single deterministic source of truth for fraud rules."""

from collections.abc import Iterable

from services.event_processor.errors import FraudRuleConfigurationError
from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.rules.account_takeover import (
    AccountTakeoverCompositeRule,
    RapidCheckoutRule,
)
from services.event_processor.fraud.rules.amount import (
    HighAmountRule,
    PaymentAmountMismatchRule,
)
from services.event_processor.fraud.rules.base import FraudRule
from services.event_processor.fraud.rules.bot import BotCheckoutRule
from services.event_processor.fraud.rules.device import NewDeviceRule
from services.event_processor.fraud.rules.geography import CountryMismatchRule
from services.event_processor.fraud.rules.payment import FailedPaymentBurstRule
from services.event_processor.fraud.rules.refund import RefundAbuseRule
from services.event_processor.fraud.rules.velocity import PaymentVelocityRule


def build_rule_registry(config: FraudConfig) -> tuple[FraudRule, ...]:
    rules: tuple[FraudRule, ...] = (
        HighAmountRule(config),
        PaymentVelocityRule(config),
        FailedPaymentBurstRule(config),
        NewDeviceRule(config),
        CountryMismatchRule(config),
        RapidCheckoutRule(config),
        AccountTakeoverCompositeRule(config),
        RefundAbuseRule(config),
        BotCheckoutRule(config),
        PaymentAmountMismatchRule(config),
    )
    validate_registry(rules, config.enabled_rule_ids)
    enabled = frozenset(config.enabled_rule_ids)
    return tuple(rule for rule in rules if rule.rule_id in enabled)


def validate_registry(
    rules: Iterable[FraudRule], enabled_rule_ids: tuple[str, ...]
) -> None:
    materialized = tuple(rules)
    ids = [rule.rule_id for rule in materialized]
    if len(ids) != len(set(ids)):
        raise FraudRuleConfigurationError("duplicate fraud rule ID")
    missing = set(enabled_rule_ids) - set(ids)
    if missing:
        raise FraudRuleConfigurationError(
            f"configured fraud rules do not exist: {sorted(missing)}"
        )
    if any(rule.maximum_score < 0 or rule.maximum_score > 100 for rule in materialized):
        raise FraudRuleConfigurationError("fraud rule score bound is invalid")
