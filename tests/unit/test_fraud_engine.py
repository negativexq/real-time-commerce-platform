"""Deterministic fraud configuration, rules, aggregation, and alert tests."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import ValidationError

from services.event_processor.errors import FraudRuleConfigurationError
from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.context import FraudContext
from services.event_processor.fraud.engine import FraudEngine
from services.event_processor.fraud.models import FraudDecision, FraudSeverity
from services.event_processor.fraud.publisher import build_alert_message
from services.event_processor.fraud.registry import (
    build_rule_registry,
    validate_registry,
)
from services.event_processor.fraud.rules.account_takeover import (
    AccountTakeoverCompositeRule,
    RapidCheckoutRule,
)
from services.event_processor.fraud.rules.amount import (
    HighAmountRule,
    PaymentAmountMismatchRule,
)
from services.event_processor.fraud.rules.bot import BotCheckoutRule
from services.event_processor.fraud.rules.device import NewDeviceRule
from services.event_processor.fraud.rules.geography import CountryMismatchRule
from services.event_processor.fraud.rules.payment import FailedPaymentBurstRule
from services.event_processor.fraud.rules.refund import RefundAbuseRule
from services.event_processor.fraud.rules.velocity import PaymentVelocityRule
from shared.commerce_common.enums import Currency, EventType
from shared.schemas import EventEnvelope, OrderCreatedPayload, parse_event
from shared.schemas.base import ContractModel

NOW = datetime(2026, 8, 1, tzinfo=UTC)
SOURCE_ID = UUID("10000000-0000-4000-8000-000000000001")
CUSTOMER_ID = UUID("10000000-0000-4000-8000-000000000002")
ORDER_ID = UUID("10000000-0000-4000-8000-000000000003")
PAYMENT_ID = UUID("10000000-0000-4000-8000-000000000004")
SESSION_ID = UUID("10000000-0000-4000-8000-000000000005")


def context(**changes: object) -> FraudContext:
    values: dict[str, object] = {
        "source_event_id": SOURCE_ID,
        "event_type": EventType.PAYMENT_COMPLETED,
        "event_time": NOW,
        "customer_id": CUSTOMER_ID,
        "order_id": ORDER_ID,
        "payment_id": PAYMENT_ID,
        "session_id": SESSION_ID,
        "amount": Decimal("100"),
        "currency": "TRY",
        "payment_status": "completed",
        "payment_method": "credit_card",
        "current_device": "device-current",
        "current_country": "TR",
        "home_country": "TR",
        "session_started_at": NOW - timedelta(minutes=5),
        "order_time": NOW - timedelta(seconds=30),
        "historical_average_amount": Decimal("100"),
        "recent_payment_attempts": 1,
        "recent_failed_payments": 0,
        "recent_failure_reasons": 0,
        "recent_distinct_devices": 1,
        "recent_distinct_countries": 1,
        "recent_orders": 1,
        "recent_refunds": 0,
        "lifetime_successful_payments": 3,
        "lifetime_failed_payments": 0,
        "lifetime_spend": Decimal("300"),
        "refundable_amount": None,
        "previous_successful_devices": frozenset({"device-current"}),
        "previous_successful_countries": frozenset({"TR"}),
        "product_view_count": 2,
        "refund_amount": None,
        "seconds_since_payment": None,
        "payment_amount_matches_order": True,
    }
    values.update(changes)
    return FraudContext(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("environment", "message"),
    [
        (
            {"FRAUD_REVIEW_THRESHOLD": "60", "FRAUD_BLOCK_THRESHOLD": "60"},
            "review threshold",
        ),
        (
            {"FRAUD_HIGH_AMOUNT_THRESHOLD": "-1"},
            "greater than or equal",
        ),
        (
            {"FRAUD_REFUND_RATE_THRESHOLD": "1.1"},
            "less than or equal",
        ),
    ],
)
def test_invalid_configuration_is_rejected(
    environment: dict[str, str], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        FraudConfig.from_environment(environment)


def test_registry_is_complete_ordered_and_rejects_duplicates() -> None:
    rules = build_rule_registry(FraudConfig())
    assert tuple(rule.rule_id for rule in rules) == FraudConfig().enabled_rule_ids
    with pytest.raises(FraudRuleConfigurationError, match="duplicate"):
        validate_registry((rules[0], rules[0]), (rules[0].rule_id,))
    with pytest.raises(FraudRuleConfigurationError, match="do not exist"):
        validate_registry(rules, ("missing",))


def test_disabled_rule_does_not_execute() -> None:
    config = FraudConfig(fraud_enabled_rules="high_amount")
    assert [rule.rule_id for rule in build_rule_registry(config)] == ["high_amount"]


@pytest.mark.parametrize(
    ("amount", "average", "expected_score"),
    [
        (Decimal("4999.99"), Decimal("2000"), 0),
        (Decimal("5000"), Decimal("2000"), 25),
        (Decimal("300"), Decimal("100"), 20),
        (Decimal("5000"), Decimal("100"), 45),
    ],
)
def test_high_amount_boundaries(
    amount: Decimal, average: Decimal, expected_score: int
) -> None:
    result = HighAmountRule(FraudConfig()).evaluate(
        context(amount=amount, historical_average_amount=average)
    )
    assert result.score == expected_score
    assert result.score <= 45


def test_high_amount_missing_history_and_decimal_precision() -> None:
    rule = HighAmountRule(FraudConfig(fraud_high_amount_threshold=Decimal("0.3")))
    assert (
        rule.evaluate(
            context(amount=Decimal("0.299999999999"), historical_average_amount=None)
        ).score
        == 0
    )
    assert rule.evaluate(
        context(amount=Decimal("0.300000000000"), historical_average_amount=None)
    ).matched


@pytest.mark.parametrize(
    ("attempts", "matched"),
    [(2, False), (3, True), (4, True)],
)
def test_velocity_threshold(attempts: int, matched: bool) -> None:
    assert (
        PaymentVelocityRule(FraudConfig())
        .evaluate(context(recent_payment_attempts=attempts))
        .matched
        is matched
    )


def test_failed_payment_burst_threshold_and_reasons() -> None:
    rule = FailedPaymentBurstRule(FraudConfig())
    assert not rule.evaluate(context(recent_failed_payments=2)).matched
    matched = rule.evaluate(context(recent_failed_payments=3, recent_failure_reasons=2))
    assert matched.matched
    assert matched.evidence["distinct_reason_count"] == 2


def test_new_device_requires_established_history() -> None:
    rule = NewDeviceRule(FraudConfig())
    new_device = {
        "current_device": "new",
        "previous_successful_devices": frozenset({"old"}),
    }
    assert not rule.evaluate(
        context(lifetime_successful_payments=0, **new_device)
    ).matched
    assert rule.evaluate(context(lifetime_successful_payments=3, **new_device)).matched


def test_country_match_mismatch_and_multi_country() -> None:
    rule = CountryMismatchRule(FraudConfig())
    assert not rule.evaluate(context()).matched
    assert rule.evaluate(context(current_country="DE")).score == 10
    assert (
        rule.evaluate(context(current_country="DE", recent_distinct_countries=2)).score
        == 20
    )


def test_rapid_checkout_exact_boundary() -> None:
    rule = RapidCheckoutRule(FraudConfig(fraud_rapid_checkout_seconds=15))
    assert rule.evaluate(
        context(session_started_at=NOW - timedelta(seconds=15))
    ).matched
    assert not rule.evaluate(
        context(session_started_at=NOW - timedelta(seconds=16))
    ).matched


def test_account_takeover_requires_signals_and_ignores_persona() -> None:
    rule = AccountTakeoverCompositeRule(FraudConfig())
    weak = context(lifetime_successful_payments=0)
    assert not hasattr(weak, "persona")
    assert not rule.evaluate(weak).matched
    strong = context(
        amount=Decimal("1000"),
        historical_average_amount=Decimal("100"),
        current_device="new",
        previous_successful_devices=frozenset({"old"}),
        current_country="DE",
        session_started_at=NOW - timedelta(seconds=2),
    )
    assert rule.evaluate(strong).matched
    assert rule.evaluate(strong).severity is FraudSeverity.CRITICAL


def test_refund_abuse_does_not_match_legitimate_single_refund() -> None:
    rule = RefundAbuseRule(FraudConfig())
    legitimate = context(
        event_type=EventType.REFUND_REQUESTED,
        recent_refunds=1,
        refund_amount=Decimal("10"),
        refundable_amount=Decimal("100"),
        seconds_since_payment=10_000,
    )
    assert not rule.evaluate(legitimate).matched
    abusive = replace(
        legitimate,
        recent_refunds=3,
        refund_amount=Decimal("100"),
        seconds_since_payment=30,
    )
    assert rule.evaluate(abusive).matched


def test_bot_views_alone_are_unsupported_but_transaction_matches() -> None:
    rule = BotCheckoutRule(FraudConfig())
    assert EventType.PRODUCT_VIEWED not in rule.supported_event_types
    assert rule.evaluate(
        context(product_view_count=20, session_started_at=NOW - timedelta(seconds=2))
    ).matched


def test_payment_amount_mismatch_is_critical_maximum() -> None:
    result = PaymentAmountMismatchRule(FraudConfig()).evaluate(
        context(payment_amount_matches_order=False)
    )
    assert (result.score, result.severity) == (100, FraudSeverity.CRITICAL)


def test_engine_thresholds_clamp_severity_and_determinism() -> None:
    config = FraudConfig()
    engine = FraudEngine(config)
    ordinary = engine.evaluate(context(lifetime_successful_payments=0))
    assert ordinary.decision is FraudDecision.APPROVE
    risky_context = context(
        amount=Decimal("10000"),
        historical_average_amount=Decimal("100"),
        recent_payment_attempts=4,
        recent_failed_payments=3,
        current_device="new",
        previous_successful_devices=frozenset({"old"}),
        current_country="DE",
        recent_distinct_countries=2,
        session_started_at=NOW - timedelta(seconds=2),
    )
    first = engine.evaluate(risky_context)
    second = engine.evaluate(risky_context)
    assert first == second
    assert first.total_score == 100
    assert first.decision is FraudDecision.BLOCK
    assert first.severity is FraudSeverity.CRITICAL
    changed = FraudEngine(config.model_copy(update={"fraud_ruleset_version": "next"}))
    assert changed.evaluate(risky_context).evaluation_id != first.evaluation_id


def source_event() -> EventEnvelope[ContractModel]:
    payload = OrderCreatedPayload(
        order_id=ORDER_ID,
        customer_id=CUSTOMER_ID,
        session_id=SESSION_ID,
        cart_id=UUID("10000000-0000-4000-8000-000000000006"),
        item_count=1,
        subtotal=Decimal("100"),
        discount_amount=Decimal("0"),
        total_amount=Decimal("100"),
        currency=Currency.TRY,
        shipping_country_code="TR",
        billing_country_code="TR",
        created_at=NOW,
    )
    return EventEnvelope[ContractModel](
        event_id=SOURCE_ID,
        event_type=EventType.ORDER_CREATED,
        event_version=1,
        event_time=NOW,
        produced_at=NOW,
        source="test",
        correlation_id=UUID("10000000-0000-4000-8000-000000000007"),
        payload=payload,
    )


def test_alert_event_is_deterministic_canonical_and_causal() -> None:
    evaluation = FraudEngine(FraudConfig()).evaluate(
        context(
            amount=Decimal("10000"),
            historical_average_amount=Decimal("100"),
            recent_failed_payments=3,
        )
    )
    first = build_alert_message(evaluation, source_event())
    second = build_alert_message(evaluation, source_event())
    assert first == second
    assert parse_event(first.payload_bytes).event_id == first.alert_event_id
    assert dict(first.headers)["causation_id"] == str(SOURCE_ID).encode()
    assert first.event.correlation_id == source_event().correlation_id
