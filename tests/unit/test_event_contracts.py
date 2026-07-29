"""Contract tests for typed commerce events and canonical JSON."""

import json
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from ipaddress import ip_address
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from shared.commerce_common.enums import (
    Currency,
    CustomerPersona,
    DeviceType,
    EventType,
    FraudDecision,
    PaymentFailureReason,
    PaymentMethod,
    SessionChannel,
)
from shared.schemas import (
    EVENT_PAYLOAD_REGISTRY,
    AddedToCartPayload,
    CheckoutStartedPayload,
    EventEnvelope,
    FraudAlertCreatedPayload,
    OrderCreatedPayload,
    PaymentCompletedPayload,
    PaymentFailedPayload,
    ProductViewedPayload,
    RefundRequestedPayload,
    SessionStartedPayload,
    UserRegisteredPayload,
    canonical_json,
    parse_event,
)
from shared.schemas.base import ContractModel

FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "events"
EVENT_ID = UUID("00000000-0000-4000-8000-000000000001")
CORRELATION_ID = UUID("00000000-0000-4000-8000-000000000100")
CUSTOMER_ID = UUID("00000000-0000-4000-8000-000000000101")
SESSION_ID = UUID("00000000-0000-4000-8000-000000000201")
ORDER_ID = UUID("00000000-0000-4000-8000-000000000301")
CART_ID = UUID("00000000-0000-4000-8000-000000000401")
PAYMENT_ID = UUID("00000000-0000-4000-8000-000000000501")
DEVICE_ID = UUID("00000000-0000-4000-8000-000000000601")
PRODUCT_ID = UUID("00000000-0000-4000-8000-000000000801")
NOW = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)


def order_payload() -> OrderCreatedPayload:
    """Build a deterministic valid order payload."""
    return OrderCreatedPayload(
        order_id=ORDER_ID,
        customer_id=CUSTOMER_ID,
        session_id=SESSION_ID,
        cart_id=CART_ID,
        item_count=2,
        subtotal=Decimal("129.90"),
        discount_amount=Decimal("10.00"),
        total_amount=Decimal("119.90"),
        currency=Currency.TRY,
        shipping_country_code="TR",
        billing_country_code="TR",
        created_at=NOW,
    )


def envelope(
    payload: ContractModel | None = None,
    event_type: EventType = EventType.ORDER_CREATED,
    **changes: Any,
) -> EventEnvelope[ContractModel]:
    """Build a deterministic event envelope for validation tests."""
    values: dict[str, Any] = {
        "event_id": EVENT_ID,
        "event_type": event_type,
        "event_version": 1,
        "event_time": NOW,
        "produced_at": NOW + timedelta(seconds=1),
        "source": "contract-tests",
        "correlation_id": CORRELATION_ID,
        "payload": payload or order_payload(),
    }
    values.update(changes)
    return EventEnvelope[ContractModel](**values)


def valid_payloads() -> dict[EventType, ContractModel]:
    """Build one valid instance for every registered payload model."""
    return {
        EventType.USER_REGISTERED: UserRegisteredPayload(
            customer_id=CUSTOMER_ID,
            email_hash="sha256:value",
            country_code="TR",
            persona=CustomerPersona.NORMAL,
            registered_at=NOW,
        ),
        EventType.SESSION_STARTED: SessionStartedPayload(
            session_id=SESSION_ID,
            customer_id=CUSTOMER_ID,
            device_id=DEVICE_ID,
            device_type=DeviceType.MOBILE,
            ip_address=ip_address("192.0.2.1"),
            country_code="TR",
            channel=SessionChannel.MOBILE_APP,
            started_at=NOW,
        ),
        EventType.PRODUCT_VIEWED: ProductViewedPayload(
            session_id=SESSION_ID,
            customer_id=CUSTOMER_ID,
            product_id=PRODUCT_ID,
            category="electronics",
            unit_price=Decimal("0"),
            currency=Currency.TRY,
            quantity_available=0,
        ),
        EventType.ADDED_TO_CART: AddedToCartPayload(
            session_id=SESSION_ID,
            customer_id=CUSTOMER_ID,
            cart_id=CART_ID,
            product_id=PRODUCT_ID,
            quantity=1,
            unit_price=Decimal("59.95"),
            currency=Currency.TRY,
        ),
        EventType.CHECKOUT_STARTED: CheckoutStartedPayload(
            session_id=SESSION_ID,
            customer_id=CUSTOMER_ID,
            cart_id=CART_ID,
            item_count=2,
            subtotal=Decimal("129.90"),
            discount_amount=Decimal("10.00"),
            total_amount=Decimal("119.90"),
            currency=Currency.TRY,
        ),
        EventType.ORDER_CREATED: order_payload(),
        EventType.PAYMENT_COMPLETED: PaymentCompletedPayload(
            payment_id=PAYMENT_ID,
            order_id=ORDER_ID,
            customer_id=CUSTOMER_ID,
            amount=Decimal("119.90"),
            currency=Currency.TRY,
            payment_method=PaymentMethod.CREDIT_CARD,
            device_id=DEVICE_ID,
            ip_address=ip_address("2001:db8::1"),
            country_code="TR",
            completed_at=NOW,
        ),
        EventType.PAYMENT_FAILED: PaymentFailedPayload(
            payment_id=PAYMENT_ID,
            order_id=ORDER_ID,
            customer_id=CUSTOMER_ID,
            amount=Decimal("119.90"),
            currency=Currency.TRY,
            payment_method=PaymentMethod.CREDIT_CARD,
            failure_reason=PaymentFailureReason.DECLINED,
            device_id=DEVICE_ID,
            ip_address=ip_address("192.0.2.2"),
            country_code="TR",
            failed_at=NOW,
        ),
        EventType.REFUND_REQUESTED: RefundRequestedPayload(
            refund_id=UUID("00000000-0000-4000-8000-000000000901"),
            payment_id=PAYMENT_ID,
            order_id=ORDER_ID,
            customer_id=CUSTOMER_ID,
            amount=Decimal("19.90"),
            currency=Currency.TRY,
            reason="customer request",
            requested_at=NOW,
        ),
        EventType.FRAUD_ALERT_CREATED: FraudAlertCreatedPayload(
            alert_id=UUID("00000000-0000-4000-8000-000000000701"),
            event_id=EVENT_ID,
            customer_id=CUSTOMER_ID,
            order_id=ORDER_ID,
            fraud_score=Decimal("87.50"),
            decision=FraudDecision.REVIEW,
            reasons=["high velocity"],
            created_at=NOW,
        ),
    }


def test_every_event_type_has_expected_value() -> None:
    """The enum must contain exactly the public contract names."""
    assert {event_type.value for event_type in EventType} == {
        "user_registered",
        "session_started",
        "product_viewed",
        "added_to_cart",
        "checkout_started",
        "order_created",
        "payment_completed",
        "payment_failed",
        "refund_requested",
        "fraud_alert_created",
    }


@pytest.mark.parametrize(("event_type", "payload"), valid_payloads().items())
def test_every_payload_builds_and_matches_registry(
    event_type: EventType,
    payload: ContractModel,
) -> None:
    """Every event type has a valid, explicitly typed payload."""
    assert type(payload) is EVENT_PAYLOAD_REGISTRY[event_type]
    assert envelope(payload, event_type).payload is payload


@pytest.mark.parametrize(
    ("fixture_name", "payload_type"),
    [
        ("user_registered.json", UserRegisteredPayload),
        ("order_created.json", OrderCreatedPayload),
        ("payment_completed.json", PaymentCompletedPayload),
        ("payment_failed.json", PaymentFailedPayload),
        ("fraud_alert_created.json", FraudAlertCreatedPayload),
    ],
)
def test_representative_fixtures_parse_to_typed_payloads(
    fixture_name: str,
    payload_type: type[ContractModel],
) -> None:
    """Deterministic JSON fixtures parse through the registry."""
    parsed = parse_event((FIXTURE_DIR / fixture_name).read_bytes())
    assert type(parsed.payload) is payload_type


def test_canonical_json_round_trip_is_deterministic() -> None:
    """Serialization round-trips and repeated output is byte-for-byte stable."""
    original = envelope()
    serialized = canonical_json(original)
    parsed = parse_event(serialized)

    assert canonical_json(parsed) == serialized
    assert isinstance(parsed.payload, OrderCreatedPayload)


def test_uuid_decimal_and_datetime_json_are_safe_strings() -> None:
    """Important scalar types never become binary floats or ambiguous timestamps."""
    decoded = json.loads(canonical_json(envelope()))

    assert decoded["event_id"] == str(EVENT_ID)
    assert decoded["payload"]["subtotal"] == "129.90"
    assert isinstance(decoded["payload"]["subtotal"], str)
    assert decoded["event_time"] == "2026-01-15T10:00:00Z"


def test_decimal_precision_is_preserved() -> None:
    """Long decimal values serialize without float precision loss."""
    payload = AddedToCartPayload(
        session_id=SESSION_ID,
        customer_id=CUSTOMER_ID,
        cart_id=CART_ID,
        product_id=PRODUCT_ID,
        quantity=1,
        unit_price=Decimal("0.123456789012345678901"),
        currency=Currency.USD,
    )
    serialized = canonical_json(envelope(payload, EventType.ADDED_TO_CART))

    assert '"unit_price":"0.123456789012345678901"' in serialized


def test_timestamps_normalize_to_utc() -> None:
    """Aware non-UTC timestamps normalize to UTC on model creation."""
    istanbul = timezone(timedelta(hours=3))
    payload = UserRegisteredPayload(
        customer_id=CUSTOMER_ID,
        email_hash="hash",
        country_code="TR",
        persona=CustomerPersona.NORMAL,
        registered_at=datetime(2026, 1, 15, 13, 0, tzinfo=istanbul),
    )

    assert payload.registered_at == NOW
    assert payload.registered_at.tzinfo is UTC


@pytest.mark.parametrize(
    ("model_type", "values"),
    [
        (
            UserRegisteredPayload,
            {
                "customer_id": CUSTOMER_ID,
                "email_hash": "hash",
                "country_code": "TR",
                "persona": CustomerPersona.NORMAL,
                "registered_at": datetime(2026, 1, 15, 10, 0),
            },
        ),
        (
            OrderCreatedPayload,
            {
                **order_payload().model_dump(),
                "created_at": datetime(2026, 1, 15, 10, 0),
            },
        ),
    ],
)
def test_naive_datetime_is_rejected(
    model_type: type[ContractModel],
    values: dict[str, Any],
) -> None:
    """Payload timestamps must contain an offset."""
    with pytest.raises(ValidationError):
        model_type.model_validate(values)


def test_blank_source_is_rejected() -> None:
    """Whitespace-only envelope sources are invalid."""
    with pytest.raises(ValidationError):
        envelope(source="   ")


@pytest.mark.parametrize("country_code", ["tr", "TUR", "1R", ""])
def test_invalid_country_code_is_rejected(country_code: str) -> None:
    """Country codes must be exactly two uppercase ASCII letters."""
    with pytest.raises(ValidationError):
        UserRegisteredPayload(
            customer_id=CUSTOMER_ID,
            email_hash="hash",
            country_code=country_code,
            persona=CustomerPersona.NORMAL,
            registered_at=NOW,
        )


@pytest.mark.parametrize("ip_address", ["999.1.1.1", "not-an-ip"])
def test_invalid_ip_is_rejected(ip_address: str) -> None:
    """Session IP addresses must be valid IPv4 or IPv6 values."""
    with pytest.raises(ValidationError):
        SessionStartedPayload.model_validate(
            {
                "session_id": SESSION_ID,
                "customer_id": CUSTOMER_ID,
                "device_id": DEVICE_ID,
                "device_type": DeviceType.DESKTOP,
                "ip_address": ip_address,
                "country_code": "TR",
                "channel": SessionChannel.WEB,
                "started_at": NOW,
            }
        )


@pytest.mark.parametrize("amount", [Decimal("-0.01"), Decimal("-100")])
def test_negative_monetary_amount_is_rejected(amount: Decimal) -> None:
    """Money fields enforce their non-negative contract."""
    with pytest.raises(ValidationError):
        ProductViewedPayload(
            session_id=SESSION_ID,
            customer_id=CUSTOMER_ID,
            product_id=PRODUCT_ID,
            category="books",
            unit_price=amount,
            currency=Currency.EUR,
            quantity_available=1,
        )


@pytest.mark.parametrize(
    ("discount", "total"),
    [
        (Decimal("101"), Decimal("0")),
        (Decimal("10"), Decimal("91")),
    ],
)
def test_checkout_arithmetic_is_validated(
    discount: Decimal,
    total: Decimal,
) -> None:
    """Checkout discounts cannot exceed subtotal and totals must reconcile."""
    with pytest.raises(ValidationError):
        CheckoutStartedPayload(
            session_id=SESSION_ID,
            customer_id=CUSTOMER_ID,
            cart_id=CART_ID,
            item_count=1,
            subtotal=Decimal("100"),
            discount_amount=discount,
            total_amount=total,
            currency=Currency.GBP,
        )


@pytest.mark.parametrize(
    ("discount", "total"),
    [
        (Decimal("130"), Decimal("0")),
        (Decimal("10"), Decimal("120.00")),
    ],
)
def test_order_arithmetic_is_validated(discount: Decimal, total: Decimal) -> None:
    """Order discounts cannot exceed subtotal and totals must reconcile."""
    values = order_payload().model_dump()
    values["discount_amount"] = discount
    values["total_amount"] = total
    with pytest.raises(ValidationError):
        OrderCreatedPayload.model_validate(values)


@pytest.mark.parametrize("score", [Decimal("-0.01"), Decimal("100.01")])
def test_invalid_fraud_score_is_rejected(score: Decimal) -> None:
    """Fraud scores are inclusive from zero through one hundred."""
    values = valid_payloads()[EventType.FRAUD_ALERT_CREATED].model_dump()
    values["fraud_score"] = score
    with pytest.raises(ValidationError):
        FraudAlertCreatedPayload.model_validate(values)


@pytest.mark.parametrize("reasons", [[], ["   "]])
def test_empty_fraud_reasons_are_rejected(reasons: list[str]) -> None:
    """Fraud alerts require at least one meaningful explanation."""
    values = valid_payloads()[EventType.FRAUD_ALERT_CREATED].model_dump()
    values["reasons"] = reasons
    with pytest.raises(ValidationError):
        FraudAlertCreatedPayload.model_validate(values)


def test_unknown_event_type_is_rejected() -> None:
    """The parser never silently accepts unregistered event names."""
    raw = json.loads((FIXTURE_DIR / "order_created.json").read_text())
    raw["event_type"] = "order_deleted"

    with pytest.raises(ValueError, match="unknown event type"):
        parse_event(json.dumps(raw))


def test_payload_event_type_mismatch_is_rejected() -> None:
    """Direct construction must honor the registry mapping."""
    with pytest.raises(ValidationError, match="requires UserRegisteredPayload"):
        envelope(event_type=EventType.USER_REGISTERED)


def test_parser_rejects_payload_event_type_mismatch() -> None:
    """Parsing a valid payload under the wrong event type fails."""
    raw = json.loads((FIXTURE_DIR / "order_created.json").read_text())
    raw["event_type"] = EventType.USER_REGISTERED.value

    with pytest.raises(ValidationError):
        parse_event(json.dumps(raw))


@pytest.mark.parametrize("location", ["envelope", "payload"])
def test_extra_fields_are_rejected(location: str) -> None:
    """Envelope and payload schemas reject undocumented fields."""
    raw = json.loads((FIXTURE_DIR / "order_created.json").read_text())
    target = raw if location == "envelope" else raw["payload"]
    target["unexpected"] = True

    with pytest.raises(ValidationError):
        parse_event(json.dumps(raw))


@pytest.mark.parametrize("raw", ["{", "not-json", b"\xff"])
def test_malformed_json_is_rejected(raw: str | bytes) -> None:
    """Malformed text and invalid UTF-8 fail with a clear parser error."""
    with pytest.raises(ValueError, match="malformed event JSON"):
        parse_event(raw)


def test_registry_is_complete_and_has_no_extra_types() -> None:
    """The registry is complete for the EventType enum."""
    assert set(EVENT_PAYLOAD_REGISTRY) == set(EventType)
    assert len(set(EVENT_PAYLOAD_REGISTRY.values())) == len(EventType)


def test_event_version_must_be_positive() -> None:
    """Envelope versions start at one."""
    with pytest.raises(ValidationError):
        envelope(event_version=0)


def test_unreasonable_producer_clock_skew_is_rejected() -> None:
    """Produced timestamps cannot be far earlier than business event time."""
    with pytest.raises(ValidationError, match="five minutes"):
        envelope(produced_at=NOW - timedelta(minutes=6))
