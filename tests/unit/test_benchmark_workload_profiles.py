import pytest

from scripts.benchmark.direct_injector import prepare_messages
from scripts.benchmark.workload_profiles import (
    FRAUD_ELIGIBLE_EVENT_TYPES,
    PreparedWorkload,
    calculate_fraud_eligible_share,
    event_type_counts,
    prepare_controlled_workload,
)
from shared.commerce_common.enums import EventType
from shared.schemas import (
    OrderCreatedPayload,
    PaymentCompletedPayload,
    PaymentFailedPayload,
    SessionStartedPayload,
    parse_event,
)


def _prepared(profile: str, target_count: int = 100_003) -> PreparedWorkload:
    return prepare_controlled_workload(
        bootstrap="localhost:29092",
        seed=20260831,
        target_count=target_count,
        profile=profile,
        client_id="test",
    )


@pytest.mark.parametrize(
    ("profile", "target"),
    [
        ("fraud_eligible_20", 0.20),
        ("fraud_eligible_10", 0.10),
        ("fraud_eligible_5", 0.05),
        ("fraud_eligible_0", 0.0),
    ],
)
def test_controlled_profiles_hit_requested_share(profile: str, target: float) -> None:
    prepared = _prepared(profile)
    counts = event_type_counts(prepared.messages)
    actual = calculate_fraud_eligible_share(counts)
    assert abs(actual - target) <= 0.005
    if target == 0:
        assert actual == 0


def test_controlled_generation_is_deterministic() -> None:
    first = _prepared("fraud_eligible_10")
    second = _prepared("fraud_eligible_10")
    assert first == second


def test_controlled_profiles_have_distinct_event_id_namespaces() -> None:
    first = _prepared("fraud_eligible_20", target_count=100)
    second = _prepared("fraud_eligible_10", target_count=100)
    assert first.messages[0].event_id != second.messages[0].event_id


def test_profile_keeps_original_random_stream_for_shared_prefix() -> None:
    first = _prepared("fraud_eligible_20", target_count=100)
    second = _prepared("fraud_eligible_10", target_count=100)
    first_events = [parse_event(message.value) for message in first.messages]
    second_events = [parse_event(message.value) for message in second.messages]
    assert [event.event_type for event in first_events[:3]] == [
        event.event_type for event in second_events[:3]
    ]
    first_session = next(
        event
        for event in first_events[:3]
        if event.event_type is EventType.SESSION_STARTED
    )
    second_session = next(
        event
        for event in second_events[:3]
        if event.event_type is EventType.SESSION_STARTED
    )
    assert isinstance(first_session.payload, SessionStartedPayload)
    assert isinstance(second_session.payload, SessionStartedPayload)
    assert first_session.payload.device_type == second_session.payload.device_type
    assert first_session.payload.ip_address == second_session.payload.ip_address


def test_different_seeds_remain_within_tolerance() -> None:
    for seed in (1, 2, 3):
        prepared = prepare_controlled_workload(
            bootstrap="localhost:29092",
            seed=seed,
            target_count=100_003,
            profile="fraud_eligible_20",
            client_id="test",
        )
        counts = event_type_counts(prepared.messages)
        assert abs(calculate_fraud_eligible_share(counts) - 0.20) <= 0.005


def test_baseline_uses_original_generator_shape() -> None:
    messages = prepare_messages(
        bootstrap="localhost:29092",
        seed=24681357,
        target_count=10_003,
        client_id="baseline-test",
    )
    assert messages
    assert any(message.event_type == "user_registered" for message in messages)
    assert all(message.anomaly_type is None for message in messages)


@pytest.mark.parametrize(
    "profile",
    ["fraud_eligible_20", "fraud_eligible_10", "fraud_eligible_5", "fraud_eligible_0"],
)
def test_controlled_components_are_valid_and_ordered(profile: str) -> None:
    prepared = _prepared(profile)
    event_ids: set[str] = set()
    for start, end in prepared.component_ranges:
        component = [
            parse_event(message.value) for message in prepared.messages[start:end]
        ]
        assert component
        assert len({str(event.event_id) for event in component}) == len(component)
        for event in component:
            assert str(event.event_id) not in event_ids
            event_ids.add(str(event.event_id))
            assert prepared.messages[start].key
        types = [event.event_type.value for event in component]
        assert types[0] in {"user_registered", "session_started"}
        assert "session_started" in types
        assert types.index("session_started") < types.index("product_viewed")
        if "added_to_cart" in types:
            assert types.index("product_viewed") < types.index("added_to_cart")
        if "checkout_started" in types:
            assert types.index("added_to_cart") < types.index("checkout_started")
            assert types.index("checkout_started") < types.index("order_created")
        if "order_created" in types:
            assert types.index("order_created") < next(
                index
                for index, event_type in enumerate(types)
                if event_type in {"payment_completed", "payment_failed"}
            )
        order_ids = {
            str(event.payload.order_id)
            for event in component
            if event.event_type is EventType.ORDER_CREATED
            and isinstance(event.payload, OrderCreatedPayload)
        }
        for event in component:
            if event.event_type in {
                EventType.PAYMENT_COMPLETED,
                EventType.PAYMENT_FAILED,
            }:
                assert isinstance(
                    event.payload, (PaymentCompletedPayload, PaymentFailedPayload)
                )
                assert str(event.payload.order_id) in order_ids


def test_share_calculation_keeps_outcomes_separate() -> None:
    counts = {"checkout_started": 2, "payment_completed": 1, "product_viewed": 7}
    assert calculate_fraud_eligible_share(counts) == pytest.approx(0.3)
    assert {
        "checkout_started",
        "order_created",
        "payment_completed",
        "payment_failed",
        "refund_requested",
    } == FRAUD_ELIGIBLE_EVENT_TYPES
