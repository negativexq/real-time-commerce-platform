"""Sprint 5 state, persona, anomaly, timing, retry, and summary tests."""

import json
import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from services.event_generator.anomalies import AnomalyInjector
from services.event_generator.config import GeneratorConfig, parse_persona_weights
from services.event_generator.generator import SeededUuidFactory, SyntheticGenerator
from services.event_generator.journey import JourneyBuilder
from services.event_generator.messages import AnomalyType
from services.event_generator.personas import PERSONA_REGISTRY
from services.event_generator.summary import RunSummary
from shared.commerce_common.enums import CustomerPersona, EventType
from shared.schemas import PaymentCompletedPayload, PaymentFailedPayload, parse_event


class LogicalClock:
    """Deterministic anchor clock; persona delays are purely logical."""

    def __init__(self) -> None:
        self.current = datetime(2026, 1, 1, tzinfo=UTC)

    def now(self) -> datetime:
        value = self.current
        self.current += timedelta(seconds=1)
        return value


def persona_builder(
    persona: CustomerPersona,
    *,
    seed: int = 42,
    new_probability: float = 0,
    **updates: object,
) -> JourneyBuilder:
    config = GeneratorConfig.model_validate(
        {
            "generator_seed": seed,
            "generator_persona": persona,
            "generator_new_customer_probability": new_probability,
            **updates,
        }
    )
    synthetic = SyntheticGenerator(random.Random(seed), SeededUuidFactory(seed))
    return JourneyBuilder(config, synthetic, LogicalClock())


def types(builder: JourneyBuilder) -> list[EventType]:
    return [event.event_type for event in builder.build().events]


def test_persona_registry_is_complete() -> None:
    assert set(PERSONA_REGISTRY) == set(CustomerPersona)
    assert {strategy.profile.persona for strategy in PERSONA_REGISTRY.values()} == set(
        CustomerPersona
    )


@pytest.mark.parametrize("invalid", ["unknown=1", "normal=-1", "normal=0"])
def test_invalid_persona_weights_fail(invalid: str) -> None:
    with pytest.raises(ValueError):
        parse_persona_weights(invalid)


def test_persona_weights_normalize() -> None:
    weights = parse_persona_weights(
        "normal=2,indecisive=1,discount_hunter=1,suspicious=0,bot=0,account_takeover=0"
    )
    assert sum(weights.values()) == pytest.approx(1)
    assert weights[CustomerPersona.NORMAL] == pytest.approx(0.5)


def test_returning_customer_keeps_id_and_registers_once() -> None:
    builder = persona_builder(CustomerPersona.NORMAL)
    first = builder.build()
    second = builder.build()
    assert first.customer_id == second.customer_id
    assert first.returning_customer is False
    assert second.returning_customer is True
    assert EventType.USER_REGISTERED in [event.event_type for event in first.events]
    assert EventType.USER_REGISTERED not in [
        event.event_type for event in second.events
    ]


def test_customer_state_counters_and_monotonic_activity() -> None:
    builder = persona_builder(CustomerPersona.NORMAL)
    builder.build()
    first = builder.state_store.customers[0]
    builder.build()
    second = builder.state_store.customers[0]
    assert second.total_journeys == 2
    assert second.total_product_views >= first.total_product_views
    assert first.last_activity_timestamp is not None
    assert second.last_activity_timestamp is not None
    assert second.last_activity_timestamp >= first.last_activity_timestamp


def test_state_selection_is_deterministic_and_pool_is_bounded() -> None:
    first = persona_builder(CustomerPersona.NORMAL, seed=9, new_probability=1)
    second = persona_builder(CustomerPersona.NORMAL, seed=9, new_probability=1)
    assert first.build().customer_id == second.build().customer_id
    bounded = persona_builder(
        CustomerPersona.NORMAL,
        seed=9,
        new_probability=1,
        generator_customer_pool_size=1,
    )
    bounded.build()
    bounded.build()
    assert len(bounded.state_store) == 1


def test_indecisive_has_more_views_and_longer_timing_than_normal() -> None:
    normal = persona_builder(CustomerPersona.NORMAL, seed=7).build()
    indecisive = persona_builder(CustomerPersona.INDECISIVE, seed=7).build()
    normal_views = sum(
        event.event_type is EventType.PRODUCT_VIEWED for event in normal.events
    )
    indecisive_views = sum(
        event.event_type is EventType.PRODUCT_VIEWED for event in indecisive.events
    )
    assert indecisive_views > normal_views
    assert indecisive.logical_journey_duration_ms > normal.logical_journey_duration_ms


def test_bot_is_bounded_fast_and_view_heavy() -> None:
    journey = persona_builder(CustomerPersona.BOT, seed=2).build()
    view_count = sum(
        event.event_type is EventType.PRODUCT_VIEWED for event in journey.events
    )
    assert 8 <= view_count <= 20
    assert journey.logical_journey_duration_ms <= len(journey.events) * 50
    assert len(journey.events) <= 23


def test_account_takeover_seeds_history_then_reuses_customer() -> None:
    builder = persona_builder(CustomerPersona.ACCOUNT_TAKEOVER, seed=3)
    history = builder.build()
    takeover = builder.build()
    assert history.persona is CustomerPersona.NORMAL
    assert EventType.PAYMENT_COMPLETED in [event.event_type for event in history.events]
    assert takeover.persona is CustomerPersona.ACCOUNT_TAKEOVER
    assert takeover.customer_id == history.customer_id
    assert takeover.returning_customer
    assert EventType.USER_REGISTERED not in [
        event.event_type for event in takeover.events
    ]
    state = builder.state_store.customers[0]
    assert len(state.known_device_ids) >= 2


def test_payment_retries_use_unique_ids_and_one_order() -> None:
    builder = persona_builder(
        CustomerPersona.SUSPICIOUS,
        seed=4,
        generator_max_payment_attempts=3,
        generator_payment_retry_probability=1,
    )
    journey = builder.build()
    payments = [
        event.payload
        for event in journey.events
        if isinstance(event.payload, (PaymentCompletedPayload, PaymentFailedPayload))
    ]
    assert len(payments) <= 3
    assert len({payment.payment_id for payment in payments}) == len(payments)
    assert len({payment.order_id for payment in payments}) <= 1


def test_no_refund_when_all_payments_fail() -> None:
    journey = persona_builder(CustomerPersona.SUSPICIOUS, seed=15).build()
    event_types = [event.event_type for event in journey.events]
    if EventType.PAYMENT_COMPLETED not in event_types:
        assert EventType.REFUND_REQUESTED not in event_types


def all_anomaly_config(maximum: int = 7) -> GeneratorConfig:
    return GeneratorConfig(
        generator_anomalies_enabled=True,
        generator_duplicate_event_probability=1,
        generator_malformed_json_probability=1,
        generator_missing_field_probability=1,
        generator_unknown_event_type_probability=1,
        generator_late_event_probability=1,
        generator_out_of_order_probability=1,
        generator_payload_mismatch_probability=1,
        generator_max_anomalies_per_journey=maximum,
    )


def test_anomalies_are_disabled_by_default() -> None:
    journey = persona_builder(CustomerPersona.NORMAL).build()
    messages = AnomalyInjector(GeneratorConfig(), random.Random(1)).prepare(
        journey.events
    )
    assert len(messages) == len(journey.events)
    assert all(message.anomaly_type is None for message in messages)
    assert all(parse_event(message.value) for message in messages)


def test_every_raw_anomaly_is_tagged_and_bounded() -> None:
    journey = persona_builder(CustomerPersona.NORMAL).build()
    messages = AnomalyInjector(all_anomaly_config(), random.Random(1)).prepare(
        journey.events
    )
    anomaly_messages = [message for message in messages if message.anomaly_type]
    assert len({message.anomaly_type for message in anomaly_messages}) <= 7
    assert {message.anomaly_type for message in anomaly_messages} == set(AnomalyType)
    for message in anomaly_messages:
        assert message.anomaly_type is not None
        assert (
            dict(message.headers)["synthetic_anomaly"]
            == message.anomaly_type.value.encode()
        )


@pytest.mark.parametrize(
    "kind",
    [
        AnomalyType.MALFORMED_JSON,
        AnomalyType.MISSING_FIELD,
        AnomalyType.UNKNOWN_EVENT_TYPE,
        AnomalyType.PAYLOAD_MISMATCH,
    ],
)
def test_invalid_anomalies_fail_typed_parsing(kind: AnomalyType) -> None:
    config_values = {
        f"generator_{kind.value}_probability": 1,
        "generator_anomalies_enabled": True,
        "generator_max_anomalies_per_journey": 1,
    }
    config = GeneratorConfig.model_validate(config_values)
    journey = persona_builder(CustomerPersona.NORMAL).build()
    anomaly = next(
        message
        for message in AnomalyInjector(config, random.Random(1)).prepare(journey.events)
        if message.anomaly_type
    )
    with pytest.raises((ValueError, ValidationError)):
        parse_event(anomaly.value)


def test_duplicate_has_identical_bytes_and_event_id() -> None:
    config = GeneratorConfig(
        generator_anomalies_enabled=True,
        generator_duplicate_event_probability=1,
        generator_max_anomalies_per_journey=1,
    )
    journey = persona_builder(CustomerPersona.NORMAL).build()
    messages = AnomalyInjector(config, random.Random(1)).prepare(journey.events)
    duplicate = next(
        message for message in messages if message.anomaly_type is AnomalyType.DUPLICATE
    )
    original = next(
        message for message in messages if message.event_id == duplicate.event_id
    )
    assert duplicate.value == original.value
    assert duplicate.key == original.key


def test_late_event_remains_valid_and_older() -> None:
    config = GeneratorConfig(
        generator_anomalies_enabled=True,
        generator_late_event_probability=1,
        generator_max_anomalies_per_journey=1,
    )
    journey = persona_builder(CustomerPersona.NORMAL).build()
    messages = AnomalyInjector(config, random.Random(1)).prepare(journey.events)
    late = next(
        message
        for message in messages
        if message.anomaly_type is AnomalyType.LATE_EVENT
    )
    original_message = next(
        message
        for message in messages
        if message.event_id == late.event_id and message.anomaly_type is None
    )
    original = parse_event(original_message.value)
    parsed_late = parse_event(late.value)
    assert parsed_late.event_time < original.event_time
    assert parsed_late.produced_at == original.produced_at


def test_out_of_order_preserves_event_timestamps() -> None:
    config = GeneratorConfig(
        generator_anomalies_enabled=True,
        generator_out_of_order_probability=1,
        generator_max_anomalies_per_journey=1,
    )
    journey = persona_builder(CustomerPersona.NORMAL).build()
    messages = AnomalyInjector(config, random.Random(1)).prepare(journey.events)
    first, second = (parse_event(message.value) for message in messages[:2])
    assert first.event_time > second.event_time
    assert messages[0].anomaly_type is AnomalyType.OUT_OF_ORDER


def test_summary_tracks_personas_events_anomalies_and_decimal_value() -> None:
    journey = persona_builder(
        CustomerPersona.NORMAL,
        generator_add_to_cart_probability=1,
        generator_checkout_probability=1,
        generator_payment_success_probability=1,
    ).build()
    summary = RunSummary()
    summary.record_journey(journey, [AnomalyType.DUPLICATE])
    assert summary.journeys_per_persona[CustomerPersona.NORMAL] == 1
    assert summary.events_per_type[EventType.PAYMENT_COMPLETED] == 1
    assert summary.anomalies_per_type[AnomalyType.DUPLICATE] == 1
    assert isinstance(summary.total_logical_commerce_value, Decimal)


def test_anomaly_json_does_not_contain_non_json_bytes() -> None:
    journey = persona_builder(CustomerPersona.NORMAL).build()
    messages = AnomalyInjector(all_anomaly_config(), random.Random(1)).prepare(
        journey.events
    )
    unknown = next(
        message
        for message in messages
        if message.anomaly_type is AnomalyType.UNKNOWN_EVENT_TYPE
    )
    assert json.loads(unknown.value)["event_type"] == "synthetic_unknown_event"
