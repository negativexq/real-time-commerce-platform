"""Unit tests for coherent journeys, producer metadata, and lifecycle."""

import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TypedDict, cast

import pytest

from services.event_generator.config import GeneratorConfig
from services.event_generator.generator import SeededUuidFactory, SyntheticGenerator
from services.event_generator.journey import JourneyBuilder, JourneyResult
from services.event_generator.main import ShutdownController, run_generation
from services.event_generator.producer import (
    DeliveryCallback,
    KafkaEventProducer,
    ProducerClient,
    ProducerDeliveryError,
    message_headers,
    message_key,
)
from shared.commerce_common.enums import EventType
from shared.schemas import (
    AddedToCartPayload,
    CheckoutStartedPayload,
    OrderCreatedPayload,
    PaymentCompletedPayload,
    RefundRequestedPayload,
    UserRegisteredPayload,
    canonical_json,
    parse_event,
)
from shared.schemas.base import ContractModel


class SteppingClock:
    """Deterministic aware clock."""

    def __init__(self) -> None:
        self._current = datetime(2026, 1, 1, tzinfo=UTC)

    def now(self) -> datetime:
        """Advance by one second."""
        value = self._current
        self._current += timedelta(seconds=1)
        return value


class BranchOptions(TypedDict, total=False):
    """Controlled probability arguments."""

    add: float
    checkout: float
    payment: float
    refund: float


def builder(
    *,
    seed: int = 42,
    add: float = 1,
    checkout: float = 1,
    payment: float = 1,
    refund: float = 1,
    max_views: int = 3,
) -> JourneyBuilder:
    """Build a deterministic state machine with controlled branches."""
    config = GeneratorConfig(
        generator_seed=seed,
        generator_add_to_cart_probability=add,
        generator_checkout_probability=checkout,
        generator_payment_success_probability=payment,
        generator_refund_probability=refund,
        generator_max_product_views=max_views,
    )
    return JourneyBuilder(
        config,
        SyntheticGenerator(random.Random(seed), SeededUuidFactory(seed)),
        SteppingClock(),
    )


def event_types(journey: JourneyResult) -> list[EventType]:
    """Return event types in journey order."""
    return [event.event_type for event in journey.events]


def payloads(
    journey: JourneyResult,
    payload_type: type[ContractModel],
) -> list[ContractModel]:
    """Select payloads by exact contract class."""
    return [
        event.payload
        for event in journey.events
        if isinstance(event.payload, payload_type)
    ]


def test_seeded_generation_is_reproducible() -> None:
    """Injected clock, RNG, and UUID factory yield identical typed events."""
    first = builder(seed=99).build()
    second = builder(seed=99).build()

    assert [canonical_json(event) for event in first.events] == [
        canonical_json(event) for event in second.events
    ]


def test_journey_starts_with_registration_then_session() -> None:
    """Every journey has the required opening states."""
    assert event_types(builder().build())[:2] == [
        EventType.USER_REGISTERED,
        EventType.SESSION_STARTED,
    ]


def test_journey_identifier_consistency() -> None:
    """Correlation, customer, session, cart, order, and refund payment IDs persist."""
    journey = builder().build()
    assert {event.correlation_id for event in journey.events} == {
        journey.correlation_id
    }
    customer_ids = {
        event.payload.customer_id
        for event in journey.events
        if hasattr(event.payload, "customer_id")
    }
    assert customer_ids == {journey.customer_id}

    session_ids = {
        event.payload.session_id
        for event in journey.events
        if hasattr(event.payload, "session_id")
    }
    assert len(session_ids) == 1

    cart_ids = {
        event.payload.cart_id
        for event in journey.events
        if hasattr(event.payload, "cart_id")
    }
    assert len(cart_ids) == 1

    order_ids = {
        event.payload.order_id
        for event in journey.events
        if hasattr(event.payload, "order_id")
    }
    assert len(order_ids) == 1

    completed = cast(
        PaymentCompletedPayload,
        payloads(journey, PaymentCompletedPayload)[0],
    )
    refund = cast(RefundRequestedPayload, payloads(journey, RefundRequestedPayload)[0])
    assert refund.payment_id == completed.payment_id


def test_timestamps_are_non_decreasing() -> None:
    """All envelope timestamps move forward within a journey."""
    timestamps = [event.event_time for event in builder().build().events]
    assert timestamps == sorted(timestamps)
    assert all(timestamp.tzinfo is UTC for timestamp in timestamps)


def test_product_views_respect_maximum() -> None:
    """The configured view bound is enforced."""
    journey = builder(max_views=1).build()
    assert event_types(journey).count(EventType.PRODUCT_VIEWED) == 1


@pytest.mark.parametrize(
    ("options", "terminal"),
    [
        ({"add": 0}, EventType.PRODUCT_VIEWED),
        ({"add": 1, "checkout": 0}, EventType.ADDED_TO_CART),
        ({"add": 1, "checkout": 1, "payment": 0}, EventType.PAYMENT_FAILED),
        (
            {"add": 1, "checkout": 1, "payment": 1, "refund": 0},
            EventType.PAYMENT_COMPLETED,
        ),
        (
            {"add": 1, "checkout": 1, "payment": 1, "refund": 1},
            EventType.REFUND_REQUESTED,
        ),
    ],
)
def test_configured_journey_branches(
    options: BranchOptions,
    terminal: EventType,
) -> None:
    """Controlled probabilities cover every basic branch."""
    assert builder(**options).build().terminal_event_type is terminal


def test_failed_payment_never_refunds() -> None:
    """Refunds are impossible after a failed payment."""
    types = event_types(builder(payment=0, refund=1).build())
    assert EventType.PAYMENT_FAILED in types
    assert EventType.REFUND_REQUESTED not in types


def test_decimal_totals_remain_consistent() -> None:
    """Checkout, order, and payment reuse exact Decimal arithmetic."""
    journey = builder(refund=0).build()
    cart = cast(AddedToCartPayload, payloads(journey, AddedToCartPayload)[0])
    checkout = cast(
        CheckoutStartedPayload,
        payloads(journey, CheckoutStartedPayload)[0],
    )
    order = cast(OrderCreatedPayload, payloads(journey, OrderCreatedPayload)[0])
    payment = cast(
        PaymentCompletedPayload,
        payloads(journey, PaymentCompletedPayload)[0],
    )

    assert isinstance(checkout.subtotal, Decimal)
    assert checkout.subtotal == cart.unit_price * cart.quantity
    assert checkout.total_amount == checkout.subtotal - checkout.discount_amount
    assert order.subtotal == checkout.subtotal
    assert order.discount_amount == checkout.discount_amount
    assert order.total_amount == checkout.total_amount
    assert payment.amount == order.total_amount


def test_generated_events_round_trip_through_registry() -> None:
    """Every generated canonical event remains parser-compatible."""
    for event in builder().build().events:
        parsed = parse_event(canonical_json(event))
        assert type(parsed.payload) is type(event.payload)


def test_kafka_key_and_headers() -> None:
    """Kafka metadata uses customer ordering and required UTF-8 headers."""
    event = builder().build().events[0]
    payload = cast(UserRegisteredPayload, event.payload)
    headers = dict(message_headers(event))

    assert message_key(event) == str(payload.customer_id).encode()
    assert headers == {
        "event_id": str(event.event_id).encode(),
        "event_type": event.event_type.value.encode(),
        "event_version": b"1",
        "correlation_id": str(event.correlation_id).encode(),
        "source": b"event-generator",
        "content_type": b"application/json",
    }


class FakeMessage:
    """Delivered-message metadata."""

    def topic(self) -> str:
        return "commerce.events"

    def partition(self) -> int:
        return 1

    def offset(self) -> int:
        return 10


class FakeProducer:
    """Mock Confluent producer boundary."""

    def __init__(
        self, delivery_error: object | None = None, remaining: int = 0
    ) -> None:
        self.delivery_error = delivery_error
        self.remaining = remaining
        self.callbacks: list[DeliveryCallback] = []
        self.produced = 0
        self.flush_timeout = 0.0

    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        headers: list[tuple[str, bytes]],
        on_delivery: DeliveryCallback,
    ) -> None:
        del topic, key, value, headers
        self.callbacks.append(on_delivery)
        self.produced += 1

    def poll(self, timeout: float) -> int:
        del timeout
        while self.callbacks:
            self.callbacks.pop(0)(self.delivery_error, FakeMessage())
        return 0

    def flush(self, timeout: float) -> int:
        self.flush_timeout = timeout
        self.poll(0)
        return self.remaining


def producer(fake: FakeProducer) -> KafkaEventProducer:
    """Build the producer wrapper around a fake boundary."""
    return KafkaEventProducer(
        GeneratorConfig(generator_flush_timeout_seconds=3),
        cast(ProducerClient, fake),
    )


def test_delivery_callback_success_and_flush() -> None:
    """Successful callbacks and bounded flush complete cleanly."""
    fake = FakeProducer()
    kafka = producer(fake)
    kafka.publish(builder().build().events[0])
    kafka.flush()

    assert kafka.delivery_failures == 0
    assert fake.flush_timeout == 3


def test_delivery_callback_failure_is_application_failure() -> None:
    """Callback errors become producer failures."""
    fake = FakeProducer(delivery_error=RuntimeError("broker error"))
    kafka = producer(fake)
    kafka.publish(builder().build().events[0])

    with pytest.raises(ProducerDeliveryError, match="callback"):
        kafka.poll()


def test_undelivered_flush_is_application_failure() -> None:
    """Messages left after the bounded flush fail shutdown."""
    kafka = producer(FakeProducer(remaining=2))
    with pytest.raises(ProducerDeliveryError, match="undelivered"):
        kafka.flush()


def test_finite_generation_count_and_graceful_flush() -> None:
    """Finite lifecycle emits exactly N journeys and flushes once."""
    config = GeneratorConfig(generator_journeys=3, generator_rate_per_second=1_000)
    fake = FakeProducer()
    kafka = producer(fake)

    assert run_generation(config, builder(), kafka, ShutdownController()) == 0
    assert fake.produced >= 3 * 3
    assert fake.flush_timeout == 3


def test_signal_requested_shutdown_stops_before_new_journey() -> None:
    """A signal-driven stop request prevents further generation and still flushes."""
    shutdown = ShutdownController()
    shutdown.request()
    fake = FakeProducer()
    kafka = producer(fake)

    assert run_generation(GeneratorConfig(), builder(), kafka, shutdown) == 0
    assert fake.produced == 0
    assert fake.flush_timeout == 3
