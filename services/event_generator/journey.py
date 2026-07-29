"""Coherent typed customer-journey state machine."""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol
from uuid import UUID

from services.event_generator.config import GeneratorConfig
from services.event_generator.generator import SyntheticGenerator
from shared.commerce_common.enums import (
    Currency,
    CustomerPersona,
    EventType,
    PaymentFailureReason,
    SessionChannel,
)
from shared.schemas import (
    CURRENT_EVENT_VERSION,
    AddedToCartPayload,
    CheckoutStartedPayload,
    EventEnvelope,
    OrderCreatedPayload,
    PaymentCompletedPayload,
    PaymentFailedPayload,
    ProductViewedPayload,
    RefundRequestedPayload,
    SessionStartedPayload,
    UserRegisteredPayload,
)
from shared.schemas.base import ContractModel


class Clock(Protocol):
    """Injectable aware clock."""

    def now(self) -> datetime:
        """Return an aware UTC-compatible timestamp."""
        ...


class SystemClock:
    """Production UTC clock."""

    def now(self) -> datetime:
        """Return the current UTC time."""
        return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class JourneyResult:
    """One complete logical journey and its summary identifiers."""

    correlation_id: UUID
    customer_id: UUID
    events: tuple[EventEnvelope[ContractModel], ...]

    @property
    def terminal_event_type(self) -> EventType:
        """Return the last emitted event type."""
        return self.events[-1].event_type


class JourneyBuilder:
    """Build valid event sequences without Kafka calls."""

    def __init__(
        self,
        config: GeneratorConfig,
        generator: SyntheticGenerator,
        clock: Clock,
    ) -> None:
        self._config = config
        self._generator = generator
        self._clock = clock
        self._last_timestamp: datetime | None = None

    def build(self) -> JourneyResult:
        """Generate one coherent basic customer journey."""
        self._last_timestamp = None
        events: list[EventEnvelope[ContractModel]] = []
        correlation_id = self._generator.uuids.new()
        customer_id = self._generator.uuids.new()
        session_id = self._generator.uuids.new()
        device_id = self._generator.uuids.new()
        currency = Currency.TRY

        timestamp = self._next_timestamp()
        self._append(
            events,
            EventType.USER_REGISTERED,
            correlation_id,
            UserRegisteredPayload(
                customer_id=customer_id,
                email_hash=self._generator.email_hash(customer_id),
                country_code="TR",
                persona=CustomerPersona.NORMAL,
                registered_at=timestamp,
            ),
            timestamp,
        )

        timestamp = self._next_timestamp()
        self._append(
            events,
            EventType.SESSION_STARTED,
            correlation_id,
            SessionStartedPayload(
                session_id=session_id,
                customer_id=customer_id,
                device_id=device_id,
                device_type=self._generator.device_type(),
                ip_address=self._generator.ip_address(),
                country_code="TR",
                channel=SessionChannel.WEB,
                started_at=timestamp,
            ),
            timestamp,
        )

        viewed_products = self._generator.choose_products(
            self._config.generator_max_product_views
        )
        for product in viewed_products:
            timestamp = self._next_timestamp()
            self._append(
                events,
                EventType.PRODUCT_VIEWED,
                correlation_id,
                ProductViewedPayload(
                    session_id=session_id,
                    customer_id=customer_id,
                    product_id=product.product_id,
                    category=product.category,
                    unit_price=product.price,
                    currency=currency,
                    quantity_available=product.available_quantity,
                ),
                timestamp,
            )

        if not self._generator.chance(self._config.generator_add_to_cart_probability):
            return self._result(correlation_id, customer_id, events)

        product = self._generator.random.choice(viewed_products)
        quantity = self._generator.quantity(product)
        cart_id = self._generator.uuids.new()
        timestamp = self._next_timestamp()
        self._append(
            events,
            EventType.ADDED_TO_CART,
            correlation_id,
            AddedToCartPayload(
                session_id=session_id,
                customer_id=customer_id,
                cart_id=cart_id,
                product_id=product.product_id,
                quantity=quantity,
                unit_price=product.price,
                currency=currency,
            ),
            timestamp,
        )

        if not self._generator.chance(self._config.generator_checkout_probability):
            return self._result(correlation_id, customer_id, events)

        subtotal = product.price * Decimal(quantity)
        discount = self._generator.discount(subtotal)
        total = subtotal - discount
        item_count = quantity

        timestamp = self._next_timestamp()
        self._append(
            events,
            EventType.CHECKOUT_STARTED,
            correlation_id,
            CheckoutStartedPayload(
                session_id=session_id,
                customer_id=customer_id,
                cart_id=cart_id,
                item_count=item_count,
                subtotal=subtotal,
                discount_amount=discount,
                total_amount=total,
                currency=currency,
            ),
            timestamp,
        )

        order_id = self._generator.uuids.new()
        timestamp = self._next_timestamp()
        self._append(
            events,
            EventType.ORDER_CREATED,
            correlation_id,
            OrderCreatedPayload(
                order_id=order_id,
                customer_id=customer_id,
                session_id=session_id,
                cart_id=cart_id,
                item_count=item_count,
                subtotal=subtotal,
                discount_amount=discount,
                total_amount=total,
                currency=currency,
                shipping_country_code="TR",
                billing_country_code="TR",
                created_at=timestamp,
            ),
            timestamp,
        )

        payment_id = self._generator.uuids.new()
        timestamp = self._next_timestamp()
        payment_succeeded = self._generator.chance(
            self._config.generator_payment_success_probability
        )
        if payment_succeeded:
            self._append(
                events,
                EventType.PAYMENT_COMPLETED,
                correlation_id,
                PaymentCompletedPayload(
                    payment_id=payment_id,
                    order_id=order_id,
                    customer_id=customer_id,
                    amount=total,
                    currency=currency,
                    payment_method=self._generator.payment_method(),
                    device_id=device_id,
                    ip_address=self._generator.ip_address(),
                    country_code="TR",
                    completed_at=timestamp,
                ),
                timestamp,
            )
            if self._generator.chance(self._config.generator_refund_probability):
                timestamp = self._next_timestamp()
                refund_amount = (total / Decimal(2)).quantize(Decimal("0.01"))
                self._append(
                    events,
                    EventType.REFUND_REQUESTED,
                    correlation_id,
                    RefundRequestedPayload(
                        refund_id=self._generator.uuids.new(),
                        payment_id=payment_id,
                        order_id=order_id,
                        customer_id=customer_id,
                        amount=refund_amount,
                        currency=currency,
                        reason="customer request",
                        requested_at=timestamp,
                    ),
                    timestamp,
                )
        else:
            self._append(
                events,
                EventType.PAYMENT_FAILED,
                correlation_id,
                PaymentFailedPayload(
                    payment_id=payment_id,
                    order_id=order_id,
                    customer_id=customer_id,
                    amount=total,
                    currency=currency,
                    payment_method=self._generator.payment_method(),
                    failure_reason=PaymentFailureReason.DECLINED,
                    device_id=device_id,
                    ip_address=self._generator.ip_address(),
                    country_code="TR",
                    failed_at=timestamp,
                ),
                timestamp,
            )

        return self._result(correlation_id, customer_id, events)

    def _next_timestamp(self) -> datetime:
        timestamp = self._clock.now()
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("generator clock returned a naive datetime")
        timestamp = timestamp.astimezone(UTC)
        if self._last_timestamp is not None and timestamp < self._last_timestamp:
            timestamp = self._last_timestamp
        self._last_timestamp = timestamp
        return timestamp

    def _append(
        self,
        events: list[EventEnvelope[ContractModel]],
        event_type: EventType,
        correlation_id: UUID,
        payload: ContractModel,
        timestamp: datetime,
    ) -> None:
        events.append(
            EventEnvelope[ContractModel](
                event_id=self._generator.uuids.new(),
                event_type=event_type,
                event_version=CURRENT_EVENT_VERSION,
                event_time=timestamp,
                produced_at=timestamp,
                source="event-generator",
                correlation_id=correlation_id,
                payload=payload,
            )
        )

    def _result(
        self,
        correlation_id: UUID,
        customer_id: UUID,
        events: list[EventEnvelope[ContractModel]],
    ) -> JourneyResult:
        return JourneyResult(correlation_id, customer_id, tuple(events))
