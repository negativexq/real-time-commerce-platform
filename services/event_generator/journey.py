"""Stateful, persona-driven typed customer-journey construction."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from ipaddress import IPv4Address
from typing import Protocol, cast
from uuid import UUID

from services.event_generator.config import GeneratorConfig
from services.event_generator.generator import (
    PRODUCT_CATALOGUE,
    Product,
    SyntheticGenerator,
)
from services.event_generator.personas import strategy_for
from services.event_generator.personas.base import PersonaProfile
from services.event_generator.state import (
    AbandonedCart,
    CustomerState,
    CustomerStateStore,
)
from shared.commerce_common.enums import (
    CustomerPersona,
    DeviceType,
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
    """Injectable aware physical clock used only to anchor logical time."""

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
    """One complete journey plus stateful diagnostics."""

    correlation_id: UUID
    customer_id: UUID
    events: tuple[EventEnvelope[ContractModel], ...]
    persona: CustomerPersona = CustomerPersona.NORMAL
    returning_customer: bool = False
    customer_lifetime_journeys: int = 1
    logical_journey_duration_ms: int = 0
    payment_attempt_count: int = 0

    @property
    def terminal_event_type(self) -> EventType:
        """Return the last emitted event type."""
        return self.events[-1].event_type


class JourneyBuilder:
    """Build valid journeys and atomically commit their plain customer state."""

    def __init__(
        self,
        config: GeneratorConfig,
        generator: SyntheticGenerator,
        clock: Clock,
        state_store: CustomerStateStore | None = None,
    ) -> None:
        self._config = config
        self._generator = generator
        self._clock = clock
        self._store = state_store or CustomerStateStore(
            generator.random,
            generator,
            config.generator_customer_pool_size,
        )
        self._cursor: datetime | None = None
        self._journey_start: datetime | None = None
        self._seeding_takeover = False

    @property
    def state_store(self) -> CustomerStateStore:
        """Expose the store for snapshots, summaries, and tests."""
        return self._store

    def build(self) -> JourneyResult:
        """Construct one persona-driven journey and commit state on success."""
        self._seeding_takeover = False
        persona = self._choose_persona()
        customer, returning = self._select_customer(persona)
        profile = strategy_for(customer.persona).profile
        working = customer.clone()
        self._start_logical_time(working, profile, returning)
        events: list[EventEnvelope[ContractModel]] = []
        correlation_id = self._generator.uuids.new()

        if not returning:
            timestamp = self._next_timestamp(timedelta(0))
            self._append(
                events,
                EventType.USER_REGISTERED,
                correlation_id,
                UserRegisteredPayload(
                    customer_id=working.customer_id,
                    email_hash=self._generator.email_hash(working.customer_id),
                    country_code=working.home_country,
                    persona=working.persona,
                    registered_at=timestamp,
                ),
                timestamp,
            )

        device_id, ip_address, country = self._identity_for_session(working, profile)
        session_id = self._generator.uuids.new()
        timestamp = self._next_timestamp(profile.action_delay)
        self._append(
            events,
            EventType.SESSION_STARTED,
            correlation_id,
            SessionStartedPayload(
                session_id=session_id,
                customer_id=working.customer_id,
                device_id=device_id,
                device_type=(
                    DeviceType.BOT
                    if working.persona is CustomerPersona.BOT
                    else self._generator.device_type()
                ),
                ip_address=ip_address,
                country_code=country,
                channel=(
                    SessionChannel.API
                    if working.persona is CustomerPersona.BOT
                    else SessionChannel.WEB
                ),
                started_at=timestamp,
            ),
            timestamp,
        )

        viewed_products = self._view_products(working, profile)
        for product in viewed_products:
            timestamp = self._next_timestamp(profile.action_delay)
            self._append(
                events,
                EventType.PRODUCT_VIEWED,
                correlation_id,
                ProductViewedPayload(
                    session_id=session_id,
                    customer_id=working.customer_id,
                    product_id=product.product_id,
                    category=product.category,
                    unit_price=product.price,
                    currency=working.preferred_currency,
                    quantity_available=product.available_quantity,
                ),
                timestamp,
            )

        add_probability = self._probability(profile, "add")
        if not self._generator.chance(add_probability):
            return self._finish(
                working, returning, profile, correlation_id, events, session_id
            )

        product = self._cart_product(working, viewed_products, profile)
        quantity = self._generator.quantity(product)
        cart_id = (
            working.abandoned_cart.cart_id
            if profile.reuse_abandoned_cart and working.abandoned_cart is not None
            else self._generator.uuids.new()
        )
        timestamp = self._next_timestamp(profile.action_delay)
        self._append(
            events,
            EventType.ADDED_TO_CART,
            correlation_id,
            AddedToCartPayload(
                session_id=session_id,
                customer_id=working.customer_id,
                cart_id=cart_id,
                product_id=product.product_id,
                quantity=quantity,
                unit_price=product.price,
                currency=working.preferred_currency,
            ),
            timestamp,
        )

        discount = self._discount(product.price * Decimal(quantity), profile, working)
        checkout_probability = self._probability(profile, "checkout")
        if working.persona is CustomerPersona.DISCOUNT_HUNTER and discount == Decimal(
            "0"
        ):
            checkout_probability = min(checkout_probability, 0.10)
        if not self._generator.chance(checkout_probability):
            working.abandoned_cart = AbandonedCart(cart_id, product.product_id)
            return self._finish(
                working, returning, profile, correlation_id, events, session_id
            )

        subtotal = product.price * Decimal(quantity)
        total = subtotal - discount
        timestamp = self._next_timestamp(profile.action_delay)
        self._append(
            events,
            EventType.CHECKOUT_STARTED,
            correlation_id,
            CheckoutStartedPayload(
                session_id=session_id,
                customer_id=working.customer_id,
                cart_id=cart_id,
                item_count=quantity,
                subtotal=subtotal,
                discount_amount=discount,
                total_amount=total,
                currency=working.preferred_currency,
            ),
            timestamp,
        )
        order_id = self._generator.uuids.new()
        timestamp = self._next_timestamp(profile.action_delay)
        self._append(
            events,
            EventType.ORDER_CREATED,
            correlation_id,
            OrderCreatedPayload(
                order_id=order_id,
                customer_id=working.customer_id,
                session_id=session_id,
                cart_id=cart_id,
                item_count=quantity,
                subtotal=subtotal,
                discount_amount=discount,
                total_amount=total,
                currency=working.preferred_currency,
                shipping_country_code=country,
                billing_country_code=working.home_country,
                created_at=timestamp,
            ),
            timestamp,
        )

        payment_attempts, successful_payment = self._payments(
            events,
            correlation_id,
            working,
            profile,
            order_id,
            total,
            device_id,
            ip_address,
            country,
        )
        if successful_payment is not None and self._generator.chance(
            self._probability(profile, "refund")
        ):
            timestamp = self._next_timestamp(profile.action_delay)
            self._append(
                events,
                EventType.REFUND_REQUESTED,
                correlation_id,
                RefundRequestedPayload(
                    refund_id=self._generator.uuids.new(),
                    payment_id=successful_payment,
                    order_id=order_id,
                    customer_id=working.customer_id,
                    amount=(total / Decimal(2)).quantize(Decimal("0.01")),
                    currency=working.preferred_currency,
                    reason="customer request",
                    requested_at=timestamp,
                ),
                timestamp,
            )
        return self._finish(
            working,
            returning,
            profile,
            correlation_id,
            events,
            session_id,
            order_id,
            payment_attempts,
        )

    def _choose_persona(self) -> CustomerPersona:
        if self._config.generator_persona is not None:
            if (
                self._config.generator_persona is CustomerPersona.ACCOUNT_TAKEOVER
                and self._store.takeover_candidate() is None
            ):
                self._seeding_takeover = True
                return CustomerPersona.NORMAL
            return self._config.generator_persona
        # Sprint 4 branch controls remain authoritative when callers override
        # them directly (including the existing deterministic tests/smoke path).
        if (
            self._config.generator_max_product_views <= 3
            or self._config.generator_add_to_cart_probability != 0.55
            or self._config.generator_checkout_probability != 0.70
            or self._config.generator_payment_success_probability != 0.85
            or self._config.generator_refund_probability != 0.05
        ):
            return CustomerPersona.NORMAL
        personas = list(self._config.persona_weights)
        weights = [self._config.persona_weights[persona] for persona in personas]
        selected = self._generator.random.choices(personas, weights=weights, k=1)[0]
        if (
            selected is CustomerPersona.ACCOUNT_TAKEOVER
            and self._store.takeover_candidate() is None
        ):
            return CustomerPersona.NORMAL
        return selected

    def _select_customer(self, persona: CustomerPersona) -> tuple[CustomerState, bool]:
        if persona is CustomerPersona.ACCOUNT_TAKEOVER:
            candidate = self._store.takeover_candidate()
            if candidate is not None:
                return candidate, True
        returning = self._store.returning(persona)
        create_new = (
            not self._config.generator_stateful_mode
            or returning is None
            or (
                len(self._store) < self._config.generator_customer_pool_size
                and self._generator.chance(
                    self._config.generator_new_customer_probability
                )
            )
        )
        if create_new:
            return self._store.create(persona, self._aware_now()), False
        return cast(CustomerState, returning), True

    def _start_logical_time(
        self,
        customer: CustomerState,
        profile: PersonaProfile,
        returning: bool,
    ) -> None:
        base = self._aware_now()
        if returning and customer.last_activity_timestamp is not None:
            base = max(base, customer.last_activity_timestamp + profile.return_delay)
        self._cursor = base
        self._journey_start = base

    def _aware_now(self) -> datetime:
        value = self._clock.now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("generator clock returned a naive datetime")
        return value.astimezone(UTC)

    def _next_timestamp(self, delay: timedelta) -> datetime:
        if self._cursor is None:
            raise RuntimeError("logical clock was not initialized")
        self._cursor += delay
        return self._cursor

    def _identity_for_session(
        self,
        customer: CustomerState,
        profile: PersonaProfile,
    ) -> tuple[UUID, IPv4Address, str]:
        change_device = self._generator.chance(profile.device_change_probability)
        if change_device:
            device_id = self._generator.uuids.new()
            ip_address = self._generator.ip_address()
            customer.known_device_ids.append(device_id)
            customer.known_ip_addresses.append(ip_address)
        else:
            device_id = customer.known_device_ids[-1]
            ip_address = customer.known_ip_addresses[-1]
        country = customer.home_country
        if self._generator.chance(profile.country_change_probability):
            country = self._generator.random.choice(["DE", "GB", "US"])
        return device_id, ip_address, country

    def _view_products(
        self,
        customer: CustomerState,
        profile: PersonaProfile,
    ) -> list[Product]:
        ranked = strategy_for(customer.persona).rank_products(PRODUCT_CATALOGUE)
        maximum = min(profile.max_views, self._config.generator_max_product_views)
        minimum = min(profile.min_views, maximum)
        count = self._generator.random.randint(minimum, maximum)
        products: list[Product] = []
        if profile.repeat_views and customer.last_viewed_products:
            product_id = customer.last_viewed_products[-1]
            prior = next(
                (item for item in PRODUCT_CATALOGUE if item.product_id == product_id),
                None,
            )
            if prior is not None:
                products.append(prior)
        while len(products) < count:
            if profile.repeat_views and products and self._generator.chance(0.45):
                products.append(products[-1])
            elif (
                profile.high_value
                or customer.persona is CustomerPersona.DISCOUNT_HUNTER
            ):
                products.append(ranked[len(products) % len(ranked)])
            else:
                products.append(self._generator.random.choice(ranked))
        return products

    def _cart_product(
        self,
        customer: CustomerState,
        products: list[Product],
        profile: PersonaProfile,
    ) -> Product:
        if profile.high_value:
            return max(products, key=lambda product: product.price)
        if customer.persona is CustomerPersona.DISCOUNT_HUNTER:
            return min(products, key=lambda product: product.price)
        return self._generator.random.choice(products)

    def _discount(
        self,
        subtotal: Decimal,
        profile: PersonaProfile,
        customer: CustomerState,
    ) -> Decimal:
        probability = profile.discount_probability
        if (
            customer.persona is CustomerPersona.DISCOUNT_HUNTER
            and customer.abandoned_cart is not None
        ):
            probability = 1
        if not self._generator.chance(probability):
            return Decimal("0.00")
        return (subtotal * Decimal(profile.discount_rate)).quantize(Decimal("0.01"))

    def _probability(self, profile: PersonaProfile, branch: str) -> float:
        if self._seeding_takeover:
            return {"add": 1.0, "checkout": 1.0, "payment": 1.0, "refund": 0.0}[branch]
        if profile.persona is CustomerPersona.NORMAL:
            return {
                "add": self._config.generator_add_to_cart_probability,
                "checkout": self._config.generator_checkout_probability,
                "payment": self._config.generator_payment_success_probability,
                "refund": self._config.generator_refund_probability,
            }[branch]
        return {
            "add": profile.add_probability,
            "checkout": profile.checkout_probability,
            "payment": profile.payment_success_probability,
            "refund": profile.refund_probability,
        }[branch]

    def _payments(
        self,
        events: list[EventEnvelope[ContractModel]],
        correlation_id: UUID,
        customer: CustomerState,
        profile: PersonaProfile,
        order_id: UUID,
        total: Decimal,
        device_id: UUID,
        ip_address: IPv4Address,
        country: str,
    ) -> tuple[int, UUID | None]:
        attempts = 0
        successful: UUID | None = None
        maximum = (
            self._config.generator_max_payment_attempts
            if profile.retry_probability > 0
            else 1
        )
        while attempts < maximum:
            attempts += 1
            payment_id = self._generator.uuids.new()
            timestamp = self._next_timestamp(profile.action_delay)
            success = self._generator.chance(self._probability(profile, "payment"))
            if success:
                self._append(
                    events,
                    EventType.PAYMENT_COMPLETED,
                    correlation_id,
                    PaymentCompletedPayload(
                        payment_id=payment_id,
                        order_id=order_id,
                        customer_id=customer.customer_id,
                        amount=total,
                        currency=customer.preferred_currency,
                        payment_method=self._generator.payment_method(),
                        device_id=device_id,
                        ip_address=ip_address,
                        country_code=country,
                        completed_at=timestamp,
                    ),
                    timestamp,
                )
                successful = payment_id
                break
            self._append(
                events,
                EventType.PAYMENT_FAILED,
                correlation_id,
                PaymentFailedPayload(
                    payment_id=payment_id,
                    order_id=order_id,
                    customer_id=customer.customer_id,
                    amount=total,
                    currency=customer.preferred_currency,
                    payment_method=self._generator.payment_method(),
                    failure_reason=self._generator.random.choice(
                        list(PaymentFailureReason)
                    ),
                    device_id=device_id,
                    ip_address=ip_address,
                    country_code=country,
                    failed_at=timestamp,
                ),
                timestamp,
            )
            retry_probability = max(
                profile.retry_probability,
                self._config.generator_payment_retry_probability,
            )
            if not self._generator.chance(retry_probability):
                break
        return attempts, successful

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

    def _finish(
        self,
        customer: CustomerState,
        returning: bool,
        profile: PersonaProfile,
        correlation_id: UUID,
        events: list[EventEnvelope[ContractModel]],
        session_id: UUID,
        order_id: UUID | None = None,
        payment_attempts: int = 0,
    ) -> JourneyResult:
        customer.total_journeys += 1
        customer.total_product_views += sum(
            event.event_type is EventType.PRODUCT_VIEWED for event in events
        )
        customer.total_cart_additions += sum(
            event.event_type is EventType.ADDED_TO_CART for event in events
        )
        customer.total_checkouts += sum(
            event.event_type is EventType.CHECKOUT_STARTED for event in events
        )
        customer.successful_payments += sum(
            event.event_type is EventType.PAYMENT_COMPLETED for event in events
        )
        customer.failed_payments += sum(
            event.event_type is EventType.PAYMENT_FAILED for event in events
        )
        customer.refunds += sum(
            event.event_type is EventType.REFUND_REQUESTED for event in events
        )
        customer.previous_session_ids.append(session_id)
        if order_id is not None:
            customer.previous_order_ids.append(order_id)
        for event in events:
            if isinstance(event.payload, ProductViewedPayload):
                customer.last_viewed_products.append(event.payload.product_id)
                customer.last_viewed_products = customer.last_viewed_products[-10:]
            if isinstance(
                event.payload, (PaymentCompletedPayload, PaymentFailedPayload)
            ):
                customer.previous_payment_ids.append(event.payload.payment_id)
            if isinstance(event.payload, PaymentCompletedPayload):
                customer.accumulated_spend += event.payload.amount
        customer.last_activity_timestamp = events[-1].event_time
        if customer.persona is CustomerPersona.NORMAL and customer.successful_payments:
            customer.prior_normal_history = True
        if any(event.event_type is EventType.CHECKOUT_STARTED for event in events):
            customer.abandoned_cart = None
        self._store.commit(customer)
        start = cast(datetime, self._journey_start)
        duration_ms = int((events[-1].event_time - start).total_seconds() * 1_000)
        return JourneyResult(
            correlation_id,
            customer.customer_id,
            tuple(events),
            customer.persona,
            returning,
            customer.total_journeys,
            duration_ms,
            payment_attempts,
        )
