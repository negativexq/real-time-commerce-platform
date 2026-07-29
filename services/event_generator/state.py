"""Process-local plain-data customer state and deterministic selection."""

import random
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal
from ipaddress import IPv4Address
from uuid import UUID

from services.event_generator.generator import SyntheticGenerator
from shared.commerce_common.enums import Currency, CustomerPersona


@dataclass(slots=True)
class AbandonedCart:
    """Minimal reusable cart state."""

    cart_id: UUID
    product_id: UUID


@dataclass(slots=True)
class CustomerState:
    """Plain state retained between journeys; never stores event objects."""

    customer_id: UUID
    persona: CustomerPersona
    registration_timestamp: datetime
    known_device_ids: list[UUID]
    known_ip_addresses: list[IPv4Address]
    home_country: str
    preferred_currency: Currency
    previous_session_ids: list[UUID] = field(default_factory=list)
    previous_order_ids: list[UUID] = field(default_factory=list)
    previous_payment_ids: list[UUID] = field(default_factory=list)
    total_journeys: int = 0
    total_product_views: int = 0
    total_cart_additions: int = 0
    total_checkouts: int = 0
    successful_payments: int = 0
    failed_payments: int = 0
    refunds: int = 0
    accumulated_spend: Decimal = Decimal("0")
    last_activity_timestamp: datetime | None = None
    last_viewed_products: list[UUID] = field(default_factory=list)
    abandoned_cart: AbandonedCart | None = None
    prior_normal_history: bool = False

    def clone(self) -> "CustomerState":
        """Return a mutation-safe copy for atomic journey construction."""
        return replace(
            self,
            known_device_ids=list(self.known_device_ids),
            known_ip_addresses=list(self.known_ip_addresses),
            previous_session_ids=list(self.previous_session_ids),
            previous_order_ids=list(self.previous_order_ids),
            previous_payment_ids=list(self.previous_payment_ids),
            last_viewed_products=list(self.last_viewed_products),
        )


class CustomerStateStore:
    """Bounded deterministic process-local customer pool."""

    def __init__(
        self,
        random_source: random.Random,
        generator: SyntheticGenerator,
        maximum_size: int,
    ) -> None:
        self._random = random_source
        self._generator = generator
        self._maximum_size = maximum_size
        self._customers: dict[UUID, CustomerState] = {}

    def __len__(self) -> int:
        return len(self._customers)

    @property
    def customers(self) -> tuple[CustomerState, ...]:
        """Expose stable snapshots for summaries and deterministic assertions."""
        return tuple(customer.clone() for customer in self._customers.values())

    def create(
        self,
        persona: CustomerPersona,
        timestamp: datetime,
    ) -> CustomerState:
        """Create but do not commit a new customer."""
        if len(self._customers) >= self._maximum_size:
            raise ValueError("customer pool is full")
        customer_id = self._generator.uuids.new()
        return CustomerState(
            customer_id=customer_id,
            persona=persona,
            registration_timestamp=timestamp,
            known_device_ids=[self._generator.uuids.new()],
            known_ip_addresses=[self._generator.ip_address()],
            home_country="TR",
            preferred_currency=Currency.TRY,
        )

    def returning(self, persona: CustomerPersona | None = None) -> CustomerState | None:
        """Choose a deterministic returning customer, optionally by persona."""
        candidates = [
            customer
            for customer in self._customers.values()
            if persona is None or customer.persona is persona
        ]
        if not candidates:
            return None
        return self._random.choice(candidates).clone()

    def takeover_candidate(self) -> CustomerState | None:
        """Choose a customer with established normal history."""
        candidates = [
            customer
            for customer in self._customers.values()
            if customer.prior_normal_history
        ]
        if not candidates:
            return None
        candidate = self._random.choice(candidates).clone()
        candidate.persona = CustomerPersona.ACCOUNT_TAKEOVER
        return candidate

    def commit(self, customer: CustomerState) -> None:
        """Atomically replace one customer snapshot after a journey is built."""
        if (
            customer.customer_id not in self._customers
            and len(self._customers) >= self._maximum_size
        ):
            raise ValueError("customer pool is full")
        current = self._customers.get(customer.customer_id)
        if (
            current is not None
            and current.last_activity_timestamp is not None
            and customer.last_activity_timestamp is not None
            and customer.last_activity_timestamp < current.last_activity_timestamp
        ):
            raise ValueError("customer activity cannot move backwards")
        self._customers[customer.customer_id] = customer.clone()
