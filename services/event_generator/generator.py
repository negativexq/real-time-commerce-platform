"""Seeded synthetic entity and catalogue generation without Kafka calls."""

import random
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from ipaddress import IPv4Address
from uuid import UUID, uuid4

from shared.commerce_common.enums import Currency, DeviceType, PaymentMethod


@dataclass(frozen=True, slots=True)
class Product:
    """One immutable catalogue entry."""

    product_id: UUID
    category: str
    price: Decimal
    currency: Currency
    available_quantity: int


PRODUCT_CATALOGUE = (
    Product(
        UUID("10000000-0000-4000-8000-000000000001"),
        "electronics",
        Decimal("1299.90"),
        Currency.TRY,
        20,
    ),
    Product(
        UUID("10000000-0000-4000-8000-000000000002"),
        "books",
        Decimal("249.50"),
        Currency.TRY,
        50,
    ),
    Product(
        UUID("10000000-0000-4000-8000-000000000003"),
        "home",
        Decimal("799.00"),
        Currency.TRY,
        12,
    ),
    Product(
        UUID("10000000-0000-4000-8000-000000000004"),
        "apparel",
        Decimal("449.90"),
        Currency.TRY,
        35,
    ),
)


class UuidFactory:
    """Production UUID factory."""

    def new(self) -> UUID:
        """Return a random UUID4."""
        return uuid4()


class SeededUuidFactory(UuidFactory):
    """Deterministic UUID4-shaped values for tests and seeded generation."""

    def __init__(self, seed: int) -> None:
        self._random = random.Random(seed)

    def new(self) -> UUID:
        """Return the next reproducible UUID."""
        value = self._random.getrandbits(128)
        value = (value & ~(0xF << 76)) | (4 << 76)
        value = (value & ~(0x3 << 62)) | (0x2 << 62)
        return UUID(int=value)


class SyntheticGenerator:
    """Seeded primitive generator used by the journey state machine."""

    def __init__(
        self,
        random_source: random.Random,
        uuid_factory: UuidFactory,
    ) -> None:
        self.random = random_source
        self.uuids = uuid_factory

    def choose_products(self, maximum: int) -> list[Product]:
        """Choose one through ``maximum`` product views from the catalogue."""
        count = self.random.randint(1, maximum)
        return [self.random.choice(PRODUCT_CATALOGUE) for _ in range(count)]

    def chance(self, probability: float) -> bool:
        """Evaluate one configured branch probability."""
        return self.random.random() < probability

    def quantity(self, product: Product) -> int:
        """Choose a small valid cart quantity."""
        return self.random.randint(1, min(product.available_quantity, 3))

    def discount(self, subtotal: Decimal) -> Decimal:
        """Return zero or a 10% discount using decimal arithmetic."""
        if not self.chance(0.30):
            return Decimal("0.00")
        return (subtotal * Decimal("0.10")).quantize(Decimal("0.01"))

    def payment_method(self) -> PaymentMethod:
        """Choose one supported payment method."""
        return self.random.choice(list(PaymentMethod))

    def device_type(self) -> DeviceType:
        """Choose one ordinary user device."""
        return self.random.choice(
            [DeviceType.DESKTOP, DeviceType.MOBILE, DeviceType.TABLET]
        )

    def ip_address(self) -> IPv4Address:
        """Return a documentation-range IP address."""
        return IPv4Address(f"198.51.100.{self.random.randint(1, 254)}")

    @staticmethod
    def email_hash(customer_id: UUID) -> str:
        """Return a deterministic non-sensitive email stand-in."""
        return sha256(str(customer_id).encode()).hexdigest()
