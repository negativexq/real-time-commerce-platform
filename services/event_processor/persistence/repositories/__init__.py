"""Typed repositories sharing one event transaction connection."""

from services.event_processor.persistence.repositories.carts import CartRepository
from services.event_processor.persistence.repositories.customers import (
    CustomerRepository,
)
from services.event_processor.persistence.repositories.fraud_alerts import (
    FraudAlertRepository,
)
from services.event_processor.persistence.repositories.orders import OrderRepository
from services.event_processor.persistence.repositories.payments import PaymentRepository
from services.event_processor.persistence.repositories.processed_events import (
    ProcessedEventRepository,
)
from services.event_processor.persistence.repositories.product_views import (
    ProductViewRepository,
)
from services.event_processor.persistence.repositories.refunds import RefundRepository
from services.event_processor.persistence.repositories.sessions import SessionRepository

__all__ = [
    "CartRepository",
    "CustomerRepository",
    "FraudAlertRepository",
    "OrderRepository",
    "PaymentRepository",
    "ProcessedEventRepository",
    "ProductViewRepository",
    "RefundRepository",
    "SessionRepository",
]
