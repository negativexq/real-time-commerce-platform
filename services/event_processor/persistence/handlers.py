"""Complete typed event-to-repository handler registry."""

from collections.abc import Mapping
from types import MappingProxyType

from services.event_processor.errors import PermanentDatabaseIntegrityError
from services.event_processor.persistence.unit_of_work import (
    TransactionalHandler,
    UnitOfWork,
)
from shared.commerce_common.enums import EventType
from shared.schemas import (
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
)
from shared.schemas.base import ContractModel


def _payload[T: ContractModel](
    event: EventEnvelope[ContractModel], expected: type[T]
) -> T:
    if not isinstance(event.payload, expected):
        raise PermanentDatabaseIntegrityError("handler payload type mismatch")
    return event.payload


class UserRegisteredHandler:
    def apply(
        self, unit_of_work: UnitOfWork, event: EventEnvelope[ContractModel]
    ) -> Mapping[str, int]:
        count = unit_of_work.customers.register(
            _payload(event, UserRegisteredPayload), event.event_id
        )
        return {"customers": count}


class SessionStartedHandler:
    def apply(
        self, unit_of_work: UnitOfWork, event: EventEnvelope[ContractModel]
    ) -> Mapping[str, int]:
        count = unit_of_work.sessions.insert(
            _payload(event, SessionStartedPayload), event.event_id
        )
        return {"sessions": count}


class ProductViewedHandler:
    def apply(
        self, unit_of_work: UnitOfWork, event: EventEnvelope[ContractModel]
    ) -> Mapping[str, int]:
        count = unit_of_work.product_views.insert(
            _payload(event, ProductViewedPayload), event.event_id, event.event_time
        )
        return {"product_views": count}


class AddedToCartHandler:
    def apply(
        self, unit_of_work: UnitOfWork, event: EventEnvelope[ContractModel]
    ) -> Mapping[str, int]:
        return unit_of_work.carts.add_item(
            _payload(event, AddedToCartPayload), event.event_id
        )


class CheckoutStartedHandler:
    def apply(
        self, unit_of_work: UnitOfWork, event: EventEnvelope[ContractModel]
    ) -> Mapping[str, int]:
        count = unit_of_work.carts.checkout(
            _payload(event, CheckoutStartedPayload), event.event_id
        )
        return {"carts": count}


class OrderCreatedHandler:
    def apply(
        self, unit_of_work: UnitOfWork, event: EventEnvelope[ContractModel]
    ) -> Mapping[str, int]:
        return unit_of_work.orders.insert(
            _payload(event, OrderCreatedPayload), event.event_id
        )


class PaymentCompletedHandler:
    def apply(
        self, unit_of_work: UnitOfWork, event: EventEnvelope[ContractModel]
    ) -> Mapping[str, int]:
        return unit_of_work.payments.insert_completed(
            _payload(event, PaymentCompletedPayload), event.event_id
        )


class PaymentFailedHandler:
    def apply(
        self, unit_of_work: UnitOfWork, event: EventEnvelope[ContractModel]
    ) -> Mapping[str, int]:
        return unit_of_work.payments.insert_failed(
            _payload(event, PaymentFailedPayload), event.event_id
        )


class RefundRequestedHandler:
    def apply(
        self, unit_of_work: UnitOfWork, event: EventEnvelope[ContractModel]
    ) -> Mapping[str, int]:
        return unit_of_work.refunds.insert(
            _payload(event, RefundRequestedPayload), event.event_id
        )


class FraudAlertCreatedHandler:
    def apply(
        self, unit_of_work: UnitOfWork, event: EventEnvelope[ContractModel]
    ) -> Mapping[str, int]:
        count = unit_of_work.fraud_alerts.insert(
            _payload(event, FraudAlertCreatedPayload)
        )
        return {"fraud_alerts": count}


def default_persistence_registry() -> Mapping[EventType, TransactionalHandler]:
    """Return one repository handler for every current shared event type."""
    return MappingProxyType(
        {
            EventType.USER_REGISTERED: UserRegisteredHandler(),
            EventType.SESSION_STARTED: SessionStartedHandler(),
            EventType.PRODUCT_VIEWED: ProductViewedHandler(),
            EventType.ADDED_TO_CART: AddedToCartHandler(),
            EventType.CHECKOUT_STARTED: CheckoutStartedHandler(),
            EventType.ORDER_CREATED: OrderCreatedHandler(),
            EventType.PAYMENT_COMPLETED: PaymentCompletedHandler(),
            EventType.PAYMENT_FAILED: PaymentFailedHandler(),
            EventType.REFUND_REQUESTED: RefundRequestedHandler(),
            EventType.FRAUD_ALERT_CREATED: FraudAlertCreatedHandler(),
        }
    )
