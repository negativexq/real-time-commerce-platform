"""Single source of truth for event type to payload model mappings."""

from types import MappingProxyType

from shared.commerce_common.enums import EventType
from shared.schemas.base import ContractModel
from shared.schemas.cart import AddedToCartPayload, CheckoutStartedPayload
from shared.schemas.customer import UserRegisteredPayload
from shared.schemas.fraud import FraudAlertCreatedPayload
from shared.schemas.order import OrderCreatedPayload
from shared.schemas.payment import PaymentCompletedPayload, PaymentFailedPayload
from shared.schemas.product import ProductViewedPayload
from shared.schemas.refund import RefundRequestedPayload
from shared.schemas.session import SessionStartedPayload

EVENT_PAYLOAD_REGISTRY: MappingProxyType[EventType, type[ContractModel]] = (
    MappingProxyType(
        {
            EventType.USER_REGISTERED: UserRegisteredPayload,
            EventType.SESSION_STARTED: SessionStartedPayload,
            EventType.PRODUCT_VIEWED: ProductViewedPayload,
            EventType.ADDED_TO_CART: AddedToCartPayload,
            EventType.CHECKOUT_STARTED: CheckoutStartedPayload,
            EventType.ORDER_CREATED: OrderCreatedPayload,
            EventType.PAYMENT_COMPLETED: PaymentCompletedPayload,
            EventType.PAYMENT_FAILED: PaymentFailedPayload,
            EventType.REFUND_REQUESTED: RefundRequestedPayload,
            EventType.FRAUD_ALERT_CREATED: FraudAlertCreatedPayload,
        }
    )
)

__all__ = ["EVENT_PAYLOAD_REGISTRY"]
