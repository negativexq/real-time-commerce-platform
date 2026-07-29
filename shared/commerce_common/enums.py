"""Enumerations used by versioned commerce event contracts."""

from enum import StrEnum


class EventType(StrEnum):
    """Supported event contract names."""

    USER_REGISTERED = "user_registered"
    SESSION_STARTED = "session_started"
    PRODUCT_VIEWED = "product_viewed"
    ADDED_TO_CART = "added_to_cart"
    CHECKOUT_STARTED = "checkout_started"
    ORDER_CREATED = "order_created"
    PAYMENT_COMPLETED = "payment_completed"
    PAYMENT_FAILED = "payment_failed"
    REFUND_REQUESTED = "refund_requested"
    FRAUD_ALERT_CREATED = "fraud_alert_created"


class CustomerPersona(StrEnum):
    """Behavioral persona assigned to a simulated customer."""

    NORMAL = "normal"
    INDECISIVE = "indecisive"
    DISCOUNT_HUNTER = "discount_hunter"
    SUSPICIOUS = "suspicious"
    BOT = "bot"
    ACCOUNT_TAKEOVER = "account_takeover"


class Currency(StrEnum):
    """Supported ISO 4217 currencies."""

    TRY = "TRY"
    EUR = "EUR"
    USD = "USD"
    GBP = "GBP"


class PaymentMethod(StrEnum):
    """Supported payment instruments."""

    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    BANK_TRANSFER = "bank_transfer"
    DIGITAL_WALLET = "digital_wallet"


class PaymentFailureReason(StrEnum):
    """Normalized payment failure categories."""

    INSUFFICIENT_FUNDS = "insufficient_funds"
    DECLINED = "declined"
    EXPIRED_CARD = "expired_card"
    INVALID_DETAILS = "invalid_details"
    PROCESSOR_ERROR = "processor_error"
    FRAUD_SUSPECTED = "fraud_suspected"


class FraudDecision(StrEnum):
    """Fraud engine decisions."""

    APPROVE = "APPROVE"
    REVIEW = "REVIEW"
    BLOCK = "BLOCK"


class DeviceType(StrEnum):
    """Client device categories."""

    DESKTOP = "desktop"
    MOBILE = "mobile"
    TABLET = "tablet"
    BOT = "bot"


class SessionChannel(StrEnum):
    """Customer acquisition/session channels."""

    WEB = "web"
    MOBILE_APP = "mobile_app"
    PARTNER = "partner"
    API = "api"
