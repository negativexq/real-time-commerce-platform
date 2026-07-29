"""Layered Kafka metadata and shared-contract validation tests."""

import json
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from services.event_processor.models import ConsumedMessage, ValidationCategory
from services.event_processor.validation import validate_message
from shared.commerce_common.enums import Currency, EventType
from shared.kafka_metadata import event_message_headers, event_message_key
from shared.schemas import EventEnvelope, OrderCreatedPayload, canonical_json
from shared.schemas.base import ContractModel

NOW = datetime(2026, 1, 1, tzinfo=UTC)
EVENT_ID = UUID("00000000-0000-4000-8000-000000000001")
CORRELATION_ID = UUID("00000000-0000-4000-8000-000000000002")
CUSTOMER_ID = UUID("00000000-0000-4000-8000-000000000003")


def event() -> EventEnvelope[ContractModel]:
    payload = OrderCreatedPayload(
        order_id=UUID("00000000-0000-4000-8000-000000000004"),
        customer_id=CUSTOMER_ID,
        session_id=UUID("00000000-0000-4000-8000-000000000005"),
        cart_id=UUID("00000000-0000-4000-8000-000000000006"),
        item_count=1,
        subtotal=Decimal("10.00"),
        discount_amount=Decimal("0"),
        total_amount=Decimal("10.00"),
        currency=Currency.TRY,
        shipping_country_code="TR",
        billing_country_code="TR",
        created_at=NOW,
    )
    return EventEnvelope[ContractModel](
        event_id=EVENT_ID,
        event_type=EventType.ORDER_CREATED,
        event_version=1,
        event_time=NOW,
        produced_at=NOW,
        source="tests",
        correlation_id=CORRELATION_ID,
        payload=payload,
    )


def message() -> ConsumedMessage:
    item = event()
    return ConsumedMessage(
        "commerce.events",
        1,
        10,
        NOW,
        event_message_key(item),
        canonical_json(item).encode(),
        event_message_headers(item),
    )


def category(record: ConsumedMessage) -> ValidationCategory | None:
    result = validate_message(record)
    return result.error.category if result.error else None


def test_valid_processor_message_parses_shared_contract() -> None:
    result = validate_message(message())
    assert result.valid
    assert result.event is not None
    assert result.event.event_type is EventType.ORDER_CREATED


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        (replace(message(), value=None), ValidationCategory.MISSING_VALUE),
        (replace(message(), key=None), ValidationCategory.MISSING_KEY),
        (
            replace(message(), headers=message().headers[1:]),
            ValidationCategory.MISSING_HEADER,
        ),
        (
            replace(message(), headers=[*message().headers, message().headers[0]]),
            ValidationCategory.DUPLICATE_HEADER,
        ),
        (
            replace(
                message(),
                headers=[
                    (name, b"\xff" if name == "source" else value)
                    for name, value in message().headers
                ],
            ),
            ValidationCategory.INVALID_HEADER_ENCODING,
        ),
        (
            replace(
                message(),
                headers=[
                    (name, b"text/plain" if name == "content_type" else value)
                    for name, value in message().headers
                ],
            ),
            ValidationCategory.INVALID_CONTENT_TYPE,
        ),
        (replace(message(), value=b"{"), ValidationCategory.MALFORMED_JSON),
        (replace(message(), key=b"wrong"), ValidationCategory.KEY_BODY_MISMATCH),
    ],
)
def test_kafka_validation_categories(
    record: ConsumedMessage, expected: ValidationCategory
) -> None:
    assert category(record) is expected


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({"event_type": "unknown"}, ValidationCategory.UNKNOWN_EVENT_TYPE),
        ({"event_version": 2}, ValidationCategory.UNSUPPORTED_EVENT_VERSION),
        ({"event_version": 0}, ValidationCategory.CONTRACT_VALIDATION_FAILED),
        ({"event_id": None}, ValidationCategory.CONTRACT_VALIDATION_FAILED),
        ({"unexpected": True}, ValidationCategory.CONTRACT_VALIDATION_FAILED),
    ],
)
def test_body_validation_categories(
    change: dict[str, object], expected: ValidationCategory
) -> None:
    body = json.loads(message().value or b"{}")
    body.update(change)
    assert category(replace(message(), value=json.dumps(body).encode())) is expected


def test_header_body_mismatch_is_rejected() -> None:
    headers = [
        (name, b"different" if name == "source" else value)
        for name, value in message().headers
    ]
    assert (
        category(replace(message(), headers=headers))
        is ValidationCategory.HEADER_BODY_MISMATCH
    )
