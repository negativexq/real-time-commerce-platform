"""DLQ schema, identity, truncation, sanitization, and delivery tests."""

import base64
import json
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

import pytest

from services.event_processor.config import ProcessorConfig
from services.event_processor.dlq import (
    DeliveryCallback,
    DlqEnvelope,
    DlqPublisher,
    ProducerClient,
    build_dlq_envelope,
)
from services.event_processor.errors import RetryableProcessingError
from services.event_processor.models import (
    ConsumedMessage,
    ValidationCategory,
    ValidationErrorInfo,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)
EVENT_ID = UUID("00000000-0000-4000-8000-000000000001")


def source() -> ConsumedMessage:
    return ConsumedMessage(
        "commerce.events",
        2,
        42,
        NOW,
        b"\xff",
        b"0123456789",
        [
            ("event_id", str(EVENT_ID).encode()),
            ("synthetic_anomaly", b"malformed_json"),
            ("authorization", b"secret"),
        ],
    )


def error() -> ValidationErrorInfo:
    return ValidationErrorInfo(
        ValidationCategory.MALFORMED_JSON,
        "x" * 1_000,
        "PermanentMessageError",
        EVENT_ID,
        "order_created",
        None,
        "malformed_json",
    )


def record() -> DlqEnvelope:
    return build_dlq_envelope(
        source(),
        error(),
        attempts=1,
        consumer_group="group",
        processor_instance_id="instance",
        maximum_payload_bytes=4,
        failed_at=NOW,
    )


def test_dlq_identity_serialization_and_truncation() -> None:
    first = record()
    second = record()
    assert first.dlq_record_id == second.dlq_record_id
    assert first.original_value_truncated
    assert base64.b64decode(first.original_value_base64 or "") == b"0123"
    assert len(first.error_message) == 512
    assert "authorization" not in first.source_headers
    assert json.loads(first.canonical_bytes())["dlq_schema_version"] == 1
    assert first.kafka_key() == str(EVENT_ID).encode()
    assert dict(first.kafka_headers())["content_type"] == b"application/json"


class FakeDelivered:
    def topic(self) -> str:
        return "commerce.events.dlq"


class FakeProducer:
    def __init__(self, error: object | None = None, remaining: int = 0) -> None:
        self.error = error
        self.remaining = remaining
        self.callback: DeliveryCallback | None = None

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
        self.callback = on_delivery

    def poll(self, timeout: float) -> int:
        del timeout
        return 0

    def flush(self, timeout: float) -> int:
        del timeout
        if self.callback is not None:
            self.callback(self.error, FakeDelivered())
        return self.remaining


def test_dlq_producer_requires_confirmed_delivery() -> None:
    DlqPublisher(ProcessorConfig(), cast(ProducerClient, FakeProducer())).publish(
        record()
    )
    with pytest.raises(RetryableProcessingError):
        DlqPublisher(
            ProcessorConfig(),
            cast(ProducerClient, FakeProducer(RuntimeError("failed"))),
        ).publish(record())
    with pytest.raises(RetryableProcessingError):
        DlqPublisher(
            ProcessorConfig(), cast(ProducerClient, FakeProducer(remaining=1))
        ).publish(record())
