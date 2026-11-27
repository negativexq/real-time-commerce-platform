"""Dead-letter triage and replay against the migrated PostgreSQL schema."""

import base64
import os
from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from services.demo_control_api.services.dead_letters import (
    REPLAY_HEADER,
    DeadLetterService,
    ReplayDeliveryError,
    ReplayNotAllowedError,
)

pytestmark = pytest.mark.integration

DSN = os.environ.get(
    "POSTGRES_DSN", "postgresql://commerce:commerce_local_dev@localhost:5432/commerce"
)
TOPIC = "commerce.events"


@pytest.fixture
def service() -> Iterator[DeadLetterService]:
    try:
        with psycopg.connect(DSN, connect_timeout=2) as connection:
            connection.execute("SELECT replay_count FROM dead_letters LIMIT 0")
    except psycopg.Error as exc:
        pytest.skip(f"migrated PostgreSQL is unavailable: {exc}")
    pool = ConnectionPool(
        DSN, min_size=1, max_size=2, kwargs={"row_factory": dict_row}, open=True
    )
    try:
        yield DeadLetterService(pool, frozenset({TOPIC}))
    finally:
        pool.close()


@pytest.fixture
def inserted() -> Iterator[list[UUID]]:
    ids: list[UUID] = []
    yield ids
    if ids:
        with psycopg.connect(DSN) as connection:
            connection.execute(
                "DELETE FROM dead_letters WHERE dlq_record_id = ANY(%s)", (ids,)
            )


def _insert(ids: list[UUID], category: str, value: bytes | None = b'{"a": 1}') -> UUID:
    record_id = uuid4()
    envelope: dict[str, Any] = {
        "source_key_base64": base64.b64encode(b"key-1").decode(),
        "original_value_base64": base64.b64encode(value).decode() if value else None,
        "original_value_truncated": False,
        "source_headers": {"event_type": "order_created", "event_id": "e-1"},
    }
    with psycopg.connect(DSN) as connection:
        connection.execute(
            """INSERT INTO dead_letters (dlq_record_id, error_category, error_message,
                 source_topic, source_partition, source_offset, failed_at, envelope,
                 dlq_topic, dlq_partition, dlq_offset)
               VALUES (%s, %s, 'boom', %s, 0, 1, now(), %s,
                       'commerce.events.dlq', 0, -1)""",
            (record_id, category, TOPIC, Jsonb(envelope)),
        )
    ids.append(record_id)
    return record_id


def test_triage_and_reopen_keep_the_note(
    service: DeadLetterService, inserted: list[UUID]
) -> None:
    record = _insert(inserted, "malformed_json")
    triaged = service.set_status(record, "TRIAGED", "Known producer bug")
    assert triaged["status"] == "TRIAGED"
    assert triaged["triage_note"] == "Known producer bug"
    assert triaged["status_changed_at"] is not None
    reopened = service.set_status(record, "NEW", None)
    assert reopened["status"] == "NEW"
    assert reopened["triage_note"] == "Known producer bug"


def test_bulk_status_moves_only_new_records_of_one_category(
    service: DeadLetterService, inserted: list[UUID]
) -> None:
    category = f"test_category_{uuid4().hex[:8]}"
    first, second, handled = (_insert(inserted, category) for _ in range(3))
    service.set_status(handled, "TRIAGED", None)
    assert service.bulk_status(category, "DISCARDED") == 2
    statuses = {r: service.detail(r)["status"] for r in (first, second, handled)}
    assert statuses == {first: "DISCARDED", second: "DISCARDED", handled: "TRIAGED"}


def test_replay_sends_the_original_record_once(
    service: DeadLetterService, inserted: list[UUID]
) -> None:
    record = _insert(inserted, "missing_business_dependency")
    sent: list[tuple[str, bytes | None, bytes, list[tuple[str, bytes]]]] = []
    result = service.replay(record, lambda *message: sent.append(message))
    assert result["status"] == "REPLAYED"
    assert result["replay_count"] == 1
    assert result["replay_blocked_reason"].startswith("Already replayed")
    topic, key, value, headers = sent[0]
    assert (topic, key, value) == (TOPIC, b"key-1", b'{"a": 1}')
    assert (REPLAY_HEADER, str(record).encode()) in headers
    with pytest.raises(ReplayNotAllowedError):
        service.replay(record, lambda *message: sent.append(message))
    assert len(sent) == 1


def test_failed_delivery_leaves_the_record_unchanged(
    service: DeadLetterService, inserted: list[UUID]
) -> None:
    record = _insert(inserted, "retry_exhausted")

    def refuse(*_: object) -> None:
        raise ReplayDeliveryError("Kafka did not confirm the record in time.")

    with pytest.raises(ReplayDeliveryError):
        service.replay(record, refuse)
    detail = service.detail(record)
    assert (detail["status"], detail["replay_count"]) == ("NEW", 0)


def test_deterministic_failures_are_not_replayed(
    service: DeadLetterService, inserted: list[UUID]
) -> None:
    record = _insert(inserted, "malformed_json")
    with pytest.raises(ReplayNotAllowedError, match="deterministic"):
        service.replay(record, lambda *_: None)
    assert service.detail(record)["payload"]["kind"] == "json"
