"""Stored dead letters: listing, triage and replay.

The dlq-sink copies every envelope the processor publishes into the
`dead_letters` table. Operators triage those rows here, and replay the ones
that can succeed on a second try by sending the original record back to its
source topic. A replayed event either applies or dead-letters again as a new
record, so replay never hides a failure.
"""

import base64
import binascii
import json
import threading
from collections.abc import Callable
from typing import Any, Literal
from uuid import UUID

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

TriageStatus = Literal["NEW", "TRIAGED", "DISCARDED"]
BulkStatus = Literal["TRIAGED", "DISCARDED"]

# Failures where a second attempt can succeed: the dependency may exist now,
# or the transient fault (database or Kafka outage) may be over.
REPLAYABLE_CATEGORIES = frozenset({"retry_exhausted", "missing_business_dependency"})
REPLAYABLE_STATUSES = frozenset({"NEW", "TRIAGED"})
REPLAY_HEADER = "replayed_from_dlq_record_id"
PAYLOAD_PREVIEW_CHARS = 4_000


def full_value(alias: str) -> str:
    """SQL: true when the envelope holds the whole original record."""
    return (
        f"({alias}envelope->>'original_value_base64') IS NOT NULL AND NOT "
        f"coalesce(({alias}envelope->>'original_value_truncated')::boolean, false)"
    )


LIST_COLUMNS = f"""dl.dlq_record_id, dl.error_category, dl.error_type,
      left(dl.error_message, 300) error_message, dl.source_topic, dl.source_partition,
      dl.source_offset, dl.original_event_id, dl.original_event_type, dl.anomaly_type,
      dl.processing_attempts, dl.failed_at, dl.status, dl.status_changed_at,
      dl.triage_note, dl.replay_count, dl.last_replayed_at,
      {full_value("dl.")} AS has_full_value,
      pe.processed_at"""

FROM_WITH_PROCESSED = """dead_letters dl
      LEFT JOIN processed_events pe ON pe.event_id = dl.original_event_id"""


class DeadLetterNotFoundError(LookupError):
    pass


class ReplayNotAllowedError(RuntimeError):
    pass


class ReplayDeliveryError(RuntimeError):
    pass


def replay_block_reason(
    category: str,
    status: str,
    has_full_value: bool,
    source_topic: str,
    allowed_topics: frozenset[str],
) -> str | None:
    """Why a record cannot be replayed, or None when it can."""
    if status not in REPLAYABLE_STATUSES:
        return f"Already {status.lower()}. Reopen it first to replay it."
    if category not in REPLAYABLE_CATEGORIES:
        return (
            "This failure is deterministic: the same record would fail the same "
            "way again. Fix the producer or the data instead."
        )
    if not has_full_value:
        return (
            "The stored payload is missing or truncated, so the original record "
            "can't be rebuilt."
        )
    if source_topic not in allowed_topics:
        return f"Replay only sends records back to {', '.join(sorted(allowed_topics))}."
    return None


def payload_preview(value_base64: str | None, truncated: bool) -> dict[str, Any]:
    """A readable, bounded view of the original record's bytes."""
    if value_base64 is None:
        return {"kind": "missing", "text": None, "truncated": truncated}
    try:
        raw = base64.b64decode(value_base64, validate=True)
    except (binascii.Error, ValueError):
        return {"kind": "unreadable", "text": None, "truncated": truncated}
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return {"kind": "binary", "text": None, "truncated": truncated}
    kind = "text"
    try:
        text = json.dumps(
            json.loads(text), indent=2, ensure_ascii=False, sort_keys=True
        )
        kind = "json"
    except ValueError:
        pass
    cut = len(text) > PAYLOAD_PREVIEW_CHARS
    return {
        "kind": kind,
        "text": text[:PAYLOAD_PREVIEW_CHARS],
        "truncated": truncated or cut,
    }


def replay_message(
    record_id: UUID, envelope: dict[str, Any]
) -> tuple[bytes | None, bytes, list[tuple[str, bytes]]]:
    """The original Kafka key, value and headers, plus a replay marker header."""
    key_b64 = envelope.get("source_key_base64")
    key = base64.b64decode(key_b64, validate=True) if key_b64 else None
    value = base64.b64decode(envelope["original_value_base64"], validate=True)
    raw_headers = envelope.get("source_headers") or {}
    if isinstance(raw_headers, str):
        raw_headers = json.loads(raw_headers)
    headers = [
        (str(name), str(header).encode("utf-8"))
        for name, header in sorted(raw_headers.items())
        if header is not None and name != REPLAY_HEADER
    ]
    headers.append((REPLAY_HEADER, str(record_id).encode("ascii")))
    return key, value, headers


class KafkaReplayProducer:
    """Sends one record and waits for the broker to confirm it."""

    def __init__(self, bootstrap_servers: str, timeout_seconds: float = 5) -> None:
        self._bootstrap = bootstrap_servers
        self._timeout = timeout_seconds
        self._producer: Any = None
        self._lock = threading.Lock()

    def _client(self) -> Any:
        with self._lock:
            if self._producer is None:
                from confluent_kafka import Producer  # type: ignore[import-untyped]

                self._producer = Producer(
                    {
                        "bootstrap.servers": self._bootstrap,
                        "enable.idempotence": True,
                        "acks": "all",
                        "client.id": "demo-control-api-dlq-replay",
                    }
                )
            return self._producer

    def send(
        self,
        topic: str,
        key: bytes | None,
        value: bytes,
        headers: list[tuple[str, bytes]],
    ) -> None:
        errors: list[str] = []

        def delivered(error: Any, _message: Any) -> None:
            if error is not None:
                errors.append(str(error))

        producer = self._client()
        try:
            producer.produce(
                topic, key=key, value=value, headers=headers, on_delivery=delivered
            )
        except Exception as exc:  # BufferError or KafkaException
            raise ReplayDeliveryError(f"Kafka refused the record: {exc}") from exc
        if producer.flush(self._timeout) > 0:
            raise ReplayDeliveryError("Kafka did not confirm the record in time.")
        if errors:
            raise ReplayDeliveryError(f"Kafka rejected the record: {errors[0]}")


Sender = Callable[[str, bytes | None, bytes, list[tuple[str, bytes]]], None]


class DeadLetterService:
    def __init__(self, pool: ConnectionPool, allowed_topics: frozenset[str]) -> None:
        self._pool = pool
        self.allowed_topics = allowed_topics

    def _rows(self, query: str, parameters: tuple[object, ...]) -> list[dict[str, Any]]:
        with self._pool.connection() as connection:
            return [
                dict(row) for row in connection.execute(query, parameters).fetchall()
            ]

    def _with_reason(self, row: dict[str, Any]) -> dict[str, Any]:
        row["replay_blocked_reason"] = replay_block_reason(
            row["error_category"],
            row["status"],
            bool(row.pop("has_full_value")),
            row["source_topic"],
            self.allowed_topics,
        )
        return row

    def page(
        self, page_size: int, status: str | None, category: str | None
    ) -> dict[str, Any]:
        where, parameters = [], []
        if status:
            where.append("dl.status = %s")
            parameters.append(status)
        if category:
            where.append("dl.error_category = %s")
            parameters.append(category)
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        items = self._rows(
            f"SELECT {LIST_COLUMNS} FROM {FROM_WITH_PROCESSED} {clause} "
            "ORDER BY dl.failed_at DESC, dl.dlq_record_id LIMIT %s",
            (*parameters, page_size),
        )
        matching = self._rows(
            f"SELECT count(*)::int AS count FROM dead_letters dl {clause}",
            tuple(parameters),
        )
        categories = self._rows(
            """SELECT error_category, count(*)::int AS count,
                 count(*) FILTER (WHERE status = 'NEW')::int AS new
               FROM dead_letters GROUP BY error_category ORDER BY count DESC""",
            (),
        )
        statuses = self._rows(
            "SELECT status, count(*)::int AS count FROM dead_letters GROUP BY status",
            (),
        )
        latest = self._rows("SELECT max(ingested_at) latest FROM dead_letters", ())
        return {
            "items": [self._with_reason(row) for row in items],
            "total": sum(row["count"] for row in categories),
            "matching": matching[0]["count"],
            "categories": [
                {**row, "replayable": row["error_category"] in REPLAYABLE_CATEGORIES}
                for row in categories
            ],
            "statuses": {row["status"]: row["count"] for row in statuses},
            "last_ingested_at": latest[0]["latest"] if latest else None,
        }

    def detail(self, record_id: UUID) -> dict[str, Any]:
        rows = self._rows(
            f"""SELECT {LIST_COLUMNS}, dl.error_message full_error_message,
                  dl.consumer_group, dl.ingested_at, dl.envelope,
                  (SELECT later.dlq_record_id FROM dead_letters later
                    WHERE later.original_event_id = dl.original_event_id
                      AND dl.last_replayed_at IS NOT NULL
                      AND later.failed_at > dl.last_replayed_at
                    ORDER BY later.failed_at DESC LIMIT 1) failed_again_record_id
                FROM {FROM_WITH_PROCESSED} WHERE dl.dlq_record_id = %s""",
            (record_id,),
        )
        if not rows:
            raise DeadLetterNotFoundError(str(record_id))
        row = self._with_reason(rows[0])
        envelope = row.pop("envelope") or {}
        row["error_message"] = row.pop("full_error_message")
        row["payload"] = payload_preview(
            envelope.get("original_value_base64"),
            bool(envelope.get("original_value_truncated")),
        )
        headers = envelope.get("source_headers") or {}
        row["headers"] = headers if isinstance(headers, dict) else {}
        key_b64 = envelope.get("source_key_base64")
        try:
            row["key"] = base64.b64decode(key_b64).decode("utf-8") if key_b64 else None
        except (binascii.Error, UnicodeDecodeError):
            row["key"] = "<binary>"
        return row

    def set_status(
        self, record_id: UUID, status: TriageStatus, note: str | None
    ) -> dict[str, Any]:
        rows = self._rows(
            """UPDATE dead_letters
               SET status = %s, status_changed_at = now(),
                   triage_note = coalesce(%s, triage_note)
               WHERE dlq_record_id = %s RETURNING dlq_record_id""",
            (status, note, record_id),
        )
        if not rows:
            raise DeadLetterNotFoundError(str(record_id))
        return self.detail(record_id)

    def bulk_status(self, category: str, status: BulkStatus) -> int:
        """Moves every NEW record of one category; handled rows stay as they are."""
        rows = self._rows(
            """WITH moved AS (
                 UPDATE dead_letters SET status = %s, status_changed_at = now()
                 WHERE error_category = %s AND status = 'NEW' RETURNING 1)
               SELECT count(*)::int AS count FROM moved""",
            (status, category),
        )
        return int(rows[0]["count"])

    def replay(self, record_id: UUID, send: Sender) -> dict[str, Any]:
        """Sends the original record back, then marks it REPLAYED.

        The row is locked for the whole step, so two clicks cannot replay it
        twice. If Kafka does not confirm the record the transaction rolls back
        and the row is left exactly as it was.
        """
        with self._pool.connection() as connection, connection.transaction():
            row = (
                connection.cursor(row_factory=dict_row)
                .execute(
                    f"""SELECT dlq_record_id, error_category, status, source_topic,
                       envelope,
                     {full_value("")} AS has_full_value
                   FROM dead_letters WHERE dlq_record_id = %s FOR UPDATE""",
                    (record_id,),
                )
                .fetchone()
            )
            if row is None:
                raise DeadLetterNotFoundError(str(record_id))
            reason = replay_block_reason(
                row["error_category"],
                row["status"],
                bool(row["has_full_value"]),
                row["source_topic"],
                self.allowed_topics,
            )
            if reason is not None:
                raise ReplayNotAllowedError(reason)
            key, value, headers = replay_message(record_id, row["envelope"])
            send(row["source_topic"], key, value, headers)
            connection.execute(
                """UPDATE dead_letters
                   SET status = 'REPLAYED', status_changed_at = now(),
                       replay_count = replay_count + 1, last_replayed_at = now()
                   WHERE dlq_record_id = %s""",
                (record_id,),
            )
        return self.detail(record_id)
