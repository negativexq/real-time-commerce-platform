"""Lease-based PostgreSQL outbox claims with abandoned-claim recovery."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

import psycopg

from services.event_processor.fraud.config import FraudConfig


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    outbox_id: UUID
    event_id: UUID
    topic: str
    message_key: bytes
    headers: tuple[tuple[str, bytes], ...]
    payload_bytes: bytes
    attempts: int
    claim_token: UUID


class OutboxRepository:
    def __init__(self, connection: psycopg.Connection[tuple[object, ...]]) -> None:
        self.connection = connection
        self.recovered_claims = 0

    def claim(self, config: FraudConfig) -> tuple[OutboxRecord, ...]:
        token = uuid4()
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE fraud_outbox
                SET status = 'PENDING', claim_token = NULL, claimed_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE status = 'PUBLISHING'
                  AND claimed_at < CURRENT_TIMESTAMP - (%s * INTERVAL '1 second')
                """,
                (config.fraud_outbox_claim_ttl_seconds,),
            )
            self.recovered_claims = cursor.rowcount
            cursor.execute(
                """
                SELECT outbox_id, event_id, topic, message_key, headers_json,
                       payload_bytes, attempts
                FROM fraud_outbox
                WHERE status = 'PENDING' AND available_at <= CURRENT_TIMESTAMP
                ORDER BY created_at, outbox_id
                FOR UPDATE SKIP LOCKED
                LIMIT %s
                """,
                (config.fraud_outbox_batch_size,),
            )
            rows = cursor.fetchall()
            ids = [row[0] for row in rows]
            if ids:
                cursor.execute(
                    """
                    UPDATE fraud_outbox
                    SET status = 'PUBLISHING', claim_token = %s,
                        claimed_at = CURRENT_TIMESTAMP,
                        attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP
                    WHERE outbox_id = ANY(%s)
                    """,
                    (token, ids),
                )
        return tuple(
            OutboxRecord(
                cast(UUID, row[0]),
                cast(UUID, row[1]),
                str(row[2]),
                cast(bytes, row[3]),
                tuple(
                    (str(name), str(value).encode())
                    for name, value in cast(list[list[Any]], row[4])
                ),
                cast(bytes, row[5]),
                cast(int, row[6]) + 1,
                token,
            )
            for row in rows
        )

    def status_snapshot(self) -> tuple[dict[str, int], float]:
        """Return bounded status counts and oldest pending age."""
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, count(*) FROM fraud_outbox
                WHERE status IN ('PENDING', 'PUBLISHING', 'FAILED')
                GROUP BY status
                """
            )
            counts = {
                str(status).lower(): cast(int, count)
                for status, count in cursor.fetchall()
            }
            cursor.execute(
                """
                SELECT COALESCE(EXTRACT(EPOCH FROM
                    (CURRENT_TIMESTAMP - MIN(created_at))), 0)
                FROM fraud_outbox WHERE status = 'PENDING'
                """
            )
            row = cursor.fetchone()
            age = float(cast(int | float, row[0])) if row else 0.0
        self.connection.commit()
        return counts, age

    def published(self, record: OutboxRecord) -> None:
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE fraud_outbox
                SET status = 'PUBLISHED', published_at = CURRENT_TIMESTAMP,
                    claim_token = NULL, claimed_at = NULL, last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE outbox_id = %s AND status = 'PUBLISHING' AND claim_token = %s
                """,
                (record.outbox_id, record.claim_token),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("outbox publication claim was lost")

    def failed(
        self, record: OutboxRecord, error: Exception, config: FraudConfig
    ) -> None:
        permanent = record.attempts >= config.fraud_outbox_max_attempts
        delay_ms = min(
            config.fraud_outbox_max_backoff_ms,
            config.fraud_outbox_initial_backoff_ms * (2 ** max(0, record.attempts - 1)),
        )
        available = datetime.now(UTC) + timedelta(milliseconds=delay_ms)
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE fraud_outbox
                SET status = %s, available_at = %s, claim_token = NULL,
                    claimed_at = NULL, last_error = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE outbox_id = %s AND claim_token = %s
                """,
                (
                    "FAILED" if permanent else "PENDING",
                    available,
                    str(error)[:512],
                    record.outbox_id,
                    record.claim_token,
                ),
            )
