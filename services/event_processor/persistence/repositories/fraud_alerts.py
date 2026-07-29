"""Persistence for valid externally supplied fraud alerts only."""

import json

from services.event_processor.persistence.repositories.base import Repository
from shared.schemas import FraudAlertCreatedPayload


class FraudAlertRepository(Repository):
    def insert(self, payload: FraudAlertCreatedPayload) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fraud_alerts (
                    event_id, customer_id, order_id, fraud_score, decision,
                    reasons, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    payload.event_id,
                    payload.customer_id,
                    payload.order_id,
                    payload.fraud_score,
                    payload.decision.value,
                    json.dumps(payload.reasons),
                    payload.created_at,
                ),
            )
            return cursor.rowcount
