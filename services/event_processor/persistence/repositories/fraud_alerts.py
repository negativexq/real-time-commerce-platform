"""Persistence for valid externally supplied fraud alerts only."""

import json
from uuid import UUID, uuid5

from services.event_processor.persistence.repositories.base import Repository
from shared.schemas import FraudAlertCreatedPayload


class FraudAlertRepository(Repository):
    def insert(self, payload: FraudAlertCreatedPayload) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO fraud_alerts (
                    alert_id, source_event_id, customer_id, order_id, score,
                    decision, severity, status, reason_codes, alert_event_id,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    CASE WHEN %s = 'BLOCK' THEN 'HIGH' ELSE 'MEDIUM' END,
                    'OPEN', %s::jsonb, %s, %s, %s
                )
                ON CONFLICT (source_event_id) DO NOTHING
                """,
                (
                    payload.alert_id,
                    payload.event_id,
                    payload.customer_id,
                    payload.order_id,
                    payload.fraud_score,
                    payload.decision.value,
                    payload.decision.value,
                    json.dumps(payload.reasons),
                    uuid5(
                        UUID("8b3f028f-d1cb-465c-a662-ebfdcfb74d38"),
                        str(payload.alert_id),
                    ),
                    payload.created_at,
                    payload.created_at,
                ),
            )
            return cursor.rowcount
