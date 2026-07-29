"""Payment attempts with exact order amount validation."""

from decimal import Decimal
from typing import cast

from services.event_processor.errors import (
    MissingBusinessDependencyError,
    PermanentDatabaseIntegrityError,
)
from services.event_processor.persistence.repositories.base import Repository
from shared.schemas import PaymentCompletedPayload, PaymentFailedPayload


class PaymentRepository(Repository):
    def insert_completed(
        self, payload: PaymentCompletedPayload, event_id: object
    ) -> dict[str, int]:
        return self._insert(payload, event_id, "completed", None, payload.completed_at)

    def insert_failed(
        self, payload: PaymentFailedPayload, event_id: object
    ) -> dict[str, int]:
        return self._insert(
            payload,
            event_id,
            "failed",
            payload.failure_reason.value,
            payload.failed_at,
        )

    def _insert(
        self,
        payload: PaymentCompletedPayload | PaymentFailedPayload,
        event_id: object,
        status: str,
        failure_reason: str | None,
        attempted_at: object,
    ) -> dict[str, int]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT customer_id, total, currency
                FROM orders WHERE order_id = %s FOR UPDATE
                """,
                (payload.order_id,),
            )
            order = cursor.fetchone()
            if order is None:
                raise MissingBusinessDependencyError("order is not persisted")
            if (
                order[0] != payload.customer_id
                or cast(Decimal, order[1]) != payload.amount
                or str(order[2]) != payload.currency.value
            ):
                raise PermanentDatabaseIntegrityError(
                    "payment amount, currency, or customer does not match order"
                )
            cursor.execute(
                """
                INSERT INTO payments (
                    payment_id, order_id, customer_id, status, amount, currency,
                    payment_method, failure_reason, device_id, ip_address,
                    country, event_id, attempted_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (payment_id) DO NOTHING
                """,
                (
                    payload.payment_id,
                    payload.order_id,
                    payload.customer_id,
                    status,
                    payload.amount,
                    payload.currency.value,
                    payload.payment_method.value,
                    failure_reason,
                    str(payload.device_id),
                    str(payload.ip_address),
                    payload.country_code,
                    event_id,
                    attempted_at,
                ),
            )
            payment_rows = cursor.rowcount
            if payment_rows == 0:
                raise PermanentDatabaseIntegrityError("payment identity conflict")
            order_rows = 0
            if status == "completed":
                cursor.execute(
                    """
                    UPDATE orders SET status = 'paid', latest_event_id = %s,
                        updated_at = CURRENT_TIMESTAMP WHERE order_id = %s
                    """,
                    (event_id, payload.order_id),
                )
                order_rows = cursor.rowcount
        return {"payments": payment_rows, "orders": order_rows}
