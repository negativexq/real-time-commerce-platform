"""Refund persistence with payment row locking and cumulative balance checks."""

from decimal import Decimal
from typing import cast

from services.event_processor.errors import (
    MissingBusinessDependencyError,
    PermanentDatabaseIntegrityError,
)
from services.event_processor.persistence.repositories.base import Repository
from shared.schemas import RefundRequestedPayload


class RefundRepository(Repository):
    def insert(
        self, payload: RefundRequestedPayload, event_id: object
    ) -> dict[str, int]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT order_id, customer_id, amount, currency, status
                FROM payments WHERE payment_id = %s FOR UPDATE
                """,
                (payload.payment_id,),
            )
            payment = cursor.fetchone()
            if payment is None:
                raise MissingBusinessDependencyError("payment is not persisted")
            if str(payment[4]) not in {"completed", "partially_refunded", "refunded"}:
                raise PermanentDatabaseIntegrityError(
                    "refund requires a successful payment"
                )
            if (
                payment[0] != payload.order_id
                or payment[1] != payload.customer_id
                or str(payment[3]) != payload.currency.value
            ):
                raise PermanentDatabaseIntegrityError(
                    "refund identity or currency does not match payment"
                )
            cursor.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM refunds WHERE payment_id = %s",
                (payload.payment_id,),
            )
            total_row = cursor.fetchone()
            assert total_row is not None
            refunded = cast(Decimal, total_row[0])
            paid = cast(Decimal, payment[2])
            if refunded + payload.amount > paid:
                raise PermanentDatabaseIntegrityError(
                    "refund exceeds remaining refundable amount"
                )
            cursor.execute(
                """
                INSERT INTO refunds (
                    refund_id, payment_id, order_id, customer_id, amount,
                    currency, reason, event_id, requested_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (refund_id) DO NOTHING
                """,
                (
                    payload.refund_id,
                    payload.payment_id,
                    payload.order_id,
                    payload.customer_id,
                    payload.amount,
                    payload.currency.value,
                    payload.reason,
                    event_id,
                    payload.requested_at,
                ),
            )
            refund_rows = cursor.rowcount
            if refund_rows == 0:
                raise PermanentDatabaseIntegrityError("refund identity conflict")
            new_total = refunded + payload.amount
            payment_status = "refunded" if new_total == paid else "partially_refunded"
            cursor.execute(
                """
                UPDATE payments SET status = %s, updated_at = CURRENT_TIMESTAMP
                WHERE payment_id = %s
                """,
                (payment_status, payload.payment_id),
            )
            cursor.execute(
                """
                UPDATE orders SET status = %s, latest_event_id = %s,
                    updated_at = CURRENT_TIMESTAMP WHERE order_id = %s
                """,
                (
                    payment_status,
                    event_id,
                    payload.order_id,
                ),
            )
        return {"refunds": refund_rows, "payments": 1, "orders": 1}
