"""Latest cart state and deterministic item replacement."""

from decimal import Decimal
from typing import cast

from services.event_processor.errors import (
    MissingBusinessDependencyError,
    PermanentDatabaseIntegrityError,
)
from services.event_processor.persistence.repositories.base import Repository
from shared.schemas import AddedToCartPayload, CheckoutStartedPayload


class CartRepository(Repository):
    def add_item(self, payload: AddedToCartPayload, event_id: object) -> dict[str, int]:
        if not self.exists("customers", "customer_id", payload.customer_id):
            raise MissingBusinessDependencyError("customer is not persisted")
        if not self.exists("sessions", "session_id", payload.session_id):
            raise MissingBusinessDependencyError("session is not persisted")
        line_total = payload.unit_price * payload.quantity
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO carts (
                    cart_id, customer_id, session_id, status, subtotal, discount,
                    total, currency, item_count, created_event_id, latest_event_id
                ) VALUES (%s, %s, %s, 'active', %s, 0, %s, %s, %s, %s, %s)
                ON CONFLICT (cart_id) DO UPDATE SET
                    latest_event_id = EXCLUDED.latest_event_id,
                    updated_at = CURRENT_TIMESTAMP
                WHERE carts.customer_id = EXCLUDED.customer_id
                  AND carts.session_id = EXCLUDED.session_id
                  AND carts.currency = EXCLUDED.currency
                """,
                (
                    payload.cart_id,
                    payload.customer_id,
                    payload.session_id,
                    line_total,
                    line_total,
                    payload.currency.value,
                    payload.quantity,
                    event_id,
                    event_id,
                ),
            )
            if cursor.rowcount == 0:
                raise PermanentDatabaseIntegrityError("cart identity conflict")
            cursor.execute(
                """
                INSERT INTO cart_items (
                    cart_id, product_id, quantity, unit_price, line_total
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (cart_id, product_id) DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    unit_price = EXCLUDED.unit_price,
                    line_total = EXCLUDED.line_total
                """,
                (
                    payload.cart_id,
                    payload.product_id,
                    payload.quantity,
                    payload.unit_price,
                    line_total,
                ),
            )
            cursor.execute(
                """
                UPDATE carts SET
                    subtotal = totals.subtotal,
                    total = totals.subtotal - discount,
                    item_count = totals.item_count,
                    latest_event_id = %s,
                    updated_at = CURRENT_TIMESTAMP
                FROM (
                    SELECT cart_id, SUM(line_total) AS subtotal,
                           SUM(quantity)::integer AS item_count
                    FROM cart_items WHERE cart_id = %s GROUP BY cart_id
                ) AS totals
                WHERE carts.cart_id = totals.cart_id
                """,
                (event_id, payload.cart_id),
            )
        return {"carts": 1, "cart_items": 1}

    def checkout(self, payload: CheckoutStartedPayload, event_id: object) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT customer_id, session_id, subtotal, currency, item_count
                FROM carts WHERE cart_id = %s FOR UPDATE
                """,
                (payload.cart_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise MissingBusinessDependencyError("cart is not persisted")
            if (
                row[0] != payload.customer_id
                or row[1] != payload.session_id
                or cast(Decimal, row[2]) != payload.subtotal
                or str(row[3]) != payload.currency.value
                or cast(int, row[4]) != payload.item_count
            ):
                raise PermanentDatabaseIntegrityError(
                    "checkout totals or cart identity do not match persisted cart"
                )
            cursor.execute(
                """
                UPDATE carts SET status = 'checkout_started', discount = %s,
                    total = %s, latest_event_id = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE cart_id = %s
                """,
                (
                    payload.discount_amount,
                    payload.total_amount,
                    event_id,
                    payload.cart_id,
                ),
            )
            return cursor.rowcount
