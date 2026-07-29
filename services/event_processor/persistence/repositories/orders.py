"""Order creation and cart conversion."""

from decimal import Decimal
from typing import cast

from services.event_processor.errors import (
    MissingBusinessDependencyError,
    PermanentDatabaseIntegrityError,
)
from services.event_processor.persistence.repositories.base import Repository
from shared.schemas import OrderCreatedPayload


class OrderRepository(Repository):
    def insert(self, payload: OrderCreatedPayload, event_id: object) -> dict[str, int]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT customer_id, session_id, subtotal, discount, total,
                       currency, item_count
                FROM carts WHERE cart_id = %s FOR UPDATE
                """,
                (payload.cart_id,),
            )
            cart = cursor.fetchone()
            if cart is None:
                raise MissingBusinessDependencyError("cart is not persisted")
            if (
                cart[0] != payload.customer_id
                or cart[1] != payload.session_id
                or cast(Decimal, cart[2]) != payload.subtotal
                or cast(Decimal, cart[3]) != payload.discount_amount
                or cast(Decimal, cart[4]) != payload.total_amount
                or str(cart[5]) != payload.currency.value
                or cast(int, cart[6]) != payload.item_count
            ):
                raise PermanentDatabaseIntegrityError(
                    "order totals or identity do not match persisted cart"
                )
            cursor.execute(
                """
                INSERT INTO orders (
                    order_id, customer_id, session_id, cart_id, status,
                    subtotal, discount, total, currency, item_count,
                    shipping_country, billing_country, created_event_id,
                    latest_event_id, ordered_at
                ) VALUES (
                    %s, %s, %s, %s, 'created', %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (order_id) DO NOTHING
                """,
                (
                    payload.order_id,
                    payload.customer_id,
                    payload.session_id,
                    payload.cart_id,
                    payload.subtotal,
                    payload.discount_amount,
                    payload.total_amount,
                    payload.currency.value,
                    payload.item_count,
                    payload.shipping_country_code,
                    payload.billing_country_code,
                    event_id,
                    event_id,
                    payload.created_at,
                ),
            )
            order_rows = cursor.rowcount
            if order_rows == 0:
                raise PermanentDatabaseIntegrityError("order identity conflict")
            cursor.execute(
                """
                UPDATE carts SET status = 'ordered', latest_event_id = %s,
                    updated_at = CURRENT_TIMESTAMP WHERE cart_id = %s
                """,
                (event_id, payload.cart_id),
            )
        return {"orders": order_rows, "carts": 1}
