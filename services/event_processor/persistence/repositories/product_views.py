"""Immutable event-level product views."""

from services.event_processor.errors import MissingBusinessDependencyError
from services.event_processor.persistence.repositories.base import Repository
from shared.schemas import ProductViewedPayload


class ProductViewRepository(Repository):
    def insert(
        self, payload: ProductViewedPayload, event_id: object, viewed_at: object
    ) -> int:
        if not self.exists("customers", "customer_id", payload.customer_id):
            raise MissingBusinessDependencyError("customer is not persisted")
        if not self.exists("sessions", "session_id", payload.session_id):
            raise MissingBusinessDependencyError("session is not persisted")
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO product_views (
                    event_id, session_id, customer_id, product_id, category,
                    price, currency, quantity_available, viewed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    event_id,
                    payload.session_id,
                    payload.customer_id,
                    payload.product_id,
                    payload.category,
                    payload.unit_price,
                    payload.currency.value,
                    payload.quantity_available,
                    viewed_at,
                ),
            )
            return cursor.rowcount
