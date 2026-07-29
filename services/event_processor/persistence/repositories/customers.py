"""Customer persistence and immutable identity conflict detection."""

from services.event_processor.errors import PermanentDatabaseIntegrityError
from services.event_processor.persistence.repositories.base import Repository
from shared.schemas import UserRegisteredPayload


class CustomerRepository(Repository):
    def register(self, payload: UserRegisteredPayload, event_id: object) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT email_hash, persona, home_country, registered_at
                FROM customers WHERE customer_id = %s
                """,
                (payload.customer_id,),
            )
            existing = cursor.fetchone()
            identity = (
                payload.email_hash,
                payload.persona.value,
                payload.country_code,
                payload.registered_at,
            )
            if existing is not None:
                if tuple(existing) != identity:
                    raise PermanentDatabaseIntegrityError(
                        "customer immutable identity conflict"
                    )
                cursor.execute(
                    """
                    UPDATE customers
                    SET last_event_id = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE customer_id = %s
                    """,
                    (event_id, payload.customer_id),
                )
                return 0
            cursor.execute(
                """
                INSERT INTO customers (
                    customer_id, email_hash, persona, home_country,
                    registered_at, first_event_id, last_event_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    payload.customer_id,
                    payload.email_hash,
                    payload.persona.value,
                    payload.country_code,
                    payload.registered_at,
                    event_id,
                    event_id,
                ),
            )
            return 1
