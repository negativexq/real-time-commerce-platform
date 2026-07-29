"""Session persistence."""

from services.event_processor.errors import (
    MissingBusinessDependencyError,
    PermanentDatabaseIntegrityError,
)
from services.event_processor.persistence.repositories.base import Repository
from shared.schemas import SessionStartedPayload


class SessionRepository(Repository):
    def insert(self, payload: SessionStartedPayload, event_id: object) -> int:
        if not self.exists("customers", "customer_id", payload.customer_id):
            raise MissingBusinessDependencyError("customer is not persisted")
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT customer_id, device_id, ip_address::text, country, started_at
                FROM sessions WHERE session_id = %s
                """,
                (payload.session_id,),
            )
            existing = cursor.fetchone()
            identity = (
                payload.customer_id,
                str(payload.device_id),
                str(payload.ip_address),
                payload.country_code,
                payload.started_at,
            )
            if existing is not None:
                if tuple(existing) != identity:
                    raise PermanentDatabaseIntegrityError("session identity conflict")
                return 0
            cursor.execute(
                """
                INSERT INTO sessions (
                    session_id, customer_id, device_id, device_type, ip_address,
                    country, channel, started_at, first_event_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    payload.session_id,
                    payload.customer_id,
                    str(payload.device_id),
                    payload.device_type.value,
                    str(payload.ip_address),
                    payload.country_code,
                    payload.channel.value,
                    payload.started_at,
                    event_id,
                ),
            )
            return 1
