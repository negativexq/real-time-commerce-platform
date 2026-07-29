"""Durable event ledger and independent database idempotency."""

from hashlib import sha256

from services.event_processor.errors import (
    AlreadyPersistedEvent,
    PermanentDatabaseIntegrityError,
)
from services.event_processor.models import ConsumedMessage, ProcessingContext
from services.event_processor.persistence.repositories.base import Repository
from shared.schemas import EventEnvelope, canonical_json
from shared.schemas.base import ContractModel


def canonical_payload_hash(event: EventEnvelope[ContractModel]) -> str:
    """Hash the canonical complete event, including envelope identity."""
    return sha256(canonical_json(event).encode()).hexdigest()


class ProcessedEventRepository(Repository):
    def insert_identity(
        self,
        event: EventEnvelope[ContractModel],
        message: ConsumedMessage,
        context: ProcessingContext,
        *,
        persist_raw_json: bool,
    ) -> None:
        digest = canonical_payload_hash(event)
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT payload_sha256, kafka_topic, kafka_partition, kafka_offset
                FROM processed_events
                WHERE event_id = %s
                """,
                (event.event_id,),
            )
            existing = cursor.fetchone()
            if existing is not None:
                if existing[0] == digest:
                    raise AlreadyPersistedEvent(str(event.event_id))
                raise PermanentDatabaseIntegrityError(
                    "event_id conflicts with different canonical event content"
                )
            cursor.execute(
                """
                SELECT event_id FROM processed_events
                WHERE kafka_topic = %s AND kafka_partition = %s AND kafka_offset = %s
                """,
                (message.topic, message.partition, message.offset),
            )
            source = cursor.fetchone()
            if source is not None:
                raise PermanentDatabaseIntegrityError(
                    "Kafka source coordinate is already assigned to another event"
                )
            payload_json = canonical_json(event) if persist_raw_json else "{}"
            cursor.execute(
                """
                INSERT INTO processed_events (
                    event_id, event_type, event_version, correlation_id, source,
                    event_time, produced_at, kafka_topic, kafka_partition,
                    kafka_offset, kafka_key, payload_json, payload_sha256,
                    processed_at, processor_instance_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s::jsonb, %s, CURRENT_TIMESTAMP, %s
                )
                """,
                (
                    event.event_id,
                    event.event_type.value,
                    event.event_version,
                    event.correlation_id,
                    event.source,
                    event.event_time,
                    event.produced_at,
                    message.topic,
                    message.partition,
                    message.offset,
                    message.key,
                    payload_json,
                    digest,
                    context.processor_instance_id,
                ),
            )
