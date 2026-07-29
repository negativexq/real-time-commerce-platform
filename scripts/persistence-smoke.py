"""Bounded PostgreSQL repository smoke scenarios."""

import random
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast
from uuid import uuid4

from services.event_generator.config import GeneratorConfig
from services.event_generator.generator import SeededUuidFactory, SyntheticGenerator
from services.event_generator.journey import JourneyBuilder
from services.event_processor.config import ProcessorConfig
from services.event_processor.errors import (
    MissingBusinessDependencyError,
    PermanentDatabaseIntegrityError,
)
from services.event_processor.models import ConsumedMessage, ProcessingContext
from services.event_processor.persistence import Database, UnitOfWorkFactory
from services.event_processor.persistence.handlers import (
    default_persistence_registry,
)
from shared.kafka_metadata import event_message_headers, event_message_key
from shared.schemas import (
    EventEnvelope,
    RefundRequestedPayload,
    canonical_json,
)
from shared.schemas.base import ContractModel


class SmokeClock:
    def __init__(self) -> None:
        self.current = datetime.now(UTC)

    def now(self) -> datetime:
        value = self.current
        self.current += timedelta(seconds=1)
        return value


def main() -> int:
    scenario = sys.argv[1] if len(sys.argv) > 1 else "normal"
    if scenario not in {
        "sample",
        "normal",
        "duplicate",
        "recovery",
        "dependency",
        "refund",
    }:
        raise ValueError(f"unknown persistence scenario: {scenario}")
    run_id = uuid4().hex
    source = f"persistence-smoke:{run_id}"
    generator_config = GeneratorConfig(
        generator_seed=7300,
        generator_stateful_mode=False,
        generator_add_to_cart_probability=1,
        generator_checkout_probability=1,
        generator_payment_success_probability=1,
        generator_refund_probability=1,
    )
    journey = JourneyBuilder(
        generator_config,
        SyntheticGenerator(random.Random(7300), SeededUuidFactory(7300)),
        SmokeClock(),
    ).build()
    events = [event.model_copy(update={"source": source}) for event in journey.events]
    config = ProcessorConfig.from_environment()
    database = Database(config)
    database.open()
    factory = UnitOfWorkFactory(database, config)
    handlers = default_persistence_registry()
    context = ProcessingContext("commerce.events", 0, 0, "persistence-smoke", run_id, 1)

    def persist(event: EventEnvelope[ContractModel], offset: int) -> bool:
        message = ConsumedMessage(
            "commerce.events",
            0,
            offset,
            event.event_time,
            event_message_key(event),
            canonical_json(event).encode(),
            event_message_headers(event),
        )
        result = factory.persist(
            event,
            message,
            replace(context, offset=offset),
            handlers[event.event_type],
        )
        return result.already_persisted

    try:
        if scenario == "dependency":
            try:
                persist(events[1], 1)
            except MissingBusinessDependencyError:
                pass
            else:
                raise AssertionError("missing customer dependency was not detected")
            persist(events[0], 0)
            persist(events[1], 1)
        else:
            for offset, event in enumerate(events):
                persist(event, offset)

        if scenario in {"duplicate", "recovery"} and not persist(events[0], 0):
            raise AssertionError("identical event was not already persisted")

        if scenario == "refund":
            original = events[-1]
            payload = original.payload
            if not isinstance(payload, RefundRequestedPayload):
                raise AssertionError("full journey did not end with refund")
            excessive = payload.model_copy(
                update={
                    "refund_id": uuid4(),
                    "amount": Decimal("999999.00"),
                    "requested_at": payload.requested_at + timedelta(seconds=1),
                }
            )
            excessive_event = original.model_copy(
                update={
                    "event_id": uuid4(),
                    "event_time": original.event_time + timedelta(seconds=1),
                    "produced_at": original.produced_at + timedelta(seconds=1),
                    "payload": excessive,
                }
            )
            try:
                persist(excessive_event, len(events))
            except PermanentDatabaseIntegrityError:
                pass
            else:
                raise AssertionError("over-refund was not rejected")

        with database.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*), COUNT(DISTINCT event_id),
                           COUNT(DISTINCT (kafka_topic, kafka_partition, kafka_offset))
                    FROM processed_events WHERE source = %s
                    """,
                    (source,),
                )
                counts = cursor.fetchone()
                assert counts is not None
                if counts[0] != counts[1] or counts[0] != counts[2]:
                    raise AssertionError("ledger uniqueness was not preserved")
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM orders o
                    JOIN carts c ON c.cart_id = o.cart_id
                    JOIN sessions s ON s.session_id = o.session_id
                    JOIN customers u ON u.customer_id = o.customer_id
                    JOIN payments p ON p.order_id = o.order_id
                    WHERE u.first_event_id IN (
                        SELECT event_id FROM processed_events WHERE source = %s
                    )
                    """,
                    (source,),
                )
                related = cursor.fetchone()
                assert related is not None
                if scenario != "dependency" and cast(int, related[0]) < 1:
                    raise AssertionError(
                        "full business relationships were not persisted"
                    )
            connection.rollback()
        print(
            f"Persistence {scenario} smoke passed for run {run_id}: "
            f"{counts[0]} durable events."
        )
    finally:
        _cleanup(database, source)
        database.close()
    return 0


def _cleanup(database: Database, source: str) -> None:
    """Delete only rows belonging to this unique smoke source."""
    with (
        database.pool.connection() as connection,
        connection.transaction(),
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "CREATE TEMP TABLE smoke_ids ON COMMIT DROP AS "
            "SELECT event_id FROM processed_events WHERE source = %s",
            (source,),
        )
        for statement in (
            """
            DELETE FROM refunds
            WHERE event_id IN (SELECT event_id FROM smoke_ids)
            """,
            """
            DELETE FROM payments
            WHERE event_id IN (SELECT event_id FROM smoke_ids)
            """,
            """
            DELETE FROM fraud_alerts
            WHERE event_id IN (SELECT event_id FROM smoke_ids)
            """,
            """
            DELETE FROM orders
            WHERE created_event_id IN (SELECT event_id FROM smoke_ids)
            """,
            """
            DELETE FROM carts
            WHERE created_event_id IN (SELECT event_id FROM smoke_ids)
            """,
            """
            DELETE FROM product_views
            WHERE event_id IN (SELECT event_id FROM smoke_ids)
            """,
            """
            DELETE FROM sessions
            WHERE first_event_id IN (SELECT event_id FROM smoke_ids)
            """,
            """
            DELETE FROM customers
            WHERE first_event_id IN (SELECT event_id FROM smoke_ids)
            """,
            """
            DELETE FROM processed_events
            WHERE event_id IN (SELECT event_id FROM smoke_ids)
            """,
        ):
            cursor.execute(statement)


if __name__ == "__main__":
    raise SystemExit(main())
