"""One explicit PostgreSQL transaction and repository set per source event."""

from collections.abc import Mapping
from time import perf_counter
from typing import Protocol

import psycopg
from psycopg_pool import PoolTimeout

from services.event_processor.config import ProcessorConfig
from services.event_processor.errors import (
    AlreadyPersistedEvent,
    MissingBusinessDependencyError,
    PermanentDatabaseIntegrityError,
    RetryableDatabaseError,
)
from services.event_processor.models import ConsumedMessage, ProcessingContext
from services.event_processor.persistence.database import Database
from services.event_processor.persistence.models import PersistenceResult
from services.event_processor.persistence.repositories import (
    CartRepository,
    CustomerRepository,
    FraudAlertRepository,
    OrderRepository,
    PaymentRepository,
    ProcessedEventRepository,
    ProductViewRepository,
    RefundRepository,
    SessionRepository,
)
from shared.schemas import EventEnvelope
from shared.schemas.base import ContractModel


class TransactionalHandler(Protocol):
    def apply(
        self, unit_of_work: "UnitOfWork", event: EventEnvelope[ContractModel]
    ) -> Mapping[str, int]: ...


class UnitOfWork:
    """Repositories bound to exactly one caller-owned psycopg connection."""

    def __init__(self, connection: psycopg.Connection[tuple[object, ...]]) -> None:
        self.processed_events = ProcessedEventRepository(connection)
        self.customers = CustomerRepository(connection)
        self.sessions = SessionRepository(connection)
        self.product_views = ProductViewRepository(connection)
        self.carts = CartRepository(connection)
        self.orders = OrderRepository(connection)
        self.payments = PaymentRepository(connection)
        self.refunds = RefundRepository(connection)
        self.fraud_alerts = FraudAlertRepository(connection)


class UnitOfWorkFactory:
    """Acquire a connection and atomically commit ledger plus business effects."""

    def __init__(self, database: Database, config: ProcessorConfig) -> None:
        self.database = database
        self.config = config

    def persist(
        self,
        event: EventEnvelope[ContractModel],
        message: ConsumedMessage,
        context: ProcessingContext,
        handler: TransactionalHandler,
    ) -> PersistenceResult:
        started = perf_counter()
        try:
            with (
                self.database.pool.connection(
                    timeout=self.config.processor_db_acquire_timeout_seconds
                ) as connection,
                connection.transaction(),
            ):
                unit_of_work = UnitOfWork(connection)
                unit_of_work.processed_events.insert_identity(
                    event,
                    message,
                    context,
                    persist_raw_json=self.config.processor_persist_raw_event_json,
                )
                rows = dict(handler.apply(unit_of_work, event))
            rows["processed_events"] = 1
            return PersistenceResult(
                False,
                tuple(sorted(name for name, count in rows.items() if count)),
                rows,
                (perf_counter() - started) * 1_000,
            )
        except AlreadyPersistedEvent:
            return PersistenceResult(True, (), {}, (perf_counter() - started) * 1_000)
        except (MissingBusinessDependencyError, PermanentDatabaseIntegrityError):
            raise
        except PoolTimeout as exc:
            raise RetryableDatabaseError("database pool acquisition timed out") from exc
        except psycopg.Error as exc:
            if exc.sqlstate is not None and (
                exc.sqlstate.startswith("08") or exc.sqlstate in {"40001", "40P01"}
            ):
                raise RetryableDatabaseError("transient PostgreSQL failure") from exc
            raise PermanentDatabaseIntegrityError(
                "deterministic PostgreSQL constraint failure"
            ) from exc
