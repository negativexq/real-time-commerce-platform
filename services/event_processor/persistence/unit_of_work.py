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
from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.context import FraudContextBuilder
from services.event_processor.fraud.engine import FraudEngine
from services.event_processor.fraud.repository import FraudRepository
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
from shared.commerce_common.enums import EventType
from shared.schemas import EventEnvelope
from shared.schemas.base import ContractModel

FRAUD_ELIGIBLE_EVENT_TYPES = frozenset(
    {
        EventType.CHECKOUT_STARTED,
        EventType.ORDER_CREATED,
        EventType.PAYMENT_FAILED,
        EventType.PAYMENT_COMPLETED,
        EventType.REFUND_REQUESTED,
    }
)


class TransactionalHandler(Protocol):
    def apply(
        self, unit_of_work: "UnitOfWork", event: EventEnvelope[ContractModel]
    ) -> Mapping[str, int]: ...


class UnitOfWork:
    """Repositories bound to exactly one caller-owned psycopg connection."""

    def __init__(
        self,
        connection: psycopg.Connection[tuple[object, ...]],
        fraud_config: FraudConfig | None = None,
    ) -> None:
        self.processed_events = ProcessedEventRepository(connection)
        self.customers = CustomerRepository(connection)
        self.sessions = SessionRepository(connection)
        self.product_views = ProductViewRepository(connection)
        self.carts = CartRepository(connection)
        self.orders = OrderRepository(connection)
        self.payments = PaymentRepository(connection)
        self.refunds = RefundRepository(connection)
        self.fraud_alerts = FraudAlertRepository(connection)
        self.fraud = FraudRepository(connection, fraud_config or FraudConfig())


class UnitOfWorkFactory:
    """Acquire a connection and atomically commit ledger plus business effects."""

    def __init__(self, database: Database, config: ProcessorConfig) -> None:
        self.database = database
        self.config = config
        self.fraud_config = FraudConfig.from_environment()
        self.fraud_context = FraudContextBuilder(self.fraud_config)
        self.fraud_engine = FraudEngine(self.fraud_config)

    def persist(
        self,
        event: EventEnvelope[ContractModel],
        message: ConsumedMessage,
        context: ProcessingContext,
        handler: TransactionalHandler,
    ) -> PersistenceResult:
        started = perf_counter()
        fraud_decision: str | None = None
        matched_rule_ids: tuple[str, ...] = ()
        try:
            with (
                self.database.pool.connection(
                    timeout=self.config.processor_db_acquire_timeout_seconds
                ) as connection,
                connection.transaction(),
            ):
                unit_of_work = UnitOfWork(connection, self.fraud_config)
                unit_of_work.processed_events.insert_identity(
                    event,
                    message,
                    context,
                    persist_raw_json=self.config.processor_persist_raw_event_json,
                )
                rows = dict(handler.apply(unit_of_work, event))
                if (
                    self.fraud_config.fraud_engine_enabled
                    and event.event_type in FRAUD_ELIGIBLE_EVENT_TYPES
                ):
                    fraud_context = self.fraud_context.build(connection, event)
                    evaluation = self.fraud_engine.evaluate(fraud_context)
                    fraud_decision = evaluation.decision.value
                    matched_rule_ids = tuple(
                        result.rule_id for result in evaluation.matched_rules
                    )
                    fraud_rows = unit_of_work.fraud.persist(evaluation, event)
                    for table, count in fraud_rows.items():
                        rows[table] = rows.get(table, 0) + count
            rows["processed_events"] = 1
            return PersistenceResult(
                False,
                tuple(sorted(name for name, count in rows.items() if count)),
                rows,
                (perf_counter() - started) * 1_000,
                fraud_decision,
                matched_rule_ids,
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
