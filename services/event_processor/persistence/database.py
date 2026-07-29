"""Bounded psycopg pool construction, health, schema checks, and lifecycle."""

from time import sleep
from typing import cast
from urllib.parse import urlsplit

import psycopg
from psycopg import sql
from psycopg_pool import ConnectionPool, PoolTimeout

from services.event_processor.config import ProcessorConfig
from services.event_processor.errors import (
    RetryableDatabaseError,
    StartupDatabaseError,
)
from services.event_processor.logging import get_logger


def safe_postgres_endpoint(dsn: str) -> str:
    """Return a credential-free endpoint suitable for diagnostics."""
    parsed = urlsplit(dsn)
    host = parsed.hostname or "unknown"
    return f"{parsed.scheme}://{host}:{parsed.port or 5432}{parsed.path}"


class Database:
    """Own the application connection pool; never expose a global connection."""

    def __init__(
        self,
        config: ProcessorConfig,
        pool: ConnectionPool[psycopg.Connection[tuple[object, ...]]] | None = None,
    ) -> None:
        self.config = config
        self.pool = pool or ConnectionPool(
            conninfo=config.postgres_dsn,
            min_size=config.processor_db_pool_min_size,
            max_size=config.processor_db_pool_max_size,
            timeout=config.processor_db_acquire_timeout_seconds,
            max_idle=config.processor_db_max_idle_seconds,
            open=False,
            kwargs={
                "autocommit": False,
                "connect_timeout": config.processor_db_connect_timeout_seconds,
                "options": (
                    f"-c statement_timeout={config.processor_db_statement_timeout_ms}"
                ),
            },
        )

    def open(self) -> None:
        """Open and verify the pool with bounded startup attempts."""
        logger = get_logger()
        last_error: Exception | None = None
        self.pool.open()
        for attempt in range(1, self.config.processor_db_startup_attempts + 1):
            try:
                self.healthcheck()
                self.verify_schema(self.config.processor_required_schema_version)
                return
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "database_startup_retry",
                    attempt=attempt,
                    endpoint=safe_postgres_endpoint(self.config.postgres_dsn),
                    error_type=type(exc).__name__,
                )
                if attempt < self.config.processor_db_startup_attempts:
                    sleep(self.config.processor_db_startup_backoff_seconds)
        self.close()
        raise StartupDatabaseError("PostgreSQL startup checks failed") from last_error

    def healthcheck(self) -> None:
        """Verify a pooled connection without logging credentials."""
        try:
            with self.pool.connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    if cursor.fetchone() != (1,):
                        raise StartupDatabaseError("PostgreSQL health check failed")
                connection.rollback()
        except (psycopg.Error, PoolTimeout) as exc:
            raise RetryableDatabaseError("PostgreSQL is unavailable") from exc

    def verify_schema(self, required_version: int) -> None:
        """Fail startup when migration history is absent or behind."""
        try:
            with self.pool.connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT COALESCE(MAX(version), 0)
                        FROM schema_migrations
                        """
                    )
                    row = cursor.fetchone()
                connection.rollback()
        except psycopg.Error as exc:
            raise StartupDatabaseError(
                "database migration history is unavailable"
            ) from exc
        actual = cast(int, row[0]) if row else 0
        if actual < required_version:
            raise StartupDatabaseError(
                "database schema version "
                f"{actual} is behind required {required_version}"
            )

    def close(self) -> None:
        """Gracefully close all pooled connections."""
        self.pool.close()


def qualified_identifier(name: str) -> sql.Identifier:
    """Build an identifier only for trusted internal schema diagnostics."""
    return sql.Identifier(name)
