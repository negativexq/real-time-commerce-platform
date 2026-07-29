"""Ordered checksum-verified PostgreSQL migration runner."""

import argparse
import hashlib
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import cast

import psycopg

from services.event_processor.config import ProcessorConfig

MIGRATION_PATTERN = re.compile(r"^(?P<version>[0-9]{3})_(?P<name>[a-z0-9_]+)\.sql$")
ADVISORY_LOCK_ID = 7_230_017_007
MIGRATION_DIR = Path.cwd() / "database" / "migrations"


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    checksum: str
    sql: str


def discover_migrations(directory: Path = MIGRATION_DIR) -> list[Migration]:
    """Load migrations in deterministic numeric order."""
    migrations: list[Migration] = []
    for path in sorted(directory.glob("*.sql")):
        match = MIGRATION_PATTERN.fullmatch(path.name)
        if match is None:
            raise ValueError(f"invalid migration filename: {path.name}")
        content = path.read_text()
        migrations.append(
            Migration(
                int(match.group("version")),
                match.group("name"),
                hashlib.sha256(content.encode()).hexdigest(),
                content,
            )
        )
    versions = [item.version for item in migrations]
    if versions != sorted(set(versions)):
        raise ValueError("migration versions must be unique and ordered")
    return migrations


def _ensure_history(connection: psycopg.Connection[tuple[object, ...]]) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                checksum CHAR(64) NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                execution_time_ms BIGINT NOT NULL CHECK (execution_time_ms >= 0)
            )
            """
        )
    connection.commit()


def apply_migrations(dsn: str, directory: Path = MIGRATION_DIR) -> list[Migration]:
    """Apply every pending migration once under a session advisory lock."""
    migrations = discover_migrations(directory)
    applied_now: list[Migration] = []
    with psycopg.connect(dsn, autocommit=True) as connection:
        _ensure_history(connection)
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_LOCK_ID,))
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT version, checksum FROM schema_migrations ORDER BY version"
                )
                applied = {int(row[0]): str(row[1]) for row in cursor.fetchall()}
            for migration in migrations:
                existing = applied.get(migration.version)
                if existing is not None:
                    if existing != migration.checksum:
                        raise RuntimeError(
                            "checksum mismatch for applied migration "
                            f"{migration.version}"
                        )
                    continue
                started = perf_counter()
                with connection.transaction(), connection.cursor() as cursor:
                    if migration.version == 1:
                        cursor.execute("SELECT to_regclass('public.processed_events')")
                        baseline_row = cursor.fetchone()
                        assert baseline_row is not None
                        baseline_exists = baseline_row[0] is not None
                        if not baseline_exists:
                            cursor.execute(migration.sql)
                    else:
                        cursor.execute(migration.sql)
                    elapsed = round((perf_counter() - started) * 1_000)
                    cursor.execute(
                        """
                        INSERT INTO schema_migrations
                            (version, name, checksum, execution_time_ms)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            migration.version,
                            migration.name,
                            migration.checksum,
                            elapsed,
                        ),
                    )
                applied_now.append(migration)
        finally:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_ID,))
    return applied_now


def migration_status(
    dsn: str, directory: Path = MIGRATION_DIR
) -> list[tuple[object, ...]]:
    """Return migration status without changing application tables."""
    migrations = discover_migrations(directory)
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass('public.schema_migrations')")
        history_row = cursor.fetchone()
        assert history_row is not None
        if history_row[0] is None:
            return [
                (item.version, item.name, item.checksum, "pending")
                for item in migrations
            ]
        cursor.execute(
            "SELECT version, name, checksum, applied_at FROM schema_migrations"
        )
        applied = {int(row[0]): row for row in cursor.fetchall()}
    return [
        (
            item.version,
            item.name,
            item.checksum,
            "applied" if item.version in applied else "pending",
        )
        for item in migrations
    ]


def schema_check(dsn: str, required_version: int) -> None:
    """Verify the required version and current application tables."""
    required = {
        "processed_events",
        "customers",
        "sessions",
        "product_views",
        "carts",
        "cart_items",
        "orders",
        "payments",
        "refunds",
        "schema_migrations",
        "fraud_evaluations",
        "fraud_outbox",
        "fraud_alerts",
        "demo_runs",
        "demo_run_event_manifest",
    }
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
        version_row = cursor.fetchone()
        assert version_row is not None
        version = cast(int, version_row[0])
        cursor.execute(
            """
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public' AND tablename = ANY(%s)
                """,
            (list(required),),
        )
        found = {str(row[0]) for row in cursor.fetchall()}
        cursor.execute(
            """
                SELECT COUNT(*) FROM pg_constraint
                WHERE conname IN (
                    'processed_events_pkey',
                    'processed_events_kafka_source_unique',
                    'payments_event_id_key',
                    'refunds_event_id_key'
                )
                """
        )
        constraint_row = cursor.fetchone()
        assert constraint_row is not None
        constraint_count = cast(int, constraint_row[0])
    if version < required_version:
        raise RuntimeError(f"schema version {version} is behind {required_version}")
    missing = required - found
    if missing:
        raise RuntimeError(f"missing required tables: {sorted(missing)}")
    if constraint_count != 4:
        raise RuntimeError("required persistence constraints are missing")


def main(arguments: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["migrate", "status", "check"])
    parsed = parser.parse_args(arguments)
    config = ProcessorConfig.from_environment()
    if parsed.command == "migrate":
        applied = apply_migrations(config.postgres_dsn)
        print(f"Applied {len(applied)} migration(s).")
    elif parsed.command == "status":
        for row in migration_status(config.postgres_dsn):
            print(*row, sep="\t")
    else:
        schema_check(config.postgres_dsn, config.processor_required_schema_version)
        print("Database schema check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
