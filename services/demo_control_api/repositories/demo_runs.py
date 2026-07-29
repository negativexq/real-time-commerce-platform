"""Parameterized run persistence and manifest-based read models."""

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

from services.demo_control_api.models.overview import OverviewScope
from services.demo_control_api.models.runs import (
    TERMINAL_STATUSES,
    DemoRun,
    Page,
    RunStatus,
    RunSummary,
    transition_allowed,
)
from services.demo_control_api.models.scenarios import RunCreate


class InvalidTransitionError(RuntimeError):
    pass


class DemoRunRepository:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def _connect(self) -> psycopg.Connection[dict[str, Any]]:
        return psycopg.connect(self._dsn, row_factory=dict_row)

    @staticmethod
    def _model(row: dict[str, Any]) -> DemoRun:
        data = dict(row)
        data["parameters"] = data.pop("parameters_json")
        return DemoRun.model_validate(data)

    def create(self, request: RunCreate) -> DemoRun:
        run_id = uuid4()
        now = datetime.now(UTC)
        scope = f"demo:{run_id}"
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO demo_runs (
                    run_id, scenario_type, status, requested_event_count,
                    requested_duration_seconds, requested_events_per_second,
                    seed, parameters_json, test_scope, created_at, updated_at
                ) VALUES (%s, %s, 'PENDING', %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
                RETURNING *
                """,
                (
                    run_id,
                    request.scenario_type.value,
                    request.event_count,
                    request.duration_seconds,
                    request.events_per_second,
                    request.seed,
                    request.model_dump_json(),
                    scope,
                    now,
                    now,
                ),
            )
            row = cursor.fetchone()
            assert row is not None
            return self._model(row)

    def get(self, run_id: UUID) -> DemoRun | None:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT * FROM demo_runs WHERE run_id = %s", (run_id,))
            row = cursor.fetchone()
            return self._model(row) if row else None

    def list(self, page: int, page_size: int) -> Page:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) AS count FROM demo_runs")
            count_row = cursor.fetchone()
            assert count_row is not None
            total = cast(int, count_row["count"])
            cursor.execute(
                "SELECT * FROM demo_runs ORDER BY created_at DESC, run_id LIMIT %s OFFSET %s",
                (page_size, (page - 1) * page_size),
            )
            return Page(
                items=[self._model(row) for row in cursor.fetchall()],
                page=page,
                page_size=page_size,
                total=total,
            )

    def overview_run(self) -> tuple[DemoRun, OverviewScope] | None:
        """Prefer the newest active run, otherwise the newest completed run."""
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM demo_runs
                WHERE status IN (
                  'PENDING', 'STARTING', 'RUNNING', 'STOP_REQUESTED', 'COMPLETED'
                )
                ORDER BY
                  CASE WHEN status IN (
                    'PENDING', 'STARTING', 'RUNNING', 'STOP_REQUESTED'
                  ) THEN 0 ELSE 1 END,
                  created_at DESC,
                  run_id
                LIMIT 1
                """
            )
            row = cursor.fetchone()
        if row is None:
            return None
        run = self.refresh(row["run_id"])
        scope: OverviewScope = (
            "ACTIVE_RUN"
            if run.status
            in {
                RunStatus.PENDING,
                RunStatus.STARTING,
                RunStatus.RUNNING,
                RunStatus.STOP_REQUESTED,
            }
            else "LATEST_COMPLETED_RUN"
        )
        return run, scope

    def transition(
        self,
        run_id: UUID,
        target: RunStatus,
        *,
        message: str | None = None,
        error_category: str | None = None,
    ) -> DemoRun:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM demo_runs WHERE run_id = %s FOR UPDATE", (run_id,)
            )
            row = cursor.fetchone()
            if row is None:
                raise KeyError(run_id)
            current = RunStatus(row["status"])
            if current == target or (
                current in TERMINAL_STATUSES and target == current
            ):
                return self._model(row)
            if not transition_allowed(current, target):
                raise InvalidTransitionError(
                    f"{current.value} cannot transition to {target.value}"
                )
            now = datetime.now(UTC)
            timestamp_column = {
                RunStatus.RUNNING: "started_at",
                RunStatus.COMPLETED: "completed_at",
                RunStatus.STOPPED: "stopped_at",
            }.get(target)
            assignments = [
                "status = %s",
                "updated_at = %s",
                "status_message = %s",
                "error_category = %s",
            ]
            params: list[object] = [target.value, now, message, error_category]
            if timestamp_column:
                assignments.append(f"{timestamp_column} = %s")
                params.append(now)
            params.append(run_id)
            cursor.execute(
                f"UPDATE demo_runs SET {', '.join(assignments)} WHERE run_id = %s RETURNING *",
                tuple(params),
            )
            updated = cursor.fetchone()
            assert updated is not None
            return self._model(updated)

    def add_manifest(self, run_id: UUID, events: Sequence[tuple[UUID, str]]) -> None:
        if not events:
            return
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO demo_run_event_manifest (run_id, event_id, expected_event_type)
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                """,
                [(run_id, event_id, event_type) for event_id, event_type in events],
            )

    def refresh(self, run_id: UUID, generated: int | None = None) -> DemoRun:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                WITH counts AS (
                    SELECT
                      count(pe.event_id)::int processed,
                      count(fe.evaluation_id) FILTER (WHERE fe.decision='APPROVE')::int approve,
                      count(fe.evaluation_id) FILTER (WHERE fe.decision='REVIEW')::int review,
                      count(fe.evaluation_id) FILTER (WHERE fe.decision='BLOCK')::int block,
                      count(fa.alert_id)::int alerts,
                      count(fo.outbox_id) FILTER (WHERE fo.status='PUBLISHED')::int published
                    FROM demo_run_event_manifest m
                    LEFT JOIN processed_events pe ON pe.event_id=m.event_id
                    LEFT JOIN fraud_evaluations fe ON fe.source_event_id=m.event_id
                    LEFT JOIN fraud_alerts fa ON fa.source_event_id=m.event_id
                    LEFT JOIN fraud_outbox fo ON fo.aggregate_id=fa.alert_id
                    WHERE m.run_id=%s
                )
                UPDATE demo_runs d SET
                  generated_event_count=COALESCE(%s, generated_event_count),
                  processed_event_count=c.processed, approve_count=c.approve,
                  review_count=c.review, block_count=c.block,
                  fraud_alert_count=c.alerts, outbox_published_count=c.published,
                  updated_at=CURRENT_TIMESTAMP
                FROM counts c WHERE d.run_id=%s RETURNING d.*
                """,
                (run_id, generated, run_id),
            )
            row = cursor.fetchone()
            if row is None:
                raise KeyError(run_id)
            return self._model(row)

    def summary(self, run_id: UUID) -> RunSummary | None:
        run = self.refresh(run_id)
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT count(fe.*)::int evaluations,
                  count(fo.*) FILTER (WHERE fo.status='PENDING')::int pending,
                  count(fo.*) FILTER (WHERE fo.status='PUBLISHED')::int published
                FROM demo_run_event_manifest m
                LEFT JOIN fraud_evaluations fe ON fe.source_event_id=m.event_id
                LEFT JOIN fraud_alerts fa ON fa.source_event_id=m.event_id
                LEFT JOIN fraud_outbox fo ON fo.aggregate_id=fa.alert_id
                WHERE m.run_id=%s
                """,
                (run_id,),
            )
            counts = cursor.fetchone()
            assert counts is not None
        end = run.completed_at or run.stopped_at or run.updated_at
        duration = (end - run.started_at).total_seconds() if run.started_at else None
        return RunSummary(
            run=run,
            validation_failures=run.dlq_count,
            postgres_committed_events=run.processed_event_count,
            fraud_evaluations=counts["evaluations"],
            outbox_pending=counts["pending"],
            outbox_published=counts["published"],
            duration_seconds=duration,
            effective_event_rate=(
                run.generated_event_count / duration
                if duration and duration > 0
                else None
            ),
        )

    def reconcile(self) -> int:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE demo_runs SET
                  status=CASE WHEN status='STOP_REQUESTED' THEN 'STOPPED' ELSE 'FAILED' END,
                  status_message='API restarted; in-memory scenario task was abandoned',
                  error_category='api_restart', updated_at=CURRENT_TIMESTAMP,
                  stopped_at=CASE WHEN status='STOP_REQUESTED' THEN CURRENT_TIMESTAMP ELSE stopped_at END
                WHERE status IN ('STARTING','RUNNING','STOP_REQUESTED')
                """
            )
            return cursor.rowcount

    def cleanup(self, run_id: UUID) -> dict[str, int]:
        run = self.get(run_id)
        if run is None:
            raise KeyError(run_id)
        if run.status not in TERMINAL_STATUSES:
            raise InvalidTransitionError("cleanup requires a terminal run")
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM demo_run_event_manifest WHERE run_id=%s", (run_id,)
            )
            manifest = cursor.rowcount
            cursor.execute("DELETE FROM demo_runs WHERE run_id=%s", (run_id,))
            return {
                "manifest_rows": manifest,
                "run_rows": cursor.rowcount,
                "business_rows": 0,
            }
