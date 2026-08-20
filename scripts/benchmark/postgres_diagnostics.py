"""External, sampling-based PostgreSQL activity/lock/wait diagnostic sampler.

Polls pg_stat_activity and pg_locks at a bounded interval to answer, for a
given benchmark rate, "what is PostgreSQL doing" - active/idle/waiting
backend counts, wait_event_type/wait_event distribution, lock contention,
transaction concurrency, and blocking relationships - without touching the
processor hot path or any PostgreSQL configuration. Never persists raw SQL
text; queries are bucketed into bounded query classes only.

This is diagnostic-only tooling: it reads PostgreSQL system catalogs and
writes a JSON sample series plus a summary. It does not change any
application, schema, or PostgreSQL behavior.
"""

import argparse
import json
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from scripts.benchmark.config import load_config

IDLE_STATES = {"idle"}
IDLE_IN_TXN_STATES = {"idle in transaction", "idle in transaction (aborted)"}

_TABLES = (
    "processed_events",
    "fraud_evaluations",
    "fraud_alerts",
    "fraud_outbox",
    "customers",
    "sessions",
    "product_views",
    "carts",
    "cart_items",
    "orders",
    "payments",
    "refunds",
)


def classify_query(query: str | None) -> str:
    """Bound, table-based query-class bucket - never returns raw SQL text."""
    if not query:
        return "none"
    normalized = " ".join(query.lower().split())
    if normalized.startswith(("select pg_stat", "select pg_locks", "select 1")):
        return "diagnostic_or_healthcheck"
    kind = (
        "select"
        if normalized.startswith("select")
        else "insert"
        if normalized.startswith("insert")
        else "update"
        if normalized.startswith("update")
        else "delete"
        if normalized.startswith("delete")
        else "commit"
        if normalized.startswith("commit")
        else "begin"
        if normalized.startswith("begin")
        else "other"
    )
    for table in _TABLES:
        if table in normalized:
            return f"{table}_{kind}"
    return f"other_{kind}"


ACTIVITY_QUERY = """
SELECT pid, backend_type, state, wait_event_type, wait_event,
       EXTRACT(EPOCH FROM (clock_timestamp() - query_start)) AS query_age_s,
       EXTRACT(EPOCH FROM (clock_timestamp() - xact_start)) AS xact_age_s,
       query, cardinality(pg_blocking_pids(pid)) AS blocked_by_count
FROM pg_stat_activity
WHERE datname = current_database()
"""

LOCKS_QUERY = """
SELECT mode, granted, count(*) AS count
FROM pg_locks
WHERE database = (SELECT oid FROM pg_database WHERE datname = current_database())
GROUP BY mode, granted
"""

DATABASE_COUNTERS_QUERY = """
SELECT xact_commit, xact_rollback
FROM pg_stat_database
WHERE datname = current_database()
"""


def _sample(connection: psycopg.Connection[dict[str, Any]]) -> dict[str, Any]:
    with connection.cursor() as cursor:
        cursor.execute(ACTIVITY_QUERY)
        activity = cursor.fetchall()
        cursor.execute(LOCKS_QUERY)
        locks = cursor.fetchall()
        cursor.execute(DATABASE_COUNTERS_QUERY)
        counters = cursor.fetchone() or {"xact_commit": 0, "xact_rollback": 0}

    active = idle = idle_in_txn = waiting = active_txns = blocked = 0
    longest_active_query_age = 0.0
    longest_txn_age = 0.0
    wait_event_type_counts: dict[str, int] = {}
    wait_event_counts: dict[str, int] = {}
    query_class_active_counts: dict[str, int] = {}
    backend_type_counts: dict[str, int] = {}

    for row in activity:
        state = row["state"]
        backend_type_counts[row["backend_type"]] = (
            backend_type_counts.get(row["backend_type"], 0) + 1
        )
        if state == "active":
            active += 1
            age = row["query_age_s"] or 0.0
            longest_active_query_age = max(longest_active_query_age, age)
            query_class_active_counts[classify_query(row["query"])] = (
                query_class_active_counts.get(classify_query(row["query"]), 0) + 1
            )
        elif state in IDLE_STATES:
            idle += 1
        elif state in IDLE_IN_TXN_STATES:
            idle_in_txn += 1
        if row["xact_age_s"] is not None:
            active_txns += 1
            longest_txn_age = max(longest_txn_age, row["xact_age_s"])
        if row["wait_event_type"]:
            waiting += 1
            wait_event_type_counts[row["wait_event_type"]] = (
                wait_event_type_counts.get(row["wait_event_type"], 0) + 1
            )
            wait_event_counts[row["wait_event"]] = (
                wait_event_counts.get(row["wait_event"], 0) + 1
            )
        if row["blocked_by_count"]:
            blocked += 1

    locks_granted_by_mode: dict[str, int] = {}
    locks_waiting_by_mode: dict[str, int] = {}
    for row in locks:
        target = locks_granted_by_mode if row["granted"] else locks_waiting_by_mode
        target[row["mode"]] = target.get(row["mode"], 0) + row["count"]

    return {
        "t": time.time(),
        "active": active,
        "idle": idle,
        "idle_in_txn": idle_in_txn,
        "waiting": waiting,
        "blocked": blocked,
        "active_txns": active_txns,
        "longest_active_query_age_s": longest_active_query_age,
        "longest_txn_age_s": longest_txn_age,
        "wait_event_type_counts": wait_event_type_counts,
        "wait_event_counts": wait_event_counts,
        "query_class_active_counts": query_class_active_counts,
        "backend_type_counts": backend_type_counts,
        "locks_granted_by_mode": locks_granted_by_mode,
        "locks_waiting_by_mode": locks_waiting_by_mode,
        "xact_commit": counters["xact_commit"],
        "xact_rollback": counters["xact_rollback"],
    }


def run(
    dsn: str, duration_seconds: float, interval_seconds: float
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    deadline = time.monotonic() + duration_seconds
    with psycopg.connect(
        dsn, row_factory=dict_row, autocommit=True, application_name="pg-diagnostics"
    ) as connection:
        while time.monotonic() < deadline:
            tick_started = time.monotonic()
            samples.append(_sample(connection))
            elapsed = time.monotonic() - tick_started
            time.sleep(max(0.0, interval_seconds - elapsed))
    return samples


def io_snapshot(dsn: str) -> list[dict[str, Any]]:
    with (
        psycopg.connect(dsn, row_factory=dict_row, autocommit=True) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "SELECT backend_type, object, context, reads, read_time, writes, "
            "write_time, extends, extend_time, hits, evictions, fsyncs, "
            "fsync_time FROM pg_stat_io"
        )
        return list(cursor.fetchall())


def checkpointer_snapshot(dsn: str) -> dict[str, Any] | None:
    with (
        psycopg.connect(dsn, row_factory=dict_row, autocommit=True) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "SELECT num_timed, num_requested, buffers_written, "
            "restartpoints_timed, restartpoints_req, restartpoints_done, "
            "write_time, sync_time FROM pg_stat_checkpointer"
        )
        return cursor.fetchone()


def summarize_samples(
    samples: list[dict[str, Any]], *, active_only: bool = True
) -> dict[str, Any]:
    """Aggregate a raw sample series into avg/max backend and wait evidence.

    With ``active_only`` (the default), samples where no backend was active
    are excluded before averaging - this approximates "under load" periods
    within a series that also spans warmup/drain/idle gaps, without needing
    precise cross-process wall-clock correlation with the benchmark's own
    load windows.
    """
    considered = [s for s in samples if not active_only or s["active"] > 0]
    if not considered:
        return {
            "sample_count": 0,
            "considered_count": 0,
            "active_avg": None,
            "active_max": None,
            "waiting_avg": None,
            "waiting_max": None,
            "idle_in_txn_avg": None,
            "idle_in_txn_max": None,
            "active_txns_avg": None,
            "active_txns_max": None,
            "longest_active_query_age_max_s": None,
            "longest_txn_age_max_s": None,
            "wait_event_type_totals": {},
            "wait_event_totals": {},
            "query_class_totals": {},
            "blocked_max": None,
            "locks_waiting_by_mode_max": {},
            "transactions_per_second": None,
        }

    def _sum_counts(key: str) -> dict[str, int]:
        totals: dict[str, int] = {}
        for sample in considered:
            for name, count in sample[key].items():
                totals[name] = totals.get(name, 0) + count
        return dict(sorted(totals.items(), key=lambda item: -item[1]))

    def _max_counts(key: str) -> dict[str, int]:
        totals: dict[str, int] = {}
        for sample in considered:
            for name, count in sample[key].items():
                totals[name] = max(totals.get(name, 0), count)
        return dict(sorted(totals.items(), key=lambda item: -item[1]))

    active_values = [s["active"] for s in considered]
    waiting_values = [s["waiting"] for s in considered]
    idle_in_txn_values = [s["idle_in_txn"] for s in considered]
    active_txn_values = [s["active_txns"] for s in considered]
    blocked_values = [s["blocked"] for s in considered]

    txn_rate = None
    if len(considered) >= 2 and considered[-1]["t"] > considered[0]["t"]:
        commit_delta = considered[-1]["xact_commit"] - considered[0]["xact_commit"]
        rollback_delta = (
            considered[-1]["xact_rollback"] - considered[0]["xact_rollback"]
        )
        seconds = considered[-1]["t"] - considered[0]["t"]
        txn_rate = (commit_delta + rollback_delta) / seconds

    return {
        "sample_count": len(samples),
        "considered_count": len(considered),
        "active_avg": sum(active_values) / len(active_values),
        "active_max": max(active_values),
        "waiting_avg": sum(waiting_values) / len(waiting_values),
        "waiting_max": max(waiting_values),
        "idle_in_txn_avg": sum(idle_in_txn_values) / len(idle_in_txn_values),
        "idle_in_txn_max": max(idle_in_txn_values),
        "active_txns_avg": sum(active_txn_values) / len(active_txn_values),
        "active_txns_max": max(active_txn_values),
        "longest_active_query_age_max_s": max(
            s["longest_active_query_age_s"] for s in considered
        ),
        "longest_txn_age_max_s": max(s["longest_txn_age_s"] for s in considered),
        "wait_event_type_totals": _sum_counts("wait_event_type_counts"),
        "wait_event_totals": _sum_counts("wait_event_counts"),
        "query_class_totals": _sum_counts("query_class_active_counts"),
        "blocked_max": max(blocked_values),
        "locks_waiting_by_mode_max": _max_counts("locks_waiting_by_mode"),
        "transactions_per_second": txn_rate,
    }


def raw_artifact_filename(label: str) -> str:
    """High-volume per-tick sample series - one file per label/rate so a
    later invocation under the same --run-tag can never overwrite an
    earlier label's raw telemetry (same overwrite class of bug as
    direct_saturation.py's rate_artifact_filename - see
    optimization-history.md). Kept local-only via .gitignore."""
    return f"postgres-diagnostics-raw-{label}.json"


def summary_artifact_filename(label: str) -> str:
    """Compact, versionable aggregate - no per-tick samples."""
    return f"postgres-diagnostics-summary-{label}.json"


def raw_artifact_path(phase_dir: str, label: str) -> Path:
    return Path(phase_dir) / raw_artifact_filename(label)


def summary_artifact_path(phase_dir: str, label: str) -> Path:
    return Path(phase_dir) / summary_artifact_filename(label)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--label", required=True, help="e.g. a rate like 1050")
    parser.add_argument("--duration-seconds", type=float, required=True)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    args = parser.parse_args()

    config = load_config(args.run_tag)
    started_at = time.time()
    io_before = io_snapshot(config.postgres_dsn)
    checkpointer_before = checkpointer_snapshot(config.postgres_dsn)
    samples = run(config.postgres_dsn, args.duration_seconds, args.interval_seconds)
    io_after = io_snapshot(config.postgres_dsn)
    checkpointer_after = checkpointer_snapshot(config.postgres_dsn)
    finished_at = time.time()

    metadata = {
        "run_tag": args.run_tag,
        "label": args.label,
        "started_at": started_at,
        "finished_at": finished_at,
        "interval_seconds": args.interval_seconds,
        "sample_count": len(samples),
        "io_before": io_before,
        "io_after": io_after,
        "checkpointer_before": checkpointer_before,
        "checkpointer_after": checkpointer_after,
    }

    phase_dir = config.phase_dir()
    Path(phase_dir).mkdir(parents=True, exist_ok=True)

    summary_path = summary_artifact_path(phase_dir, args.label)
    summary_path.write_text(
        json.dumps(
            {**metadata, "summary": summarize_samples(samples)},
            indent=2,
            default=str,
        )
    )
    print(f"wrote {summary_path}")

    raw_path = raw_artifact_path(phase_dir, args.label)
    raw_path.write_text(
        json.dumps({**metadata, "samples": samples}, indent=2, default=str)
    )
    print(f"wrote {raw_path} ({len(samples)} samples)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
