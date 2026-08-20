"""Steady-state saturation benchmark for the real primary pipeline.

Each measured run is isolated by an idle/drain barrier, followed by a short
warm-up and a fixed-duration load.  The run continues sampling after load
ends until the primary consumer-group lag returns to zero.
"""

import argparse
import json
import subprocess
import time
from datetime import datetime
from typing import Any

from scripts.benchmark.artifacts import now_iso, phase_path, write_json
from scripts.benchmark.config import derive_seed, load_config
from scripts.benchmark.demo_api import DemoApiClient
from scripts.benchmark.kafka_lag import total_lag
from scripts.benchmark.pg import query_all
from scripts.benchmark.prom import PrometheusClient

RATES = (100, 150, 200, 250, 300)
SQL_OPERATIONS = (
    "processed_events_insert",
    "processed_events_select",
    "business_payments",
    "fraud_context_customer",
    "fraud_context_customer_order",
    "fraud_context_order_session",
    "fraud_context_session",
    "fraud_context_order",
    "fraud_context_recent_payments",
    "fraud_context_prior_payments",
    "fraud_context_refunds",
    "fraud_context_recent_orders",
    "fraud_context_product_views",
    "fraud_context_refund_facts",
    "fraud_context_other",
    "fraud_evaluation_select",
    "fraud_evaluation_write",
    "fraud_alert_write",
    "outbox_insert",
)
SCENARIO = "mixed_traffic"
PERSONAS = {
    "normal": 50,
    "suspicious": 15,
    "bot": 10,
    "account_takeover": 5,
    "discount_hunter": 10,
    "indecisive": 10,
}


def _parse_time(value: str | None) -> float | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _optional_delta(before: float | None, after: float | None) -> float | None:
    if before is None or after is None:
        return None
    return after - before


def _body(
    config_tag: str, label: str, rate: int, count: int, seed_part: str
) -> dict[str, Any]:
    return {
        "scenario_type": SCENARIO,
        "event_count": count,
        "events_per_second": rate,
        "seed": derive_seed(config_tag, "saturation", seed_part),
        "persona_distribution": PERSONAS,
        "notes": f"benchmark:{config_tag}:saturation:{label}:{rate}",
    }


def _runtime_snapshot(project: str) -> dict[str, dict[str, float | None]]:
    result = subprocess.run(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}",
        ],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    wanted = {
        f"{project}-demo-control-api-1",
        f"{project}-kafka-1",
        f"{project}-postgres-1",
    }
    snapshots: dict[str, dict[str, float | None]] = {}
    for line in result.stdout.splitlines():
        parts = line.split("|")
        if len(parts) != 4 or (
            parts[0] not in wanted
            and not parts[0].startswith(f"{project}-event-processor-")
        ):
            continue
        cpu = parts[1].rstrip("%").strip()
        memory = parts[3].rstrip("%").strip()
        snapshots[parts[0]] = {
            "cpu_percent": float(cpu) if cpu else None,
            "memory_percent": float(memory) if memory else None,
        }
    return snapshots


def _quantiles(
    prom: PrometheusClient, metric: str, window: int
) -> dict[str, float | None]:
    return {
        f"p{percentile}": prom.quantile(metric, percentile / 100, window)
        for percentile in (50, 95, 99)
    }


def _label_quantiles(
    prom: PrometheusClient,
    metric: str,
    label: str,
    value: str,
    window: int,
    scale: float = 1.0,
) -> dict[str, float | None]:
    selector = "{" + label + '="' + value + '"}'
    return {
        f"p{percentile}": (
            None
            if (raw := prom.quantile(metric, percentile / 100, window, selector))
            is None
            else raw * scale
        )
        for percentile in (50, 95, 99)
    }


def _label_average(
    prom: PrometheusClient,
    metric: str,
    label: str,
    value: str,
    window: int,
    scale: float = 1.0,
) -> float | None:
    selector = "{" + label + '="' + value + '"}'
    raw = prom.instant(
        f"sum(rate({metric}_sum{selector}[{window}s]))"
        f" / sum(rate({metric}_count{selector}[{window}s]))"
    )
    return None if raw is None else raw * scale


def _histogram_average(
    prom: PrometheusClient,
    metric: str,
    window: int,
    scale: float = 1.0,
) -> float | None:
    raw = prom.instant(
        f"sum(rate({metric}_sum[{window}s])) / sum(rate({metric}_count[{window}s]))"
    )
    return None if raw is None else raw * scale


TX_DECOMPOSITION_TABLES = (
    "processed_events",
    "customers",
    "sessions",
    "product_views",
    "carts",
    "cart_items",
    "orders",
    "payments",
    "refunds",
    "fraud_evaluations",
    "fraud_alerts",
    "fraud_outbox",
)


def _postgres_snapshot(config: Any) -> dict[str, Any]:
    database = query_all(
        config.postgres_dsn,
        """
        SELECT xact_commit, xact_rollback, blks_read, blks_hit,
               temp_files, temp_bytes, deadlocks, blk_read_time, blk_write_time,
               tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
               conflicts
        FROM pg_stat_database WHERE datname = current_database()
        """,
    )[0]
    wal = query_all(
        config.postgres_dsn,
        """
        SELECT wal_records, wal_fpi, wal_bytes, wal_buffers_full,
               stats_reset::text FROM pg_stat_wal
        """,
    )[0]
    activity = query_all(
        config.postgres_dsn,
        """
        SELECT count(*) AS total,
               count(*) FILTER (WHERE state = 'active') AS active,
               count(*) FILTER (WHERE wait_event IS NOT NULL) AS waiting,
               count(*) FILTER (WHERE wait_event_type = 'Lock') AS lock_waiting,
               count(*) FILTER (WHERE wait_event_type = 'IO') AS io_waiting
        FROM pg_stat_activity WHERE datname = current_database()
        """,
    )[0]
    settings = query_all(
        config.postgres_dsn,
        """
        SELECT name, setting FROM pg_settings
        WHERE name IN ('synchronous_commit', 'fsync', 'full_page_writes')
        """,
    )
    # Read-only, always-available views (no pg_stat_statements dependency):
    # per-table scan/tuple activity, to distinguish index scans from
    # sequential scans and to see per-table write volume.
    tables = query_all(
        config.postgres_dsn,
        """
        SELECT relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch,
               n_tup_ins, n_tup_upd, n_tup_del, n_live_tup, n_dead_tup
        FROM pg_stat_user_tables
        WHERE relname = ANY(%s)
        """,
        (list(TX_DECOMPOSITION_TABLES),),
    )
    bgwriter = query_all(
        config.postgres_dsn,
        """
        SELECT num_timed AS checkpoints_timed, num_requested AS checkpoints_requested,
               buffers_written AS checkpoint_buffers_written
        FROM pg_stat_checkpointer
        """,
    )
    locks = query_all(
        config.postgres_dsn,
        """
        SELECT mode, count(*) AS count
        FROM pg_locks
        WHERE database = (
            SELECT oid FROM pg_database WHERE datname = current_database()
        )
        GROUP BY mode
        """,
    )
    return {
        "database": database,
        "wal": wal,
        "activity": activity,
        "settings": {row["name"]: row["setting"] for row in settings},
        "tables": {row["relname"]: row for row in tables},
        "bgwriter": bgwriter[0] if bgwriter else {},
        "locks": {row["mode"]: row["count"] for row in locks},
        "captured_at": time.time(),
    }


def _numeric_delta(
    before: dict[str, Any], after: dict[str, Any], seconds: float
) -> dict[str, float]:
    result: dict[str, float] = {}
    for section in ("database", "wal"):
        for key, value in after[section].items():
            old = before[section].get(key)
            if isinstance(value, (int, float)) and isinstance(old, (int, float)):
                result[f"{section}.{key}_per_second"] = (value - old) / max(
                    seconds, 0.001
                )
    return result


def _lag_snapshot(config: Any, prom: PrometheusClient) -> dict[str, Any]:
    prom_total = prom.instant(
        f'sum(kafka_consumergroup_lag{{consumergroup="{config.primary_consumer_group}"}})'
    )
    return {
        "t": time.time(),
        "total": int(round(prom_total)) if prom_total is not None else None,
        "prometheus": prom_total,
        "partitions": {},
        "inflight": prom.instant("sum(commerce_processor_inflight_events)"),
    }


def _wait_idle(
    config: Any, prom: PrometheusClient, timeout: float = 300
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last = _lag_snapshot(config, prom)
    while last["total"] not in (None, 0) and time.monotonic() < deadline:
        time.sleep(2)
        last = _lag_snapshot(config, prom)
    cli_deadline = time.monotonic() + timeout
    cli_lag = None
    cli_zero_samples = 0
    while cli_zero_samples < 2 and time.monotonic() < cli_deadline:
        cli_lag = total_lag(config.compose_project, config.primary_consumer_group)
        if cli_lag == 0:
            cli_zero_samples += 1
        else:
            cli_zero_samples = 0
        time.sleep(2)
    if cli_lag != 0:
        raise TimeoutError(f"consumer group did not drain before benchmark: {last}")
    last["cli_total"] = cli_lag
    return last


def _run_one(
    config: Any,
    api: DemoApiClient,
    prom: PrometheusClient,
    rate: int,
    repeat: int,
    steady_seconds: int,
    warmup_seconds: int,
) -> dict[str, Any]:
    idle_before = _wait_idle(config, prom)
    warmup = api.run_scenario(
        _body(
            config.run_tag,
            "warmup",
            rate,
            rate * warmup_seconds,
            f"warmup:{rate}:{repeat}",
        ),
        timeout=max(120, warmup_seconds * 6),
    )
    _wait_idle(config, prom)

    event_count = rate * steady_seconds
    postgres_before = _postgres_snapshot(config)
    sql_counter_before = {
        operation: prom.instant(
            "sum("
            + "commerce_database_sql_statement_count_total"
            + '{operation="'
            + operation
            + '"}'
            + ")"
        )
        for operation in SQL_OPERATIONS
    }
    run_id = api.create_run(
        _body(
            config.run_tag, "measured", rate, event_count, f"measured:{rate}:{repeat}"
        )
    )
    wall_started = time.time()
    load_end: float | None = None
    load_samples: list[dict[str, Any]] = []
    drain_samples: list[dict[str, Any]] = []
    peak_lag = 0
    zero_samples = 0
    detail: dict[str, Any] = {}
    deadline = time.monotonic() + max(300, steady_seconds * 8)

    while time.monotonic() < deadline:
        detail = api._get(f"/api/v1/runs/{run_id}")
        sample = _lag_snapshot(config, prom)
        sample["generated_event_count"] = detail.get("generated_event_count")
        sample["processed_event_count"] = detail.get("processed_event_count")
        sample["runtime"] = _runtime_snapshot(config.compose_project)
        peak_lag = max(peak_lag, sample["total"] or 0)
        generated = int(detail.get("generated_event_count") or 0)
        if load_end is None and (
            generated >= event_count
            or detail.get("status") in {"COMPLETED", "FAILED", "STOPPED"}
        ):
            load_end = time.time()
        if load_end is None:
            load_samples.append(sample)
        else:
            drain_samples.append(sample)
            if sample["total"] == 0:
                zero_samples += 1
                if zero_samples >= 3:
                    break
            else:
                zero_samples = 0
        time.sleep(2)

    if load_end is None:
        api.stop_and_wait(run_id, timeout=30)
        raise TimeoutError(f"run {run_id} did not generate {event_count} events")

    measured = api.wait_for_terminal(run_id, timeout=120)
    postgres_after = _postgres_snapshot(config)
    sql_counter_after = {
        operation: prom.instant(
            "sum("
            + "commerce_database_sql_statement_count_total"
            + '{operation="'
            + operation
            + '"}'
            + ")"
        )
        for operation in sql_counter_before
    }
    drain_end = drain_samples[-1]["t"] if drain_samples else time.time()
    generation_duration = load_end - (_parse_time(measured.started_at) or wall_started)
    total_duration = drain_end - wall_started
    window = max(30, int(total_duration) + 20)
    summary = measured.summary
    runtime_samples = load_samples + drain_samples
    runtime_max: dict[str, dict[str, float | None]] = {}
    for sample in runtime_samples:
        for service, values in sample["runtime"].items():
            current = runtime_max.setdefault(
                service, {"cpu_percent": 0.0, "memory_percent": 0.0}
            )
            for field in current:
                if values[field] is not None:
                    current[field] = max(current[field] or 0.0, values[field] or 0.0)
    transaction_breakdown: dict[str, dict[str, float | None]] = {
        "transaction_total": {
            "avg": _histogram_average(
                prom, "commerce_database_transaction_duration_seconds", window, 1000
            ),
            **{
                k: None if v is None else v * 1000
                for k, v in _quantiles(
                    prom,
                    "commerce_database_transaction_duration_seconds_bucket",
                    window,
                ).items()
            },
        },
        "pool_acquire": {
            "avg": _histogram_average(
                prom, "commerce_database_pool_acquire_duration_seconds", window, 1000
            ),
            **{
                k: None if v is None else v * 1000
                for k, v in _quantiles(
                    prom,
                    "commerce_database_pool_acquire_duration_seconds_bucket",
                    window,
                ).items()
            },
        },
    }
    for operation in SQL_OPERATIONS:
        name = operation
        transaction_breakdown[name] = {
            "avg": _label_average(
                prom,
                "commerce_database_sql_duration_seconds",
                "operation",
                operation,
                window,
                1000,
            ),
            **{
                k: None if v is None else v * 1000
                for k, v in _label_quantiles(
                    prom,
                    "commerce_database_sql_duration_seconds_bucket",
                    "operation",
                    operation,
                    window,
                ).items()
            },
        }
    context_averages: list[float] = []
    for operation in SQL_OPERATIONS:
        average = transaction_breakdown[operation]["avg"]
        if operation.startswith("fraud_context_") and average is not None:
            context_averages.append(average)
    transaction_breakdown["fraud_context_sql"] = {
        "avg": sum(context_averages),
        "p50": None,
        "p95": None,
        "p99": None,
    }
    for name, stage in (
        ("fraud_persistence", "fraud_persistence"),
        ("commit", "commit"),
    ):
        transaction_breakdown[name] = {
            "avg": _label_average(
                prom,
                "commerce_database_stage_duration_seconds",
                "stage",
                stage,
                window,
                1000,
            ),
            **{
                k: None if v is None else v * 1000
                for k, v in _label_quantiles(
                    prom,
                    "commerce_database_stage_duration_seconds_bucket",
                    "stage",
                    stage,
                    window,
                ).items()
            },
        }
    postgres_elapsed = postgres_after["captured_at"] - postgres_before["captured_at"]
    return {
        "rate": rate,
        "repeat": repeat,
        "event_count": event_count,
        "warmup": {"run_id": warmup.run_id, "status": warmup.status},
        "run_id": run_id,
        "status": measured.status,
        "idle_before": idle_before,
        "load_started_at": wall_started,
        "load_ended_at": load_end,
        "generation_duration_seconds": generation_duration,
        "steady_load_duration_seconds": load_end - wall_started,
        "drain_time_seconds": drain_end - load_end,
        "total_run_duration_seconds": total_duration,
        "requested_events_per_second": rate,
        "generated_events": int(detail.get("generated_event_count") or event_count),
        "processed_events": int(
            summary.get(
                "postgres_committed_events", detail.get("processed_event_count") or 0
            )
        ),
        "generated_events_per_second": (
            int(detail.get("generated_event_count") or event_count)
            / max(generation_duration, 0.001)
        ),
        "progress_count_mismatch": int(detail.get("generated_event_count") or 0)
        != event_count,
        "processed_events_per_second": event_count / max(total_duration, 0.001),
        "peak_lag": peak_lag,
        "max_inflight": max(
            (
                float(sample["inflight"])
                for sample in load_samples + drain_samples
                if sample.get("inflight") is not None
            ),
            default=None,
        ),
        "end_of_load_lag": load_samples[-1]["total"] if load_samples else 0,
        "lag_samples": load_samples + drain_samples,
        "partition_lag_end": (drain_samples[-1] if drain_samples else load_samples[-1])[
            "partitions"
        ],
        "handler_latency_ms": _quantiles(
            prom, "commerce_processor_event_processing_duration_seconds_bucket", window
        ),
        "poll_to_handler_ms": {
            k: None if v is None else v * 1000
            for k, v in _quantiles(
                prom,
                "commerce_processor_poll_to_handler_duration_seconds_bucket",
                window,
            ).items()
        },
        "loop_gap_ms": {
            k: None if v is None else v * 1000
            for k, v in _quantiles(
                prom, "commerce_processor_loop_gap_duration_seconds_bucket", window
            ).items()
        },
        "db_transaction_ms": {
            k: None if v is None else v * 1000
            for k, v in _quantiles(
                prom, "commerce_database_transaction_duration_seconds_bucket", window
            ).items()
        },
        "db_pool_acquire_ms": {
            k: None if v is None else v * 1000
            for k, v in _quantiles(
                prom, "commerce_database_pool_acquire_duration_seconds_bucket", window
            ).items()
        },
        "transaction_breakdown_ms": transaction_breakdown,
        "sql_statement_counts": {
            operation: _optional_delta(
                sql_counter_before[operation], sql_counter_after[operation]
            )
            for operation in sql_counter_before
        },
        "postgres_before": postgres_before,
        "postgres_after": postgres_after,
        "postgres_rates": _numeric_delta(
            postgres_before, postgres_after, postgres_elapsed
        ),
        "redis_latency_ms": {
            k: None if v is None else v * 1000
            for k, v in _quantiles(
                prom, "commerce_processor_redis_duration_seconds_bucket", window
            ).items()
        },
        "e2e_latency_ms": summary.get("latency_ms", {}),
        "runtime_max": runtime_max,
        "errors": summary.get("errors", 0),
        "redis_errors": summary.get("redis_errors", 0),
        "retry_count": summary.get("retry_count", 0),
        "dlq_count": summary.get("dlq_count", 0),
        "duplicate_side_effects": 0,
        "outbox_success": summary.get("outbox_published_events", 0),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--rates", default=",".join(map(str, RATES)))
    parser.add_argument("--steady-seconds", type=int, default=30)
    parser.add_argument("--warmup-seconds", type=int, default=10)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    config = load_config(args.run_tag)
    api = DemoApiClient(config.demo_api_base_url)
    prom = PrometheusClient(config.prometheus_url)
    rates = [int(value) for value in args.rates.split(",")]
    results = []
    for rate in rates:
        for repeat in range(args.repeats):
            print(f"rate={rate} repeat={repeat} starting", flush=True)
            result = _run_one(
                config,
                api,
                prom,
                rate,
                repeat,
                args.steady_seconds,
                args.warmup_seconds,
            )
            results.append(result)
            print(
                json.dumps(
                    {
                        k: result[k]
                        for k in (
                            "rate",
                            "repeat",
                            "generated_events_per_second",
                            "processed_events_per_second",
                            "peak_lag",
                            "drain_time_seconds",
                        )
                    }
                ),
                flush=True,
            )
    payload = {
        "run_tag": config.run_tag,
        "rates": rates,
        "steady_seconds": args.steady_seconds,
        "warmup_seconds": args.warmup_seconds,
        "repeats": args.repeats,
        "results": results,
        "captured_at": now_iso(),
    }
    path = phase_path(config.phase_dir(), "saturation")
    write_json(path, payload)
    print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
