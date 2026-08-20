"""Isolated Kafka -> processor saturation benchmark without Demo Control API."""

import argparse
import dataclasses
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from scripts.benchmark.config import derive_seed, load_config
from scripts.benchmark.kafka_replay import (
    read_publish_timestamps,
    topic_watermarks,
)
from scripts.benchmark.pg import query_all
from scripts.benchmark.prom import PrometheusClient
from scripts.benchmark.saturation import (
    SQL_OPERATIONS,
    _histogram_average,
    _label_average,
    _label_quantiles,
    _postgres_snapshot,
    _runtime_snapshot,
    _wait_idle,
)
from scripts.benchmark.stats import percentiles
from services.event_generator.logging import configure_logging


def _lag(config: Any, prom: PrometheusClient) -> float:
    value = prom.instant(
        f'sum(kafka_consumergroup_lag{{consumergroup="{config.primary_consumer_group}"}})'
    )
    return float(value or 0)


def _counter(prom: PrometheusClient, metric: str, selector: str = "") -> float:
    return float(prom.instant(f"sum({metric}{selector})") or 0)


def _quantiles(
    prom: PrometheusClient, metric: str, window: int, labels: str = ""
) -> dict[str, float | None]:
    return {
        key: None
        if (value := prom.quantile(metric, q, window, labels)) is None
        else value * 1000
        for key, q in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99))
    }


EVENT_TYPES = (
    "user_registered",
    "session_started",
    "product_viewed",
    "added_to_cart",
    "checkout_started",
    "order_created",
    "payment_completed",
    "payment_failed",
    "refund_requested",
)
FRAUD_ELIGIBLE_EVENT_TYPES = frozenset(
    {
        "checkout_started",
        "order_created",
        "payment_failed",
        "payment_completed",
        "refund_requested",
    }
)


def _sql_statement_counts(prom: PrometheusClient) -> dict[str, float]:
    return {
        operation: prom.instant(
            f'sum(commerce_database_sql_statement_count_total{{operation="{operation}"}})'
        )
        or 0.0
        for operation in SQL_OPERATIONS
    }


def _transaction_breakdown(prom: PrometheusClient, window: int) -> dict[str, Any]:
    """Stage-level and SQL-class-level cost using the already-active
    production instrumentation (InstrumentedConnection + database_stage_duration),
    reading it through Prometheus rather than adding any new instrumentation."""
    breakdown: dict[str, dict[str, float | None]] = {
        "transaction_total": {
            "avg": _histogram_average(
                prom, "commerce_database_transaction_duration_seconds", window, 1000
            ),
            **{
                k: None if v is None else v
                for k, v in _label_quantiles(
                    prom,
                    "commerce_database_transaction_duration_seconds_bucket",
                    "result",
                    "committed",
                    window,
                    1000,
                ).items()
            },
        },
        "pool_acquire": {
            "avg": _histogram_average(
                prom, "commerce_database_pool_acquire_duration_seconds", window, 1000
            ),
        },
        "connection_release": {
            "avg": _histogram_average(
                prom,
                "commerce_database_connection_release_duration_seconds",
                window,
                1000,
            ),
        },
        "fraud_evaluation_cpu": {
            "avg": _histogram_average(
                prom, "commerce_fraud_evaluation_duration_seconds", window, 1000
            ),
        },
    }
    for name, stage in (
        ("processed_events_insert", "processed_events"),
        ("business_persistence", "business_persistence"),
        ("fraud_context", "fraud_context"),
        ("fraud_persistence", "fraud_persistence"),
        ("commit", "commit"),
    ):
        breakdown[name] = {
            "avg": _label_average(
                prom,
                "commerce_database_stage_duration_seconds",
                "stage",
                stage,
                window,
                1000,
            ),
        }
    for operation in SQL_OPERATIONS:
        breakdown[operation] = {
            "avg": _label_average(
                prom,
                "commerce_database_sql_duration_seconds",
                "operation",
                operation,
                window,
                1000,
            ),
            **{
                k: None if v is None else v
                for k, v in _label_quantiles(
                    prom,
                    "commerce_database_sql_duration_seconds_bucket",
                    "operation",
                    operation,
                    window,
                    1000,
                ).items()
            },
        }
    return breakdown


def phase_group_breakdown(
    breakdown: dict[str, dict[str, float | None]],
) -> dict[str, float | None]:
    """Group the already-active per-stage instrumentation
    (commerce_database_stage_duration_seconds, already emitted by
    UnitOfWorkFactory.persist for every transaction - see
    persistence/unit_of_work.py) into read/write/commit lifecycle phases,
    for Transaction Lifecycle Decomposition v3. This adds no new metric
    and issues no new Prometheus query - it is a pure regrouping of
    averages ``_transaction_breakdown`` already fetched.

    Read phase: ``fraud_context`` - the ~9 bounded SELECTs building
    FraudContext; the only read-heavy stage (fraud-eligible events only).
    Write phase: ``processed_events_insert`` + ``business_persistence`` +
    ``fraud_persistence`` - every stage that inserts/updates rows.
    Commit: the ``commit`` stage (time from end of transaction body to
    after ``connection.transaction()`` exits, i.e. the actual COMMIT).
    ``unattributed_ms`` is what's left after subtracting every phase plus
    pool acquire/connection release from the transaction total - Python
    overhead between phases, handler dispatch outside the stages, etc.
    """

    def avg(key: str) -> float | None:
        value = breakdown.get(key, {}).get("avg")
        return value

    read_phase = avg("fraud_context")
    write_parts = [
        avg("processed_events_insert"),
        avg("business_persistence"),
        avg("fraud_persistence"),
    ]
    present_write_parts = [part for part in write_parts if part is not None]
    write_phase = sum(present_write_parts) if present_write_parts else None
    commit = avg("commit")
    pool_acquire = avg("pool_acquire")
    connection_release = avg("connection_release")
    total = avg("transaction_total")

    known = [
        value
        for value in (read_phase, write_phase, commit, pool_acquire, connection_release)
        if value is not None
    ]
    unattributed = (total - sum(known)) if total is not None and known else None

    return {
        "read_phase_ms": read_phase,
        "write_phase_ms": write_phase,
        "commit_ms": commit,
        "pool_acquire_ms": pool_acquire,
        "connection_release_ms": connection_release,
        "transaction_total_ms": total,
        "unattributed_ms": unattributed,
    }


def _event_type_handler_latency(
    prom: PrometheusClient, window: int
) -> dict[str, float | dict[str, float | None] | None]:
    per_type = {
        event_type: _label_average(
            prom,
            "commerce_processor_event_processing_duration_seconds",
            "event_type",
            event_type,
            window,
            1000,
        )
        for event_type in EVENT_TYPES
    }
    fraud_eligible = [
        v
        for k, v in per_type.items()
        if k in FRAUD_ELIGIBLE_EVENT_TYPES and v is not None
    ]
    non_fraud = [
        v
        for k, v in per_type.items()
        if k not in FRAUD_ELIGIBLE_EVENT_TYPES and v is not None
    ]
    result: dict[str, float | dict[str, float | None] | None] = dict(per_type)
    result["_fraud_eligible_mean_ms"] = (
        sum(fraud_eligible) / len(fraud_eligible) if fraud_eligible else None
    )
    result["_non_fraud_mean_ms"] = (
        sum(non_fraud) / len(non_fraud) if non_fraud else None
    )
    return result


def _run_one(
    config: Any,
    prom: PrometheusClient,
    rate: int,
    repeat: int,
    warmup_seconds: int,
    steady_seconds: int,
) -> dict[str, Any]:
    _wait_idle(config, prom)
    # Warm up the direct producer and processor, then drain before measurement.
    subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.benchmark.direct_injector",
            "--run-tag",
            f"{config.run_tag}-warmup-{rate}-{repeat}",
            "--rate",
            str(rate),
            "--duration-seconds",
            str(warmup_seconds),
            "--events-topic",
            config.events_topic,
        ],
        check=True,
        env={
            **dict(os.environ),
            "BENCH_KAFKA_BOOTSTRAP": config.kafka_bootstrap_servers,
        },
        stdout=subprocess.DEVNULL,
    )
    _wait_idle(config, prom)
    before_pg = _postgres_snapshot(config)
    before_sql_counts = _sql_statement_counts(prom)
    before_processed = _counter(
        prom, "commerce_processor_events_terminal_total", '{result="processed"}'
    )
    before_errors = _counter(
        prom,
        "commerce_processor_events_terminal_total",
        '{result=~"failed|unresolved|dlq"}',
    )
    before_empty_polls = _counter(prom, "commerce_processor_empty_polls_total")
    wall_start = time.monotonic()
    samples: list[dict[str, Any]] = []
    last_runtime_at = 0.0
    last_runtime: dict[str, dict[str, float | None]] = {}

    def sample() -> None:
        nonlocal last_runtime_at, last_runtime
        now = time.monotonic()
        if now - last_runtime_at >= 5 or not last_runtime:
            last_runtime = _runtime_snapshot(config.compose_project)
            last_runtime_at = now
        samples.append(
            {
                "t": now,
                "lag": _lag(config, prom),
                "inflight": prom.instant("sum(commerce_processor_inflight_events)"),
                "fetchq_records": prom.instant(
                    "sum(commerce_processor_consumer_fetchq_records)"
                ),
                "broker_rtt_ms": prom.instant(
                    "avg(commerce_processor_consumer_broker_rtt_ms)"
                ),
                "runtime": last_runtime,
            }
        )

    injector_output = Path(config.phase_dir()) / f"injector-{rate}-{repeat}.json"
    injector = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "scripts.benchmark.direct_injector",
            "--run-tag",
            f"{config.run_tag}-{rate}-{repeat}",
            "--rate",
            str(rate),
            "--duration-seconds",
            str(steady_seconds),
            "--seed",
            str(derive_seed(config.run_tag, "direct-load", str(rate), str(repeat))),
            "--output",
            str(injector_output),
            "--events-topic",
            config.events_topic,
        ],
        env={
            **dict(os.environ),
            "BENCH_KAFKA_BOOTSTRAP": config.kafka_bootstrap_servers,
        },
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    while injector.poll() is None:
        sample()
        time.sleep(1)
    if injector.returncode != 0:
        raise RuntimeError(f"direct injector failed with code {injector.returncode}")
    injected = json.loads(injector_output.read_text())
    load_end = time.monotonic()
    load_sample_count = len(samples)
    zero_samples = 0
    drain_started = load_end
    while time.monotonic() - drain_started < max(300, steady_seconds * 8):
        current_lag = _lag(config, prom)
        samples.append(
            {
                "t": time.monotonic(),
                "lag": current_lag,
                "inflight": prom.instant("sum(commerce_processor_inflight_events)"),
                "fetchq_records": prom.instant(
                    "sum(commerce_processor_consumer_fetchq_records)"
                ),
                "broker_rtt_ms": prom.instant(
                    "avg(commerce_processor_consumer_broker_rtt_ms)"
                ),
                "runtime": _runtime_snapshot(config.compose_project),
            }
        )
        if current_lag == 0:
            zero_samples += 1
            if zero_samples >= 3:
                break
        else:
            zero_samples = 0
        time.sleep(1)
    drain_time = time.monotonic() - load_end
    after_pg = _postgres_snapshot(config)
    after_sql_counts = _sql_statement_counts(prom)
    sql_call_deltas = {
        operation: after_sql_counts[operation] - before_sql_counts.get(operation, 0.0)
        for operation in SQL_OPERATIONS
    }
    after_processed = _counter(
        prom, "commerce_processor_events_terminal_total", '{result="processed"}'
    )
    after_errors = _counter(
        prom,
        "commerce_processor_events_terminal_total",
        '{result=~"failed|unresolved|dlq"}',
    )
    after_empty_polls = _counter(prom, "commerce_processor_empty_polls_total")
    load_samples = samples[:load_sample_count]
    if len(load_samples) >= 2:
        lag_slope = (load_samples[-1]["lag"] - load_samples[0]["lag"]) / max(
            load_samples[-1]["t"] - load_samples[0]["t"], 0.001
        )
    else:
        lag_slope = None
    arrival_rate = float(injected["actual_published_rate"])
    service_rate = arrival_rate - lag_slope if lag_slope is not None else None
    event_ids = injected.pop("event_ids")
    injected.pop("send_timestamps", None)
    rows = query_all(
        config.postgres_dsn,
        "SELECT event_id::text, "
        "EXTRACT(EPOCH FROM processed_at) * 1000 AS processed_at_ms "
        "FROM processed_events WHERE event_id::text = ANY(%s)",
        (event_ids,),
    )
    end_offsets = topic_watermarks(config.kafka_bootstrap_servers, config.events_topic)
    start_offsets = {
        partition: max(offset - 200_000, 0) for partition, offset in end_offsets.items()
    }
    publish_times = read_publish_timestamps(
        config.kafka_bootstrap_servers,
        config.events_topic,
        start_offsets,
        end_offsets,
        set(event_ids),
        timeout_seconds=max(30, steady_seconds * 2),
    )
    e2e = [
        float(row["processed_at_ms"]) - publish_times[row["event_id"]]
        for row in rows
        if row["event_id"] in publish_times
    ]
    runtime_max: dict[str, dict[str, float | None]] = {}
    for sample_item in samples:
        for service, values in sample_item["runtime"].items():
            service_max = runtime_max.setdefault(
                service, {"cpu_percent": 0.0, "memory_percent": 0.0}
            )
            for field in service_max:
                service_max[field] = max(service_max[field] or 0, values[field] or 0)
    window = max(30, int(time.monotonic() - wall_start) + 20)
    return {
        "rate": rate,
        "repeat": repeat,
        "requested_rate": rate,
        "injected": injected,
        "processed_count": len(rows),
        "processed_counter_delta": after_processed - before_processed,
        "actual_service_rate": service_rate,
        "lag_slope_events_per_second": lag_slope,
        "peak_lag": max((sample_item["lag"] for sample_item in samples), default=0),
        "end_of_load_lag": max(
            (load_samples[-1]["lag"] if load_samples else 0,),
            default=0,
        ),
        "drain_time_seconds": drain_time,
        "e2e_latency_ms": percentiles(e2e) | {"sample_count": len(e2e)},
        "handler_latency_ms": _quantiles(
            prom, "commerce_processor_event_processing_duration_seconds_bucket", window
        ),
        "transaction_latency_ms": _quantiles(
            prom, "commerce_database_transaction_duration_seconds_bucket", window
        ),
        "redis_latency_ms": _quantiles(
            prom, "commerce_processor_redis_duration_seconds_bucket", window
        ),
        "poll_to_handler_ms": _quantiles(
            prom, "commerce_processor_poll_to_handler_duration_seconds_bucket", window
        ),
        "consumer_queue_wait_ms": _quantiles(
            prom,
            "commerce_processor_consumer_queue_wait_duration_seconds_bucket",
            window,
        ),
        "loop_gap_ms": _quantiles(
            prom, "commerce_processor_loop_gap_duration_seconds_bucket", window
        ),
        "poll_duration_ms": _quantiles(
            prom, "commerce_processor_poll_duration_seconds_bucket", window
        ),
        "empty_polls_delta": after_empty_polls - before_empty_polls,
        "max_fetchq_records_sampled": max(
            (
                float(sample_item["fetchq_records"])
                for sample_item in samples
                if sample_item.get("fetchq_records") is not None
            ),
            default=None,
        ),
        "max_broker_rtt_ms_sampled": max(
            (
                float(sample_item["broker_rtt_ms"])
                for sample_item in samples
                if sample_item.get("broker_rtt_ms") is not None
            ),
            default=None,
        ),
        "offset_commit_ms": _quantiles(
            prom, "commerce_processor_offset_commit_duration_seconds_bucket", window
        ),
        "max_inflight_sampled": max(
            (
                float(sample_item["inflight"])
                for sample_item in samples
                if sample_item["inflight"] is not None
            ),
            default=None,
        ),
        "errors_delta": after_errors - before_errors,
        "runtime_max": runtime_max,
        "postgres_rates": {
            key: value for key, value in _postgres_rates(before_pg, after_pg).items()
        },
        "postgres_table_deltas": _postgres_table_deltas(before_pg, after_pg),
        "postgres_bgwriter_delta": _postgres_bgwriter_delta(before_pg, after_pg),
        "postgres_locks_after": after_pg.get("locks", {}),
        "sql_call_deltas": sql_call_deltas,
        "sql_calls_per_processed_event": {
            operation: (delta / len(rows) if rows else None)
            for operation, delta in sql_call_deltas.items()
        },
        "transaction_breakdown_ms": (
            transaction_breakdown := _transaction_breakdown(prom, window)
        ),
        "phase_group_breakdown_ms": phase_group_breakdown(transaction_breakdown),
        "event_type_handler_latency_ms": _event_type_handler_latency(prom, window),
        "correctness": {
            "unique_event_ids": len(set(event_ids)),
            "processed_rows": len(rows),
            "matched_e2e": len(e2e),
        },
        "samples": samples,
    }


def _postgres_rates(before: dict[str, Any], after: dict[str, Any]) -> dict[str, float]:
    seconds = max(after["captured_at"] - before["captured_at"], 0.001)
    result: dict[str, float] = {}
    for section in ("database", "wal"):
        for key, value in after[section].items():
            old = before[section].get(key)
            if isinstance(value, (int, float)) and isinstance(old, (int, float)):
                result[f"{section}.{key}_per_second"] = (value - old) / seconds
    return result


def _postgres_table_deltas(
    before: dict[str, Any], after: dict[str, Any]
) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for table, after_stats in after.get("tables", {}).items():
        before_stats = before.get("tables", {}).get(table, {})
        result[table] = {
            key: int(value) - int(before_stats.get(key) or 0)
            for key, value in after_stats.items()
            if key != "relname" and isinstance(value, (int, float))
        }
    return result


def _postgres_bgwriter_delta(
    before: dict[str, Any], after: dict[str, Any]
) -> dict[str, float]:
    before_bg = before.get("bgwriter", {})
    after_bg = after.get("bgwriter", {})
    return {
        key: float(value) - float(before_bg.get(key) or 0)
        for key, value in after_bg.items()
        if isinstance(value, (int, float))
    }


def rate_artifact_filename(rate: int, prefix: str = "direct-saturation") -> str:
    """Deterministic, rate-scoped artifact filename.

    A shared ``direct-saturation.json`` name let a later single-rate
    invocation under the same ``--run-tag`` silently overwrite an earlier
    rate's complete results (discovered in the Stage 20 PostgreSQL
    saturation diagnosis - see optimization-history.md). Every rate now
    gets its own file so no invocation can destroy another rate's output,
    whether rates are passed together in one ``--rates`` call or across
    several sequential single-rate invocations.
    """
    return f"{prefix}-{rate}.json"


def rate_artifact_path(
    phase_dir: str, rate: int, prefix: str = "direct-saturation"
) -> Path:
    return Path(phase_dir) / rate_artifact_filename(rate, prefix)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--rates", default="300,400,500,600,750,1000")
    parser.add_argument("--warmup-seconds", type=int, default=10)
    parser.add_argument("--steady-seconds", type=int, default=30)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--events-topic", default="commerce.events")
    parser.add_argument("--consumer-group", default="commerce-event-processor-v1")
    args = parser.parse_args()
    configure_logging("WARNING")
    config = load_config(args.run_tag)
    config = dataclasses.replace(
        config,
        events_topic=args.events_topic,
        primary_consumer_group=args.consumer_group,
    )
    prom = PrometheusClient(config.prometheus_url)
    rates = [int(value) for value in args.rates.split(",")]
    results_by_rate: dict[int, list[dict[str, Any]]] = {rate: [] for rate in rates}
    for rate in rates:
        for repeat in range(args.repeats):
            print(f"rate={rate} repeat={repeat} starting", flush=True)
            result = _run_one(
                config, prom, rate, repeat, args.warmup_seconds, args.steady_seconds
            )
            results_by_rate[rate].append(result)
            print(
                json.dumps(
                    {
                        "rate": rate,
                        "repeat": repeat,
                        "injected": result["injected"]["actual_published_rate"],
                        "service": result["actual_service_rate"],
                        "lag_slope": result["lag_slope_events_per_second"],
                        "peak_lag": result["peak_lag"],
                    }
                ),
                flush=True,
            )
    output_dir = Path(config.phase_dir())
    output_dir.mkdir(parents=True, exist_ok=True)
    for rate in rates:
        path = rate_artifact_path(config.phase_dir(), rate)
        path.write_text(
            json.dumps(
                {
                    "run_tag": config.run_tag,
                    "rate": rate,
                    "warmup_seconds": args.warmup_seconds,
                    "steady_seconds": args.steady_seconds,
                    "repeats": args.repeats,
                    "results": results_by_rate[rate],
                },
                indent=2,
            )
        )
        print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
