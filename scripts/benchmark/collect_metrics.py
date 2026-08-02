"""Aggregate all phase JSON artifacts for one run_tag into summary.json,
computing median + range across repeated runs where multiple repeats exist.
"""

import argparse
import glob
import os
import sys
from typing import Any

from scripts.benchmark.artifacts import now_iso, read_json, write_json
from scripts.benchmark.config import load_config
from scripts.benchmark.stats import percentile


def _median(values: list[float]) -> float | None:
    return percentile(values, 0.5)


def _numeric_field_stats(runs: list[dict[str, Any]], path: list[str]) -> dict[str, Any] | None:
    values: list[float] = []
    for run in runs:
        node: Any = run
        for key in path:
            if not isinstance(node, dict) or key not in node:
                node = None
                break
            node = node[key]
        if isinstance(node, int | float):
            values.append(float(node))
    if not values:
        return None
    return {
        "median": _median(values),
        "min": min(values),
        "max": max(values),
        "values": values,
        "n": len(values),
    }


def _load_repeats(phase_dir: str, prefix: str) -> list[dict[str, Any]]:
    paths = sorted(glob.glob(os.path.join(phase_dir, f"{prefix}_run*.json")))
    return [read_json(path) for path in paths]


def collect(run_tag: str) -> dict[str, Any]:
    config = load_config(run_tag)
    phase_dir = config.phase_dir()

    environment = None
    env_path = os.path.join(phase_dir, "environment.json")
    if os.path.exists(env_path):
        environment = read_json(env_path)

    warmup = None
    warmup_path = os.path.join(phase_dir, "warmup.json")
    if os.path.exists(warmup_path):
        warmup = read_json(warmup_path)

    throughput_runs = _load_repeats(phase_dir, "throughput_latency")
    idempotency_runs = _load_repeats(phase_dir, "idempotency")
    retry_runs = _load_repeats(phase_dir, "retry_dlq_retry")
    outbox_runs = _load_repeats(phase_dir, "outbox")

    lag_burst = None
    burst_path = os.path.join(phase_dir, "lag_recovery_burst.json")
    if os.path.exists(burst_path):
        lag_burst = read_json(burst_path)

    lag_outage = None
    outage_path = os.path.join(phase_dir, "lag_recovery_outage.json")
    if os.path.exists(outage_path):
        lag_outage = read_json(outage_path)

    malformed = None
    malformed_path = os.path.join(phase_dir, "retry_dlq_malformed.json")
    if os.path.exists(malformed_path):
        malformed = read_json(malformed_path)

    summary: dict[str, Any] = {
        "run_tag": run_tag,
        "generated_at": now_iso(),
        "environment": environment,
        "warmup": warmup,
        "throughput": {
            "n_runs": len(throughput_runs),
            "average_throughput_events_per_second": _numeric_field_stats(
                throughput_runs, ["average_throughput_events_per_second"]
            ),
            "peak_throughput_events_per_second": _numeric_field_stats(
                throughput_runs, ["peak_throughput_events_per_second"]
            ),
            "total_events_processed": sum(
                run.get("processed_event_count", 0) for run in throughput_runs
            ),
            "raw_runs": throughput_runs,
        },
        "latency": {
            "processor_latency_ms": {
                q: _numeric_field_stats(throughput_runs, ["processor_latency_ms", q])
                for q in ("p50", "p95", "p99")
            },
            "end_to_end_latency_ms": {
                q: _numeric_field_stats(throughput_runs, ["end_to_end_latency_ms", q])
                for q in ("p50", "p95", "p99")
            },
            "api_latency_ms": {
                route: {
                    q: _numeric_field_stats(throughput_runs, ["api_latency_ms", route, q])
                    for q in ("p95", "p99")
                }
                for route in ("/api/v1/runs", "/api/v1/runs/{run_id}")
            },
        },
        "consumer_lag": {
            "burst": lag_burst,
            "outage": lag_outage,
        },
        "idempotency": {
            "n_runs": len(idempotency_runs),
            "total_deliveries": _numeric_field_stats(idempotency_runs, ["total_deliveries"]),
            "unique_event_ids": _numeric_field_stats(idempotency_runs, ["unique_event_ids"]),
            "duplicate_deliveries_implied": _numeric_field_stats(
                idempotency_runs, ["duplicate_deliveries_implied"]
            ),
            "duplicate_durable_side_effects_total": sum(
                run.get("duplicate_durable_side_effects_processed_events", 0)
                + run.get("duplicate_durable_side_effects_entity_tables", 0)
                for run in idempotency_runs
            ),
            "raw_runs": idempotency_runs,
        },
        "retry_dlq": {
            "malformed": malformed,
            "retry": {
                "n_runs": len(retry_runs),
                "retry_attempts_total": _numeric_field_stats(retry_runs, ["retry_attempts_total"]),
                "retry_success_rate": _numeric_field_stats(retry_runs, ["retry_success_rate"]),
                "dlq_rate": _numeric_field_stats(retry_runs, ["dlq_rate"]),
                "raw_runs": retry_runs,
            },
        },
        "outbox": {
            "n_runs": len(outbox_runs),
            "publish_success_rate": _numeric_field_stats(outbox_runs, ["publish_success_rate"]),
            "publish_delay_ms": {
                q: _numeric_field_stats(outbox_runs, ["publish_delay_ms", q])
                for q in ("p50", "p95", "p99")
            },
            "raw_runs": outbox_runs,
        },
    }
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    args = parser.parse_args()
    summary = collect(args.run_tag)
    config = load_config(args.run_tag)
    out_path = os.path.join(config.phase_dir(), "summary.json")
    write_json(out_path, summary)
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
