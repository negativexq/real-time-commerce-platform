"""Throughput and three-way latency measurement through the real primary
pipeline (event-generator -> commerce.events -> event-processor consumer
group ``commerce-event-processor-v1`` -> Postgres/Redis), driven through the
existing demo control API scenario runner.

Latency is reported as three distinct, separately labeled measurements:
  * processor latency   - commerce_processor_event_processing_duration_seconds
                           (handler-internal timing, Prometheus histogram)
  * end-to-end latency   - processed_at (Postgres) minus the real Kafka
                           broker publish timestamp (CreateTime), NOT the
                           payload's synthetic ``produced_at`` field (which
                           can be a backdated/simulated journey timestamp),
                           scoped to this run's events via
                           demo_run_event_manifest
  * API latency          - commerce_demo_api_request_duration_seconds for
                           the run-creation/status endpoints actually used
"""

import argparse
import sys
import time
from datetime import datetime
from typing import Any

from scripts.benchmark.artifacts import now_iso, phase_path, write_json
from scripts.benchmark.config import derive_seed, load_config
from scripts.benchmark.demo_api import DemoApiClient
from scripts.benchmark.kafka_replay import read_publish_timestamps, topic_watermarks
from scripts.benchmark.pg import query_all
from scripts.benchmark.prom import PrometheusClient
from scripts.benchmark.stats import percentiles


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def run_once(
    config,
    prom: PrometheusClient,
    api: DemoApiClient,
    *,
    event_count: int,
    events_per_second: int,
    seed: int,
    repeat_index: int,
) -> dict[str, Any]:
    body = {
        "scenario_type": "mixed_traffic",
        "event_count": event_count,
        "events_per_second": events_per_second,
        "seed": seed,
        "persona_distribution": {
            "normal": 50,
            "suspicious": 15,
            "bot": 10,
            "account_takeover": 5,
            "discount_hunter": 10,
            "indecisive": 10,
        },
        "notes": f"benchmark:{config.run_tag}:throughput:{repeat_index}",
    }
    # Subtract a safety margin from the captured start offsets: occasional
    # organic duplicate publishes were observed on this topic even outside
    # the explicit duplicate_delivery scenario (see docs/performance-report.md
    # Limitations), and a duplicate's *earlier* copy landing at an offset
    # just before our exact watermark snapshot must not be excluded from the
    # scan window, or read_publish_timestamps would incorrectly anchor on a
    # later copy and produce a negative (impossible) latency.
    raw_start_offsets = topic_watermarks(config.kafka_bootstrap_servers, config.events_topic)
    start_offsets = {p: max(o - 500, 0) for p, o in raw_start_offsets.items()}
    wall_start = time.monotonic()
    wall_start_unix = time.time()
    result = api.run_scenario(
        body, timeout=max(120.0, event_count / max(events_per_second, 1) * 4 + 60)
    )
    wall_elapsed = time.monotonic() - wall_start
    end_offsets = topic_watermarks(config.kafka_bootstrap_servers, config.events_topic)

    started = _parse_ts(result.started_at)
    completed = _parse_ts(result.completed_at)
    summary_duration = result.summary.get("duration_seconds")
    if summary_duration:
        run_duration_seconds = float(summary_duration)
    elif started and completed:
        run_duration_seconds = (completed - started).total_seconds()
    else:
        run_duration_seconds = wall_elapsed
    run_duration_seconds = max(run_duration_seconds, 1.0)

    processed = int(result.summary.get("postgres_committed_events", 0))
    avg_throughput = processed / run_duration_seconds

    end_ts = time.time()
    start_ts = wall_start_unix - 5
    peak_throughput = None
    rate_series = prom.range(
        'sum(rate(commerce_processor_events_terminal_total{result="processed"}[15s]))',
        start_ts,
        end_ts + 5,
        step="5s",
    )
    if rate_series:
        peak_throughput = max(value for _, value in rate_series)

    # Rate/quantile windows must cover the full wall-clock time since the
    # test began (not just the reported run duration), including whatever
    # time has already passed while this function was doing its own I/O, so
    # the histogram_quantile() rate() window still contains all the samples.
    window = max(int(round(time.monotonic() - wall_start)) + 15, 30)
    processor_latency_ms = {
        "p50": _seconds_to_ms(
            prom.quantile(
                "commerce_processor_event_processing_duration_seconds_bucket",
                0.50,
                window,
            )
        ),
        "p95": _seconds_to_ms(
            prom.quantile(
                "commerce_processor_event_processing_duration_seconds_bucket",
                0.95,
                window,
            )
        ),
        "p99": _seconds_to_ms(
            prom.quantile(
                "commerce_processor_event_processing_duration_seconds_bucket",
                0.99,
                window,
            )
        ),
    }

    api_window = max(int(round(time.monotonic() - wall_start)) + 15, 30)
    api_latency_ms = {}
    for route in ("/api/v1/runs", "/api/v1/runs/{run_id}"):
        api_latency_ms[route] = {
            "p95": _api_quantile(prom, route, 0.95, api_window),
            "p99": _api_quantile(prom, route, 0.99, api_window),
        }

    processed_rows = query_all(
        config.postgres_dsn,
        """
        SELECT m.event_id::text AS event_id,
               EXTRACT(EPOCH FROM pe.processed_at) * 1000 AS processed_at_ms
        FROM demo_run_event_manifest m
        JOIN processed_events pe ON pe.event_id = m.event_id
        WHERE m.run_id = %s
        """,
        (result.run_id,),
    )
    wanted_ids = {row["event_id"] for row in processed_rows}
    publish_ts = read_publish_timestamps(
        config.kafka_bootstrap_servers,
        config.events_topic,
        start_offsets,
        end_offsets,
        wanted_ids,
        timeout_seconds=max(30.0, run_duration_seconds),
    )
    e2e_latencies_ms = [
        float(row["processed_at_ms"]) - publish_ts[row["event_id"]]
        for row in processed_rows
        if row["event_id"] in publish_ts
    ]
    e2e_stats = percentiles(e2e_latencies_ms)
    e2e_stats["sample_count"] = len(e2e_latencies_ms)
    e2e_stats["matched_of_processed"] = f"{len(e2e_latencies_ms)}/{len(processed_rows)}"

    # The demo-control-api's own reported run duration includes an internal
    # bounded completion-polling loop (services/demo_control_api/services/
    # scenario_runner.py::_wait_for_processing, capped at 60s) and can
    # therefore over-state actual data-plane time. The observed processing
    # window below is derived directly from real Kafka publish timestamps
    # and Postgres processed_at timestamps for this run's events, and is
    # used as the primary throughput denominator; the API-reported duration
    # is kept alongside for transparency.
    observed_window_seconds = None
    observed_throughput = None
    if publish_ts and processed_rows:
        first_publish_ms = min(publish_ts.values())
        last_processed_ms = max(float(row["processed_at_ms"]) for row in processed_rows)
        observed_window_seconds = max((last_processed_ms - first_publish_ms) / 1000.0, 0.001)
        observed_throughput = len(processed_rows) / observed_window_seconds

    return {
        "run_id": result.run_id,
        "status": result.status,
        "requested_event_count": event_count,
        "requested_events_per_second": events_per_second,
        "seed": seed,
        "api_reported_duration_seconds": run_duration_seconds,
        "observed_processing_window_seconds": observed_window_seconds,
        "processed_event_count": processed,
        "average_throughput_events_per_second": observed_throughput or avg_throughput,
        "average_throughput_method": (
            "observed_kafka_publish_to_postgres_processed_window"
            if observed_throughput
            else "api_reported_duration_fallback"
        ),
        "peak_throughput_events_per_second": peak_throughput,
        "processor_latency_ms": processor_latency_ms,
        "end_to_end_latency_ms": e2e_stats,
        "api_latency_ms": api_latency_ms,
        "captured_at": now_iso(),
    }


def _seconds_to_ms(value: float | None) -> float | None:
    return None if value is None else value * 1000.0


def _api_quantile(
    prom: PrometheusClient, route: str, quantile: float, window: int
) -> float | None:
    query = (
        f"histogram_quantile({quantile}, sum(rate("
        f'commerce_demo_api_request_duration_seconds_bucket{{route_template="{route}"}}'
        f"[{window}s])) by (le))"
    )
    value = prom.instant(query)
    return _seconds_to_ms(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--repeat-index", type=int, required=True)
    parser.add_argument("--event-count", type=int, default=1000)
    parser.add_argument("--events-per-second", type=int, default=100)
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Defaults to a run_tag-derived seed to avoid colliding with "
        "event_ids from earlier sessions (see config.derive_seed).",
    )
    parser.add_argument(
        "--output-name",
        default=None,
        help="Artifact base name; defaults to throughput_latency_run<repeat-index>. "
        "Pass 'warmup' for a discarded warm-up run.",
    )
    args = parser.parse_args()

    config = load_config(args.run_tag)
    prom = PrometheusClient(config.prometheus_url)
    api = DemoApiClient(config.demo_api_base_url)

    seed = (
        args.seed
        if args.seed is not None
        else derive_seed(config.run_tag, "throughput", str(args.repeat_index))
    )
    result = run_once(
        config,
        prom,
        api,
        event_count=args.event_count,
        events_per_second=args.events_per_second,
        seed=seed,
        repeat_index=args.repeat_index,
    )
    out_path = phase_path(
        config.phase_dir(), args.output_name or f"throughput_latency_run{args.repeat_index}"
    )
    write_json(out_path, result)
    print(f"wrote {out_path}")
    print(
        f"avg={result['average_throughput_events_per_second']:.2f} evt/s "
        f"peak={result['peak_throughput_events_per_second']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
