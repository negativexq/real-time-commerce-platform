"""Isolated Kafka -> processor saturation benchmark without Demo Control API."""

import argparse
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
    before_processed = _counter(
        prom, "commerce_processor_events_terminal_total", '{result="processed"}'
    )
    before_errors = _counter(
        prom,
        "commerce_processor_events_terminal_total",
        '{result=~"failed|unresolved|dlq"}',
    )
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
    after_processed = _counter(
        prom, "commerce_processor_events_terminal_total", '{result="processed"}'
    )
    after_errors = _counter(
        prom,
        "commerce_processor_events_terminal_total",
        '{result=~"failed|unresolved|dlq"}',
    )
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--rates", default="300,400,500,600,750,1000")
    parser.add_argument("--warmup-seconds", type=int, default=10)
    parser.add_argument("--steady-seconds", type=int, default=30)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    configure_logging("WARNING")
    config = load_config(args.run_tag)
    prom = PrometheusClient(config.prometheus_url)
    results: list[dict[str, Any]] = []
    for rate in (int(value) for value in args.rates.split(",")):
        for repeat in range(args.repeats):
            print(f"rate={rate} repeat={repeat} starting", flush=True)
            result = _run_one(
                config, prom, rate, repeat, args.warmup_seconds, args.steady_seconds
            )
            results.append(result)
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
    output = Path(config.phase_dir())
    output.mkdir(parents=True, exist_ok=True)
    (output / "direct-saturation.json").write_text(
        json.dumps(
            {
                "run_tag": config.run_tag,
                "rates": [int(value) for value in args.rates.split(",")],
                "warmup_seconds": args.warmup_seconds,
                "steady_seconds": args.steady_seconds,
                "repeats": args.repeats,
                "results": results,
            },
            indent=2,
        )
    )
    print(f"wrote {output / 'direct-saturation.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
