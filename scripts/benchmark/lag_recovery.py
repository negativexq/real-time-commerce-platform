"""Kafka consumer-group lag and recovery measurement for the real primary
pipeline (consumer group ``commerce-event-processor-v1``).

Two independent sub-tests:

* ``burst``  - non-disruptive: a staged rate ramp through the demo control
  API (load only, no container is touched). Always safe to run.
* ``outage`` - disruptive: stops the running ``event-processor`` container
  for a bounded window while a burst is published, then restarts it and
  measures recovery. This stops a live service and must only be invoked
  after the operator has explicitly confirmed it in chat immediately
  before running it - this module does not prompt on its own since it may
  be invoked non-interactively.
"""

import argparse
import subprocess
import sys
import time
from typing import Any

from scripts.benchmark.artifacts import now_iso, phase_path, write_json
from scripts.benchmark.config import derive_seed, load_config
from scripts.benchmark.demo_api import DemoApiClient
from scripts.benchmark.kafka_lag import total_lag
from scripts.benchmark.prom import PrometheusClient


def _sample_lag(compose_project: str, group: str, prom: PrometheusClient) -> dict[str, Any]:
    cli_lag = total_lag(compose_project, group)
    prom_lag = prom.instant(f'sum(kafka_consumergroup_lag{{consumergroup="{group}"}})')
    return {"t": time.time(), "lag_cli": cli_lag, "lag_prometheus": prom_lag}


def _poll_until(
    compose_project: str,
    group: str,
    prom: PrometheusClient,
    *,
    duration_seconds: float,
    interval_seconds: float = 3.0,
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    deadline = time.monotonic() + duration_seconds
    while time.monotonic() < deadline:
        samples.append(_sample_lag(compose_project, group, prom))
        time.sleep(interval_seconds)
    samples.append(_sample_lag(compose_project, group, prom))
    return samples


def run_burst(config, prom: PrometheusClient, api: DemoApiClient, stages: list[tuple[int, int]]) -> dict[str, Any]:
    """stages: list of (events_per_second, stage_duration_seconds)."""
    group = config.primary_consumer_group
    baseline = _sample_lag(config.compose_project, group, prom)

    stage_results: list[dict[str, Any]] = []
    all_samples: list[dict[str, Any]] = [baseline]
    for rate, duration in stages:
        event_count = max(rate * duration, 1)
        body = {
            "scenario_type": "mixed_traffic",
            "event_count": event_count,
            "events_per_second": rate,
            "seed": derive_seed(config.run_tag, "lag-burst", str(rate)),
            "persona_distribution": {
                "normal": 50,
                "suspicious": 15,
                "bot": 10,
                "account_takeover": 5,
                "discount_hunter": 10,
                "indecisive": 10,
            },
            "notes": f"benchmark:{config.run_tag}:lag-burst:{rate}",
        }
        run_id = api.create_run(body)
        stage_start_lag = _sample_lag(config.compose_project, group, prom)
        samples = _poll_until(
            config.compose_project,
            group,
            prom,
            duration_seconds=duration + 5,
            interval_seconds=3.0,
        )
        all_samples.extend(samples)
        first_half = samples[: len(samples) // 2] or samples
        second_half = samples[len(samples) // 2 :] or samples

        def _avg(values: list[int | None]) -> float | None:
            present = [v for v in values if v is not None]
            return sum(present) / len(present) if present else None

        first_avg = _avg([s["lag_cli"] for s in first_half])
        second_avg = _avg([s["lag_cli"] for s in second_half])
        still_growing = (
            first_avg is not None and second_avg is not None and second_avg > first_avg * 1.1
        )
        stage_terminated = True
        try:
            api.wait_for_terminal(run_id, timeout=max(60.0, duration * 3))
        except TimeoutError:
            # Never leave a stage's run occupying a DEMO_MAX_CONCURRENT_RUNS
            # slot for the rest of the benchmark - explicitly stop it before
            # moving on to the next stage.
            stage_terminated = False
            api.stop_and_wait(run_id, timeout=30.0)
        stage_results.append(
            {
                "requested_events_per_second": rate,
                "stage_duration_seconds": duration,
                "event_count": event_count,
                "run_id": run_id,
                "stage_terminated_on_its_own": stage_terminated,
                "lag_at_stage_start": stage_start_lag["lag_cli"],
                "lag_first_half_avg": first_avg,
                "lag_second_half_avg": second_avg,
                "lag_still_growing_through_stage": still_growing,
            }
        )

    max_lag = max((s["lag_cli"] for s in all_samples if s["lag_cli"] is not None), default=None)
    exceeded_stage = next(
        (s["requested_events_per_second"] for s in stage_results if s["lag_still_growing_through_stage"]),
        None,
    )

    return {
        "sub_test": "burst",
        "disruptive": False,
        "baseline_lag": baseline["lag_cli"],
        "baseline_lag_prometheus": baseline["lag_prometheus"],
        "stages": stage_results,
        "max_lag_observed": max_lag,
        "first_stage_rate_with_growing_lag": exceeded_stage,
        "note": (
            "first_stage_rate_with_growing_lag reports the lowest tested rate "
            "at which lag was still net-increasing through the whole stage in "
            "this run; it is an observed data point under this local "
            "configuration, not a certified maximum capacity."
        ),
        "all_samples": all_samples,
        "captured_at": now_iso(),
    }


def run_outage(config, prom: PrometheusClient, api: DemoApiClient, *, outage_seconds: float, burst_rate: int, burst_count: int) -> dict[str, Any]:
    group = config.primary_consumer_group
    baseline = _sample_lag(config.compose_project, group, prom)

    subprocess.run(
        ["docker", "compose", "-p", config.compose_project, "stop", "event-processor"],
        check=True,
        capture_output=True,
        text=True,
    )
    stopped_at = time.time()

    body = {
        "scenario_type": "mixed_traffic",
        "event_count": burst_count,
        "events_per_second": burst_rate,
        "seed": derive_seed(config.run_tag, "lag-outage"),
        "persona_distribution": {
            "normal": 60,
            "suspicious": 15,
            "bot": 10,
            "account_takeover": 5,
            "discount_hunter": 5,
            "indecisive": 5,
        },
        "notes": f"benchmark:{config.run_tag}:lag-outage",
    }
    run_id = api.create_run(body)

    outage_samples = _poll_until(
        config.compose_project,
        group,
        prom,
        duration_seconds=outage_seconds,
        interval_seconds=3.0,
    )

    subprocess.run(
        ["docker", "compose", "-p", config.compose_project, "start", "event-processor"],
        check=True,
        capture_output=True,
        text=True,
    )
    restarted_at = time.time()

    recovery_samples: list[dict[str, Any]] = []
    deadline = time.monotonic() + 300
    baseline_value = baseline["lag_cli"] or 0
    recovered_at = None
    while time.monotonic() < deadline:
        sample = _sample_lag(config.compose_project, group, prom)
        recovery_samples.append(sample)
        if sample["lag_cli"] is not None and sample["lag_cli"] <= baseline_value + 2:
            recovered_at = sample["t"]
            break
        time.sleep(3.0)

    try:
        api.wait_for_terminal(run_id, timeout=60)
    except TimeoutError:
        pass

    all_samples = [baseline] + outage_samples + recovery_samples
    max_lag = max((s["lag_cli"] for s in all_samples if s["lag_cli"] is not None), default=None)
    max_lag_sample = next((s for s in all_samples if s["lag_cli"] == max_lag), None)
    recovery_time_seconds = (recovered_at - restarted_at) if recovered_at else None
    drain_rate = (
        (max_lag - baseline_value) / recovery_time_seconds
        if max_lag is not None and recovery_time_seconds and recovery_time_seconds > 0
        else None
    )

    return {
        "sub_test": "outage",
        "disruptive": True,
        "baseline_lag": baseline["lag_cli"],
        "outage_window_seconds": restarted_at - stopped_at,
        "burst_events_per_second": burst_rate,
        "burst_event_count": burst_count,
        "run_id": run_id,
        "max_lag_observed": max_lag,
        "max_lag_observed_at": max_lag_sample["t"] if max_lag_sample else None,
        "restarted_at": restarted_at,
        "recovered_at": recovered_at,
        "recovery_time_seconds": recovery_time_seconds,
        "drain_rate_events_per_second": drain_rate,
        "recovered_within_timeout": recovered_at is not None,
        "all_samples": all_samples,
        "captured_at": now_iso(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("sub_test", choices=["burst", "outage"])
    parser.add_argument("--stages", default="50:30,200:30,500:30,900:20")
    parser.add_argument("--outage-seconds", type=float, default=30.0)
    parser.add_argument("--burst-rate", type=int, default=200)
    parser.add_argument("--burst-count", type=int, default=2000)
    parser.add_argument(
        "--i-understand-this-stops-event-processor",
        action="store_true",
        help="Required to actually run the outage sub-test.",
    )
    args = parser.parse_args()

    config = load_config(args.run_tag)
    prom = PrometheusClient(config.prometheus_url)
    api = DemoApiClient(config.demo_api_base_url)

    if args.sub_test == "burst":
        stages = []
        for chunk in args.stages.split(","):
            rate_str, dur_str = chunk.split(":")
            stages.append((int(rate_str), int(dur_str)))
        result = run_burst(config, prom, api, stages)
        out_name = "lag_recovery_burst"
    else:
        if not args.i_understand_this_stops_event_processor:
            print(
                "Refusing to run the outage sub-test without "
                "--i-understand-this-stops-event-processor. This stops the "
                "live event-processor container for a bounded window.",
                file=sys.stderr,
            )
            return 2
        result = run_outage(
            config,
            prom,
            api,
            outage_seconds=args.outage_seconds,
            burst_rate=args.burst_rate,
            burst_count=args.burst_count,
        )
        out_name = "lag_recovery_outage"

    out_path = phase_path(config.phase_dir(), out_name)
    write_json(out_path, result)
    print(f"wrote {out_path}")
    print(f"max_lag={result['max_lag_observed']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
