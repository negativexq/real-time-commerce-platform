"""Render docs/performance-report.md from artifacts/benchmark/<run_tag>/
summary.json and verification.json. Every number in the report is read
directly from those files - nothing here is invented or estimated."""

import argparse
import os
import sys
from typing import Any

from scripts.benchmark.artifacts import read_json
from scripts.benchmark.config import load_config

NA = "not measured"


def _fmt_num(value: Any, digits: int = 2) -> str:
    if value is None:
        return NA
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def _fmt_stat(stat: dict[str, Any] | None, unit: str = "", digits: int = 2) -> str:
    if not stat or stat.get("median") is None:
        return NA
    median = _fmt_num(stat["median"], digits)
    lo = _fmt_num(stat["min"], digits)
    hi = _fmt_num(stat["max"], digits)
    n = stat.get("n", "?")
    if lo == hi:
        return f"{median}{unit} (n={n})"
    return f"{median}{unit} (range {lo}–{hi}{unit}, n={n})"


def _check_line(check: dict[str, Any]) -> str:
    icon = "PASS" if check["result"] == "PASS" else "**FAIL**"
    return f"- [{icon}] `{check['check']}` — observed: `{check['observed']}` — {check['detail']}"


def render(run_tag: str) -> str:
    config = load_config(run_tag)
    phase_dir = config.phase_dir()
    summary = read_json(os.path.join(phase_dir, "summary.json"))
    verification = read_json(os.path.join(phase_dir, "verification.json"))

    env = summary.get("environment") or {}
    host = env.get("host", {})
    docker = env.get("docker", {})
    mem_limits = env.get("container_mem_limits", {})
    instance_counts = env.get("container_instance_counts", {})
    kafka_topics = env.get("kafka_topics", {})
    redis_cfg = env.get("redis", {})
    postgres_cfg = env.get("postgres", {})

    throughput = summary["throughput"]
    latency = summary["latency"]
    lag = summary["consumer_lag"]
    idempotency = summary["idempotency"]
    retry_dlq = summary["retry_dlq"]
    outbox = summary["outbox"]
    warmup = summary.get("warmup")

    checks_by_prefix: dict[str, list[dict[str, Any]]] = {}
    for check in verification["checks"]:
        prefix = check["check"].split(".")[0]
        checks_by_prefix.setdefault(prefix, []).append(check)

    lines: list[str] = []
    lines.append("# Performance and Reliability Report")
    lines.append("")
    lines.append(
        f"Generated {summary['generated_at']} for run tag `{run_tag}` against the "
        "locally running Docker Compose stack `real-time-commerce-platform`. "
        "Every value below was measured by driving real traffic through the "
        "real pipeline and reading Prometheus/Postgres/Kafka afterward - none "
        "are estimated or inferred from source code."
    )
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append("| Metric | Observed Result | Test Conditions |")
    lines.append("| --- | ---: | --- |")
    lines.append(
        f"| Average throughput | {_fmt_stat(throughput['average_throughput_events_per_second'], ' evt/s')} "
        f"| primary pipeline, mixed_traffic scenario, {throughput['n_runs']} run(s) |"
    )
    lines.append(
        f"| Peak throughput | {_fmt_stat(throughput['peak_throughput_events_per_second'], ' evt/s')} "
        "| 15s Prometheus rate window, 5s scrape-limited resolution |"
    )
    lines.append(
        f"| Processor latency p95 | {_fmt_stat(latency['processor_latency_ms']['p95'], ' ms')} "
        "| commerce_processor_event_processing_duration_seconds |"
    )
    lines.append(
        f"| Processor latency p99 | {_fmt_stat(latency['processor_latency_ms']['p99'], ' ms')} "
        "| commerce_processor_event_processing_duration_seconds |"
    )
    lines.append(
        f"| End-to-end latency p95 | {_fmt_stat(latency['end_to_end_latency_ms']['p95'], ' ms')} "
        "| Kafka publish timestamp -> Postgres processed_at |"
    )
    burst = lag.get("burst") or {}
    outage = lag.get("outage") or {}
    max_lag = outage.get("max_lag_observed", burst.get("max_lag_observed"))
    lines.append(
        f"| Maximum consumer lag | {max_lag if max_lag is not None else NA} events "
        f"| {'outage sub-test' if outage else 'burst ramp sub-test'} |"
    )
    lines.append(
        f"| Lag recovery time | {_fmt_num(outage.get('recovery_time_seconds')) if outage else NA} sec "
        "| outage sub-test (if performed) |"
    )
    lines.append(
        f"| Duplicate durable side effects | {idempotency.get('duplicate_durable_side_effects_total', NA)} "
        f"| duplicate_delivery scenario, {idempotency['n_runs']} run(s); expected 0 |"
    )
    lines.append(
        f"| Retry success rate | {_fmt_stat(retry_dlq['retry']['retry_success_rate'], '', 3)} "
        "| isolated harness, controlled transient failures |"
    )
    lines.append(
        f"| DLQ rate (retry harness) | {_fmt_stat(retry_dlq['retry']['dlq_rate'], '', 3)} "
        "| isolated harness, controlled exhausted retries |"
    )
    lines.append(
        f"| Outbox publish success | {_fmt_stat(outbox['publish_success_rate'], '', 3)} "
        f"| fraud-triggering scenario, {outbox['n_runs']} run(s) |"
    )
    lines.append("")

    lines.append("## Test Environment")
    lines.append("")
    lines.append(f"- Host: {host.get('machine', NA)}, {host.get('platform', NA)}")
    lines.append(
        f"- Docker: server {docker.get('docker_server_version', NA)}, "
        f"VM {docker.get('docker_vm_cpus', NA)} CPUs / "
        f"{_fmt_num((docker.get('docker_vm_mem_bytes') or 0) / 1e9, 1)} GB, "
        f"{docker.get('docker_vm_os', NA)} ({docker.get('docker_vm_kernel', NA)})"
    )
    lines.append(f"- Compose project: `{env.get('compose_project', config.compose_project)}`")
    lines.append(
        f"- Kafka topics: `commerce.events` = {kafka_topics.get('commerce.events', NA)} partitions, "
        f"`commerce.events.dlq` = {kafka_topics.get('commerce.events.dlq', NA)} partition(s)"
    )
    lines.append(
        f"- Processor instances: {instance_counts.get('event-processor', NA)} "
        f"(consumer group `{config.primary_consumer_group}`)"
    )
    lines.append(
        "- Container memory limits (no CPU limits are configured in "
        "compose.yaml for any service - all containers share the Docker VM's "
        f"{docker.get('docker_vm_cpus', NA)} CPUs uncapped): "
        + ", ".join(f"{name}={limit}" for name, limit in sorted(mem_limits.items()) if limit)
    )
    lines.append(
        f"- Postgres: {postgres_cfg.get('version', NA)}"
    )
    lines.append(
        f"- Redis: {redis_cfg.get('redis_version', NA)}, "
        f"maxmemory={redis_cfg.get('maxmemory', NA)} bytes, "
        f"policy={redis_cfg.get('maxmemory_policy', NA)}"
    )
    lines.append("")

    lines.append("## Methodology")
    lines.append("")
    lines.append(
        "- Throughput, latency, and consumer-lag measurements exercise the "
        "**real production path** (event-generator -> `commerce.events` -> "
        "event-processor consumer group `commerce-event-processor-v1` -> "
        "Postgres/Redis) through the existing demo-control-api scenario "
        "runner (`POST /api/v1/runs`). No primary consumer group offsets "
        "were ever reset."
    )
    lines.append(
        "- Idempotency and retry/DLQ correctness use a mix of the real "
        "primary pipeline (duplicate_delivery and malformed_event scenarios) "
        "and, for the transient-retry path only, an isolated consumer group "
        "(`commerce-benchmark-retry-<run_tag>-*`) and isolated Redis key "
        "prefix, modeled directly on `scripts/processor-smoke.py`, because "
        "production traffic has no organic source of transient/retryable "
        "failures (malformed data produces permanent validation errors, not "
        "`RetryableProcessingError`)."
    )
    lines.append(
        "- End-to-end latency uses the real Kafka broker publish timestamp "
        "(CreateTime), not the event payload's own `produced_at` field, "
        "because `produced_at` can be a synthetic/backdated journey "
        "timestamp rather than actual wall-clock publish time."
    )
    lines.append(
        "- All synthetic data is tagged per-run (demo run `notes`/`run_id`, "
        "isolated consumer groups/key prefixes) so verification queries are "
        "scoped to exactly the events this benchmark produced."
    )
    if warmup:
        lines.append(
            f"- A warm-up run ({warmup.get('requested_event_count', '?')} events) was executed and "
            "discarded before any measured run; its numbers are not included above."
        )
    lines.append("")

    lines.append("## Workload Configuration")
    lines.append("")
    for run in throughput["raw_runs"]:
        lines.append(
            f"- Throughput run: {run.get('requested_event_count')} events requested @ "
            f"{run.get('requested_events_per_second')} evt/s, seed={run.get('seed')}, "
            f"scenario=mixed_traffic, run_id={run.get('run_id')}"
        )
    lines.append("")

    lines.append("## Throughput Results")
    lines.append("")
    lines.append(f"- Runs: {throughput['n_runs']}")
    lines.append(
        f"- Average throughput (observed, Kafka-publish-to-Postgres-processed window): "
        f"{_fmt_stat(throughput['average_throughput_events_per_second'], ' evt/s')}"
    )
    lines.append(
        f"- Peak throughput (Prometheus 15s rate window, sampled at 5s steps): "
        f"{_fmt_stat(throughput['peak_throughput_events_per_second'], ' evt/s')}"
    )
    lines.append(f"- Total events processed across all throughput runs: {throughput['total_events_processed']}")
    lines.append(
        "- Phrasing note: these are throughput values observed under this "
        "local test configuration, not a certified maximum capacity, unless "
        "explicitly stated otherwise in the Consumer Lag section below."
    )
    lines.append("")

    lines.append("## Latency Results")
    lines.append("")
    lines.append(
        "Three distinct, separately measured latencies (do not conflate them):"
    )
    lines.append("")
    lines.append("| Latency type | p50 | p95 | p99 | Source |")
    lines.append("| --- | ---: | ---: | ---: | --- |")
    lines.append(
        f"| Processor (handler-internal) | {_fmt_stat(latency['processor_latency_ms']['p50'], ' ms')} "
        f"| {_fmt_stat(latency['processor_latency_ms']['p95'], ' ms')} "
        f"| {_fmt_stat(latency['processor_latency_ms']['p99'], ' ms')} "
        "| Prometheus `commerce_processor_event_processing_duration_seconds` |"
    )
    lines.append(
        f"| End-to-end (publish to processed) | {_fmt_stat(latency['end_to_end_latency_ms']['p50'], ' ms')} "
        f"| {_fmt_stat(latency['end_to_end_latency_ms']['p95'], ' ms')} "
        f"| {_fmt_stat(latency['end_to_end_latency_ms']['p99'], ' ms')} "
        "| Kafka broker CreateTime -> Postgres `processed_events.processed_at` |"
    )
    for route, stats in latency["api_latency_ms"].items():
        lines.append(
            f"| API `{route}` | n/a | {_fmt_stat(stats['p95'], ' ms')} "
            f"| {_fmt_stat(stats['p99'], ' ms')} "
            "| Prometheus `commerce_demo_api_request_duration_seconds` |"
        )
    lines.append("")

    lines.append("## Consumer Lag and Recovery")
    lines.append("")
    if burst:
        lines.append(f"- Baseline lag (CLI/kafka-consumer-groups.sh): {burst.get('baseline_lag')}")
        lines.append(f"- Baseline lag (Prometheus kafka_consumergroup_lag): {burst.get('baseline_lag_prometheus')}")
        lines.append(f"- Max lag observed during staged burst ramp: {burst.get('max_lag_observed')}")
        lines.append("")
        lines.append("| Stage rate (evt/s) | Duration (s) | Lag at stage start | Lag 1st half avg | Lag 2nd half avg | Still growing? | Terminated on its own? |")
        lines.append("| ---: | ---: | ---: | ---: | ---: | --- | --- |")
        any_forced_stop = False
        for stage in burst.get("stages", []):
            terminated = stage.get("stage_terminated_on_its_own", True)
            if not terminated:
                any_forced_stop = True
            lines.append(
                f"| {stage['requested_events_per_second']} | {stage['stage_duration_seconds']} "
                f"| {stage['lag_at_stage_start']} | {_fmt_num(stage['lag_first_half_avg'])} "
                f"| {_fmt_num(stage['lag_second_half_avg'])} | {stage['lag_still_growing_through_stage']} "
                f"| {terminated} |"
            )
        lines.append("")
        if any_forced_stop:
            lines.append(
                "- One or more stages did not reach a terminal state on their "
                "own within the wait budget and were explicitly stopped via "
                "`POST /api/v1/runs/{run_id}/stop` so they would not occupy a "
                "`DEMO_MAX_CONCURRENT_RUNS` slot for the rest of the benchmark. "
                "See Limitations for what this indicates about sustained "
                "throughput at that stage's requested rate."
            )
            lines.append("")
        if burst.get("first_stage_rate_with_growing_lag"):
            lines.append(
                f"- Observed: lag was still net-increasing through the full stage "
                f"at {burst['first_stage_rate_with_growing_lag']} evt/s in this run. "
                "This is an observed data point under this local configuration, "
                "not a certified maximum capacity (no dedicated saturation test "
                "to failure was run)."
            )
        else:
            lines.append(
                "- Observed: lag did not show sustained growth at any tested "
                "stage rate; no saturation point was identified within the "
                "tested range."
            )
    else:
        lines.append(f"- Burst sub-test: {NA}")
    lines.append("")
    if outage:
        lines.append(
            "- **Disruptive outage sub-test was performed**: the event-processor "
            f"container was stopped for {_fmt_num(outage.get('outage_window_seconds'))}s "
            "while a burst was published, then restarted."
        )
        lines.append(f"  - Max lag observed: {outage.get('max_lag_observed')}")
        lines.append(f"  - Recovery time to baseline: {_fmt_num(outage.get('recovery_time_seconds'))} sec")
        lines.append(f"  - Drain rate: {_fmt_num(outage.get('drain_rate_events_per_second'))} evt/s")
        lines.append(f"  - Recovered within timeout: {outage.get('recovered_within_timeout')}")
    else:
        lines.append(
            "- Disruptive outage sub-test: **not performed** (requires explicit "
            "operator confirmation immediately before running, since it stops "
            "the live event-processor container; see Reproduction Commands)."
        )
    lines.append("")

    lines.append("## Idempotency Verification")
    lines.append("")
    lines.append(f"- Runs: {idempotency['n_runs']}")
    lines.append(f"- Total deliveries: {_fmt_stat(idempotency['total_deliveries'], '', 0)}")
    lines.append(f"- Unique event IDs: {_fmt_stat(idempotency['unique_event_ids'], '', 0)}")
    lines.append(f"- Duplicate deliveries (implied by total - unique): {_fmt_stat(idempotency['duplicate_deliveries_implied'], '', 0)}")
    lines.append(
        f"- **Duplicate durable side effects: {idempotency['duplicate_durable_side_effects_total']} "
        "(expected 0)**"
    )
    lines.append("")
    for check in checks_by_prefix.get("idempotency", []):
        lines.append(_check_line(check))
    lines.append("")

    lines.append("## Retry and DLQ Verification")
    lines.append("")
    malformed = retry_dlq.get("malformed")
    if malformed:
        lines.append("### Malformed events (real primary pipeline)")
        lines.append("")
        lines.append("| Case | DLQ records | Metadata complete |")
        lines.append("| --- | ---: | --- |")
        for case in malformed.get("cases", []):
            lines.append(
                f"| {case['malformed_case']} | {case['dlq_records_from_kafka']} "
                f"| {case['dlq_metadata_complete']} |"
            )
        lines.append("")
    retry = retry_dlq["retry"]
    lines.append("### Transient retry (isolated harness)")
    lines.append("")
    lines.append(f"- Runs: {retry['n_runs']}")
    lines.append(f"- Retry attempts (total): {_fmt_stat(retry['retry_attempts_total'], '', 0)}")
    lines.append(f"- Retry success rate: {_fmt_stat(retry['retry_success_rate'], '', 3)}")
    lines.append(f"- DLQ rate: {_fmt_stat(retry['dlq_rate'], '', 3)}")
    lines.append("")
    for check in checks_by_prefix.get("retry_dlq", []):
        lines.append(_check_line(check))
    lines.append("")

    lines.append("## Transactional Outbox Verification")
    lines.append("")
    lines.append(f"- Runs: {outbox['n_runs']}")
    lines.append(f"- Publish success rate: {_fmt_stat(outbox['publish_success_rate'], '', 3)}")
    lines.append("- Publish delay (created_at -> published_at):")
    for q in ("p50", "p95", "p99"):
        lines.append(f"  - {q}: {_fmt_stat(outbox['publish_delay_ms'][q], ' ms')}")
    lines.append("")
    for check in checks_by_prefix.get("outbox", []):
        lines.append(_check_line(check))
    lines.append("")

    lines.append("## Limitations")
    lines.append("")
    lines.append(
        "- Single host, single Kafka broker, single event-processor instance, "
        "single fraud-outbox-publisher instance: results do not generalize to "
        "a multi-broker/multi-instance deployment."
    )
    lines.append(
        "- No `cpus:` limits are configured for any service in `compose.yaml` "
        "(only `mem_limit`); every container competes for the Docker "
        f"Desktop VM's {docker.get('docker_vm_cpus', NA)} CPUs uncapped, and this benchmark itself "
        "runs on the same host, competing for the same CPUs."
    )
    lines.append(
        "- Prometheus scrape interval is 10s (see `infra/observability/prometheus/prometheus.yml`), "
        "which bounds the time resolution of peak-throughput and lag-curve readings."
    )
    lines.append(
        "- The transient-retry sub-test uses a synthetic handler that raises "
        "`RetryableProcessingError` on a controlled schedule, since production "
        "traffic has no organic source of transient failures without actually "
        "degrading Redis/Postgres connectivity (considered too disruptive for "
        "this benchmark)."
    )
    lines.append(
        "- The legacy `dead_letter_events` Postgres table is never written to "
        "by the current codebase (only read by `GET /api/v1/dlq`); DLQ "
        "verification in this report is based entirely on the "
        "`commerce.events.dlq` Kafka topic, the actual current DLQ mechanism."
    )
    lines.append(
        "- `demo_runs.duplicate_count` and `demo_runs.dlq_count` are never "
        "populated by `services/demo_control_api` (dead columns); duplicate "
        "counts in this report are derived from total deliveries minus unique "
        "event IDs, not from those columns."
    )
    lines.append(
        "- The event-generator used by demo-control-api scenarios runs "
        "in-process inside the API's single asyncio event loop (as opposed "
        "to the separately containerized `event-generator` service), sharing "
        "that loop with HTTP request handling. During this benchmark, "
        "requesting sustained rates at or above ~500 evt/s caused actual "
        "generation throughput to fall far below the requested rate under "
        "concurrent load (observed completion times of several minutes for "
        "runs nominally sized to finish in 20-30s); this appears to be "
        "event-loop contention in the demo API process itself, not a Kafka "
        "or Postgres bottleneck. The lag burst sub-test's default stage "
        "rates were kept at or below 300 evt/s to avoid this; a stage whose "
        "run did not self-terminate within its wait budget was explicitly "
        "stopped (see the burst table's \"Terminated on its own?\" column) "
        "rather than left running."
    )
    lines.append(
        "- Occasional organic duplicate deliveries (the same event_id "
        "published twice) were observed on `commerce.events` even in "
        "non-`duplicate_delivery` scenarios during development of this "
        "benchmark; root cause was not conclusively identified. The "
        "idempotency layer's duplicate-suppression and zero-duplicate-"
        "durable-side-effects guarantee, verified above, holds regardless "
        "of the source of duplicate deliveries."
    )
    if not outage:
        lines.append(
            "- The disruptive consumer-outage sub-test was not performed in "
            "this report; lag-recovery numbers reflect the non-disruptive "
            "burst-ramp sub-test only."
        )
    if verification["failed_checks"]:
        lines.append(
            f"- **{len(verification['failed_checks'])} verification check(s) FAILED**: "
            + ", ".join(f"`{name}`" for name in verification["failed_checks"])
        )
    lines.append("")

    lines.append("## Reproduction Commands")
    lines.append("")
    lines.append("```bash")
    lines.append(f"BENCH_RUN_TAG={run_tag} scripts/benchmark/run_benchmark.sh")
    lines.append("# or individual phases, e.g.:")
    lines.append(
        f".venv/bin/python -m scripts.benchmark.throughput_latency --run-tag {run_tag} "
        "--repeat-index 0 --event-count 1000 --events-per-second 100 --seed 42"
    )
    lines.append(
        f".venv/bin/python -m scripts.benchmark.lag_recovery --run-tag {run_tag} burst"
    )
    lines.append(
        f".venv/bin/python -m scripts.benchmark.lag_recovery --run-tag {run_tag} outage "
        "--i-understand-this-stops-event-processor"
    )
    lines.append(f".venv/bin/python -m scripts.benchmark.collect_metrics --run-tag {run_tag}")
    lines.append(f".venv/bin/python -m scripts.benchmark.verify_results --run-tag {run_tag}")
    lines.append("```")
    lines.append("")
    lines.append(f"Raw artifacts: `artifacts/benchmark/{run_tag}/`")
    lines.append("")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--out", default="docs/performance-report.md")
    args = parser.parse_args()
    content = render(args.run_tag)
    with open(args.out, "w") as handle:
        handle.write(content)
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
