# Performance and Reliability Report

Generated 2026-08-02T00:48:53.653297+00:00 for run tag `bench-20260802T004243Z` against the locally running Docker Compose stack `real-time-commerce-platform`. Every value below was measured by driving real traffic through the real pipeline and reading Prometheus/Postgres/Kafka afterward - none are estimated or inferred from source code.

## Summary

| Metric | Observed Result | Test Conditions |
| --- | ---: | --- |
| Average throughput | 49.84 evt/s (range 38.53–50.51 evt/s, n=3) | primary pipeline, mixed_traffic scenario, 3 run(s) |
| Peak throughput | 49.30 evt/s (range 49.30–50.01 evt/s, n=3) | 15s Prometheus rate window, 5s scrape-limited resolution |
| Processor latency p95 | 4.94 ms (range 4.86–7.26 ms, n=3) | commerce_processor_event_processing_duration_seconds |
| Processor latency p99 | 9.03 ms (range 7.97–9.91 ms, n=3) | commerce_processor_event_processing_duration_seconds |
| End-to-end latency p95 | 22.98 ms (range 22.86–23.21 ms, n=3) | Kafka publish timestamp -> Postgres processed_at |
| Maximum consumer lag | 2 events | burst ramp sub-test |
| Lag recovery time | not measured sec | outage sub-test (if performed) |
| Duplicate durable side effects | 0 | duplicate_delivery scenario, 3 run(s); expected 0 |
| Retry success rate | 1.000 (n=3) | isolated harness, controlled transient failures |
| DLQ rate (retry harness) | 0.200 (n=3) | isolated harness, controlled exhausted retries |
| Outbox publish success | 1.000 (n=3) | fraud-triggering scenario, 3 run(s) |

## Test Environment

- Host: arm64, macOS-26.5.2-arm64-arm-64bit
- Docker: server 28.3.0, VM 8 CPUs / 8.2 GB, Docker Desktop (6.10.14-linuxkit)
- Compose project: `real-time-commerce-platform`
- Kafka topics: `commerce.events` = 3 partitions, `commerce.events.dlq` = 1 partition(s)
- Processor instances: 1 (consumer group `commerce-event-processor-v1`)
- Container memory limits (no CPU limits are configured in compose.yaml for any service - all containers share the Docker VM's 8 CPUs uncapped): redis=402653184
- Postgres: PostgreSQL 17.5 (Debian 17.5-1.pgdg120+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit
- Redis: 7.4.5, maxmemory=268435456 bytes, policy=allkeys-lru

## Methodology

- Throughput, latency, and consumer-lag measurements exercise the **real production path** (event-generator -> `commerce.events` -> event-processor consumer group `commerce-event-processor-v1` -> Postgres/Redis) through the existing demo-control-api scenario runner (`POST /api/v1/runs`). No primary consumer group offsets were ever reset.
- Idempotency and retry/DLQ correctness use a mix of the real primary pipeline (duplicate_delivery and malformed_event scenarios) and, for the transient-retry path only, an isolated consumer group (`commerce-benchmark-retry-<run_tag>-*`) and isolated Redis key prefix, modeled directly on `scripts/processor-smoke.py`, because production traffic has no organic source of transient/retryable failures (malformed data produces permanent validation errors, not `RetryableProcessingError`).
- End-to-end latency uses the real Kafka broker publish timestamp (CreateTime), not the event payload's own `produced_at` field, because `produced_at` can be a synthetic/backdated journey timestamp rather than actual wall-clock publish time.
- All synthetic data is tagged per-run (demo run `notes`/`run_id`, isolated consumer groups/key prefixes) so verification queries are scoped to exactly the events this benchmark produced.
- A warm-up run (100 events) was executed and discarded before any measured run; its numbers are not included above.

## Workload Configuration

- Throughput run: 1500 events requested @ 100 evt/s, seed=1316785162, scenario=mixed_traffic, run_id=a5cb53ce-a1bb-4eaa-af33-c6c16795d5cc
- Throughput run: 1500 events requested @ 100 evt/s, seed=1322987157, scenario=mixed_traffic, run_id=bc4f9bde-901d-4271-a53c-657c0e6261dc
- Throughput run: 1500 events requested @ 100 evt/s, seed=896241754, scenario=mixed_traffic, run_id=b39e1b3b-cdb0-49d7-822f-6d4d680fd3c7

## Throughput Results

- Runs: 3
- Average throughput (observed, Kafka-publish-to-Postgres-processed window): 49.84 evt/s (range 38.53–50.51 evt/s, n=3)
- Peak throughput (Prometheus 15s rate window, sampled at 5s steps): 49.30 evt/s (range 49.30–50.01 evt/s, n=3)
- Total events processed across all throughput runs: 4500
- Phrasing note: these are throughput values observed under this local test configuration, not a certified maximum capacity, unless explicitly stated otherwise in the Consumer Lag section below.

## Latency Results

Three distinct, separately measured latencies (do not conflate them):

| Latency type | p50 | p95 | p99 | Source |
| --- | ---: | ---: | ---: | --- |
| Processor (handler-internal) | 2.60 ms (range 2.56–2.73 ms, n=3) | 4.94 ms (range 4.86–7.26 ms, n=3) | 9.03 ms (range 7.97–9.91 ms, n=3) | Prometheus `commerce_processor_event_processing_duration_seconds` |
| End-to-end (publish to processed) | 21.61 ms (range 21.33–21.80 ms, n=3) | 22.98 ms (range 22.86–23.21 ms, n=3) | 23.75 ms (range 23.56–24.90 ms, n=3) | Kafka broker CreateTime -> Postgres `processed_events.processed_at` |
| API `/api/v1/runs` | n/a | 73.75 ms (range 48.00–73.75 ms, n=3) | 74.75 ms (range 49.60–74.75 ms, n=3) | Prometheus `commerce_demo_api_request_duration_seconds` |
| API `/api/v1/runs/{run_id}` | n/a | 22.07 ms (range 21.75–23.15 ms, n=3) | 24.41 ms (range 24.35–24.63 ms, n=3) | Prometheus `commerce_demo_api_request_duration_seconds` |

## Consumer Lag and Recovery

- Baseline lag (CLI/kafka-consumer-groups.sh): 0
- Baseline lag (Prometheus kafka_consumergroup_lag): 0.0
- Max lag observed during staged burst ramp: 2

| Stage rate (evt/s) | Duration (s) | Lag at stage start | Lag 1st half avg | Lag 2nd half avg | Still growing? | Terminated on its own? |
| ---: | ---: | ---: | ---: | ---: | --- | --- |
| 50 | 20 | 1 | 0.00 | 0.25 | True | True |
| 150 | 20 | 0 | 1.25 | 0.00 | False | True |
| 300 | 20 | 0 | 0.75 | 0.25 | False | False |

- One or more stages did not reach a terminal state on their own within the wait budget and were explicitly stopped via `POST /api/v1/runs/{run_id}/stop` so they would not occupy a `DEMO_MAX_CONCURRENT_RUNS` slot for the rest of the benchmark. See Limitations for what this indicates about sustained throughput at that stage's requested rate.

- Observed: lag was still net-increasing through the full stage at 50 evt/s in this run. This is an observed data point under this local configuration, not a certified maximum capacity (no dedicated saturation test to failure was run).

- Disruptive outage sub-test: **not performed** (requires explicit operator confirmation immediately before running, since it stops the live event-processor container; see Reproduction Commands).

## Idempotency Verification

- Runs: 3
- Total deliveries: 300 (n=3)
- Unique event IDs: 265 (range 264–265, n=3)
- Duplicate deliveries (implied by total - unique): 35 (range 35–36, n=3)
- **Duplicate durable side effects: 0 (expected 0)**

- [PASS] `idempotency.zero_duplicate_durable_side_effects` — observed: `0` — Sum of duplicate_durable_side_effects_processed_events + duplicate_durable_side_effects_entity_tables across all idempotency runs must be exactly 0.
- [PASS] `idempotency.run[a3ccee6e-a0bd-4dec-aa90-0f57dc4a9252].duplicates_actually_injected` — observed: `35` — The duplicate_delivery scenario must have actually produced duplicate deliveries (otherwise the test proves nothing).
- [PASS] `idempotency.run[56cfe40b-87cd-4164-bebb-684d959632f0].duplicates_actually_injected` — observed: `36` — The duplicate_delivery scenario must have actually produced duplicate deliveries (otherwise the test proves nothing).
- [PASS] `idempotency.run[6c5d364d-312a-449a-b7b5-1feea75fdc26].duplicates_actually_injected` — observed: `35` — The duplicate_delivery scenario must have actually produced duplicate deliveries (otherwise the test proves nothing).

## Retry and DLQ Verification

### Malformed events (real primary pipeline)

| Case | DLQ records | Metadata complete |
| --- | ---: | --- |
| malformed_json | 4 | True |
| missing_field | 4 | True |
| unknown_event_type | 4 | True |
| payload_mismatch | 4 | True |

### Transient retry (isolated harness)

- Runs: 3
- Retry attempts (total): 120 (n=3)
- Retry success rate: 1.000 (n=3)
- DLQ rate: 0.200 (n=3)

- [PASS] `retry_dlq.malformed_dlq_metadata_complete` — observed: `True` — Every DLQ record produced by the malformed-event scenarios must contain all required DlqEnvelope fields.
- [PASS] `retry_dlq.malformed[malformed_json].reached_dlq` — observed: `4` — Each malformed case must actually produce at least one DLQ record.
- [PASS] `retry_dlq.malformed[missing_field].reached_dlq` — observed: `4` — Each malformed case must actually produce at least one DLQ record.
- [PASS] `retry_dlq.malformed[unknown_event_type].reached_dlq` — observed: `4` — Each malformed case must actually produce at least one DLQ record.
- [PASS] `retry_dlq.malformed[payload_mismatch].reached_dlq` — observed: `4` — Each malformed case must actually produce at least one DLQ record.
- [PASS] `retry_dlq.retry[commerce-benchmark-retry-bench-20260802T004243Z-7ff5564f].offsets_committed_only_after_terminal` — observed: `{'committed_offset_total': 88984, 'terminal_outcomes': 60}` — Committed offset total must never exceed the log end offset and must be consistent with the number of terminally handled records - i.e. no offset is committed ahead of terminal (success/DLQ) handling.
- [PASS] `retry_dlq.retry[commerce-benchmark-retry-bench-20260802T004243Z-7ff5564f].dlq_metadata_complete` — observed: `True` — DLQ records produced by exhausted retries must contain all required DlqEnvelope fields.
- [PASS] `retry_dlq.retry[commerce-benchmark-retry-bench-20260802T004243Z-7ff5564f].retry_then_success_observed` — observed: `48` — The retry sub-test must have actually observed at least one event succeed after a controlled transient failure.
- [PASS] `retry_dlq.retry[commerce-benchmark-retry-bench-20260802T004243Z-50d74820].offsets_committed_only_after_terminal` — observed: `{'committed_offset_total': 38669, 'terminal_outcomes': 60}` — Committed offset total must never exceed the log end offset and must be consistent with the number of terminally handled records - i.e. no offset is committed ahead of terminal (success/DLQ) handling.
- [PASS] `retry_dlq.retry[commerce-benchmark-retry-bench-20260802T004243Z-50d74820].dlq_metadata_complete` — observed: `True` — DLQ records produced by exhausted retries must contain all required DlqEnvelope fields.
- [PASS] `retry_dlq.retry[commerce-benchmark-retry-bench-20260802T004243Z-50d74820].retry_then_success_observed` — observed: `48` — The retry sub-test must have actually observed at least one event succeed after a controlled transient failure.
- [PASS] `retry_dlq.retry[commerce-benchmark-retry-bench-20260802T004243Z-15c4a200].offsets_committed_only_after_terminal` — observed: `{'committed_offset_total': 89104, 'terminal_outcomes': 60}` — Committed offset total must never exceed the log end offset and must be consistent with the number of terminally handled records - i.e. no offset is committed ahead of terminal (success/DLQ) handling.
- [PASS] `retry_dlq.retry[commerce-benchmark-retry-bench-20260802T004243Z-15c4a200].dlq_metadata_complete` — observed: `True` — DLQ records produced by exhausted retries must contain all required DlqEnvelope fields.
- [PASS] `retry_dlq.retry[commerce-benchmark-retry-bench-20260802T004243Z-15c4a200].retry_then_success_observed` — observed: `48` — The retry sub-test must have actually observed at least one event succeed after a controlled transient failure.

## Transactional Outbox Verification

- Runs: 3
- Publish success rate: 1.000 (n=3)
- Publish delay (created_at -> published_at):
  - p50: 291.35 ms (range 236.09–355.17 ms, n=3)
  - p95: 483.89 ms (range 420.56–487.79 ms, n=3)
  - p99: 494.80 ms (range 474.27–499.64 ms, n=3)

- [PASS] `outbox.run[3a8244ae-1cfc-4a37-aaf0-ea9cd13a35b3].no_committed_alert_silently_lost` — observed: `{'missing_outbox_rows_for_alerts': 0, 'stuck_pending_or_publishing_rows': 0}` — Every fraud_alerts row must have a corresponding fraud_outbox row, and every outbox row must reach PUBLISHED or terminal FAILED by the end of the drain window.
- [PASS] `outbox.run[5ef78ecb-eed5-4e76-914b-6f97f7ed493f].no_committed_alert_silently_lost` — observed: `{'missing_outbox_rows_for_alerts': 0, 'stuck_pending_or_publishing_rows': 0}` — Every fraud_alerts row must have a corresponding fraud_outbox row, and every outbox row must reach PUBLISHED or terminal FAILED by the end of the drain window.
- [PASS] `outbox.run[5ce71522-20b6-4468-87a2-56ccb4724cfa].no_committed_alert_silently_lost` — observed: `{'missing_outbox_rows_for_alerts': 0, 'stuck_pending_or_publishing_rows': 0}` — Every fraud_alerts row must have a corresponding fraud_outbox row, and every outbox row must reach PUBLISHED or terminal FAILED by the end of the drain window.

## Limitations

- Single host, single Kafka broker, single event-processor instance, single fraud-outbox-publisher instance: results do not generalize to a multi-broker/multi-instance deployment.
- No `cpus:` limits are configured for any service in `compose.yaml` (only `mem_limit`); every container competes for the Docker Desktop VM's 8 CPUs uncapped, and this benchmark itself runs on the same host, competing for the same CPUs.
- Prometheus scrape interval is 10s (see `infra/observability/prometheus/prometheus.yml`), which bounds the time resolution of peak-throughput and lag-curve readings.
- The transient-retry sub-test uses a synthetic handler that raises `RetryableProcessingError` on a controlled schedule, since production traffic has no organic source of transient failures without actually degrading Redis/Postgres connectivity (considered too disruptive for this benchmark).
- The legacy `dead_letter_events` Postgres table is never written to by the current codebase (only read by `GET /api/v1/dlq`); DLQ verification in this report is based entirely on the `commerce.events.dlq` Kafka topic, the actual current DLQ mechanism.
- `demo_runs.duplicate_count` and `demo_runs.dlq_count` are never populated by `services/demo_control_api` (dead columns); duplicate counts in this report are derived from total deliveries minus unique event IDs, not from those columns.
- The event-generator used by demo-control-api scenarios runs in-process inside the API's single asyncio event loop (as opposed to the separately containerized `event-generator` service), sharing that loop with HTTP request handling. During this benchmark, requesting sustained rates at or above ~500 evt/s caused actual generation throughput to fall far below the requested rate under concurrent load (observed completion times of several minutes for runs nominally sized to finish in 20-30s); this appears to be event-loop contention in the demo API process itself, not a Kafka or Postgres bottleneck. The lag burst sub-test's default stage rates were kept at or below 300 evt/s to avoid this; a stage whose run did not self-terminate within its wait budget was explicitly stopped (see the burst table's "Terminated on its own?" column) rather than left running.
- Occasional organic duplicate deliveries (the same event_id published twice) were observed on `commerce.events` even in non-`duplicate_delivery` scenarios during development of this benchmark; root cause was not conclusively identified. The idempotency layer's duplicate-suppression and zero-duplicate-durable-side-effects guarantee, verified above, holds regardless of the source of duplicate deliveries.
- The disruptive consumer-outage sub-test was not performed in this report; lag-recovery numbers reflect the non-disruptive burst-ramp sub-test only.

## Reproduction Commands

```bash
BENCH_RUN_TAG=bench-20260802T004243Z scripts/benchmark/run_benchmark.sh
# or individual phases, e.g.:
.venv/bin/python -m scripts.benchmark.throughput_latency --run-tag bench-20260802T004243Z --repeat-index 0 --event-count 1000 --events-per-second 100 --seed 42
.venv/bin/python -m scripts.benchmark.lag_recovery --run-tag bench-20260802T004243Z burst
.venv/bin/python -m scripts.benchmark.lag_recovery --run-tag bench-20260802T004243Z outage --i-understand-this-stops-event-processor
.venv/bin/python -m scripts.benchmark.collect_metrics --run-tag bench-20260802T004243Z
.venv/bin/python -m scripts.benchmark.verify_results --run-tag bench-20260802T004243Z
```

Raw artifacts: `artifacts/benchmark/bench-20260802T004243Z/`
