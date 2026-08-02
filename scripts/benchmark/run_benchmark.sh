#!/usr/bin/env bash
# Orchestrates the full performance/reliability benchmark against the
# already-running local Docker Compose stack. Reads BENCH_* env vars (see
# scripts/benchmark/config.py) for connection settings; everything else is
# a fixed, documented default below so the run is reproducible.
#
# The disruptive consumer-outage sub-test is NEVER run by this script
# automatically - it stops the live event-processor container and must be
# invoked separately, explicitly, after operator confirmation:
#   scripts/benchmark/run_benchmark.sh --with-outage-test
# only after you have been told in chat exactly what that does and agreed.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

PY=".venv/bin/python"
REPEATS="${BENCH_REPEATS:-3}"
RUN_TAG="${BENCH_RUN_TAG:-bench-$(date -u +%Y%m%dT%H%M%SZ)}"
export BENCH_RUN_TAG="$RUN_TAG"

THROUGHPUT_EVENT_COUNT="${BENCH_THROUGHPUT_EVENT_COUNT:-1500}"
THROUGHPUT_RATE="${BENCH_THROUGHPUT_RATE:-100}"
IDEMPOTENCY_EVENT_COUNT="${BENCH_IDEMPOTENCY_EVENT_COUNT:-300}"
IDEMPOTENCY_RATE="${BENCH_IDEMPOTENCY_RATE:-50}"
RETRY_BATCH_SIZE="${BENCH_RETRY_BATCH_SIZE:-60}"
RETRY_EXHAUSTED_FRACTION="${BENCH_RETRY_EXHAUSTED_FRACTION:-0.2}"
OUTBOX_EVENT_COUNT="${BENCH_OUTBOX_EVENT_COUNT:-150}"
OUTBOX_RATE="${BENCH_OUTBOX_RATE:-30}"
MALFORMED_EVENT_COUNT="${BENCH_MALFORMED_EVENT_COUNT:-40}"
MALFORMED_RATE="${BENCH_MALFORMED_RATE:-20}"
LAG_STAGES="${BENCH_LAG_STAGES:-50:20,150:20,300:20}"

WITH_OUTAGE=false
for arg in "$@"; do
  if [[ "$arg" == "--with-outage-test" ]]; then
    WITH_OUTAGE=true
  fi
done

echo "=== Benchmark run_tag: $RUN_TAG ==="
echo "Artifacts will be written to artifacts/benchmark/$RUN_TAG/"
echo

echo "--- Prerequisite checks ---"
COMPOSE_PROJECT="${BENCH_COMPOSE_PROJECT:-real-time-commerce-platform}"
required_services=(kafka postgres redis event-processor fraud-outbox-publisher demo-control-api prometheus)
running=$(docker compose -p "$COMPOSE_PROJECT" ps --format '{{.Service}} {{.State}}' 2>/dev/null || true)
for svc in "${required_services[@]}"; do
  if ! echo "$running" | grep -q "^${svc} running"; then
    echo "FATAL: required service '$svc' is not running under compose project '$COMPOSE_PROJECT'." >&2
    echo "$running" >&2
    exit 1
  fi
done
echo "All required services are running."

if ! curl -sf "http://127.0.0.1:8082/api/v1/health" > /dev/null; then
  echo "FATAL: demo-control-api health check failed." >&2
  exit 1
fi
if ! curl -sf "http://127.0.0.1:9090/-/healthy" > /dev/null; then
  echo "FATAL: prometheus health check failed." >&2
  exit 1
fi
echo "demo-control-api and prometheus are healthy."
echo

echo "--- Environment capture ---"
"$PY" -m scripts.benchmark.environment --run-tag "$RUN_TAG"
echo

echo "--- Warm-up (discarded, not part of measured results) ---"
"$PY" -m scripts.benchmark.throughput_latency \
  --run-tag "$RUN_TAG" --repeat-index 0 \
  --event-count 100 --events-per-second 20 \
  --output-name warmup
echo

echo "--- Throughput & latency ($REPEATS repeats) ---"
for i in $(seq 0 $((REPEATS - 1))); do
  "$PY" -m scripts.benchmark.throughput_latency \
    --run-tag "$RUN_TAG" --repeat-index "$i" \
    --event-count "$THROUGHPUT_EVENT_COUNT" --events-per-second "$THROUGHPUT_RATE"
done
echo

echo "--- Kafka consumer lag: non-disruptive burst ramp ---"
"$PY" -m scripts.benchmark.lag_recovery --run-tag "$RUN_TAG" burst --stages "$LAG_STAGES"
echo

if [[ "$WITH_OUTAGE" == "true" ]]; then
  echo "--- Kafka consumer lag: DISRUPTIVE outage sub-test (confirmed via --with-outage-test) ---"
  "$PY" -m scripts.benchmark.lag_recovery --run-tag "$RUN_TAG" outage \
    --i-understand-this-stops-event-processor
  echo
else
  echo "--- Skipping disruptive outage sub-test (pass --with-outage-test to include it) ---"
  echo
fi

echo "--- Idempotency ($REPEATS repeats) ---"
for i in $(seq 0 $((REPEATS - 1))); do
  "$PY" -m scripts.benchmark.idempotency_bench \
    --run-tag "$RUN_TAG" --repeat-index "$i" \
    --event-count "$IDEMPOTENCY_EVENT_COUNT" --events-per-second "$IDEMPOTENCY_RATE"
done
echo

echo "--- Retry & DLQ: malformed events (real pipeline, all cases) ---"
"$PY" -m scripts.benchmark.retry_dlq_bench --run-tag "$RUN_TAG" malformed \
  --event-count "$MALFORMED_EVENT_COUNT" --events-per-second "$MALFORMED_RATE"
echo

echo "--- Retry & DLQ: transient retry (isolated harness, $REPEATS repeats) ---"
for i in $(seq 0 $((REPEATS - 1))); do
  "$PY" -m scripts.benchmark.retry_dlq_bench --run-tag "$RUN_TAG" retry \
    --batch-size "$RETRY_BATCH_SIZE" --exhausted-fraction "$RETRY_EXHAUSTED_FRACTION" \
    --repeat-index "$i"
done
echo

echo "--- Transactional outbox ($REPEATS repeats) ---"
for i in $(seq 0 $((REPEATS - 1))); do
  "$PY" -m scripts.benchmark.outbox_bench \
    --run-tag "$RUN_TAG" --repeat-index "$i" \
    --event-count "$OUTBOX_EVENT_COUNT" --events-per-second "$OUTBOX_RATE"
done
echo

echo "--- Collecting and verifying results ---"
"$PY" -m scripts.benchmark.collect_metrics --run-tag "$RUN_TAG"
verify_status=0
"$PY" -m scripts.benchmark.verify_results --run-tag "$RUN_TAG" || verify_status=$?
echo

echo "--- Generating report ---"
"$PY" -m scripts.benchmark.generate_report --run-tag "$RUN_TAG" --out docs/performance-report.md

echo
echo "=== Done: run_tag=$RUN_TAG ==="
echo "Report: docs/performance-report.md"
echo "Raw artifacts: artifacts/benchmark/$RUN_TAG/"
if [[ "$verify_status" != "0" ]]; then
  echo "WARNING: one or more verification checks FAILED - see artifacts/benchmark/$RUN_TAG/verification.json"
fi
exit "$verify_status"
