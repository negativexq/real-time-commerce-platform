# Performance Optimization History

This is the decision log for the performance work. Each entry records the
hypothesis, measurement, change, and decision. Failed experiments are kept on
purpose.

## Per-event progress refresh

- **Hypothesis:** synchronous `repository.refresh()` in the generation hot
  path blocks the event loop and limits generation throughput.
- **Measurement:** the original 100 evt/s request produced approximately
  49.84 evt/s; refresh was executed per event.
- **Change:** remove per-event refresh, refresh periodically, and perform a
  final refresh after generation. The remaining synchronous refresh was later
  moved off the event loop with a coalescing updater and `asyncio.to_thread()`.
- **Result:** generation improved to approximately 77–80 evt/s, but did not
  yet reach 100 evt/s.
- **Decision:** keep the change; it removed a confirmed hot-path cost without
  changing status semantics.

## Fixed-rate pacing

- **Hypothesis:** `work(); sleep(1/rate)` adds work time to every interval.
- **Measurement:** approximately 82 evt/s at a requested 100 evt/s.
- **Change:** monotonic deadline-based pacing with `loop.time()`.
- **Result:** approximately 97–99 evt/s generated in the final 100 evt/s
  benchmark.
- **Decision:** keep the change; it corrected pacing drift without changing
  event semantics.

## Transaction decomposition

- **Hypothesis:** PostgreSQL is the bottleneck, but the expensive operation
  must be identified before changing SQL or configuration.
- **Measurement:** approximately 10–11 statements/event; payment-history
  SELECTs were roughly 6–7 ms each. Pool acquire and commit/WAL were not the
  dominant costs.
- **Change:** low-overhead aggregate instrumentation for transaction stages.
- **Result:** payment-history lookups were the highest-cost SQL class.
- **Decision:** use the evidence to select one index experiment.

## Combined payment query — rejected

- **Hypothesis:** one combined recent/prior payment query reduces round trips
  and increases throughput.
- **Measurement:** combined query ~14.1–15.3 ms versus original two-query
  total ~13.7–13.9 ms. At 200 evt/s, processed throughput fell ~94.5 →
  ~82.6 evt/s and E2E p95 rose ~15.3 → ~21.5 s.
- **Change:** two payment queries were temporarily combined under `UNION ALL`.
- **Result:** steady-state system performance worsened despite a plausible
  micro-level plan.
- **Decision:** reverted. Fewer round trips did not produce a faster system.

## Payments composite index — kept

- **Hypothesis:** the payments `Seq Scan` dominates recent/prior lookup cost.
- **Measurement:** `payments(customer_id, attempted_at DESC)` changed the
  observed plan to bitmap index/heap access. Recent lookup fell 10.897 →
  0.253 ms; prior lookup 7.143 → 0.100 ms.
- **Change:** add `database/migrations/005_payments_customer_attempted_at.sql`.
- **Result:** transaction average fell ~6.9 → ~1.7–1.8 ms; at 200 evt/s,
  processed throughput rose ~94.5 → ~145.7 evt/s, peak lag fell ~2057 →
  ~3–7, and E2E p95 fell ~15.3 s → ~28 ms.
- **Decision:** keep the index. It is the strongest confirmed system-level
  optimization in the study.

## Direct injector

- **Hypothesis:** the Demo Control API generator ceiling masks processor
  capacity.
- **Measurement:** the persisted Demo sweep at 400 requested reports
  approximately 275.6 generated evt/s median; an earlier ~291 evt/s operator
  observation has no matching retained JSON. Persisted injector artifacts
  reach approximately 889.4/s at 900 requested. A 975.7/s result at 1000 was
  recorded during standalone validation but its JSON was not retained.
- **Change:** add benchmark-only `direct_injector.py` and
  `direct_saturation.py`.
- **Result:** processor capacity could be measured independently.
- **Decision:** keep as a benchmark tool, not as an application path.

## Horizontal consumer scaling

- **Hypothesis:** serial processing capacity increases with multiple consumers
  in the same Kafka group.
- **Measurement:** one worker sustainably handled ~500 evt/s; three workers
  with one partition each sustainably handled ~750 evt/s. Two workers had a
  2/1 partition assignment and improved throughput non-linearly.
- **Change:** controlled container-count changes only; no processing refactor.
- **Result:** scaling helped, but the 3-worker transition was 750–775 evt/s.
- **Decision:** close the performance study with the documented ceiling; no
  further optimization was applied. (Superseded below: this ceiling was a
  property of per-event synchronous offset commits, not of horizontal
  scaling itself.)

## Bounded batched offset commits — kept

- **Hypothesis:** `KafkaEventConsumer.commit_terminal()` issued one
  synchronous `commit(asynchronous=False)` Kafka round trip per terminal
  record. At the documented 3-worker/3-partition boundary (~750 evt/s
  sustainable, 775 non-sustainable in all three repeats, 800 unstable), this
  per-event commit overhead was a plausible contributor to the ceiling.
  Batching commits into bounded windows (by record count or elapsed time,
  whichever comes first) should reduce commit-call frequency without
  weakening at-least-once delivery, since an offset is only ever considered
  safe to commit once every record before it in that partition has reached a
  terminal state.
- **Change:** added `services/event_processor/offset_tracker.py`
  (`OffsetCommitTracker`), a per-partition contiguous-offset accumulator
  that flushes a single batched `commit()` call when
  `PROCESSOR_OFFSET_COMMIT_BATCH_SIZE` (default 50) terminal records have
  accumulated, or `PROCESSOR_OFFSET_COMMIT_INTERVAL_MS` (default 100ms) has
  elapsed since the last flush, whichever happens first. The commit call
  itself stayed synchronous (`asynchronous=False`) - only its frequency
  changed - so a commit failure is still detected immediately and no pending
  state is silently discarded. Partition revocation synchronously flushes
  only the revoked partitions before ownership is relinquished (partitions
  not being revoked are unaffected and keep accumulating normally), and
  graceful shutdown synchronously flushes every remaining partition before
  the consumer closes. A companion fix (`KafkaEventConsumer.maybe_flush_idle()`,
  called from the main poll loop whenever `poll()` returns no message) makes
  the interval threshold fire on wall-clock time rather than only when a new
  terminal record arrives - without it, a partial batch below the batch-size
  threshold at the end of a bounded run would never get committed once
  traffic went idle, which was caught by this experiment's own benchmark
  smoke test hanging indefinitely in `_wait_idle()`, not by a unit test.
  Twelve new unit tests cover contiguous-gap enforcement, both threshold
  triggers, multi-partition independence, revoke/shutdown flush scoping,
  commit-failure state preservation, duplicate-mark idempotence, and the
  idle-flush fix itself.
- **Measurement:** isolated 3-worker/3-partition direct-injector sweep at
  750/775/800 evt/s, 10s warmup, 45s steady state, 3 repeats each (same
  methodology as the horizontal-scaling boundary sweep), artifact
  [`bench-batched-commit-3w-boundary`](../../artifacts/benchmark/bench-batched-commit-3w-boundary/).

  | Rate | Lag slope (3 repeats) | E2E p95 (3 repeats) | Correctness |
  | ---: | --- | --- | --- |
  | 750 | +0.74, +0.70, +2.03/s | 83.9, 64.8, 67.8 ms | unique = processed = matched in all 3 |
  | 775 | +1.08, +0.87, +1.24/s | 60.6, 61.6, 52.5 ms | unique = processed = matched in all 3 |
  | 800 | +1.44, +0.71, +2.03/s | 83.8, 66.2, 88.5 ms | unique = processed = matched in all 3 |

  All nine repeats across 750/775/800 evt/s showed a near-zero lag slope
  (≤+2.03/s) and drained within 14.6–17.9s, versus the prior baseline's 775
  evt/s repeats at +52.9/+86.8/+93.4 events/s (clearly non-sustainable) and
  800 evt/s's unstable mix of two near-zero and one +47.4 events/s repeat.
  E2E p95 stayed in the 52–89ms range at every tested rate, compared to the
  baseline 3-worker 750 evt/s figure of ~954ms. Handler latency (~4.8ms p95)
  and per-call offset-commit latency (~4.75–4.79ms p95) were essentially
  unchanged, confirming the improvement came from calling `commit()` far
  less often, not from any single call getting faster: during the sweep,
  Prometheus recorded 125,669 terminal events against 4,385 actual Kafka
  commit calls (4,194 interval-triggered, 191 batch-size-triggered) - a
  ~28.6x reduction in commit round trips. Every run's correctness triad
  (`unique_event_ids == processed_rows == matched_e2e`) held exactly, so no
  duplicate durable side effects or lost events were introduced.
- **Result:** the 750 evt/s point was already near-sustainable in the
  baseline and stayed about the same. The 775 and 800 evt/s points, which
  were non-sustainable or unstable in the baseline, became clearly
  sustainable with batched commits. The true new ceiling was not
  determined - this experiment only re-tested the three previously
  documented boundary rates, per the assigned scope.
- **Decision:** keep the change. `PROCESSOR_OFFSET_COMMIT_BATCH_SIZE` and
  `PROCESSOR_OFFSET_COMMIT_INTERVAL_MS` are validated, documented
  environment variables so the batching window can be tuned or (at
  batch_size=1, interval_ms=1) made to approximate the old per-event
  behavior without a code change. The crash-replay window grew from
  "at most one event" to "at most `PROCESSOR_OFFSET_COMMIT_BATCH_SIZE`
  events or `PROCESSOR_OFFSET_COMMIT_INTERVAL_MS` of wall-clock time,
  whichever is smaller" per partition; this does not weaken durable business
  idempotency (Postgres uniqueness plus the Redis reservation lease still
  make replayed events no-ops), it only changes how many already-processed
  events a crash can cause to be redelivered.

## Post-batching capacity discovery — measurement only, no code change

- **Hypothesis:** the batched-offset-commit experiment above only re-tested
  the three previously-documented boundary rates (750/775/800 evt/s); the
  true new sustainable ceiling was unknown and likely higher.
- **Change:** none. This was a pure capacity-discovery benchmark: same 3
  worker / 3 partition / 1/1/1-assignment topology (verified via
  `kafka-consumer-groups.sh --describe` immediately before each sweep, all
  three consumer IDs distinct, lag 0 at rest), same batched-commit settings,
  same PostgreSQL/Redis/Kafka/Docker configuration, same workload. Only the
  requested injection rate varied.
- **Measurement:** broad sweep at 850/900/950/1000 evt/s (10s warmup, 45s
  steady state, 3 repeats,
  [`bench-batched-commit-3w-ceiling-broad`](../../artifacts/benchmark/bench-batched-commit-3w-ceiling-broad/)),
  followed by a refinement rate at 925 evt/s (same methodology,
  [`bench-batched-commit-3w-ceiling-refinement`](../../artifacts/benchmark/bench-batched-commit-3w-ceiling-refinement/)).

  | Rate | Lag slope (3 repeats, events/s) | E2E p95 (3 repeats) | Classification |
  | ---: | --- | --- | --- |
  | 850 | +1.59, +1.79, +3.88 | 73, 82, 125 ms | Sustainable |
  | 900 | +1.07, +1.26, +1.77 | 152, 136, 138 ms | Sustainable (highest clean point) |
  | 925 | +2.71, +1.28, +1.15 | 321, 1027, 1092 ms | Mixed/transition band - slope alone looks fine but E2E p95 spikes above 1000ms in 2/3 repeats |
  | 950 | +1.74, +6.22, +9.42 | 175, 368, 343 ms | Not clearly sustainable - slope and end-of-load lag escalate repeat-over-repeat |
  | 1000 | +11.48, +16.27, +6.09 | 356, 784, 548 ms | Non-sustainable - service rate visibly trails injected, E2E p99 up to 1210ms |

  Injector-capacity check: injected rate stayed ≥99.5% of requested through
  950 evt/s and ≥99% at 1000 evt/s (990-993 of 1000), both above this
  project's 95% floor, so saturation at 1000 evt/s is attributed to the
  processor/database path, not to the benchmark injector falling behind.
  Correctness: all 15 repeats (12 broad + 3 refinement) satisfied
  `unique_event_ids == processed_rows == matched_e2e` exactly - no duplicate
  durable side effects, no lost events, at every tested rate including the
  saturated ones. Resource observations: PostgreSQL container CPU rose
  ~76% (900) → ~71-95% (950 repeats) → ~103% (1000); WAL full-page-image
  writes rose ~4/s (900) → ~33/s (950) → ~342/s (1000), roughly an 85x jump
  from 900 to 1000. Processor worker CPU stayed moderate throughout (36-65%
  peak across the three workers) and never approached saturation. Kafka
  broker CPU samples were low and did not trend upward with rate.
- **Result:** the highest artifact-backed sustainable rate is **900 evt/s**
  (up from 750 evt/s, a ~20% increase: `(900-750)/750×100 = 20%`).
  Repeatable saturation begins in the **900-950 evt/s** band; 925 evt/s
  inside that band showed mixed evidence (bounded lag slope, but E2E p95
  latency spikes far above the 850/900 range), which is why it is reported
  as part of the transition rather than as a new clean ceiling. 1000 evt/s
  is unambiguously saturated.
- **Decision:** N/A - measurement only, nothing to keep or revert. The
  measured evidence points to **PostgreSQL as the strongest next-bottleneck
  hypothesis** (rising CPU and WAL full-page-image rate while processor CPU
  stays moderate and Kafka CPU does not trend upward), consistent with the
  payment-history read cost identified in the Transaction decomposition
  entry above, but this is a hypothesis from aggregate resource
  measurements, not a proven root cause - a fresh transaction-level
  instrumentation pass (mirroring the earlier Transaction decomposition
  experiment) would be the natural next isolated experiment, followed by
  targeted PostgreSQL investigation if that measurement confirms it.

## Successful-event log moved to DEBUG — kept (operational, not a throughput win)

- **Hypothesis:** `MessageProcessor.process()` emitted a structured `INFO`
  log (`event_processed`, with 13 fields) for every successfully processed
  record. At ~900-950 evt/s across 3 workers this is several hundred
  structured log calls per second per worker; the cost of argument
  construction, structlog processing, serialization, and the stdout/stderr
  write could be measurably consuming processor CPU even if it is not the
  primary system bottleneck.
- **Change:** exactly one line in `services/event_processor/processor.py`:
  `self._logger.info("event_processed", ...)` → `self._logger.debug(...)`.
  No fields changed, no log removed, no sampling added, no async logging
  introduced, no structlog/Docker log-driver configuration touched, no
  global `processor_log_level` default changed. Before making the change,
  `duplicate_event_skipped` (also `INFO`, in the same file) was checked and
  confirmed to be a *different* code path (duplicate-skip, not normal
  success) that fires far less often under normal load - it was left
  unchanged, as were all `WARNING`-level retry/DLQ/integrity logs and the
  `INFO`-level startup/shutdown/assignment/revocation logs.
- **Measurement:** same 3-worker/3-partition/1/1/1-assignment topology
  (re-verified before each sweep), same batched-offset-commit defaults, same
  Postgres/Redis/Kafka/Docker configuration, same workload. Two back-to-back
  sweeps at 900/925/950 evt/s (10s warmup, 45s steady state, 3 repeats each,
  same rates the capacity-discovery stage used):
  a fresh `INFO` control run
  ([`bench-info-baseline-control-3w-boundary`](../../artifacts/benchmark/bench-info-baseline-control-3w-boundary/),
  pre-change image) followed by the `DEBUG` experiment
  ([`bench-debug-success-log-3w-boundary`](../../artifacts/benchmark/bench-debug-success-log-3w-boundary/),
  rebuilt image, containers recreated so log volume could be measured from a
  clean window).

  | Metric | INFO control (9 repeats) | DEBUG experiment (9 repeats) | Delta |
  | --- | --- | --- | --- |
  | `event_processed` lines in the measured window | 456,181 | 0 | Eliminated |
  | stdout+stderr bytes in the measured window (3 workers, `docker logs --since/--until`) | ~254.1 MB | ~7.0 KB | ~36,000x fewer bytes |
  | Mean processor CPU (3 workers, `docker stats` peak samples) | ~53.4% | ~51.5% | ~2 points lower - within normal run-to-run noise |
  | Handler/processing p95 | 4.87-4.89 ms | 4.75-4.94 ms | No material change |
  | PostgreSQL transaction p95 | 4.81-4.83 ms | 4.85-4.86 ms | No material change |
  | PostgreSQL CPU (peak samples) | ~62-87% (mean ~76%) | ~60-118% (mean ~82%, one 117.6% outlier) | No reduction; noise-dominated, not attributable to the log change |
  | Lag slope range across the 18 repeats | +1.33 to +15.93/s | +1.35 to +7.30/s | Both conditions show occasional single-repeat spikes at every rate; no consistent pattern tied to the log level |
  | E2E p95 range | 270-1541 ms | 504-1373 ms | Noisy in both directions; no systematic improvement |
  | Correctness (`unique_event_ids == processed_rows == matched_e2e`) | held in all 9 repeats | held in all 9 repeats | Zero business-semantic effect from the log-level change |

  Both sweeps showed one unusually high-lag-slope repeat scattered across
  different rates/repeats (INFO: 900-rep2 at +15.93/s, 925-rep0 at +11.32/s,
  950-rep2 at +14.80/s; DEBUG: 900-rep0 at +7.30/s only), consistent with
  general run-to-run system variance (this benchmark was run back-to-back
  after roughly 40 minutes of continuous heavy load on a MacBook Air host)
  rather than a pattern caused by the logging change. Processor smoke tests
  (`processor-smoke`, `-duplicate-smoke`, `-dlq-smoke`, `-retry-smoke`) and
  the full pytest/ruff/mypy suite all passed unchanged.
- **Result: Outcome C (negligible-to-marginal performance difference).**
  Stdout/log volume dropped essentially to zero for the changed log line,
  which is a real and substantial operational reduction. Processor CPU
  dropped modestly (~2 points) but not decisively outside normal
  measurement noise. Handler latency, PostgreSQL latency, and PostgreSQL CPU
  showed no material change. The 900/925/950 sustainability classification
  did **not** change: 900 evt/s remained mostly clean with an occasional
  single-repeat spike in both conditions, 925 evt/s remained a mixed/
  transition-band result in both conditions, and 950 evt/s remained
  borderline in both conditions (its DEBUG sweep happened to run cleaner
  this particular time, but with only 3 repeats per condition this is not
  strong enough evidence to claim the boundary moved). This strengthens,
  rather than replaces, the PostgreSQL bottleneck hypothesis: removing the
  per-event INFO log did not move PostgreSQL CPU or transaction latency at
  all, so the earlier Postgres CPU/WAL evidence from the capacity-discovery
  stage is not attributable to processor-side logging overhead.
- **Decision: kept.** Not because it changed throughput - it did not - but
  because Prometheus already provides equivalent successful-event
  observability (`commerce_processor_events_terminal_total`,
  `commerce_processor_event_processing_duration_seconds`, etc.) without the
  per-event log line, the same event-level diagnostic remains available on
  demand by running with `PROCESSOR_LOG_LEVEL=DEBUG`, and eliminating
  ~254 MB of routine stdout per 9-repeat benchmark window (extrapolating:
  roughly 28 KB per 1,000 events at these field widths) is an operationally
  meaningful reduction in log-shipping/storage cost regardless of its
  (here, negligible) effect on throughput.

## Transaction decomposition v2 — diagnosis only, no change made

This stage answers a narrower question than the entries above: **which part
of the already-instrumented transaction grows expensive as load approaches
the 900-950 evt/s boundary identified by the ceiling-discovery and logging
stages?** No SQL, index, schema, Kafka, Redis, worker/partition, offset
batching, logging, pool, PostgreSQL, fraud-rule, producer, or container
configuration was changed. This is attribution, not optimization.

- **Why this run:** the ceiling-discovery stage (Stage 14) and the logging
  stage (Stage 15) both found PostgreSQL CPU and WAL full-page-image (FPI)
  growth near 950-1000 evt/s but stopped short of naming which query class or
  transaction stage was responsible. Sprint 8's fraud stage adds up to 10
  extra PostgreSQL round trips per fraud-eligible event inside the same
  commit boundary as the business write, so it was the leading suspect.
- **Topology:** 3 `commerce.events` partitions, 3 processor workers,
  re-verified 1/1/1 assignment via `kafka-consumer-groups.sh --describe`
  immediately before the sweep. Same batched offset commit defaults
  (`PROCESSOR_OFFSET_COMMIT_BATCH_SIZE=50` / `..._INTERVAL_MS=100`), same
  `DEBUG`-level success logging, same mixed workload, same Docker CPU/memory
  allocation as every prior stage.
- **Instrumentation added:** none in application code. Sprint 8/9 already
  wrap every PostgreSQL call in `InstrumentedConnection`/`InstrumentedCursor`
  ([`persistence/instrumentation.py`](../../services/event_processor/persistence/instrumentation.py))
  and record per-stage histograms in
  [`persistence/unit_of_work.py`](../../services/event_processor/persistence/unit_of_work.py)
  (`commerce_database_stage_duration_seconds{stage}` for pool acquire,
  `processed_events`, `business_persistence`, `fraud_context`,
  `fraud_persistence`, `commit`, connection release, plus
  `commerce_database_sql_duration_seconds{operation}` with the same bounded,
  static `SQL_OPERATIONS` labels used by the v1 decomposition — never event,
  customer, order, run ID, or raw SQL text). All of this instrumentation is
  already active in production whenever `metrics` is passed to
  `UnitOfWorkFactory`, which `main.py` always does. This stage only extended
  the **benchmark tooling** (`scripts/benchmark/saturation.py`,
  `scripts/benchmark/direct_saturation.py`) to read these existing
  Prometheus series before/after each load window and to snapshot
  `pg_stat_database`, `pg_stat_user_tables`, `pg_stat_wal`,
  `pg_stat_checkpointer`, `pg_stat_activity`, and `pg_locks` — no new
  application code path was added, so there is no new per-event application
  overhead to isolate; the added cost is a handful of read-only Prometheus/
  PostgreSQL queries executed once before and once after each load window,
  never during it.
- **`pg_stat_statements` limitation:** not enabled in this environment
  (confirmed via `pg_extension`/`shared_preload_libraries`), and enabling it
  requires a PostgreSQL restart, which the task rules for this stage
  explicitly forbid. All SQL-class evidence below therefore comes from the
  application-side `commerce_database_sql_duration_seconds{operation}`
  histograms (already bounded/labeled by the v1 instrumentation) and from
  always-available system views, not from `pg_stat_statements`.
- **Benchmark:** direct Kafka injection (not the Demo API, which cannot
  sustain 900+ evt/s cleanly — see the direct-injector stage) at 900, 925,
  950 evt/s; 10s warmup, 45s steady state, 3 repeats per rate; artifact tag
  [`bench-tx-decomposition-v2-3w-boundary`](../../artifacts/benchmark/bench-tx-decomposition-v2-3w-boundary/).
  Commands:

  ```bash
  docker compose -p real-time-commerce-platform exec -T kafka \
    /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
    --describe --group commerce-event-processor-v1
  docker compose --profile processor --profile fraud --profile observability \
    --profile demo up -d --scale event-processor=3 event-processor
  python -m scripts.benchmark.direct_saturation --rates 900 925 950 \
    --warmup-seconds 10 --steady-seconds 45 --repeats 3 \
    --run-tag bench-tx-decomposition-v2-3w-boundary
  ```

- **Instrumentation-overhead note:** because no new application code path
  was added (only additional before/after Prometheus reads in the benchmark
  driver), there is no isolated "instrumented vs. uninstrumented" processor
  build to compare in this stage — the running image is identical to Stage
  15's `DEBUG` image. The one confound worth naming plainly: this sweep ran
  after many hours of continuous benchmarking in the same session, so
  `customers`/`orders`/`payments`/`product_views`/`sessions` hold far more
  rows than Stage 15's snapshot did, against a reused, finite synthetic
  customer pool. That inflates absolute lag-slope and E2E-p95 numbers versus
  Stage 15 (see the table below) and is a real limitation on any *absolute*
  before/after comparison — but it does not undermine the *relative*,
  same-sweep findings below (which stage/query class costs more than which
  other, and which grows from 900→950), since those are measured within the
  same data state.

  | Metric (900/925/950, 3 repeats each) | Stage 15 DEBUG sweep | This sweep |
  | --- | --- | --- |
  | Lag slope range | +1.35 to +7.30 evt/s | +1.37 to +42.27 evt/s |
  | E2E p95 range | 504-1373 ms | 385-4057 ms |
  | PostgreSQL CPU (peak samples) | ~60-118% (mean ~82%) | ~79-111% (mean ~93%) |
  | Correctness | held in all 9 repeats | held in all 9 repeats |

  The wider lag-slope/E2E spread is consistent with the accumulated-data-
  volume confound above (worst outliers correlate with the EXPLAIN-confirmed
  non-composite-indexed queries below, whose cost scales with per-customer
  row count), not with anything the instrumentation itself does.

- **Stage-level breakdown (mean of 9 repeats, ms, share of transaction
  total):**

  | Stage | Avg (ms) | Share of transaction total |
  | --- | --- | --- |
  | `fraud_context` (10 SELECTs) | 1.055 | ~49% |
  | `business_persistence` | 0.443 | ~21% |
  | `fraud_persistence` | 0.411 | ~19% |
  | `commit` | 0.333 | ~16% |
  | `processed_events_insert` | 0.151 | ~7% |
  | `pool_acquire` | 0.018 | <1% |
  | `connection_release` | 0.013 | <1% |
  | **transaction_total** | **2.147** | 100% |
  | `fraud_evaluation_cpu` (Python, not a DB stage) | 0.046 | n/a |

  By rate, `transaction_total` and `fraud_context` avg (mean of 3 repeats):

  | Rate | transaction_total avg (ms) | fraud_context avg (ms) | fraud_context share |
  | --- | --- | --- | --- |
  | 900 evt/s | 3.413 | 1.564 | ~46% |
  | 925 evt/s | 1.511 | 0.701 | ~46% |
  | 950 evt/s | 1.517 | 0.900 | ~59% |

  `fraud_context` is the largest single DB-side stage at every rate and its
  *share* of the transaction grows from 900→950 even where the absolute
  transaction cost itself is noisy across repeats (a symptom of the queued/
  saturated regime this boundary sits in, where per-event cost depends on
  how much the pool and buffer cache are already loaded when that event
  runs). `pool_acquire` and `connection_release` stay negligible (≤0.04 ms)
  at every rate, and `fraud_evaluation_cpu` (pure Python rule evaluation, no
  DB call) stays ≤0.09 ms at every rate.

- **SQL-class breakdown (mean of 9 repeats, ms, calls/processed event):**

  | Operation | Avg (ms) | Calls/event |
  | --- | --- | --- |
  | `business_payments` | 0.286 | 0.051 |
  | `fraud_evaluation_write` | 0.265 | 0.154 |
  | `processed_events_insert` | 0.151 | 0.358 |
  | `fraud_context_recent_payments` | 0.126 | 0.154 |
  | `fraud_context_product_views` | 0.108 | 0.154 |
  | `fraud_context_prior_payments` | 0.107 | 0.154 |
  | `fraud_evaluation_select` | 0.103 | 0.154 |
  | `processed_events_select` | 0.100 | 0.716 |
  | `fraud_context_recent_orders` | 0.091 | 0.154 |
  | `fraud_context_refund_facts` | 0.090 | 0.051 |
  | `fraud_context_order` | 0.083 | 0.154 |
  | `fraud_context_refunds` | 0.077 | 0.154 |
  | `fraud_context_session` | 0.076 | 0.154 |
  | `fraud_context_order_session` | 0.068 | 0.051 |
  | `fraud_context_customer` | 0.067 | 0.154 |

  Calls/event are per *processed* event (fraud-context queries only fire for
  the ~15% fraud-eligible share of the mixed workload, so their true
  per-fraud-eligible-event cost is roughly 6-7x the listed calls/event
  average). No single fraud-context query dominates — the cost is spread
  across all ~10 bounded lookups, which matches the "share grows, no single
  query explodes" pattern seen in the stage-level table.

- **PostgreSQL resource evidence:** `pg_stat_user_tables` deltas across all
  9 repeats show **zero sequential scans** on any of the 12 tracked tables
  (`orders`, `product_views`, `payments`, `customers`, etc. — all activity is
  `idx_scan`), ruling out a seq-scan regression. Buffer cache hit ratio
  (`blks_hit / (blks_hit + blks_read)`) stayed above 97.5% at every rate,
  ruling out cache-eviction-driven I/O growth. PostgreSQL container CPU
  ranged 79.2-110.7% (i.e. consistently using more than one core) across all
  9 repeats with no clean monotonic trend by rate — the highest single value
  (110.7%) occurred at 900 evt/s, not 950, reinforcing that the system is
  already resource-constrained at the low end of this boundary, not just at
  its top.
- **WAL / write-amplification evidence:** `wal.wal_fpi_per_second` ranged
  83.9-786.4 across the 9 repeats with no clean correlation to rate or to
  lag slope (e.g. 950-rep0 had the highest FPI rate, 786.4/s, but the
  *lowest* lag slope of the three 950 repeats, +1.37 evt/s). This pattern -
  large swings uncorrelated with load - is consistent with PostgreSQL
  checkpoint timing (a checkpoint landing inside the sampling window forces
  a burst of full-page images regardless of event rate) rather than with
  fraud-context or business-write volume. Write amplification is **not**
  ruled out as a contributor to the previously observed 950-1000 FPI growth,
  but this stage's evidence does not tie it specifically to transaction
  volume at 900-950; a checkpoint-interval-aware measurement would be needed
  to separate the two.
- **`EXPLAIN (ANALYZE, BUFFERS)` findings (read-only, representative
  customer with 91 payments / ~190 orders, no index changes made):** the
  `fraud_context_recent_payments`/`prior_payments`/`refunds` queries use the
  composite `(customer_id, attempted_at)` index kept from the earlier
  "Composite index" stage and resolve via `Index Cond` in 0.19-1.7 ms.
  `fraud_context_recent_orders` (`orders`) and `fraud_context_product_views`
  (`product_views`) have **no equivalent composite index** — both resolve
  via a customer_id-only `Bitmap Heap Scan` with the date-range bound applied
  as a post-scan `Filter`, not an `Index Cond`, costing 9.2 ms and 4.0 ms
  respectively against the same representative customer, 4-20x the
  composite-indexed queries despite firing at the identical per-fraud-
  eligible-event frequency. This cost scales with each customer's
  accumulated row count, which is consistent with this sweep's elevated
  outliers (see the data-volume confound note above) and is the strongest
  single piece of evidence produced by this stage.
- **Event-type cost differences (mean of 9 repeats,
  `commerce_processor_event_processing_duration_seconds{event_type}`):**
  fraud-eligible event types (`checkout_started`, `order_created`,
  `payment_failed`, `payment_completed`, `refund_requested`) consistently
  cost 2-5x more handler time than non-fraud-eligible types
  (`user_registered`, `session_started`, `product_viewed`,
  `added_to_cart`) in every one of the 9 repeats — e.g. 900-rep1: 8.47 ms vs
  3.23 ms; 925-rep1: 7.94 ms vs 0.78 ms. This directly ties per-event cost to
  workload mix: a run with a higher fraud-eligible share will show worse
  aggregate latency purely from mix, independent of any query-level
  regression.
- **Proven bottleneck:** the fraud-context stage (10 bounded SELECTs
  executed inside the same transaction as the business write, before commit)
  is the largest single database-side cost component of a fraud-eligible
  transaction at every measured rate, and its share of transaction time
  grows from ~46% at 900 evt/s to ~59% at 950 evt/s. Within that stage,
  `fraud_context_recent_orders` and `fraud_context_product_views` are proven
  (via `EXPLAIN ANALYZE BUFFERS`) to run as `Filter`-based Bitmap Heap Scans
  rather than `Index Cond` scans, 4-20x slower per call than the
  composite-indexed payment/refund lookups in the same stage, and their cost
  scales with accumulated per-customer row count.
- **Strong hypotheses (not proven by this stage alone):** (1) the missing
  `orders`/`product_views` composite indexes are a material contributor to
  the elevated lag-slope outliers seen in this sweep versus Stage 15,
  because the outlier repeats correlate with cumulative data growth rather
  than with rate; a controlled reset-and-rerun sweep would be needed to
  isolate data volume from rate as the driver. (2) PostgreSQL CPU
  (consistently >1 core, 79-111%) is the proximate saturation resource at
  this boundary, given buffer cache hit ratio stays high and there are no
  sequential scans — but this stage does not isolate which specific
  operation(s) consume that CPU beyond the fraud-context evidence above.
- **Ruled out:** connection pool contention (`pool_acquire` ≤0.018 ms mean,
  `connection_release` ≤0.013 ms mean, at every rate) and fraud rule
  evaluation CPU (`fraud_evaluation_cpu`, pure Python, ≤0.09 ms mean at
  every rate) are not meaningful contributors to the cost growth near this
  boundary. Sequential-scan regression and buffer-cache eviction are also
  ruled out (zero seq scans, >97.5% hit ratio at every rate in all 9
  repeats). Kafka-side lag growth is a *symptom* measured by this stage, not
  a cause investigated by it — no Kafka broker/producer resource evidence
  was collected here (out of scope for a PostgreSQL transaction
  decomposition), so Kafka is neither implicated nor ruled out.
- **Ranked candidate next isolated optimization experiments (not
  implemented in this stage):**
  1. **Add a composite `(customer_id, ordered_at)` index on `orders` and
     `(customer_id, viewed_at)` (or equivalent timestamp column) on
     `product_views`**, mirroring the existing `payments` composite index.
     *Evidence:* EXPLAIN-proven `Filter`-based scans costing 4-20x the
     composite-indexed equivalent queries in the same stage, at identical
     call frequency. *Mechanism:* converts the date-range bound from a
     post-scan filter to an index condition, the same mechanism that fixed
     the `payments` lookups in the earlier "Composite index" stage.
     *Correctness risk:* low — read-only query path, same pattern already
     validated in production for `payments`; index write-side cost (insert/
     update amplification, WAL) was previously measured for the payments
     index and should be re-measured here rather than assumed identical.
     *Expected impact:* should reduce `fraud_context` stage cost and,
     because it's ~half of transaction time, may materially reduce
     transaction latency and the fraud-eligible/non-fraud latency gap.
     *Validation benchmark:* repeat this same 900/925/950 x 3 transaction
     decomposition after the index change, comparing `fraud_context_recent_orders`/
     `fraud_context_product_views` avg/p95 and stage share directly against
     this stage's numbers (same data volume, ideally via the reset-and-rerun
     control below run first).
  2. **Reset-and-rerun control sweep** on truncated benchmark tables (scoped
     synthetic data only) at the same 900/925/950 x 3 topology, to separate
     the data-volume confound from the rate-driven signal before or in
     parallel with experiment 1. *Evidence:* this stage's lag-slope/E2E
     spread versus Stage 15 correlates with hours of accumulated benchmark
     history, not with a documented pipeline change since Stage 15.
     *Mechanism:* removing the confound isolates whether 925-950 evt/s is a
     stable boundary or whether it has drifted downward purely from data
     growth. *Correctness risk:* none to production code; only scoped
     benchmark/synthetic rows are affected, and this must run only when the
     stack is idle (never mid-sweep). *Expected impact:* clarifies whether
     experiment 1 is solving today's real bottleneck or a bottleneck that
     is partly an artifact of long-running benchmark data accumulation.
     *Validation benchmark:* the same transaction-decomposition sweep run
     immediately after a scoped truncate, tagged separately (e.g.
     `bench-tx-decomposition-v2-reset-control`) so it is never mixed with
     this stage's artifact.
  3. **Checkpoint-interval-aware WAL/FPI measurement** (e.g. sampling
     `pg_stat_checkpointer` deltas alongside WAL FPI rate within each
     repeat, not just before/after) to separate checkpoint-driven FPI bursts
     from load-driven WAL growth. *Evidence:* this stage's WAL FPI rate
     swung 83.9-786.4/s with no clean rate correlation, plausibly explained
     by checkpoint timing rather than by the fraud/business write volume
     this stage set out to attribute. *Mechanism:* pure measurement change,
     no risk. *Expected impact:* clarifies whether the ceiling-discovery
     stage's 950-1000 FPI growth finding is transaction-volume-driven (and
     therefore addressable by reducing per-transaction write cost) or
     checkpoint-interval-driven (addressable only by checkpoint tuning,
     which is out of scope for this task's constraints). *Validation
     benchmark:* an extended sweep with per-repeat checkpoint-delta sampling
     at the same 900/925/950 rates.

## Orders composite index — kept

- **Hypothesis:** Transaction decomposition v2 proved (via `EXPLAIN ANALYZE
  BUFFERS`) that the `fraud_context_recent_orders` query - the bounded
  history count backing `FraudContext.recent_orders` - runs as a
  `customer_id`-only `Bitmap Heap Scan` with the `ordered_at` range applied
  as a post-scan `Filter`, unlike the already-indexed `payments` lookups in
  the same stage. The exact hot query, read from
  [`services/event_processor/fraud/context.py`](../../services/event_processor/fraud/context.py):

  ```sql
  SELECT COUNT(*) FROM (
      SELECT 1 FROM orders
      WHERE customer_id = %s AND ordered_at <= %s AND ordered_at >= %s
      ORDER BY ordered_at DESC LIMIT %s
  ) bounded
  ```

  One equality predicate (`customer_id`), one range predicate
  (`ordered_at` bounded both sides by the fraud lookback window), `ORDER BY
  ordered_at DESC`, a bounded `LIMIT`, no join. Following the
  equality-then-range/order principle already used for
  `idx_payments_customer_attempted_at`, the justified column order is
  `(customer_id, ordered_at DESC)` - equality column first, then the
  range/sort column in the query's own sort direction. `orders` had only
  `idx_orders_customer_id` (customer_id only); this is the single missing
  piece relative to the payments case.
- **Baseline EXPLAIN** (representative customer, 84 orders, fresh controlled
  data - see below): `Bitmap Heap Scan on orders` via
  `idx_orders_customer_id`, `Recheck Cond` on `customer_id`, `Filter` on the
  `ordered_at` range, followed by a `Sort` (quicksort) and `Limit`. 39 shared
  buffer hits, 0.195 ms execution time. Full plan retained at
  [`artifacts/benchmark/bench-orders-index-explain/baseline-orders-query-explain.txt`](../../artifacts/benchmark/bench-orders-index-explain/baseline-orders-query-explain.txt).
- **Change:** exactly one migration,
  [`database/migrations/006_orders_customer_ordered_at.sql`](../../database/migrations/006_orders_customer_ordered_at.sql):
  `CREATE INDEX idx_orders_customer_ordered_at ON orders (customer_id,
  ordered_at DESC)`. No existing index removed, no query rewritten, no other
  schema/config/worker/partition/Kafka/Redis/logging/pool change.
- **Indexed EXPLAIN** (same query shape, a comparable representative
  customer with 88 orders from the post-index controlled dataset):
  `Index Only Scan using idx_orders_customer_ordered_at`, `Index Cond`
  covers all three predicates, `Heap Fetches: 0`, no `Sort` step (the index
  already returns rows in `ordered_at DESC` order). 5 shared buffer hits,
  0.073 ms execution time. Full plan retained at
  [`artifacts/benchmark/bench-orders-index-explain/indexed-orders-query-explain.txt`](../../artifacts/benchmark/bench-orders-index-explain/indexed-orders-query-explain.txt).
  Execution time improved ~2.7x; buffer hits fell ~7.8x; the heap access and
  sort step were both eliminated entirely.
- **Controlled reset methodology:** the live database had accumulated over
  2.5M `processed_events` rows and ~357K orders across many hours of prior
  benchmarking in this session - far beyond any realistic per-customer
  history and a direct confound for a clean A/B. Before the baseline sweep,
  all benchmark-populated tables (`customers`, `sessions`, `product_views`,
  `carts`, `cart_items`, `orders`, `payments`, `refunds`,
  `fraud_evaluations`, `fraud_alerts`, `fraud_outbox`, `processed_events`)
  were fully truncated via a new `scripts/reset-benchmark-data.sql` (no
  existing full-reset tool covered this; the existing
  `reset-persistence-test-data.sql` only scopes by a `persistence-smoke:`
  run-tag, which the direct-injector benchmark path does not use).
  `schema_migrations` and unrelated tables (`demo_runs`,
  `demo_run_event_manifest`) were left untouched. The single-variable
  control came from *migration ordering*, not a worktree: the baseline
  sweep ran to completion while migration 006 did not yet exist on disk (so
  `postgres-migrate` - which runs at every stack start via `depends_on` -
  had nothing new to apply), then the benchmark tables were truncated a
  second time, the migration file was added, `make db-migrate` applied it
  (after rebuilding the `postgres-migrate` image, since
  `database/migrations` is `COPY`-baked into it), and the indexed sweep ran
  against that identically-reset, freshly-indexed schema. Both sweeps then
  went through the exact same warmup/rate/repeat sequence from the same
  empty starting volume.
- **Baseline benchmark:** 3 workers, 3 partitions, re-verified 1/1/1
  assignment, 900/925/950 evt/s, 10s warmup, 45s steady state, 3 repeats;
  tag [`bench-orders-index-baseline-3w-boundary`](../../artifacts/benchmark/bench-orders-index-baseline-3w-boundary/).
  Command: `python -m scripts.benchmark.direct_saturation --rates 900,925,950
  --warmup-seconds 10 --steady-seconds 45 --repeats 3 --run-tag
  bench-orders-index-baseline-3w-boundary`. All 9 repeats correctness-clean
  (`unique_event_ids == processed_rows == matched_e2e`) and markedly
  cleaner than the earlier confounded sweep (lag slope +1.2 to +6.7/s vs.
  up to +42/s), confirming that prior sweep's data-volume hypothesis.
- **Indexed benchmark:** identical parameters and topology; tag
  [`bench-orders-index-3w-boundary`](../../artifacts/benchmark/bench-orders-index-3w-boundary/).
  Same command with `--run-tag bench-orders-index-3w-boundary`. All 9
  repeats correctness-clean; lag slope +0.6 to +2.2/s.
- **System-level result (mean of 3 repeats per rate):**

  | Rate | Lag slope: base → idx | E2E p95 (ms): base → idx | PostgreSQL CPU: base → idx |
  | --- | --- | --- | --- |
  | 900 | 3.06 → 1.48 (‑52%) | 230 → 120 (‑48%) | 57.8% → 56.2% (flat) |
  | 925 | 1.83 → 1.24 (‑32%) | 140 → 98 (‑30%) | 62.1% → 60.0% (flat) |
  | 950 | 1.84 → 1.25 (‑32%) | 246 → 181 (‑26%) | 71.7% → 77.2% (+8%, noisy) |

  Lag slope and E2E p95 both improved, consistently, at every one of the
  three rates - the strongest non-cherry-picked signal in this experiment.
  PostgreSQL CPU showed no consistent direction (flat-to-noisy), and
  processor CPU was unchanged (~129-142% summed across 3 workers in both
  conditions), as expected for a database-only change. Transaction-total
  and `fraud_context` stage averages (Prometheus histogram means, n=3 per
  rate) were noisy in both directions at the sub-2ms scale these stages
  operate at and did not show a clean trend - the EXPLAIN evidence above is
  the reliable signal for the query-level effect; the histogram averages
  are not precise enough at this sample size to resolve a sub-millisecond
  per-query change buried inside an 8-10-query stage. This is reported
  plainly rather than obscured: the system-level (lag slope, E2E) effect is
  real and consistent; the stage-level histogram effect is not resolvable
  from this data.
- **Write-cost check:** `ORDER_CREATED` handler latency, `business_persistence`
  stage duration, `orders` insert counts, commit duration, and WAL
  records/sec all showed no consistent regression between baseline and
  indexed runs (differences were small and inconsistent in direction across
  the three rates - e.g. WAL records/s: 12734→12956, 13385→13058,
  13937→13547). WAL FPI/s was likewise noisy in both directions (consistent
  with the checkpoint-timing dominance already established in Transaction
  decomposition v2), not a clean function of the new index. No measurable
  write-amplification cost was found for maintaining this index at this
  data volume and workload mix.
- **Boundary interpretation:** 900 evt/s remained clean and the control
  point; both conditions' 925 evt/s repeats stayed in the same
  low-single-digit lag-slope range as before (transition band, not
  reclassified); at 950 evt/s, all three indexed repeats were clean
  (+0.9, +1.4, +1.4/s) and materially better than the already-cleaner fresh
  baseline (+1.3 to +2.4/s). Per this experiment's own success criteria:
  **the index materially improved behavior at the previous 950 evt/s
  transition boundary.** This does **not** establish a new sustainable
  ceiling above 950 evt/s; that would require a separate ceiling-discovery
  sweep at higher rates.
- **Correctness:** `unique_event_ids == processed_rows == matched_e2e` held
  in all 18 repeats (9 baseline + 9 indexed). All four processor smoke
  scenarios (normal, duplicate, DLQ, retry) passed cleanly against the
  post-index schema, using the established procedure (stop the shared
  production consumer briefly so it cannot double-consume the smoke tests'
  shared-topic poison messages, run the four scenarios, restart it).
- **Decision: kept.** This is Outcome A - the query plan improved
  (`Bitmap Heap Scan` + `Filter` + `Sort` → `Index Only Scan`, zero heap
  fetches, no sort), and a consistent system-level improvement followed at
  every measured rate (lower lag slope, lower E2E p95), with no measurable
  write-cost or PostgreSQL-CPU regression. The migration
  (`006_orders_customer_ordered_at.sql`) is retained.

## Product-views composite index — kept

- **Hypothesis:** Stage 16 found `fraud_context_product_views` running the
  same `customer_id`-only Bitmap-Heap-Scan-plus-Filter pattern as the
  pre-index `orders` lookup. The exact hot query, from
  [`services/event_processor/fraud/context.py`](../../services/event_processor/fraud/context.py):

  ```sql
  SELECT COUNT(*) FROM (
      SELECT 1 FROM product_views
      WHERE customer_id = %s AND viewed_at <= %s AND viewed_at >= %s
      ORDER BY viewed_at DESC LIMIT %s
  ) bounded
  ```

  One equality predicate (`customer_id`), one range predicate on
  `viewed_at` (upper bound the event time, lower bound the current
  session's `started_at` when known, otherwise the same 30-day fraud
  lookback), `ORDER BY viewed_at DESC`, bounded `LIMIT`, no join - the same
  shape as the `orders` query. `product_views` had only
  `idx_product_views_customer_id` (customer_id only) plus
  `idx_product_views_session_id`, neither aligned with this predicate. Same
  equality-then-range/order principle as the `orders` and `payments`
  indexes: `(customer_id, viewed_at DESC)`.
- **Baseline EXPLAIN** (representative customer, 177 product views, fresh
  controlled data): `Bitmap Heap Scan on product_views` via
  `idx_product_views_customer_id`, `Recheck Cond` on `customer_id`,
  `Filter` on the `viewed_at` range, `Sort` (quicksort), `Limit`. 60 shared
  buffer hits, 0.613 ms execution time. Retained at
  [`artifacts/benchmark/bench-product-views-index-explain/baseline-product-views-query-explain.txt`](../../artifacts/benchmark/bench-product-views-index-explain/baseline-product-views-query-explain.txt).
- **Change:** exactly one migration,
  [`database/migrations/007_product_views_customer_viewed_at.sql`](../../database/migrations/007_product_views_customer_viewed_at.sql):
  `CREATE INDEX idx_product_views_customer_viewed_at ON product_views
  (customer_id, viewed_at DESC)`. The `orders` and `payments` composite
  indexes, all fraud SQL, and every other configuration variable were left
  untouched - the single performance variable in this experiment.
- **Indexed EXPLAIN** (comparable customer, 175 product views, same
  post-index controlled dataset): `Index Only Scan using
  idx_product_views_customer_viewed_at`, `Index Cond` covers all three
  predicates, `Heap Fetches: 0`, no `Sort`. 5 shared buffers (4 hit + 1
  read), 0.172 ms execution time. Retained at
  [`artifacts/benchmark/bench-product-views-index-explain/indexed-product-views-query-explain.txt`](../../artifacts/benchmark/bench-product-views-index-explain/indexed-product-views-query-explain.txt).
  Execution time improved ~3.6x; buffer usage fell ~12x; heap access and
  sort both eliminated - directionally the same mechanism as the `orders`
  index, though the `orders` EXPLAIN improved by a larger relative margin
  (2.7x vs 3.6x is actually larger here, but from a smaller absolute base:
  0.613→0.172 ms vs 0.195→0.073 ms; product-views buffers fell further in
  absolute terms, 60→5 vs 39→5, because the representative customer's
  history was larger and less of it matched the filter).
- **Controlled reset methodology, mirroring the orders-index experiment:**
  all benchmark-populated tables truncated via
  `scripts/reset-benchmark-data.sql` before the baseline sweep (0 rows
  confirmed); baseline sweep ran while migration 007 did not yet exist;
  truncated again (0 rows confirmed); migration 007 added,
  `postgres-migrate` image rebuilt (`database/migrations` is `COPY`-baked
  into it) and applied; indexed sweep ran from the identically-reset,
  freshly-indexed schema. 1/1/1 partition assignment and lag=0
  re-verified immediately before both sweeps. The representative customer
  used for each EXPLAIN had a comparable row count (177 vs 175 product
  views) - a valid apples-to-apples comparison, not a repeat of the earlier
  accumulated-data-volume confound.
- **Baseline benchmark:** 3 workers/3 partitions/1/1/1, 900/925/950 evt/s,
  10s warmup, 45s steady, 3 repeats; tag
  [`bench-product-views-index-baseline-3w-boundary`](../../artifacts/benchmark/bench-product-views-index-baseline-3w-boundary/).
  All 9 repeats correctness-clean; lag slope +0.56 to +4.79/s.
- **Indexed benchmark:** identical parameters; tag
  [`bench-product-views-index-3w-boundary`](../../artifacts/benchmark/bench-product-views-index-3w-boundary/).
  All 9 repeats correctness-clean; lag slope +1.20 to +2.21/s.
- **System-level result (mean of 3 repeats per rate):**

  | Rate | Lag slope base→idx | E2E p95 base→idx | fraud_context base→idx | Processor CPU base→idx |
  | --- | --- | --- | --- | --- |
  | 900 | +3.06→+1.20/s (‑61%) | 263→113 ms (‑57%) | 1.03→0.87 ms (‑15%) | 147→128% (‑13%) |
  | 925 | +1.59→+1.37/s (‑14%) | 132→132 ms (flat) | 0.82→0.86 ms (+5%) | 133→144% (+8%) |
  | 950 | +2.33→+1.84/s (‑21%) | 139→163 ms (+17%, noisy) | 1.12→0.86 ms (‑24%) | 132→146% (+10%) |

  900 evt/s shows a clear, consistent win across every metric, similar in
  character to the `orders` result. 925 evt/s is a wash - small
  directional improvements and regressions that cancel out, within normal
  run-to-run noise for this transition band. 950 evt/s is mixed: lag slope
  and peak lag improved in the aggregate and in most individual repeats,
  but mean E2E p95 was pulled higher by one especially clean baseline
  repeat (64 ms) against three more typically-noisy indexed repeats
  (124-228 ms); per-repeat peak lag was lower in 2 of 3 indexed repeats.
  This is reported as noise-dominated at 950, not a systematic regression -
  `product_views` insert counts, `PRODUCT_VIEWED` handler latency, and WAL
  records/s all moved within ±2% or in the *improving* direction at 950
  (see write-cost check below), which is inconsistent with a genuine
  system-level regression from the index.
- **Write-cost check:** `product_views` insert counts (±1% at every rate),
  WAL records/s (±2% at every rate), and `business_persistence` duration
  showed no consistent regression at any rate. `PRODUCT_VIEWED` handler
  latency was mixed (+24% at 900, +7% at 925, -37% at 950) - noisy, not a
  one-directional write-cost signal. WAL FPI/s swung by large relative
  amounts (e.g. +63% at 900, -72% at 925) but from small absolute values
  (5-34/s), consistent with the checkpoint-timing dominance already
  established for this table family rather than with index-maintenance
  cost. No measurable write-amplification cost was found.
- **Boundary interpretation:** 900 evt/s remained clean and materially
  better indexed. 925 evt/s remained a transition band in both conditions,
  essentially unchanged by the index (not reclassified). 950 evt/s stayed
  borderline-noisy in both conditions; unlike the `orders` experiment, not
  all three indexed 950 repeats were unambiguously cleaner than baseline,
  so per this experiment's own criteria **no boundary-improvement claim is
  made at 950 evt/s** for this index specifically - the improvement here is
  concentrated at 900 evt/s.
- **Correctness:** `unique_event_ids == processed_rows == matched_e2e` held
  in all 18 repeats (9 baseline + 9 indexed). All four processor smoke
  scenarios (normal, duplicate, DLQ, retry) passed cleanly against the
  post-index schema using the established procedure (stop the shared
  production consumer, run the four scenarios, restart it).
- **Comparison with the `orders` index:**
  - **Larger EXPLAIN improvement:** `product_views` improved execution
    time by a larger relative factor (3.6x vs 2.7x) and a larger absolute
    buffer-count drop (60→5 vs 39→5), though both eliminated heap access
    and sort entirely and reached an `Index Only Scan`.
  - **Larger system-level lag/E2E improvement:** the `orders` index, which
    improved lag slope and E2E p95 consistently at *all three* rates
    (900/925/950). The `product_views` index only showed a clean win at
    900 evt/s; 925 was a wash and 950 was noise-dominated with no clear
    direction.
  - **Larger write/WAL maintenance cost:** neither index showed a
    measurable write-cost or WAL regression in this workload/data volume;
    the two are comparable (both "no cost found").
  - **Remaining `fraud_context` cost after both indexes:** `fraud_context`
    still spans ~8 additional lookups per fraud-eligible event beyond
    `recent_orders`/`product_views` (customer, session, order,
    recent/prior payments, refunds, refund-facts), all already
    composite-indexed except the two now-fixed lookups and the
    single-row-by-primary-key lookups (customer, session, order) which
    cannot benefit from a composite index. The remaining `fraud_context`
    cost is spread evenly across this set (Stage 16's SQL-class table
    showed no query dominating), not concentrated in a single remaining
    unindexed hot path.
- **Decision: kept.** No rate showed a measurable write-cost or
  PostgreSQL-CPU regression, the query-plan improvement is real and
  substantial (EXPLAIN-proven), and 900 evt/s showed a clear system-level
  win. 925/950 evt/s showed no clear win but also no regression - this is
  Outcome A at 900 evt/s and Outcome B (neutral, no downside) at 925/950,
  not Outcome C. The trade-off favors keeping: a real read benefit with no
  identified write cost. The migration
  (`007_product_views_customer_viewed_at.sql`) is retained.

## Post-index capacity discovery — measurement only, no code change

- **Why:** with both the `orders` and `product_views` composite indexes
  retained, the previous ~900/925/950/1000 evt/s sustainability boundary
  (from the batched-offset-commit era, before either index) was stale. This
  stage re-establishes the actual boundary under the current retained
  configuration - no optimization, pure measurement.
- **Schema/index state verified before benchmarking:** all 7 migrations
  applied (`schema_migrations` max version 7); `idx_payments_customer_attempted_at`,
  `idx_orders_customer_ordered_at`, and `idx_product_views_customer_viewed_at`
  all present via `pg_indexes`. No index modified.
- **Clean-reset methodology:** `scripts/reset-benchmark-data.sql` run once
  before the entire sweep (not between repeats, per the established
  methodology) - `customers`/`orders`/`payments`/`product_views`/
  `processed_events` all confirmed at 0 rows immediately before starting.
  Consumer lag was 0 at reset time.
- **Topology:** scaled to 3 workers, 1/1/1 assignment re-verified via
  `kafka-consumer-groups.sh --describe` before the broad sweep.
- **Broad sweep** (950/1000/1050/1100 evt/s, 10s warmup, 45s steady, 3
  repeats; tag
  [`bench-post-index-3w-ceiling-broad`](../../artifacts/benchmark/bench-post-index-3w-ceiling-broad/)):

  | Rate | Repeats clean | Lag slope range | E2E p95 range | Peak PG CPU |
  | --- | --- | --- | --- | --- |
  | 950 | 2/3 (1 elevated: slope 19.3, E2E p95 1743ms) | +2.0 to +19.3/s | 129-1743ms | 110% |
  | 1000 | 3/3 | +0.7 to +1.7/s | 145-283ms | 77% |
  | 1050 | 3/3 (bounded, one repeat's PG CPU hit 100%) | +1.1 to +2.5/s | 152-583ms | 100% |
  | 1100 | 1/3 (2 degraded: slope 11.6/24.9, E2E p95 2101/2474ms) | +1.8 to +24.9/s | 439-2474ms | 132% |

  Injector kept pace at 99.4-99.9% of requested rate at every single repeat
  through 1100 evt/s - the degradation above 1000 evt/s is genuine
  processor/PostgreSQL saturation, not an injector limitation.
- **Refinement** (1075 evt/s, same 10s/45s/3-repeat methodology; tag
  [`bench-post-index-3w-ceiling-refinement`](../../artifacts/benchmark/bench-post-index-3w-ceiling-refinement/)):
  slope +10.0, +3.0, +34.8/s across the 3 repeats; E2E p95 407/552/2275ms.
  2 of 3 repeats show meaningful degradation (slope >9/s, one severely so)
  - the same "one clean repeat, two degraded" pattern the methodology
  flags as non-sustainable, not a clean result. Injector still kept pace
  (99.8-99.9% of requested).
- **Boundary:** 1000 evt/s and 1050 evt/s are the highest rates where all 3
  repeats stayed bounded (lag slope <3/s, E2E p95 under ~600ms, drain
  predictable). 1075 evt/s is the first rate with a repeatable (2/3)
  non-sustainable pattern, and 1100 evt/s confirms it more severely (also
  2/3 degraded, worse E2E). Transition interval: **~1050-1075 evt/s**, a
  25 evt/s window - within the task's acceptable 10-25 evt/s range, so no
  further refinement was performed.
- **Resource trend (measured):** PostgreSQL CPU rose the most sharply and
  the most monotonically across the sweep (mean/max: 950 evt/s 75%/110%,
  1000 evt/s 64%/77%, 1050 evt/s 81%/100%, 1100 evt/s 110%/132%). Processor
  CPU (summed across 3 workers) also rose but stayed well short of its
  ~600% ceiling (137-169% mean), leaving headroom per worker. Per-
  transaction cost (`transaction_total`, `fraud_context` Prometheus
  histogram averages) stayed essentially flat across all four rates
  (~1.5-1.6ms / ~0.82-0.85ms) - the marginal transaction is not getting
  more expensive as rate rises; there are simply more of them contending
  for the same PostgreSQL capacity. WAL FPI/s trended upward with rate
  (19.6→47.2→37.5→86.4, noisy but broadly increasing) alongside WAL
  records/s (13910→14854→15410→16724, cleanly monotonic).
- **Hypothesis:** PostgreSQL CPU is the most likely next bottleneck for any
  further capacity increase - it is the resource that grew most
  consistently as the system moved from sustainable (1000) through
  transition (1050-1075) toward saturated (1100), while per-transaction
  cost itself did not change, pointing at contention/scheduling under
  concurrent load rather than any single expensive query. This is a
  hypothesis, not a proven causal claim - no further isolation was
  performed in this measurement-only stage.
- **Correctness:** `unique_event_ids == processed_rows == matched_e2e` held
  in all 15 repeats (12 broad + 3 refinement). All four processor smoke
  scenarios passed against the current (post-both-index) schema using the
  established procedure.
- **Capacity comparison:**
  - vs. the previous post-batching boundary (~900 evt/s):
    `(1050 - 900) / 900 = +16.7%`.
  - vs. the earlier pre-batched-commit boundary (~750 evt/s):
    `(1050 - 750) / 750 = +40.0%`.
  - These improvements are **not** attributable to a single change. Batched
    Kafka offset commits moved the boundary from ~750 to ~900 evt/s
    (Stage 14). The `orders` and `product_views` composite indexes (Stages
    17-18) were then followed by this fresh capacity sweep, which
    establishes the new ~1000-1050 evt/s boundary. Each experiment's
    contribution is kept distinct; this stage does not attribute the full
    750→1050 change to the two indexes alone.
- **No implementation changes were made in this stage** - reset, topology
  verification, and benchmarking only. Environment restored to 1 worker,
  lag 0, after completion.

## PostgreSQL saturation diagnosis — measurement only, no code change

- **Motivation:** Stage 19 established that PostgreSQL CPU is the strongest
  measured saturation-resource signal near the 1050-1100 evt/s boundary,
  but did not say *what inside PostgreSQL* changes - active-query
  concurrency, lock/LWLock waits, IO waits, WAL/write coordination, or
  aggregate query-execution volume. This stage answers that, using only
  external, sampling-based PostgreSQL observation.
- **Rates:** 1050, 1075, 1100 evt/s; 10s warmup, 45s steady, 3 repeats;
  same 3-worker/1/1/1 topology, re-verified before starting from a
  `scripts/reset-benchmark-data.sql`-clean state with all 7 migrations and
  all three composite indexes confirmed present. Tag
  [`bench-postgres-saturation-diagnosis-3w`](../../artifacts/benchmark/bench-postgres-saturation-diagnosis-3w/).
  Command per rate:
  `python -m scripts.benchmark.direct_saturation --rates <rate> --warmup-seconds 10 --steady-seconds 45 --repeats 3 --run-tag bench-postgres-saturation-diagnosis-3w`.
- **Diagnostic sampler:** new
  [`scripts/benchmark/postgres_diagnostics.py`](../../scripts/benchmark/postgres_diagnostics.py),
  an external, read-only sampler polling `pg_stat_activity` and `pg_locks`
  once per second (`--interval-seconds 1.0`) for the duration of each
  rate's 3-repeat sweep, plus one `pg_stat_io`/`pg_stat_checkpointer`
  snapshot before and after. It never persists raw SQL text - queries are
  bucketed into bounded `<table>_<statement-kind>` classes
  (`classify_query()`). Command per rate:
  `python -m scripts.benchmark.postgres_diagnostics --run-tag bench-postgres-saturation-diagnosis-3w --label <rate> --duration-seconds 400 --interval-seconds 1.0`,
  started just before the matching benchmark invocation. Output:
  `postgres-diagnostics-<rate>.json` per rate, each with a raw sample
  series plus an aggregated `summary` (`summarize_samples()`, unit-tested
  in
  [`tests/unit/test_postgres_diagnostics.py`](../../tests/unit/test_postgres_diagnostics.py)).
  Summary statistics exclude samples where zero backends were active by
  default (`active_only=True`), approximating "under load" periods within
  a series that also spans warmup/idle/drain gaps, since precise
  cross-process wall-clock correlation with the benchmark's own load
  windows was out of scope for this lightweight sampler.
- **Instrumentation overhead:** four small catalog-view queries per tick
  (`pg_stat_activity`, `pg_locks`, `pg_stat_database` counters) against a
  server already running ~450-460 transactions/sec at these rates is a
  negligible fraction of total query volume (roughly 0.02-0.03% of
  transaction rate at 1 tick/second); no formal isolated A/B was run to
  quantify this further, since the sampler issues no query anywhere near
  the hot fraud-context/business-write path and adds no per-event cost.
- **Known tooling limitation:** `direct_saturation.py` writes
  `direct-saturation.json` unconditionally to the run tag's directory on
  every invocation; running it three times sequentially under one shared
  run tag (once per rate) overwrote that file, leaving only the final
  (1100 evt/s) rate's full JSON (transaction/stage breakdown, PostgreSQL
  container CPU, WAL rates) retained in that file. This is a real gap for
  future multi-rate-under-one-tag runs of this exact pattern, not a data
  loss in general - the `injector-<rate>-<repeat>.json` files (raw
  per-event samples, unaffected by this) and this stage's own
  `postgres-diagnostics-<rate>.json` files (the primary evidence for this
  stage's question, unaffected since each has a distinct filename by
  label) remained fully intact for all three rates. Where possible this
  gap was closed by direct recomputation (below); PostgreSQL container
  CPU% and `pg_stat_wal`-based WAL rates for 1050/1075 specifically were
  not recoverable (they were only ever sampled live via `docker stats`/
  direct SQL and never exposed through Prometheus, so there was nothing to
  query retroactively) - only the 1100 evt/s repeats retain those two
  metrics directly from this stage's own run. This should be fixed in any
  future run of this tool by using a distinct run tag per rate or an
  output-path override.
- **Correctness (recovered where needed):** `unique_event_ids ==
  processed_rows` was independently reverified for all six 1050/1075
  repeats by re-querying `processed_events` against each repeat's retained
  `injector-<rate>-<repeat>.json` event-ID list (all six matched exactly);
  1100's three repeats matched `unique_event_ids == processed_rows ==
  matched_e2e` directly from the retained `direct-saturation.json`. E2E
  latency for the six 1050/1075 repeats was also recomputed from the same
  event-ID lists against Kafka publish timestamps and `processed_events.processed_at`
  (matching direct_saturation.py's own method) - full detail below. All
  correctness held; zero duplicate durable side effects, zero missing
  durable events, across all 9 repeats.
- **System-level results (lag slope / peak lag / E2E p50-p95-p99 ms):**

  | Rate | Repeat | Lag slope | Peak lag | E2E p50/p95/p99 |
  | --- | --- | ---: | ---: | --- |
  | 1050 | 0 | +1.99/s | 610 | 32 / 1529 / 2568 |
  | 1050 | 1 | +1.72/s | 535 | 30 / 149 / 411 |
  | 1050 | 2 | +2.49/s | 426 | 45 / 197 / 526 |
  | 1075 | 0 | +5.70/s | 267 | 54 / 483 / 563 |
  | 1075 | 1 | +10.37/s | 477 | 42 / 518 / 593 |
  | 1075 | 2 | +11.06/s | 517 | 46 / 697 / 916 |
  | 1100 | 0 | +3.70/s | 173 | 34 / 265 / 729 |
  | 1100 | 1 | +3.14/s | 743 | 38 / 496 / 629 |
  | 1100 | 2 | +9.80/s | 458 | 54 / 716 / 1175 |

  This fresh sweep's own 1100 repeats came back somewhat cleaner than
  Stage 19's broad-sweep 1100 result (which had 2/3 repeats with slope
  11.6-24.9/s and E2E p95 up to 2.47s) - consistent with the
  already-documented run-to-run variance in this saturated regime, not a
  contradiction; both sweeps agree that 1075-1100 evt/s is materially
  noisier and higher-tailed than 1050. 1050 rep 0's E2E p99 spike (2568 ms)
  despite a low lag slope (+1.99/s) is itself an example of the
  "bounded lag, inflated tail latency" pattern this task asked to watch
  for - not treated as disqualifying 1050, since 2 of 3 repeats were clean
  on both dimensions, but noted as a real, repeatable-enough possibility
  even at the "sustainable" rate.
- **Active/waiting backend evidence (aggregated per rate from the sampler,
  1s ticks, load-active samples only):**

  | Metric | 1050 | 1075 | 1100 |
  | --- | ---: | ---: | ---: |
  | Active backends avg/max | 1.24 / 4 | 1.28 / 4 | 1.32 / 4 |
  | Waiting backends (any `wait_event`) avg/max | 6.53 / 8 | 5.03 / 6 | 4.83 / 6 |
  | Idle-in-transaction avg/max | 0.42 / 3 | 0.48 / 3 | 0.55 / 3 |
  | Active transactions avg/max | 1.66 / 4 | 1.76 / 5 | 1.87 / 5 |
  | Longest active-query age (max) | 0.66 s | 3.21 s | 4.55 s |
  | Longest transaction age (max) | 0.66 s | 3.21 s | 4.55 s |
  | Transactions/sec | 443.4 | 452.9 | 462.8 |
  | Blocked backends (max) | 0 | 0 | 0 |

  Active-backend concurrency is essentially flat (1.24 → 1.32 avg) across
  the boundary - **this is not a concurrency-explosion pattern.** The
  "waiting" count is dominated overwhelmingly by `ClientRead`
  (`wait_event_type=Client`), which is the normal, benign state of a
  pooled backend idling between queries, not contention; it is *higher* at
  1050 than at 1075/1100 (more idle capacity when the system keeps up).
  The one value that grows sharply and monotonically with the boundary is
  the **longest single active-query/transaction age observed: 0.66s →
  3.21s → 4.55s** - a small number of individual transactions occasionally
  stall well past this workload's normal sub-millisecond-to-low-millisecond
  cost at 1075/1100, while nothing in this sample series ever reaches
  above ~0.7s at 1050.
- **Wait-event distribution (summed counts across all ticks, per rate):**

  | wait_event_type | 1050 | 1075 | 1100 |
  | --- | ---: | ---: | ---: |
  | Client (`ClientRead`) | 2558 | 1946 | 1865 |
  | IO | 41 | 43 | 34 |
  | LWLock | 4 | 8 | 9 |
  | Timeout (`VacuumDelay`, autovacuum's own cost-based throttling) | 2 | 11 | 20 |
  | Lock (heavyweight) | 0 | 0 | 0 |
  | IPC / BufferPin / Activity / Extension | 0 | 0 | 0 |

  **Lock, IPC, BufferPin, Activity, and Extension waits never appeared at
  any rate.** LWLock waits are present but stay in the single digits out
  of ~400 one-second ticks at every rate - a mild upward trend (4 → 8 → 9)
  from a near-zero base, not a repeatable contention signature. IO waits
  are flat (41/43/34). The only wait class that clearly grows with rate is
  `VacuumDelay` (2 → 11 → 20), which is autovacuum's own internal
  cost-based throttling pausing itself, not an application query waiting
  on anything - it reflects more vacuum work being needed as insert/update
  volume rises, not query-path contention.
- **Lock evidence:** `locks_waiting_by_mode` was empty at every tick at all
  three rates (max ungranted-lock count across the entire series: 0).
  `pg_blocking_pids()` never returned a non-empty result for any sampled
  backend at any rate (`blocked_max = 0` throughout). **There is no
  heavyweight lock contention or blocking anywhere in this data.**
- **Transaction concurrency:** active-transaction count tracked active
  backends closely (1.66 → 1.76 → 1.87 avg) - a small, gradual rise
  consistent with more work in flight at higher throughput, not a
  qualitative shift. Transactions/sec rose in proportion to the requested
  rate (443 → 453 → 463/s), exactly as expected from more events being
  processed per second; this is not itself evidence of a bottleneck.
- **Query-class accumulation (`pg_stat_statements` is not enabled in this
  environment - confirmed via `pg_extension`/`shared_preload_libraries`,
  unchanged from prior stages; this experiment did not enable it, per the
  task constraint. The existing `commerce_database_sql_duration_seconds`/
  `commerce_database_sql_statement_count_total` Prometheus histograms were
  used instead, queried historically via Prometheus's own retained data
  for windows ending at each rate's last repeat):** the same query classes
  dominate accumulated DB time at every rate, in the same rank order -
  `processed_events_select` (duplicate-check/idempotency reads, multiple
  per event), `processed_events_insert`, `fraud_evaluation_write`, then
  the `fraud_context_*` lookups (recent/prior payments, recent orders,
  product views, refunds, in roughly even proportion, matching Stage 16's
  finding that no single fraud-context query dominates). Per-operation
  **mean latency stayed flat to noisy across all three rates** (e.g.
  `fraud_context_recent_payments`: 0.109 → 0.134 → 0.096 ms;
  `processed_events_select`: 0.110 → 0.092 → 0.086 ms) - **no query class
  got slower per call as rate rose.** Total accumulated time per class grew
  roughly in proportion to call volume at every rate, which is exactly the
  signature of aggregate call-volume-driven cost, not per-query
  degradation.
- **WAL/write-path and IO/buffer evidence:** PostgreSQL container CPU and
  `pg_stat_wal`-based rates were only retained for this stage's own 1100
  repeats (see the known tooling limitation above): 62.2/75.1/68.0% CPU,
  WAL FPI 9.1/185.7/23.1 per second (noisy, consistent with the
  checkpoint-timing dominance already established), WAL records/sec
  16112/16089/16611 (flat across the three 1100 repeats). `pg_stat_io` and
  `pg_stat_checkpointer` before/after deltas were captured at all three
  rates: checkpointer buffer writes were 5202 → 15088 → 12760 (one timed
  checkpoint per window at every rate, plus one *requested* checkpoint at
  1075 specifically - the only rate where `restartpoints_req`/
  `num_requested` moved, suggesting WAL volume crossed
  `max_wal_size` during that window). More strikingly, **autovacuum-worker
  IO in the `vacuum` context grew sharply with rate: reads 58,285 →
  226,578 → 219,241; writes 1,463 → 19,935 → 54,773**, and background
  writer's normal-context buffer writes also rose (34,416 → 54,250 →
  63,579). This points to a secondary, plausible contributor: higher
  insert/update volume produces more dead tuples per unit time, driving
  more autovacuum and background-writer I/O activity competing for the
  same shared buffers/IO bandwidth as the query path - separate from, and
  additional to, the aggregate query-CPU explanation above.
- **CPU interpretation:** combining the evidence above, PostgreSQL CPU
  rising near the boundary is best explained by **(A) aggregate
  query-execution CPU scaling with call volume** (flat per-query mean
  latency, proportional total-time growth, no lock/LWLock/IO wait
  explosion) as the primary driver, with **increasing autovacuum/
  background-writer I/O activity** as a secondary, additive contributor
  that also scales with insert/update volume. There is no evidence
  supporting (B) connection/concurrency-driven degradation (active
  backends nearly flat), (C) lock/LWLock contention (zero heavyweight
  locks, LWLock counts trivial), or a dominant (D) WAL-wait/checkpoint
  cause (checkpoint activity present but not clearly correlated with the
  degraded repeats specifically). The one still-unexplained, genuinely
  growing signal - the longest single active-query/transaction age
  reaching several seconds at 1075/1100 while never exceeding ~0.7s at
  1050 - does not show up as any captured `wait_event`, which points
  toward host/container CPU scheduling contention (the query is marked
  `active` in PostgreSQL's own view the whole time, meaning PostgreSQL
  itself has no internal wait to report, but the OS/hypervisor may not be
  scheduling that backend's CPU time slice promptly under load) rather
  than anything visible inside PostgreSQL's own instrumentation - this is
  a hypothesis, not proven by this stage's evidence.
- **Hypotheses ruled out:** connection/query-concurrency explosion (active
  backends 1.24→1.32 avg, essentially flat); heavyweight lock contention
  (zero at every rate, every tick); LWLock contention as a dominant cause
  (present but trivial in absolute count); a single runaway query class
  (rank order and per-call mean latency both stable across all three
  rates); IO wait explosion (41/43/34, flat).
- **Strongest supported diagnosis:** aggregate query-execution CPU,
  proportional to event/call volume, is what changes inside PostgreSQL
  between 1050 and 1075-1100 evt/s - not concurrency, not locking, not a
  single slow query. A secondary, additive autovacuum/background-writer
  I/O contribution was also measured and grows with rate. **Confidence:
  moderate.** The wait-event and lock evidence is clean and consistent
  across all three rates (strong), but the CPU-vs-scheduling distinction
  for the growing longest-query-age signal is inferred, not directly
  measured by this sampler, and the `active_only` aggregation methodology
  is a simplification rather than a precisely load-window-correlated
  measurement.
- **Correctness:** held for all 9 repeats (6 recomputed, 3 direct); all
  four processor smoke scenarios passed using the established procedure.
- **Next isolated experiment recommended:** a lightweight host/container
  CPU-scheduling probe (e.g. sampling `docker stats` for the PostgreSQL
  container at sub-second granularity alongside a synthetic
  fixed-cost query loop) to test the "OS/hypervisor scheduling delay,
  not a PostgreSQL-internal wait" hypothesis directly, before considering
  any reduction in per-event PostgreSQL read/write volume (e.g.
  precomputed/rolling fraud-context state) as a throughput-focused
  follow-up. No optimization was made or recommended for implementation
  in this stage.

## Benchmark artifact reliability — overwrite fix, tooling only

- **Bug:** the PostgreSQL saturation diagnosis stage above was run as
  three sequential single-rate `direct_saturation.py` invocations under
  one shared `--run-tag`. Each invocation wrote its full results to the
  same unconditional path, `<run-tag>/direct-saturation.json` - so the
  1075 evt/s invocation silently overwrote 1050's complete results, and
  the 1100 evt/s invocation overwrote 1075's, leaving only 1100's data
  recoverable from that file (Stage 20 above closed the resulting gap by
  recomputing correctness/E2E for 1050/1075 from retained
  `injector-<rate>-<repeat>.json` and `processed_events` data).
- **Fix (benchmark tooling only, no load-generation/timing/correctness
  semantics changed):** `direct_saturation.py` now writes one file per
  rate - `rate_artifact_path()`/`rate_artifact_filename()` produce
  `direct-saturation-<rate>.json` - so any rate's results always land in
  their own file, whether all rates are passed in one `--rates` call or
  across several sequential single-rate invocations under the same run
  tag. Rerunning the *same* rate under the same tag still overwrites that
  rate's own file, which is intentional (mirrors `--repeats` already being
  bundled inside one rate's file, not a naming defect).
- **`postgres_diagnostics.py` had the same class of gap** (its raw sample
  series and compact summary were combined into one
  `postgres-diagnostics-<label>.json`, which also made it impossible to
  git-ignore the high-volume raw part without also ignoring the compact
  summary). Fixed by splitting output into
  `postgres-diagnostics-raw-<label>.json` (per-tick samples, git-ignored)
  and `postgres-diagnostics-summary-<label>.json` (aggregate summary plus
  the small, duration-independent `pg_stat_io`/`pg_stat_checkpointer`
  before/after snapshots, tracked). The now-unused `--output` CLI flag was
  removed since both paths are always derived from `--run-tag`/`--label`.
- **Existing Stage 20 artifacts migrated losslessly** (no re-benchmarking)
  from the old combined/single-rate-collision-prone filenames to the new
  convention: `direct-saturation.json` (1100 evt/s only, the surviving
  data from the overwrite bug) → `direct-saturation-1100.json`;
  `postgres-diagnostics-{1050,1075,1100}.json` → split into
  `postgres-diagnostics-{raw,summary}-{1050,1075,1100}.json`. No sample or
  metric was altered or dropped during migration.
- **`.gitignore`** updated: `artifacts/benchmark/**/direct-saturation.json`
  (retained, for any not-yet-migrated historical directory still using the
  pre-fix name) plus a new
  `artifacts/benchmark/**/direct-saturation-*.json` and
  `artifacts/benchmark/**/postgres-diagnostics-raw-*.json`.
  `postgres-diagnostics-summary-*.json` is deliberately **not** matched by
  any ignore rule and stays versionable.
- **Tests:** `tests/unit/test_direct_saturation_artifacts.py` (6 tests) and
  three new cases in `tests/unit/test_postgres_diagnostics.py` verify
  different rates/labels always produce distinct paths, the same
  rate/label intentionally reuses one path, raw and summary filenames can
  never collide with each other, and path construction stays scoped to the
  given phase directory - without running any real sampling or load.
- **No benchmark execution semantics changed:** event injection pacing,
  rate calculations, warmup/steady-state timing, repeat counts,
  correctness logic, Kafka/PostgreSQL behavior, sampler SQL, worker count,
  and resource-collection semantics are all unchanged; this is artifact
  naming/organization only.

## Host / Container CPU Scheduling Diagnosis — measurement only, no code change

- **Motivation:** Stage 20 found the strongest saturation signal was
  PostgreSQL CPU rising near 1075-1100 evt/s, with no lock/LWLock/IO wait
  explosion and flat per-query mean latency - pointing at aggregate
  query-execution CPU - but confidence was capped at *moderate* because
  host/container CPU scheduling itself was never directly measured. This
  stage closes that gap: is PostgreSQL genuinely consuming CPU on
  aggregate query work, or is Docker/cgroup throttling, host CPU
  saturation, or scheduler pressure delaying it?
- **Environment detected:** this repository runs on Docker Desktop for
  macOS - the host is Darwin/arm64 (no `/proc`, no Linux cgroups directly
  visible), and every container shares one Linux VM kernel (`linuxkit`,
  8 vCPUs). This means `/proc/stat`, `/proc/loadavg`, and
  `/proc/pressure/cpu` read from *any* one container reflect the whole VM,
  not that container in isolation - confirmed by inspection before writing
  any sampler code, per this stage's own instruction not to assume Linux
  host counters are visible or to fabricate values Docker Desktop hides.
  cgroup v2 (`cpu.stat`, `cpu.max`) and PSI (`/proc/pressure/cpu`) were
  both confirmed present and populated inside containers - richer
  visibility than Docker Desktop is often assumed to expose.
- **Rates/topology:** 1050, 1075, 1100 evt/s; 10s warmup, 45s steady, 3
  repeats; 3 workers, 1/1/1 assignment re-verified; migrations 1-7 and all
  three composite indexes confirmed present; no extra processor/smoke
  consumers active; lag=0 and injector idle before starting; a full
  `scripts/reset-benchmark-data.sql` reset before the sweep. Tag
  [`bench-cpu-scheduling-diagnosis-3w`](../../artifacts/benchmark/bench-cpu-scheduling-diagnosis-3w/).
  Benchmark command per rate:
  `python -m scripts.benchmark.direct_saturation --rates <rate> --warmup-seconds 10 --steady-seconds 45 --repeats 3 --run-tag bench-cpu-scheduling-diagnosis-3w`
  (using the collision-safe per-rate artifact naming from the prior fix -
  correctness was verified directly from each rate's own
  `direct-saturation-<rate>.json`, confirming no overwrite occurred).
- **Sampler methodology:** new
  [`scripts/benchmark/cpu_scheduling_diagnostics.py`](../../scripts/benchmark/cpu_scheduling_diagnostics.py),
  external and read-only. Command per rate:
  `python -m scripts.benchmark.cpu_scheduling_diagnostics --run-tag bench-cpu-scheduling-diagnosis-3w --label <rate> --duration-seconds 400 --interval-seconds 1.0 --processor-containers event-processor-1,event-processor-2,event-processor-3`,
  started just before the matching benchmark invocation and left running
  through warmup/steady/drain/idle. Each tick: (1) a fixed-cost `SELECT 1`
  over a persistent diagnostic connection (measures connection dispatch +
  trivial round-trip latency, *not* PostgreSQL CPU cost in isolation -
  this limitation is explicit in the code and here); (2) concurrent
  `docker exec` into PostgreSQL + each processor worker reading cgroup v2
  `cpu.stat`/`cpu.max` and aggregating `/proc/<pid>/stat`
  (utime/stime)+`/proc/<pid>/status` (voluntary/involuntary context
  switches) across all `postgres`-comm child processes (or the single PID
  1 for each processor); (3) one additional `docker exec` for the
  VM-wide `/proc/loadavg`, `/proc/stat` (aggregate + per-core), and
  `/proc/pressure/cpu`; (4) `docker stats --no-stream`-based
  container CPU%/mem% (postgres, kafka, redis, each processor worker),
  gated to every 3rd tick since `docker stats` has its own ~2s intrinsic
  sampling window. Raw SQL text and business values are never persisted.
  286 samples were collected per rate over the ~400s window (mean tick
  interval ~1.4s, close to the nominal 1.0s on non-`docker-stats` ticks
  and ~2-3s on the gated ticks, exactly as designed).
- **Instrumentation overhead:** a single `docker exec` measured ~150-350ms
  in this environment (timed before choosing the design) - far from
  negligible - which is why per-tick container reads are dispatched
  concurrently (4 containers in parallel measured ~316ms total) rather
  than sequentially, and why `docker stats` runs on its own coarser
  cadence. The `SELECT 1` probe itself measured 0.25-1.6ms at idle before
  the sweep - negligible added server load at ~1 query/second against a
  server already running 440-460+ TPS. 1050 evt/s behavior in this sweep
  (lag slope +1.24 to +3.05/s) stayed within the range already established
  by earlier 1050 sweeps, so the diagnostic did not materially perturb the
  system it was measuring.
- **Container CPU (docker stats, mean/max across the whole ~400s window,
  including idle/warmup/drain periods - not directly comparable to Stage
  19's peak-during-load-only `runtime_max`):**

  | Container | 1050 avg/max | 1075 avg/max | 1100 avg/max |
  | --- | --- | --- | --- |
  | PostgreSQL | 25.1% / 156.0% | 24.9% / 94.5% | 26.8% / 81.3% |
  | processor-1 | 16.1% / 59.7% | 17.0% / 58.7% | 20.8% / 66.7% |
  | processor-2 | 16.4% / 57.6% | 18.4% / 66.4% | 17.7% / 59.9% |
  | processor-3 | 15.6% / 65.9% | 17.3% / 64.4% | 18.2% / 60.9% |
  | Kafka | 18.8% / 148.4% | 16.9% / 155.5% | 14.5% / 117.2% |
  | Redis | 5.3% / 88.7% | 5.0% / 68.5% | 5.2% / 76.9% |

  Averages are essentially flat across all three rates for every
  container; the highest single PostgreSQL CPU spike (156.0%) occurred at
  **1050**, not 1100 - consistent with Stage 19/20's repeated finding that
  single-repeat spikes occur near this boundary independent of exact rate.
  No container's CPU trends toward its ceiling as rate rises.
- **cgroup throttling: none, and structurally impossible in the current
  configuration.** `nr_periods` delta was **0** for every container at
  every rate across the whole ~400s window each - not just
  `nr_throttled`, but `nr_periods` itself never advanced, because
  `cpu.max` reports `max` (unlimited quota) for PostgreSQL and every
  processor worker, matching `compose.yaml`'s documented absence of any
  `cpus:` limit. Cgroup v2 only accounts throttling periods when a quota
  is configured; with no quota, the kernel has nothing to throttle against
  by construction. **Docker/cgroup CPU throttling is not occurring, and
  cannot occur under the current Docker resource configuration.**
- **Host/VM-wide CPU and scheduler evidence:**

  | Metric | 1050 | 1075 | 1100 |
  | --- | ---: | ---: | ---: |
  | load1 avg/max (of 8 vCPUs) | 1.45 / 2.03 | 1.73 / 2.56 | 2.18 / 4.54 |
  | Context switches/sec | 45,849 | 46,215 | 47,007 |
  | PSI `cpu some avg10` mean | 2.01% | 2.06% | 2.17% |
  | Busiest single core (mean / max %) | cpu7: 17.7 / 93.1 | cpu3: 17.6 / 92.1 | cpu6: 18.0 / 94.0 |

  Load average rises mildly with rate but stays well under the VM's 8
  vCPUs even at its peak (4.54 at 1100 evt/s). Context switches/sec are
  essentially flat (+2.5% from 1050 to 1100). PSI `some avg10` - the
  fraction of time at least one task was stalled waiting for CPU - stays
  under 2.2% at every rate, rising only marginally; `full` PSI (all tasks
  stalled simultaneously) was 0.00 throughout. A single core does spike to
  ~92-94% briefly at every rate - including 1050, the sustainable rate -
  so this is baseline bursty single-core activity, not a rate-driven
  pattern specific to the degraded region.
- **PostgreSQL and processor process-level evidence:** aggregated
  utime/stime and voluntary/involuntary context-switch counters were
  collected per tick across all `postgres`-comm child processes (and each
  processor's single PID). Combined with the throttling and PSI findings
  above, there is no process-level scheduling-starvation signature: no
  cgroup throttling, no PSI pressure, no context-switch explosion, and (see
  below) no fixed-cost probe inflation that would indicate PostgreSQL
  backends specifically were being scheduled less promptly.
- **Fixed-cost `SELECT 1` probe (858 samples total across the sweep,
  idle baseline was 0.25-1.6ms before starting):**

  | Percentile | 1050 | 1075 | 1100 |
  | --- | ---: | ---: | ---: |
  | p50 | 1.50 ms | 1.78 ms | 1.84 ms |
  | p95 | 6.99 ms | 7.43 ms | 8.41 ms |
  | p99 | 12.88 ms | 14.57 ms | 11.20 ms |
  | max | 16.05 ms | 24.30 ms | 16.14 ms |

  p50/p95 rise only mildly and gradually (single-digit milliseconds); p99
  and max are **not monotonic** (1100's p99 is lower than 1075's). This is
  the key differentiator against host-wide scheduling starvation: Stage 20
  found individual *application* transactions occasionally stalling to
  3.2-4.6 **seconds** at 1075/1100, three orders of magnitude larger than
  anything seen here. If PostgreSQL backends in general were being starved
  by the OS/hypervisor scheduler, this probe - itself just another
  PostgreSQL backend contending for the same CPU - would show comparable
  multi-second inflation at least some of the time. It does not, in any of
  858 samples. This makes application-transaction-specific cost (not
  host-wide scheduling delay) the better-supported explanation for those
  multi-second stalls.
- **Lag/E2E correlation:** lag slope and E2E tail latency remained noisy
  and non-monotonic across repeats at every rate in this sweep too (e.g.
  1050 rep 0: slope +1.24/s but E2E p99 2926 ms; 1100 rep 2: slope only
  +1.78/s but peak lag 3169 and E2E p99 3542 ms) - consistent with the
  established "bounded lag can still hide inflated tail latency" pattern,
  and with none of it correlating to cgroup throttling, PSI, or probe
  inflation (all flat/negligible in every repeat).
- **Relationship to Stage 20:** this stage adds the one category of
  evidence Stage 20 could not measure - host/container scheduling - and it
  comes back clean at every rate. Combined with Stage 20's already-clean
  lock/LWLock/IO-wait/connection-concurrency evidence and flat per-query
  mean latency, all ten items in this experiment's own evidence checklist
  are now satisfied:

  1. no lock contention (Stage 20)
  2. no material LWLock contention (Stage 20)
  3. no IO-wait explosion (Stage 20)
  4. no connection/backend-concurrency explosion (Stage 20)
  5. no cgroup throttling (this stage - structurally impossible, confirmed)
  6. no host scheduler pressure (this stage - PSI/load/ctxt all flat)
  7. no fixed-cost probe inflation (this stage - single-digit ms only)
  8. query-class means broadly stable (Stage 20)
  9. call volume/total accumulated DB work rises with event rate (Stage 20)
  10. PostgreSQL CPU rises with that aggregate work (Stage 19/20)
- **Hypotheses ruled out:** Docker/cgroup CPU throttling (Outcome B -
  structurally impossible under the current unlimited `cpu.max`
  configuration); host CPU saturation (Outcome C - load average stays
  under 5 of 8 vCPUs even at peak, PSI negligible); processor-worker CPU
  starvation (Outcome D - processor container CPU% averages 15-21%, well
  under its ceiling, at every rate); general PostgreSQL-backend scheduling
  starvation (the fixed-cost probe would have shown it and did not).
- **Strongest supported mechanism: Outcome A - aggregate PostgreSQL
  execution CPU from many small per-event operations.** No throttling, no
  host saturation, no PSI pressure, no fixed-cost-probe inflation, no
  lock/LWLock/IO-wait signature, flat per-query-class mean latency, and
  PostgreSQL CPU rising in proportion to rising call volume together
  satisfy this experiment's own "strong evidence" checklist in full.
- **Confidence: STRONG** (upgraded from Stage 20's "moderate" now that
  host/container scheduling has been directly measured and found clean).
- **Correctness:** held for all 9 repeats, verified directly from each
  rate's own `direct-saturation-<rate>.json` (confirming the prior
  overwrite fix works as intended). All four processor smoke scenarios
  passed using the established procedure.
- **Recommended next isolated experiment (not implemented in this
  stage):** with genuine aggregate per-event PostgreSQL work now
  strongly supported as the ceiling's cause, the next isolated experiment
  should target *reducing* that aggregate work - e.g. measuring the effect
  of consolidating or precomputing part of the ~10-query `fraud_context`
  read set (Stage 16 already found no single query dominates it, so
  reducing round-trip *count* rather than any one query's cost is the more
  promising direction) - as a controlled, single-variable A/B against the
  current ~1050 evt/s boundary, following the same reset methodology used
  throughout this series.
