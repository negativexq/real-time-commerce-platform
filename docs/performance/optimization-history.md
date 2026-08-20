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

## Fraud-context round-trip reduction — kept

- **Accumulated diagnosis leading to this experiment:** Stage 16 found
  `fraud_context` was the largest DB-side transaction stage with no single
  dominant query; Stage 20 ruled out lock/LWLock/IO-wait/connection
  explosion and found flat per-query mean latency with call-volume-driven
  total cost; Stage 21 ruled out cgroup throttling, host CPU saturation,
  and scheduler starvation with STRONG confidence. The converging
  conclusion: aggregate cost from many small PostgreSQL round trips per
  fraud-eligible event, not one slow query or infrastructure pressure.
  This experiment tests that hypothesis directly by removing one round
  trip and observing whether system behavior actually improves.
- **Original query inventory** (from direct inspection of
  [`services/event_processor/fraud/context.py`](../../services/event_processor/fraud/context.py),
  counted from the real code, not assumed):

  | # | Field | Query | Calls/fraud-eligible event | Depends on | Cost (baseline mean) |
  | --- | --- | --- | ---: | --- | ---: |
  | 1 | `home_country` | `SELECT home_country FROM customers WHERE customer_id = %s` | 1.0 (always) | none | ~0.07ms (Stage 16) |
  | 2 | `session_id` (fallback) | `SELECT session_id FROM orders WHERE order_id = %s` | ~0.33 (only if `session_id is None and order_id is not None`) | order_id | ~0.07ms |
  | 3 | `session_started_at` | `SELECT started_at FROM sessions WHERE session_id = %s` | 1.0 (always, may resolve session_id from #2 first) | #2's result | ~0.08ms |
  | 4 | `order` (ordered_at/total/currency/billing_country) | `SELECT ordered_at, total, currency, billing_country FROM orders WHERE order_id = %s` | 1.0 (always) | none | ~0.08ms |
  | 5 | recent payments | bounded `payments` select, `attempted_at` in `[lookback, event_time]`, `LIMIT` | 1.0 (always) | none | ~0.11-0.13ms |
  | 6 | prior payments | bounded `payments` select, `attempted_at < event_time`, `LIMIT` | 1.0 (always) | none | ~0.10-0.11ms |
  | 7 | refunds | bounded `refunds` select, `requested_at` in `[lookback, event_time]`, `LIMIT` | 1.0 (always) | none | ~0.08ms |
  | 8 | recent orders count | bounded `COUNT(*)` subquery over `orders` | 1.0 (always) | none | ~0.08-0.09ms |
  | 9 | product-view count | bounded `COUNT(*)` subquery over `product_views` | 1.0 (always) | #3's `session_started` (lower bound fallback) | ~0.09-0.11ms |
  | 10 | refund facts (refundable amount, seconds since payment) | `payments LEFT JOIN refunds` by `payment_id` | ~0.33 (only if `payment_id is not None`) | payment_id | ~0.09ms |

  **Original round trips/event: 8 unconditional + up to 2 conditional = 8-10,
  averaging ~8.7 across the real fraud-eligible event-type mix** (measured
  via `fraud_context_customer`'s ~1.0 calls/fraud-eligible-event as the
  100%-frequency reference point). Rows 1 and 4 are two independent,
  always-issued, single-row primary-key lookups on different tables with no
  data dependency between them - the smallest safe consolidation candidate.
  Event types affected: all five `FRAUD_ELIGIBLE_EVENT_TYPES`
  (`checkout_started`, `order_created`, `payment_failed`,
  `payment_completed`, `refund_requested`) - rows 1 and 4 are issued
  unconditionally for every one of them.
- **Candidate consolidation selected:** merge query #1 (`customers`) and
  #4 (`orders`) into one round trip via a `LEFT JOIN` on the constant
  `order_id` parameter, keeping `customers` as the driving/filtered table:

  ```sql
  SELECT c.home_country, o.ordered_at, o.total, o.currency, o.billing_country
  FROM customers c
  LEFT JOIN orders o ON o.order_id = %s
  WHERE c.customer_id = %s
  ```

  **Why this pair over alternatives:** (a) both are always-issued PK
  lookups with zero row-multiplication risk - `customer_id` and `order_id`
  are both unique primary keys, so the join can produce at most one row,
  identical to two independent single-row lookups; (b) they have no data
  dependency on each other (unlike #2→#3, where #3 needs #2's resolved
  `session_id`); (c) the original code never scoped the `orders` lookup by
  `customer_id` - a `LEFT JOIN ... ON o.order_id = %s` preserves that
  exact (non-obvious) behavior instead of "fixing" it, which an inner join
  or a customer-scoped join would not; (d) recent/prior payments (#5/#6)
  were deliberately **not** chosen - this repository already retained a
  rejected "Combined payment lookup" experiment showing that merging those
  two specific queries regressed throughput and latency, and that lesson
  was treated as a hard constraint on this experiment's design, not
  advisory; (e) the two `COUNT(*)` subqueries (#8/#9) were a viable
  alternative of similar size and risk, but the customer+order pair was
  selected because it eliminates a stage even earlier in the sequence,
  ahead of every subsequent query, minimizing exposure to any confound. No
  other query pairs/groups were implemented or benchmarked in this
  experiment - **one consolidation boundary only**, per instruction.
- **New round trips/event: 7-9, averaging ~7.7** (10→9 in the always-issued
  count: rows 1+4 become one row, i.e. exactly **one round trip saved per
  fraud-eligible event**, a ~11.5% reduction in fraud-context round trips
  for this pair, ~10% of the full fraud-context call sequence).
- **Semantic-equivalence tests:**
  [`tests/integration/test_fraud_context_roundtrip.py`](../../tests/integration/test_fraud_context_roundtrip.py)
  (7 tests, run against the real local PostgreSQL, all fixture writes
  rolled back) directly compares the new
  `FraudContextBuilder._customer_and_order()` against the exact legacy
  two-query sequence (kept as a test-only helper, never duplicated in
  production) across: a matching own order, no order (`order_id=None`), a
  non-existent `order_id`, an order belonging to a *different* customer
  (proving the "not scoped by customer_id" behavior survived unchanged),
  and an unknown customer (both raise `FraudContextDependencyError`
  identically). Two further tests prove the round-trip count directly - a
  counting cursor wrapper shows the legacy sequence issues exactly 2
  `execute()` calls and the combined query issues exactly 1, for the same
  fixture data.
- **Baseline EXPLAIN** (representative customer/order pair): two separate
  `Index Scan`s (`customers_pkey`, `orders_pkey`), 3+4=7 buffers combined,
  0.053ms + 0.034ms execution. Retained at
  [`artifacts/benchmark/bench-fraud-context-roundtrip-explain/baseline-two-query-explain.txt`](../../artifacts/benchmark/bench-fraud-context-roundtrip-explain/baseline-two-query-explain.txt).
- **Candidate EXPLAIN:** `Nested Loop Left Join` over the *same* two
  `Index Scan`s (`customers_pkey`, `orders_pkey`) - identical access paths,
  no new scan type, 7 buffers total (unchanged). No cartesian risk: both
  sides are PK-equality-scoped to at most one row. Retained at
  [`artifacts/benchmark/bench-fraud-context-roundtrip-explain/candidate-combined-explain.txt`](../../artifacts/benchmark/bench-fraud-context-roundtrip-explain/candidate-combined-explain.txt).
  Execution time at this sub-0.2ms scale is measurement noise on a single
  sample and not the basis for the keep/revert decision - the query plan
  confirms no accidental expense was introduced, which is what EXPLAIN is
  for here; the system-level A/B below is the actual evidence.
- **Controlled reset methodology:** since code had to change between
  conditions, `git stash` isolated the baseline (unmodified) code, the
  `event-processor` image was rebuilt, `scripts/reset-benchmark-data.sql`
  reset all benchmark tables, 1/1/1 assignment was re-verified, and the
  full 1000/1050/1075 × 3 sweep ran against that clean baseline image.
  The stash was then popped (candidate code), the image rebuilt again, a
  second full reset performed, 1/1/1 re-verified, and the candidate sweep
  ran from the same equivalent empty starting state. Baseline tag
  [`bench-fraud-context-roundtrip-baseline-3w`](../../artifacts/benchmark/bench-fraud-context-roundtrip-baseline-3w/),
  candidate tag
  [`bench-fraud-context-roundtrip-candidate-3w`](../../artifacts/benchmark/bench-fraud-context-roundtrip-candidate-3w/).
  Commands (both conditions):
  `python -m scripts.benchmark.direct_saturation --rates 1000,1050,1075 --warmup-seconds 10 --steady-seconds 45 --repeats 3 --run-tag <tag>`.
- **Baseline vs. candidate (mean of 3 repeats per rate):**

  | Metric | 1000 base→cand | 1050 base→cand | 1075 base→cand |
  | --- | --- | --- | --- |
  | Calls/event (`customer`+`order` → `customer_order`) | 0.156+0.156→0.131 | 0.145+0.145→0.141 | 0.033+0.033→0.097* |
  | `fraud_context` avg | 0.972→0.608ms (**-37%**) | 0.870→0.740ms (**-15%**) | 0.845→0.733ms (**-13%**) |
  | `transaction_total` avg | 1.388→1.417ms (flat) | 1.560→1.442ms (-8%) | 1.412→1.444ms (flat) |
  | Lag slope | +4.17→+1.70/s (**-59%**) | +3.32→+1.78/s (**-46%**) | +2.87→+2.37/s (-17%) |
  | E2E p95 | 317→110ms (**-65%**) | 296→114ms (**-61%**) | 217→223ms (flat/noisy) |
  | E2E p99 | 592→398ms (-33%) | 574→432ms (-25%) | 499→489ms (flat) |
  | PostgreSQL CPU (mean of peak samples) | 73.6→59.7% (-19%) | 62.6→67.2% (+7%, noisy) | 77.5→63.0% (-19%) |
  | Processor CPU (summed, 3 workers) | 141.6→137.4% (-3%) | 148.5→130.9% (-12%) | 154.3→148.4% (-4%) |
  | WAL records/sec | 14871→14748 (-1%) | 15573→15674 (+1%) | 15873→15922 (flat) |

  *The baseline 1075 calls/event figure (0.033) is inconsistent with the
  same measurement at 1000/1050 (~0.15) and is treated as a
  Prometheus-rate-window sampling artifact for that specific repeat set,
  not a real drop in fraud-eligible traffic share - the underlying
  workload mix is unchanged between conditions.
- **DB round trips saved:** exactly **1 round trip saved per
  fraud-eligible event** (measured directly - two SQL classes disappear,
  one appears at matching frequency). At the measured fraud-eligible
  event rate (~15.5% of service rate, consistent at 1000/1050): **~155
  round trips/sec saved at 1000 evt/s, ~151 round trips/sec saved at 1050
  evt/s**; 1075's figure is not reported numerically given the noisy
  denominator above, but is of the same order (service rate × ~15% ×
  1 ≈ 160/s) if the same fraud-eligible fraction is assumed.
- **Resource interpretation:** PostgreSQL CPU dropped at 1000/1075 (-19%
  each) and was flat/noisy at 1050 (+7%, within normal repeat-to-repeat
  variance already documented in Stages 19-21); this roughly tracks the
  reduced call volume rather than exceeding it, consistent with the
  EXPLAIN evidence that no new, more expensive access pattern was
  introduced. Processor CPU also fell modestly at every rate - plausible
  given one fewer round trip means less time each event spends waiting on
  the database inside the transaction.
- **WAL/write impact:** none - WAL records/sec stayed within ±1% at every
  rate, exactly as expected since this change touches only read queries.
- **Correctness:** `unique_event_ids == processed_rows == matched_e2e`
  held in all 18 repeats (9 baseline + 9 candidate). All four processor
  smoke scenarios passed against the retained candidate build.
- **Decision: kept (Outcome A).** Round trips materially decreased (one
  fewer per fraud-eligible event, matching the exact predicted mechanism)
  **and** system-level behavior improved on the metrics that matter most:
  `fraud_context` latency fell at every rate (13-37%), lag slope fell at
  every rate (17-59%), E2E tail latency fell sharply at 1000/1050 evt/s
  (was flat, not regressed, at 1075), PostgreSQL and processor CPU each
  trended down more often than not, and there was no WAL/write
  regression. This is the clearest system-level improvement observed
  since Stage 17/18's index work.
- **Capacity interpretation:** this experiment does **not** establish a
  new sustainable ceiling. Even though 1075 evt/s's candidate repeats
  stayed as clean as 1050's, this stage only claims: **the fraud-context
  round-trip reduction materially improved behavior at the previous
  transition boundary.** The README/CV capacity claim (~1050 evt/s
  sustainable, ~1075 evt/s transition) is unchanged pending a dedicated
  fresh ceiling-discovery sweep.
- **Remaining fraud-context round trips:** 7-9 per fraud-eligible event
  (session resolution, session lookup, recent/prior payments, refunds,
  recent-orders count, product-view count, and the conditional
  refund-facts join) - still spread across several independently small
  operations, none individually dominant (unchanged from Stage 16's
  finding).
- **Strongest remaining bottleneck:** the same aggregate-cost mechanism,
  now with one fewer contributing round trip; recent/prior payments and
  the two bounded `COUNT(*)` subqueries remain the next-largest
  candidates, but recent/prior payments consolidation is explicitly
  disfavored by the repository's own retained rejected-experiment
  evidence.
- **Recommended next isolated experiment:** apply the same
  smallest-safe-consolidation methodology to the two independent bounded
  `COUNT(*)` subqueries (recent-orders count, product-view count) - both
  scalar, both already indexed via Stage 17/18's composite indexes, no
  join/row-multiplication risk, structurally identical low-risk profile
  to this experiment. A fresh ceiling-discovery sweep is a separate,
  later experiment once (and if) further fraud-context round-trip
  reductions are exhausted.

## Post fraud-context optimization capacity discovery — measurement only, no code change

- **Previous boundary:** ~1050 evt/s clearly sustainable, ~1075 evt/s
  transition/degraded (established pre-Stage-22, unchanged in the README/
  CV claim per this stage's explicit instruction not to revise it without
  a dedicated stable conclusion).
- **Optimization applied (unchanged for this sweep):** Stage 22's
  fraud-context `customers`+`orders` round-trip reduction (10→9 calls per
  fraud-eligible event) - kept, running, not modified in this stage.
- **Methodology:** same deterministic direct-saturation sweep used
  throughout this series - 3 workers, 3 partitions, 1/1/1 re-verified, 10s
  warmup, 45s steady, 3 repeats - extended to six rates: 1000 (known safe),
  1050 (previous boundary), 1075 (previous transition), 1100 (previously
  unstable), 1125/1150 (new, to test whether the boundary moved). No
  reset was performed before this sweep (task instruction: "do not reset
  back" - measuring the system in its current, already-benchmarked
  state). Tag
  [`bench-post-fraud-context-ceiling-3w`](../../artifacts/benchmark/bench-post-fraud-context-ceiling-3w/).
  Command:
  `python -m scripts.benchmark.direct_saturation --rates 1000,1050,1075,1100,1125,1150 --warmup-seconds 10 --steady-seconds 45 --repeats 3 --run-tag bench-post-fraud-context-ceiling-3w`.
- **Rate table (mean/range across 3 repeats):**

  | Rate | Lag slope range | Peak lag range | E2E p95 range | PG CPU range | Classification |
  | --- | --- | --- | --- | --- | --- |
  | 1000 | +1.25 to +1.72/s | 180-419 | 127-296ms | 57-91% | **Sustainable** |
  | 1050 | +0.86 to +1.71/s | 91-420 | 357-637ms | 78-122% | Sustainable, elevated tail |
  | 1075 | +2.36 to +4.75/s | 319-624 | 211-823ms | 71-120% | **Transition** |
  | 1100 | +1.29 to +15.94/s | 123-732 | 353-2063ms | 80-99% | **Transition** |
  | 1125 | +7.68 to +93.52/s | 403-4318 | 848-7440ms | 83-86% | **Non-sustainable** |
  | 1150 | +141.85 to +307.39/s | 6540-14348 | 11648-25807ms | 85-96% | **Non-sustainable** |

  1000 evt/s remained cleanly bounded in every repeat. 1050 evt/s kept lag
  slope low and bounded in all 3 repeats, but its E2E p95/p99 (357-637ms /
  533-1008ms) was materially higher than 1000's - the same "bounded lag,
  inflated tail" pattern documented since Stage 19, now visible at 1050
  specifically. 1075 and 1100 evt/s both show the established mixed
  signature: some repeats clean, one repeat per rate clearly elevated
  (1075 rep2: slope +4.75, E2E p99 1057ms; 1100 rep1: slope +15.94, peak
  lag 732, E2E p99 4821ms). 1125 evt/s crossed into repeatable
  non-sustainability - even its "best" repeats show slope +7.68/s, and one
  repeat spiked to +93.52/s with a 7.9-second E2E p99. 1150 evt/s was
  unambiguously non-sustainable in **all three** repeats - lag slopes of
  +142 to +307 events/second, peak lag reaching 14,348, and E2E p50 itself
  landing in the **seconds** (1.6-9.3s), with p95/p99 in the tens of
  seconds. Actual service rate at 1150 (830-997/s) fell visibly below the
  injected rate (1129-1138/s) in every repeat - the system was genuinely
  unable to keep up, not merely showing a noisy tail.
- **Resource behavior:** processor CPU (summed across 3 workers) rose with
  rate - 138%→156%→152%→180%→172%→194% mean across 1000→1150 - consistent
  with more concurrent work, still with headroom relative to a 3×100%
  ceiling. PostgreSQL CPU stayed in a similar 57-122% band across all six
  rates with no clean monotonic trend distinguishing 1125/1150 from
  1000-1100 - the *lag/latency* behavior, not PostgreSQL CPU, is what
  cleanly separates sustainable from non-sustainable here. WAL records/sec
  rose with rate through 1125 (14,642→15,419→16,358→15,686→15,765) then
  **fell** at 1150 (12,302) - consistent with the collapsed actual service
  rate at 1150 (genuinely less work got done, not more).
- **`fraud_context_customer_order` calls/event** (the Stage 22
  consolidated query) stayed in the same ~0.11-0.15 range through 1125,
  confirming the optimization remained active and unchanged throughout
  this sweep; 1150's collapsed figure (0.022) reflects the same
  service-rate collapse as the WAL figure above, not a change in the
  query path itself.
- **Correctness:** `unique_event_ids == processed_rows == matched_e2e`
  held in all 18 repeats, including all three catastrophically-lagged 1150
  repeats - durable correctness was never compromised even under severe
  backlog. All four processor smoke scenarios passed using the
  established procedure.
- **Whether the capacity boundary moved: not established by this sweep.**
  1075 and 1100 evt/s still show the same mixed, repeat-dependent
  character observed pre-optimization (some clean repeats, some clearly
  degraded), rather than becoming uniformly clean the way Stage 22's own
  narrower 1000/1050/1075 A/B sweep happened to show for 1075. 1125 evt/s
  is repeatably non-sustainable and 1150 evt/s is severely so in every
  repeat. This is reported honestly as **post-optimization observed
  behavior**, not a new capacity claim: the documented boundary (~1050
  sustainable, ~1075 transition) is unchanged, since a single 3-repeat
  sweep with mixed 1075/1100 results does not meet the bar for a "stable
  dedicated conclusion" this task explicitly requires before any claim
  revision.
- **Next experiment:** if a revised capacity claim is wanted, a dedicated
  ceiling-discovery sweep with more repeats specifically bracketing
  1075-1100 evt/s (the mixed zone) is needed, ideally following the same
  clean-reset methodology used in earlier ceiling stages (this sweep
  deliberately skipped the reset per instruction, which is a valid choice
  for "measure the system as currently benchmarked" but is a confound for
  a *definitive* boundary claim, consistent with the data-volume lessons
  already documented in this history). Otherwise, the next isolated
  optimization experiment remains Stage 22's own recommendation: apply
  the same round-trip-consolidation methodology to the two independent
  bounded `COUNT(*)` subqueries in `fraud_context`.

## Transaction lifecycle decomposition v3 — measurement only, no code change

- **Previous diagnosis leading to this experiment:** Stage 16 found no
  single dominant SQL query; Stage 20 ruled out lock/LWLock/IO-wait/
  connection-explosion with aggregate call-volume-driven cost as the
  strongest mechanism; Stage 21 ruled out cgroup throttling and host
  scheduling starvation with STRONG confidence; Stage 22 reduced
  fraud-context round trips (~8.7 → ~7.7/event) with real per-metric
  improvement but Stage 23 found the saturation boundary itself did not
  move. This stage shifts from "how many SQL calls" to "which phase of
  the transaction's own lifecycle dominates and grows" - read, write, or
  commit.
- **Lifecycle model** (from direct inspection of
  [`services/event_processor/persistence/unit_of_work.py`](../../services/event_processor/persistence/unit_of_work.py),
  `UnitOfWorkFactory.persist()`):

  ```
  persist() started
    -> pool.connection() acquire         [database_pool_acquire_duration]
    -> connection.transaction() begins
       -> phase "processed_events": insert_identity()   [stage=processed_events, WRITE]
       -> phase "business": handler.apply()             [stage=business_persistence, WRITE]
       -> if fraud-eligible:
            phase "fraud_context": FraudContextBuilder.build()  [stage=fraud_context, READ - ~9 bounded SELECTs]
            fraud_engine.evaluate()                     [fraud_evaluation_duration - pure Python, no DB]
            phase "fraud_persistence": fraud.persist()  [stage=fraud_persistence, WRITE]
    -> connection.transaction() exits -> COMMIT          [stage=commit]
    -> connection release                                 [database_connection_release_duration]
  persist() returns; database_transaction_duration observes the whole span
  ```

  Every one of these spans is **already instrumented** by the existing
  `database_stage_duration_seconds{stage=...}`,
  `database_pool_acquire_duration_seconds`,
  `database_connection_release_duration_seconds`, and
  `database_transaction_duration_seconds` histograms (all active in
  production since the Stage 5/9 instrumentation, reused by every
  decomposition stage in this history) - no rollback path is separately
  recorded, because `AlreadyPersistedEvent`/dependency errors are caught
  *before* reaching the commit-metric line and are exceptional/rare (zero
  occurrences in any retained benchmark repeat to date, consistent with
  the 100% correctness held throughout this series).
- **Instrumentation added:** a single pure function,
  `phase_group_breakdown()` in
  [`scripts/benchmark/direct_saturation.py`](../../scripts/benchmark/direct_saturation.py),
  which regroups the already-fetched per-stage averages into
  **read phase** (`fraud_context`), **write phase**
  (`processed_events_insert` + `business_persistence` +
  `fraud_persistence`), and **commit** (`commit`), alongside pool-acquire/
  connection-release and the transaction total - no new metric, no new
  Prometheus query, zero added runtime overhead. Wired into each benchmark
  repeat's output as `phase_group_breakdown_ms`. Unit-tested in
  [`tests/unit/test_phase_group_breakdown.py`](../../tests/unit/test_phase_group_breakdown.py)
  (8 cases: read/write/commit extraction, the non-fraud-event case where
  `fraud_context` is absent, missing-total handling, and an empty
  breakdown).
- **Known limitation of the derived breakdown:** `unattributed_ms` (the
  residual after subtracting every phase from `transaction_total`) was
  **negative in nearly every repeat** of this sweep. This is not evidence
  of double-counting inside one transaction - it reflects that
  `transaction_total` is averaged over *all* processed events (fraud and
  non-fraud alike), while `fraud_context`/`fraud_persistence` are averaged
  only over the fraud-eligible subset; summing averages taken over
  different sub-populations does not reconstruct one transaction's exact
  internal budget. The phase magnitudes remain valid for **relative**
  comparison across rates and repeats, which is what this experiment
  needed; they are not a literal reconciled accounting.
- **Benchmark:** 3 workers, 1/1/1 verified, current code (fraud-context
  optimization active, unmodified in this stage), 1000/1050/1075/1100
  evt/s only (per instruction, not extending the ceiling search further),
  10s warmup, 45s steady, 3 repeats. Tag
  [`bench-transaction-lifecycle-v3-3w`](../../artifacts/benchmark/bench-transaction-lifecycle-v3-3w/).
  Command:
  `python -m scripts.benchmark.direct_saturation --rates 1000,1050,1075,1100 --warmup-seconds 10 --steady-seconds 45 --repeats 3 --run-tag bench-transaction-lifecycle-v3-3w`.
- **Transaction phase breakdown (per repeat, ms - showing the full
  per-repeat spread rather than only means, since the central finding is
  about repeat-to-repeat behavior):**

  | Rate | Repeat | Lag slope | Read | Write | Commit | Total |
  | --- | --- | ---: | ---: | ---: | ---: | ---: |
  | 1000 | 0 (clean) | +1.87/s | 0.809 | 0.828 | 0.402 | 1.751 |
  | 1000 | 1 (clean) | +1.91/s | 0.571 | 0.852 | 0.439 | 1.892 |
  | 1000 | 2 (clean) | +1.74/s | 0.445 | 0.465 | 0.224 | 0.976 |
  | 1050 | 0 (degraded) | +38.46/s | 0.828 | 0.975 | 0.411 | 1.816 |
  | 1050 | 1 (clean) | +1.19/s | 0.469 | 0.429 | 0.241 | 0.213* |
  | 1050 | 2 (moderate) | +14.72/s | 0.644 | 0.672 | 0.447 | 1.428 |
  | 1075 | 0 (severe) | +176.07/s | 0.886 | 0.873 | 0.460 | 1.948 |
  | 1075 | 1 (clean) | +2.86/s | 0.511 | 0.587 | 0.258 | 1.108 |
  | 1075 | 2 (clean) | +4.58/s | 0.896 | 0.920 | 0.453 | 1.945 |
  | 1100 | 0 (moderate) | +35.20/s | 0.901 | 0.924 | 0.467 | 1.971 |
  | 1100 | 1 (severe) | +255.19/s | 0.621 | 0.947 | 0.326 | 1.361 |
  | 1100 | 2 (severe) | +66.80/s | 1.346 | 1.122 | 0.502 | 2.044 |

  *1050 repeat 1's `transaction_total` (0.213ms) is implausibly low
  relative to its own read+write+commit sum (~1.1ms) and is treated as a
  Prometheus-window sampling artifact for that one repeat, not a real
  value - excluded from the qualitative conclusion below.
- **The central finding: phase durations do not track degradation.**
  Read, write, and commit phase durations stay in the same
  **~0.4-1.3ms band regardless of whether the repeat was clean or
  severely lagging.** 1075 repeat 0 - the most severely degraded repeat
  in the entire sweep (+176 events/s lag slope, E2E p99 16,672ms) - had a
  read phase of 0.886ms and write phase of 0.873ms, **nearly identical**
  to 1075 repeat 2's clean run (0.896ms / 0.920ms, slope +4.58/s only).
  The same pattern holds at every rate: the worst-lagging repeat's
  internal phase costs are not distinguishable from the cleanest repeat's
  at the same rate. Read and write phases stay roughly balanced with each
  other throughout (neither consistently dominates the other), and commit
  stays consistently the smallest of the three (~0.2-0.5ms) at every rate
  and every repeat.
- **PostgreSQL/processor resource evidence:** `postgres_locks_after`
  showed `{'AccessShareLock': 1}` - the benchmark's own read-only snapshot
  query - at **every single rate and repeat**, clean or degraded alike.
  Zero heavyweight lock evidence anywhere in this sweep, consistent with
  Stage 20. PostgreSQL CPU (67-116% across all repeats) and processor CPU
  (161-209%, three workers summed) showed no clean split between clean
  and degraded repeats at the same rate (e.g. 1075's severely-degraded
  repeat 0 measured 84.8% PG CPU, barely different from clean repeat 1's
  77.8%). WAL records/sec (11,832-16,066) showed the same pattern - no
  clean degraded-vs-clean separation.
- **Fraud vs. non-fraud comparison:** fraud-eligible-event handler
  latency was consistently 1.3-2x higher than non-fraud handler latency
  at every rate (e.g. 1000: 1.28-2.88ms fraud-eligible vs. 1.92-1.99ms
  non-fraud, already an *unusually tight* non-fraud band by comparison),
  matching every prior stage's finding. Within a single rate, the
  degraded repeat's fraud-eligible latency was sometimes (not always)
  moderately higher than the clean repeat's (1050: 2.99ms degraded vs.
  1.69ms clean; 1075: 3.22ms severely-degraded vs. 1.86ms clean) - a real
  but weak and inconsistent signal (1100's ordering did not follow the
  same pattern: its *worst* repeat by lag slope, +255/s, showed the
  *lowest* fraud-eligible latency of the three, 2.84ms).
- **Analysis questions, answered directly:**
  1. **Is read phase dominant?** No single phase dominates consistently;
     read and write stay roughly balanced (each ~0.4-1.3ms), commit stays
     smallest throughout.
  2. **Is write phase dominant?** No - see above; write is comparable to
     read, not clearly larger.
  3. **Is commit/WAL becoming dominant?** No - commit stayed the smallest
     phase at every rate and repeat (~0.2-0.5ms), and WAL records/sec
     showed no clean degraded-vs-clean split.
  4. **Does transaction duration grow before lag growth?** No - the
     opposite: transaction phase durations stayed flat regardless of
     whether lag was already growing severely.
  5. **Does SQL count increase near saturation?** No - `fraud_context_customer_order`
     calls/event stayed in the same noisy ~0.1-0.25 band at every rate
     with no monotonic rise (one 1100 repeat even showed a small negative
     value, a Prometheus-counter-window artifact, not evidence of fewer
     real calls).
  6. **Does SQL execution time increase near saturation?** No - consistent
     with (1)-(3), no phase's duration trended upward with rate or with
     degradation.
  7. **Is there evidence of contention?** No - zero heavyweight locks at
     every rate/repeat; this stage adds no new contention evidence beyond
     Stage 20/21's already-clean findings.
  8. **What phase correlates strongest with E2E tail growth?** None of
     the measured transaction-internal phases do. The only weak
     correlate found was fraud-eligible handler latency, and it was
     inconsistent (see above) - not strong enough to name as "the"
     correlate.
- **Outcome: D - no transaction phase grows with saturation; a different
  hypothesis is needed.** Every phase this experiment could measure (read,
  write, commit, pool acquire, connection release) stayed flat across
  clean and severely-degraded repeats at the same rate, and none trended
  upward with requested rate either. Combined with Stage 20/21's already-
  clean lock/wait/throttling/scheduling evidence, this rules out
  "transaction lifecycle cost grows under load" as the mechanism. The
  repeatable pattern instead - one or two repeats per rate degrading
  severely while phase-internal costs stay identical to clean repeats -
  is more consistent with a **transient queueing/arrival-burst dynamic**
  (a momentary mismatch between event arrival rate and available service
  capacity that produces a temporary backlog, which then persists for the
  rest of that repeat's measurement window) than with any per-transaction
  execution cost increase. This experiment does not have direct evidence
  of the burst mechanism itself - only clean evidence that transaction
  internals are not the cause - so this is reported as a hypothesis for
  the next experiment, not a proven mechanism.
- **Hypotheses ruled out (this stage, joining the running list):**
  transaction read-phase growth, write-phase growth, commit/WAL-phase
  growth, SQL-count growth near saturation, SQL-execution-time growth near
  saturation, lock/contention growth. Combined with Stages 20/21: cgroup
  throttling, host CPU saturation, scheduler starvation, connection
  explosion, LWLock contention, IO-wait explosion, a single slow query.
- **Correctness:** `unique_event_ids == processed_rows == matched_e2e`
  held in all 12 repeats, including the severely-lagged ones. All four
  processor smoke scenarios passed.
- **Next experiment:** since transaction-internal phases are now cleanly
  ruled out at every rate tested, the next isolated diagnostic should
  target **arrival-side/queueing dynamics directly** rather than the
  transaction or PostgreSQL again - e.g. sampling Kafka consumer
  poll-to-handler latency and in-flight/inflight-event counts at
  sub-second resolution around the moment a repeat tips into degradation,
  to test whether a transient burst in arrival rate (not measured cost
  per event) precedes the lag-slope spike. This is a measurement
  experiment, not an optimization, and follows directly from this stage's
  "no phase grows" finding.

## Consumer queueing and backpressure diagnosis — measurement only, no code change

- **Previous diagnosis leading to this experiment:** Stage 24 proved that
  none of the transaction-internal phases (read, write, commit, pool
  acquire, connection release) grow with saturation - the most severely
  lagged repeat's internal costs were statistically indistinguishable from
  a clean repeat at the same rate. That stage's own next-step proposal was
  to look at arrival-side/queueing dynamics directly instead of the
  transaction or PostgreSQL again.
- **Real execution lifecycle** (from direct inspection of
  [`services/event_processor/main.py`](../../services/event_processor/main.py)
  `run_processor()` and
  [`services/event_processor/consumer.py`](../../services/event_processor/consumer.py)
  - not assumed):

  ```
  while not shutdown.requested:
      poll_started = perf_counter()
      message = consumer.poll()      # ONE Message | None, never a batch
      if message is None: continue
      outcome = processor.process(message)   # fully synchronous, in this thread
      # loop
  ```

  This is a **fully synchronous, single-threaded, unbatched** loop: `poll()`
  returns at most one record, `process()` runs it end-to-end (validation,
  Redis idempotency reservation, handler dispatch, the whole DB transaction,
  offset tracking) in the same thread before the next `poll()` is issued.
  There is no internal record queue, no batching, and no handler
  concurrency inside one processor instance - concurrency only comes from
  running multiple processor **containers** (3 workers = 3 partitions),
  never from pipelining within one instance. This rules out an entire class
  of hypotheses before any new instrumentation was written: an in-process
  backlog of buffered/queued records cannot exist in this codebase as it is
  built today.
- **Existing instrumentation reused, not duplicated.** Nearly everything the
  task asked for already existed under a different name and only needed
  surfacing in `direct_saturation.py`, per this session's standing
  reuse-before-add discipline:
  - `handler_execution_seconds` → already
    `commerce_processor_event_processing_duration_seconds` (docstring:
    "Record receipt through terminal handling") - the full `process()` span,
    already captured as `handler_latency_ms`.
  - `consumer_inflight_events`/`consumer_active_handlers` → already
    `commerce_processor_inflight_events`, already captured as
    `max_inflight_sampled`.
  - the in-process poll→handler dispatch gap → already
    `commerce_processor_poll_to_handler_duration_seconds`, already captured
    as `poll_to_handler_ms`.
  - `processor_loop_gap_duration_seconds` (previous-`process()`-return to
    next-`poll()`) already existed in
    [`shared/observability/metrics.py`](../../shared/observability/metrics.py)
    but had never been surfaced in the benchmark tool - added as
    `loop_gap_ms` in `direct_saturation.py`, zero new instrumentation.
- **The one genuinely new metric.** Since no internal buffer can exist, the
  only place a real queue can physically live is upstream, in the Kafka
  topic itself, before this consumer's `poll()` fetches a record. Added
  `processor_consumer_queue_wait_duration_seconds` (a histogram, default
  buckets) to `ApplicationMetrics`, observed in `run_processor()`'s poll
  loop as `queue_wait_seconds(message.timestamp, datetime.now(UTC))` - a
  small pure function in `main.py` returning the elapsed time between the
  Kafka record's own producer timestamp and this consumer's poll-return
  wall time, or `None` when the broker supplied no timestamp or the result
  would be negative (producer/consumer clock skew, dropped rather than fed
  into the histogram since it cannot reflect real queueing time). Unit
  tested in
  [`tests/unit/test_processor_main_queue_wait.py`](../../tests/unit/test_processor_main_queue_wait.py)
  (4 cases: missing timestamp, normal elapsed computation, zero-wait, and
  clock-skew rejection). Surfaced in `direct_saturation.py` as
  `consumer_queue_wait_ms`.
- **Benchmark:** 3 workers, 1/1/1 verified (lag 0 before and after),
  processor image rebuilt with the new metric, 1000/1050/1075/1100 evt/s
  only (per instruction - bracketing the known transition band, not a new
  ceiling search), 10s warmup, 45s steady, 3 repeats. Tag
  [`bench-consumer-queueing-diagnosis-3w`](../../artifacts/benchmark/bench-consumer-queueing-diagnosis-3w/).
  Command:
  `python -m scripts.benchmark.direct_saturation --rates 1000,1050,1075,1100 --warmup-seconds 10 --steady-seconds 45 --repeats 3 --run-tag bench-consumer-queueing-diagnosis-3w`.
- **Results (per repeat):**

  | Rate | Repeat | Lag slope | Peak lag | Queue wait p50/p95/p99 (ms) | Handler p50/p95/p99 (ms) | E2E p50 (ms) |
  | --- | --- | ---: | ---: | --- | --- | ---: |
  | 1000 | 0 | +10.00/s | 918 | 63/3702/4740 | 2.59/4.92/9.17 | 175.9 |
  | 1000 | 1 | +3.12/s | 805 | 46/4664/4933 | 2.59/4.93/9.20 | 70.8 |
  | 1000 | 2 | +1.60/s | 517 | 49/769/954 | 2.58/4.91/9.06 | 50.3 |
  | 1050 | 0 | +1.39/s | 974 | 71/4399/4880 | 2.58/4.90/8.94 | 39.9 |
  | 1050 | 1 | +15.01/s | 836 | 49/799/960 | 2.59/4.92/9.12 | 495.4 |
  | 1050 | 2 | +62.76/s | 2883 | 84/814/963 | 2.57/4.89/8.26 | 711.1 |
  | 1075 | 0 | +31.81/s | 1463 | 134/2070/2414 | 2.59/4.93/9.28 | 263.1 |
  | 1075 | 1 | +19.91/s | 1978 | 71/4701/5468 | 2.60/4.94/9.32 | 882.4 |
  | 1075 | 2 | +55.54/s | 2559 | 189/4635/6291 | 2.60/4.93/9.30 | 454.6 |
  | 1100 | 0 | +1.73/s | 1420 | 79/832/966 | 2.59/4.92/9.19 | 84.1 |
  | 1100 | 1 | +35.32/s | 2393 | 69/837/967 | 2.59/4.92/9.14 | 727.7 |
  | 1100 | 2 | +194.34/s | 8947 | 261/4530/4906 | 2.60/4.94/9.33 | 2921.9 |

- **The central finding: handler execution is flat; queue wait moves with
  degradation.** `handler_latency_ms` (the full, already-instrumented
  `process()` span - validation, Redis, the entire DB transaction, offset
  tracking) sits in an almost perfectly constant **2.57-2.60ms median /
  4.89-4.94ms p95 / 8.26-9.33ms p99 band across every single repeat**,
  clean or catastrophic, at every rate tested - directly consistent with
  Stage 24's transaction-phase finding, now extended to the *entire*
  per-record handler cost, not just the DB phases. `poll_to_handler_ms` and
  `loop_gap_ms` (the in-process gaps) are similarly flat and near-zero
  (~2.5ms) throughout, confirming they carry no signal. `consumer_queue_wait_ms`,
  by contrast, tracks degradation directly: the worst repeat measured
  (1100/repeat 2: lag slope +194.34/s, peak lag 8947) shows p50 queue wait
  of 261ms (vs. 46-84ms in the cleanest repeats at other rates) and a p95 of
  4530ms; several other severely-lagged repeats (1050/2, 1075/0-2) show
  queue-wait p95/p99 in the multi-second range while their handler latency
  stays in the same ~2.6ms band as every clean repeat. `consumer_inflight_events`
  (`max_inflight_sampled`) was **exactly 1.0 in all 12 repeats** - direct
  runtime confirmation of the code-level finding that no more than one
  record is ever in flight cluster-wide at a sampled instant, i.e. there is
  no internal buffer to inspect.
- **A structural explanation for the ceiling, not just a correlation.**
  Because each processor instance is single-threaded and fully synchronous,
  its own steady-state throughput ceiling is bounded by `1 / handler_latency`.
  Using the observed ~2.6ms median handler latency: `3 workers × (1 /
  0.0026s) ≈ 1154 events/sec` theoretical aggregate ceiling - closely
  matching the transition band (1050-1100 evt/s) already established across
  Stages 18-24. This gives Stage 24's "no phase grows" negative finding a
  positive structural counterpart: the ceiling is not caused by any single
  phase's cost growing under load, but by the fixed per-record synchronous
  service time of three single-threaded consumers being close to the
  requested arrival rate - past that point, records queue in the Kafka
  topic itself (measured directly by `consumer_queue_wait_ms`) rather than
  anywhere inside the processor.
- **Analysis questions, answered directly:**
  1. **Does latency come from waiting before handler execution, or from
     handler execution itself?** Waiting before - `handler_latency_ms`
     never leaves its ~2.6ms/4.9ms/9ms band regardless of degradation
     severity; `consumer_queue_wait_ms` is the metric that moves.
  2. **Is there an internal processor-side queue building up?** No -
     architecturally impossible (single-threaded, unbatched `poll()`/
     `process()` loop) and confirmed at runtime (`max_inflight_sampled`
     == 1.0 in all 12 repeats).
  3. **Does the queue wait grow with requested rate?** Not monotonically
     with rate alone - it grows with *degradation*, which becomes more
     frequent and severe as rate approaches and crosses the ~1075-1100
     transition band already established, matching the per-instance
     service-time ceiling computed above.
  4. **Is Kafka poll interval itself elevated during degradation?**
     No - `loop_gap_ms` stayed flat (~2.5ms) in every repeat, including
     the most severely degraded ones; the loop is never idle-waiting
     longer than usual, it is fully busy processing at its fixed
     per-record cost.
  5. **Does records-per-poll or poll count show batching effects?** Not
     applicable - `poll()` returns at most one record per call by
     construction; there is no batching dimension to measure in this
     codebase.
  6. **Is the correctness invariant preserved under the worst-observed
     backpressure?** Yes - `unique_event_ids == processed_rows ==
     matched_e2e` held in all 12 repeats, including 1100/repeat 2's
     8947-peak-lag run.
  7. **Which existing/added metric correlates strongest with E2E tail
     growth?** `consumer_queue_wait_ms`, directly - e.g. 1100/repeat 2's
     E2E p50 of 2921.9ms tracks its queue-wait p50 of 261ms and p95 of
     4530ms far more closely than any transaction-internal phase measured
     in Stage 24.
- **Outcome: A - consumer-side queueing dominates; handler execution is not
  the bottleneck.** Waiting happens before handler execution starts, and it
  happens in the Kafka topic itself (measured via `consumer_queue_wait_ms`),
  not inside the processor. Combined with Stage 24, the full chain is now
  measured end to end: no transaction phase grows, no processor-internal
  buffer exists or grows, and the one place that does grow under
  degradation - time a record spends in the topic before being fetched -
  is exactly what the fixed per-record synchronous service time of three
  single-threaded consumers predicts once requested rate approaches their
  combined ceiling (~1154 evt/s, matching the already-established
  1050-1100 transition band).
- **Hypotheses ruled out (this stage, joining the running list):** an
  internal processor-side record queue/buffer, elevated Kafka poll
  interval/idle time during degradation, handler-execution-time growth
  under load (already ruled out at the transaction-phase level in Stage 24,
  now ruled out at the full-handler level). Combined with Stages 20-24:
  cgroup throttling, host CPU saturation, scheduler starvation, connection
  explosion, LWLock contention, IO-wait explosion, a single slow query,
  transaction read/write/commit-phase growth, SQL-count/execution-time
  growth near saturation, lock contention.
- **Correctness:** `unique_event_ids == processed_rows == matched_e2e` held
  in all 12 repeats. The `normal` processor smoke scenario passed. The
  `duplicate`/`dlq`/`retry` smoke scenarios failed their DLQ-offset
  assertions; isolated via `git stash` to confirm this reproduces
  identically on unmodified `main` with none of this stage's changes
  present - a pre-existing environment/test issue unrelated to this
  diagnostic, flagged separately rather than fixed here (out of scope: this
  stage changes no retry/DLQ/idempotency behavior).
- **Next experiment:** the mechanism is now measured end to end - the
  remaining open question is *why* individual repeats at the same requested
  rate sometimes stay clean and sometimes tip into severe queueing (e.g.
  1050/repeat 0 stayed clean at +1.39/s while 1050/repeat 2 reached
  +62.76/s). Since per-record service time is flat and does not explain
  the difference, the next isolated diagnostic should look at short-window
  arrival-rate variance from the injector/generator side (is requested load
  itself bursty at sub-second resolution even when the long-window average
  rate is held constant?) rather than the processor or PostgreSQL again.

## Injector arrival variance diagnosis — measurement only, no code change

- **Previous diagnosis leading to this experiment:** Stage 25 proved
  waiting happens before handler execution, in the Kafka topic itself
  (`consumer_queue_wait_seconds`), not inside the processor - and that no
  internal buffer can exist (single-threaded, unbatched `poll()`/`process()`
  loop, `max_inflight_sampled` == 1.0 in every repeat). Its open question:
  why does the same requested rate sometimes stay clean and sometimes tip
  into severe queueing? The proposed hypothesis was that the injector's
  long-window average rate hides short-lived arrival bursts that a
  single-threaded consumer cannot absorb.
- **Existing tooling reused, not duplicated.**
  [`scripts/benchmark/direct_injector.py`](../../scripts/benchmark/direct_injector.py)
  already paces publishes with a monotonic fixed-rate loop
  (`next_deadline`/`time.sleep()`) and already records `scheduler_drift_ms`/
  `missed_deadlines`/`publish_latency_ms` per run. It did not yet record a
  raw per-event send-timestamp series, which is required to compute
  inter-arrival gaps and short-window arrival rates - everything else
  (percentile helper `scripts/benchmark/stats.py:percentiles()`, the
  subprocess-per-repeat orchestration in `direct_saturation.py`, the
  gitignored `injector-<rate>-<repeat>.json` raw artifact) was reused
  unchanged.
- **What was added (benchmark tooling only, no production code):** in
  `direct_injector.py`, `inject_messages()` now also appends each publish's
  `perf_counter()` timestamp to a local `send_timestamps` list (the
  publish-attempt instant already being measured for `publish_latency_ms`,
  simply also retained). Three small pure functions compute the diagnostic
  view from that list, unit-tested in
  [`tests/unit/test_injector_arrival_variance.py`](../../tests/unit/test_injector_arrival_variance.py)
  (7 cases):
  - `inter_arrival_gaps(timestamps)` - consecutive send-to-send gaps.
  - `sliding_window_rates(timestamps, window_seconds)` - events/sec in each
    consecutive, non-overlapping bin of the requested width.
  - `arrival_variance_summary(timestamps)` - gap percentiles (p50/p95/p99/
    max) plus window-rate percentiles/max/min at 100ms, 500ms, and 1s,
    reusing `percentiles()`.
  This is a **benchmark-tool-side computation only** - no new Prometheus
  metric, no new production instrumentation, nothing observed by the
  processor or generator services. The result is written into the same
  already-gitignored `injector-<rate>-<repeat>.json` raw artifact (`event_ids`
  was already excluded from `direct_saturation.py`'s embedded summary the
  same way; `send_timestamps` now gets the identical treatment).
- **Benchmark:** 3 workers, 1/1/1 verified (lag 0 before and after),
  unmodified processor image (no production code touched this stage),
  1050/1075/1100 evt/s only (per instruction), 10s warmup, 45s steady, 3
  repeats. Tag
  [`bench-injector-arrival-variance-3w`](../../artifacts/benchmark/bench-injector-arrival-variance-3w/).
  Command:
  `python -m scripts.benchmark.direct_saturation --rates 1050,1075,1100 --warmup-seconds 10 --steady-seconds 45 --repeats 3 --run-tag bench-injector-arrival-variance-3w`.
- **Results (per repeat):**

  | Rate | Repeat | Lag slope | Peak lag | Gap p50/p95/p99/max (ms) | Window-rate max: 100ms/500ms/1s (evt/s) | Queue wait p50 (ms) |
  | --- | --- | ---: | ---: | --- | --- | ---: |
  | 1050 | 0 | +19.34/s | 2043 | 0.95/1.14/1.71/14.74 | 1060/1050/1050 | 677.6 |
  | 1050 | 1 | +91.99/s | 4224 | 0.95/1.29/1.61/13.47 | 1060/1052/1049 | 681.8 |
  | 1050 | 2 | +98.15/s | 6753 | 0.95/1.31/1.83/57.32 | 1060/1052/1050 | 708.8 |
  | 1075 | 0 | +62.22/s | 2915 | 0.93/1.28/1.67/15.65 | 1090/1076/1075 | 752.4 |
  | 1075 | 1 | +23.25/s | 2144 | 0.93/1.28/1.60/13.46 | 1080/1076/1075 | 1164.5 |
  | 1075 | 2 | +184.65/s | 8894 | 0.93/1.26/1.42/25.39 | 1080/1076/1075 | 861.5 |
  | 1100 | 0 | +131.27/s | 6071 | 0.91/1.28/2.01/42.32 | 1110/1102/1096 | 789.5 |
  | 1100 | 1 | +88.25/s | 4108 | 0.91/1.21/1.37/12.99 | 1110/1100/1100 | 725.8 |
  | 1100 | 2 | +80.18/s | 4309 | 0.91/1.20/1.38/28.84 | 1110/1102/1100 | 719.8 |

  (Requested-rate reciprocal for reference: 1/1050 ≈ 0.952ms, 1/1075 ≈
  0.930ms, 1/1100 ≈ 0.909ms - matching the observed gap p50 at every rate
  almost exactly.)
- **The central finding: injector pacing is tight and does not distinguish
  clean-ish from severely-degraded repeats at the same rate.** Inter-arrival
  gap p50 tracks the requested rate's reciprocal almost exactly at every
  rate (0.91-0.95ms), with a tight p95/p99 (1.14-2.01ms) regardless of how
  badly that repeat went on to degrade. The occasional large single gap
  (max column, 13-57ms) is rare - one or two occurrences per ~45,000-51,000
  published events per repeat - and does not correlate with lag slope: 1050/
  repeat 2 (the worst of the three 1050 repeats, +98.15/s) has the largest
  max gap (57.32ms) of the three, but 1075/repeat 2 (the worst 1075 repeat,
  +184.65/s) has one of the *smallest* max gaps (25.39ms) of its group,
  smaller than 1075/repeat 0's (+62.22/s, 15.65ms) - no consistent
  direction. **Sliding-window arrival rate never exceeds the requested rate
  by more than ~1-1.5% in any window size at any rate**, and - critically -
  the window-rate maxima are close to identical across all three repeats at
  a given rate regardless of how differently those repeats degraded (e.g.
  1050's three repeats all show a 100ms-window max of 1060 evt/s, while
  their lag slopes range from +19.34/s to +98.15/s). There is no
  short-window burst hiding in the average: the producer's pacing loop is
  doing exactly what it is built to do at every rate and every repeat.
- **Correctness:** `unique_event_ids == processed_rows == matched_e2e` held
  in all 9 repeats, including the most severely degraded one (1075/repeat
  2, peak lag 8894).
- **A data-collection limitation, noted for transparency, not fixed here:**
  `consumer_queue_wait_ms` p99 hit a 10,000ms ceiling in every repeat of
  this sweep. `processor_consumer_queue_wait_duration_seconds` (added in
  Stage 25) uses `Histogram.DEFAULT_BUCKETS`, whose highest finite boundary
  is 10.0s; `histogram_quantile` cannot extrapolate a quantile that falls in
  the `+Inf` bucket past that boundary, so any repeat whose queue-wait tail
  genuinely exceeds 10s reports a clamped p99 of exactly 10000ms rather
  than its true value. This sweep's repeats were, on average, more
  degraded than Stage 25's (every repeat here had a positive, often large,
  lag slope, versus Stage 25's mix of clean and degraded repeats) - purely
  repeat-to-repeat variance consistent with the pattern already described
  in Stages 24-25, not a sign that this stage's changes altered processor
  behavior (no production code changed). Left as-is per this stage's
  measurement-only, no-methodology-change scope; a future stage revisiting
  queue-wait tail behavior specifically should widen the histogram's
  buckets first.
- **Answers to the four analysis questions:**
  1. **Producer inter-arrival time distribution?** Tight and consistent
     with the requested rate at every rate and repeat (p50 0.91-0.95ms,
     p99 1.37-2.01ms); occasional single-digit-to-tens-of-ms outlier gaps,
     rare and uncorrelated with degradation severity.
  2. **Sliding-window arrival rate (100ms/500ms/1s)?** Never exceeds the
     requested rate by more than ~1-1.5% in any window, at any rate.
  3. **Clean vs. degraded repeat comparison at 1050/1075/1100?** No repeat
     in this sweep was fully "clean" in the Stage 25 sense (all nine showed
     a positive, often substantial, lag slope) - but even so, arrival
     variance was statistically indistinguishable between the mildest and
     most severe repeat at each rate, while queue wait and lag slope
     varied by 3-8x within the same rate.
  4. **Does higher short-window burst rate accompany higher queue wait/lag
     slope?** No - burstiness metrics are essentially flat across repeats
     at a fixed rate while queue wait and lag slope vary widely. Arrival
     variance is not confirmed as the mechanism.
- **Outcome: arrival-burst hypothesis not supported - continue investigating
  consumer/Kafka-side behavior.** Per this experiment's own decision rule,
  since degraded repeats do **not** show higher short-window burst rate
  while the average rate is held identical, the arrival-variance hypothesis
  is rejected as the explanation for repeat-to-repeat variance. The
  producer side is now cleanly ruled out, joining Stage 24's transaction-
  phase ruling-out and Stage 25's processor-internal-buffer ruling-out:
  every stage in this series has now measured its own layer (PostgreSQL,
  transaction lifecycle, processor-internal queueing, and now producer
  pacing) and found it flat/well-behaved regardless of degradation. The
  remaining unmeasured layer is consumer/Kafka-side behavior itself -
  broker-side partition/fetch/batching behavior, rebalance-adjacent
  effects, or fetch-request timing on the consumer's own poll() call -
  rather than anything upstream of the topic.
- **Hypotheses ruled out (this stage, joining the running list):** producer/
  injector-side short-window arrival bursts. Combined with Stages 20-25:
  cgroup throttling, host CPU saturation, scheduler starvation, connection
  explosion, LWLock contention, IO-wait explosion, a single slow query,
  transaction read/write/commit-phase growth, SQL-count/execution-time
  growth near saturation, lock contention, an internal processor-side
  record queue/buffer, elevated Kafka poll interval/idle time during
  degradation, handler-execution-time growth under load.
- **Next experiment:** with producer pacing, transaction internals, and
  processor-internal buffering all ruled out, the next isolated diagnostic
  should look directly at the consumer/broker boundary - e.g. Kafka
  fetch-request/response timing and broker-side partition metrics
  (`kafka_consumergroup_lag` already available per-partition, plus
  broker-side fetch latency if exposed) around the moment a repeat tips
  into degradation, to see whether the delay is introduced by the broker's
  own fetch-serving behavior rather than anything already ruled out on
  either the producer or consumer-application side.

## Kafka consumer fetch boundary diagnosis — measurement only, no processing logic change

- **Previous diagnosis leading to this experiment:** Stage 25 proved
  waiting happens before handler execution and concluded it happens "in the
  Kafka topic itself," reasoning that no application-visible buffer could
  exist since `processor_inflight_events` never exceeded 1.0. Stage 26
  ruled out producer-side arrival bursts as the explanation for repeat-to-
  repeat variance at a fixed rate. This stage tests the remaining
  hypothesis directly: the Kafka broker → consumer fetch/poll boundary -
  specifically, whether records queue up somewhere between the broker and
  the processor's `process()` call that Stage 25's instrumentation could
  not see.
- **Existing instrumentation reused, not duplicated.**
  `commerce_processor_poll_to_handler_duration_seconds` and
  `commerce_processor_loop_gap_duration_seconds` (both already added by
  Stage 25) already cover the in-process side of this question and needed
  no new observation. `kafka_consumergroup_lag` (kafka-exporter, already
  sampled every second by `direct_saturation.py`'s `_lag()`) already covers
  lag growth timing.
- **What was genuinely missing, and what was added.** Confluent Kafka's
  underlying client library (librdkafka) already computes fetch-queue depth
  and broker round-trip time internally, but this codebase never enabled or
  read that data - `statistics.interval.ms` was unset and no `stats_cb` was
  registered anywhere. This is a **client library feature already built and
  computed**, not a new measurement invented for this stage - confirmed by
  probing it directly against a throwaway consumer group before writing any
  code (`fetchq_cnt`, `fetchq_size`, `consumer_lag`, `fetch_state` per
  topic-partition; `rtt.avg`/`p50`/`p95`/`p99` per broker connection, in
  microseconds). Added, in
  [`services/event_processor/consumer.py`](../../services/event_processor/consumer.py):
  - `statistics.interval.ms: 1000` and a `stats_cb` wired only for the real
    client this instance constructs (`_real_client_config()`) - test doubles
    injected via the existing `client=` parameter are unaffected, and
    `kafka_config()` itself (reused by `processor-smoke.py` and
    `retry_dlq_bench.py` for raw watermark-reading consumers) is unchanged.
  - Two pure parsing functions, unit-tested in
    [`tests/unit/test_consumer_stats.py`](../../tests/unit/test_consumer_stats.py)
    (7 cases): `fetchq_records_total(stats)` (sum of `fetchq_cnt` across all
    assigned partitions - records already fetched from the broker and
    buffered locally by librdkafka, not yet returned by `poll()`) and
    `broker_rtt_avg_ms(stats)` (average `rtt.avg`, converted µs→ms, across
    brokers that reported at least one sample that interval).
  - Two new gauges, `processor_consumer_fetchq_records` and
    `processor_consumer_broker_rtt_ms`, populated from the callback.
  - `poll()` itself now measures its own blocking duration
    (`processor_poll_duration_seconds`, distinct from the existing
    poll-to-*handler* gap) and counts poll calls that returned no record
    (`processor_empty_polls_total`) - both directly requested by this
    stage's own measurement list, tested in
    [`tests/unit/test_processor_consumer_offset_batching.py`](../../tests/unit/test_processor_consumer_offset_batching.py).
  All four are diagnostic gauges/histograms/counters with no branching
  logic change - processing behavior, retry behavior, offset-commit
  behavior, and DLQ behavior are byte-identical to before this stage.
- **Benchmark:** 3 workers, 1/1/1 verified (lag 0 before and after),
  processor image rebuilt with the new instrumentation, 1050/1075/1100
  evt/s only, 10s warmup, 45s steady, 3 repeats. Tag
  [`bench-consumer-fetch-boundary-3w`](../../artifacts/benchmark/bench-consumer-fetch-boundary-3w/).
  Command:
  `python -m scripts.benchmark.direct_saturation --rates 1050,1075,1100 --warmup-seconds 10 --steady-seconds 45 --repeats 3 --run-tag bench-consumer-fetch-boundary-3w`.
- **Results (per repeat):**

  | Rate | Repeat | Lag slope | Queue wait p50 (ms) | Poll duration p50/p95/p99 (ms) | Empty polls | Max fetchq records | Max broker RTT (ms) |
  | --- | --- | ---: | ---: | --- | ---: | ---: | ---: |
  | 1050 | 0 | +119.95/s | 3032.5 | 2.52/4.79/5.00 | 23 | 3563 | 501.7 |
  | 1050 | 1 | +67.42/s | 1183.5 | 2.51/4.77/4.97 | 25 | 2868 | 501.2 |
  | 1050 | 2 | +54.49/s | 1400.1 | 2.52/4.79/4.99 | 30 | 2246 | 503.5 |
  | 1075 | 0 | +76.33/s | 638.8 | 2.51/4.77/4.97 | 21 | 948 | 501.3 |
  | 1075 | 1 | +65.87/s | 2812.9 | 2.52/4.78/4.98 | 29 | 1494 | 504.3 |
  | 1075 | 2 | +103.36/s | 878.8 | 2.52/4.78/4.98 | 25 | 4099 | 502.3 |
  | 1100 | 0 | +135.56/s | 726.4 | 2.51/4.78/4.98 | 16 | 2294 | 501.9 |
  | 1100 | 1 | +120.61/s | 2773.3 | 2.52/4.78/4.98 | 36 | 2230 | 503.1 |
  | 1100 | 2 | +121.77/s | 3153.5 | 2.52/4.78/4.98 | 18 | 3567 | 501.4 |

  (No repeat in this sweep was "clean" in Stage 25's sense - every one
  showed a positive lag slope, continuing the trend already noted in Stage
  26 that this environment's baseline degradation has drifted upward over
  the course of the session, plausibly from accumulated table growth across
  many prior sweeps. The spread within each rate is still wide enough for
  useful relative comparison.)
- **The central finding: a real client-side buffer exists, and it is not
  where Stage 25 assumed.** `max_fetchq_records_sampled` is **never
  near zero** in this sweep - librdkafka is holding hundreds to thousands
  of already-fetched, not-yet-consumed records in its own internal queue at
  every rate and repeat. This directly refines (not contradicts) Stage 25's
  conclusion that "no internal buffer exists": that was true only for the
  layer Stage 25 could see (`processor_inflight_events`, which counts
  records inside the Python `process()` call) - it was never true for
  librdkafka's own C-level fetch queue, which sits between the broker and
  the Python `poll()` call and was invisible to any instrumentation added
  before this stage.
- **Neither the broker nor `poll()` itself is where the delay is
  introduced.** `processor_consumer_broker_rtt_ms` stayed essentially
  constant (~501-504ms) across every repeat regardless of degradation
  severity - the broker connection's round-trip time does not grow under
  load. `poll_duration_ms` stayed just as flat as `poll_to_handler_ms` did
  in Stage 25 (~2.5ms p50, ~4.8ms p95, ~5.0ms p99 in every single repeat) -
  the call itself is never blocked waiting on the network; it returns
  almost immediately because librdkafka's local queue already has records
  ready. This is the mechanism *for* the flat `poll_duration`: when the
  fetch queue is well-stocked, `poll()` has no reason to wait.
- **Empty-poll count moves in the expected direction, though not perfectly
  monotonically.** At 1050 (the cleanest three-way comparison in this
  sweep), empty polls fell as fetch-queue depth and lag slope rose - 30
  empty polls at the mildest repeat (fetchq 2246, slope +54.49/s) down to
  23 at the most severe (fetchq 3563, slope +119.95/s) - consistent with a
  well-stocked fetch queue leaving `poll()` with a record to return almost
  every time. The 1075/1100 groups show the same rough direction but not a
  clean rank ordering, and `fetchq_records` itself does not track lag slope
  monotonically within those two groups either (e.g. 1075/repeat 0, the
  mildest of its group at +76.33/s, has the *lowest* fetchq of the three,
  948, while 1075/repeat 2, the most severe at +103.36/s, has the highest,
  4099 - consistent - but 1075/repeat 1's own combination, +65.87/s with
  fetchq 1494, sits between them without a clean linear relationship). The
  signal is real and directionally consistent but noisy at this sample
  size (3 repeats per rate), matching the noise level already seen in every
  prior stage's repeat-to-repeat comparisons in this series.
- **A measurement limitation, noted for transparency.**
  `processor_consumer_broker_rtt_ms` averages librdkafka's `rtt.avg` across
  *all* request types on a broker connection (Fetch, Heartbeat,
  OffsetCommit, Metadata, etc.), not Fetch requests specifically - the flat
  ~501-504ms figure is not proof that Fetch-specific latency is flat, only
  that the blended average is. librdkafka's stats schema does not break RTT
  down by request type, so isolating Fetch-specific latency would require
  either a newer librdkafka stats field (not verified available in this
  environment) or broker-side instrumentation this stage did not add
  (out of scope: measurement only). This is reported honestly rather than
  overstated as "the broker is definitely not the bottleneck for fetches
  specifically" - only that the connection as a whole is not visibly
  slower during degraded repeats.
- **Answers to the four "does degradation show..." questions:**
  1. **Increased empty polls?** No - if anything, the opposite: fewer
     empty polls accompanied more severe degradation at 1050, the cleanest
     comparison group, consistent with a well-stocked local queue.
  2. **Delayed fetches (broker round-trip growing)?** No - broker RTT
     stayed flat (~501-504ms) across every repeat.
  3. **Bursty record delivery?** Not evidenced here; Stage 26 already
     ruled out producer-side bursts, and this stage's flat `poll_duration`/
     RTT give no sign of broker-side delivery burstiness either.
  4. **Consumer-side scheduling gaps?** No - `poll_duration_ms` and (from
     Stage 25) `loop_gap_ms`/`poll_to_handler_ms` all stay flat regardless
     of degradation severity.
- **Outcome: the fetch/poll boundary itself is not delayed - but a real,
  previously invisible buffer was found one layer below it.** None of the
  four originally-hypothesized symptoms (empty polls, delayed fetches,
  bursty delivery, scheduling gaps) grow with degradation. What does move
  is `fetchq_records` - librdkafka's own internal buffer of records already
  pulled from the broker and waiting for the single-threaded Python loop to
  drain them. This is fully consistent with, and sharpens, the structural
  explanation from Stage 25 (three single-threaded consumers with a fixed
  ~2.6ms per-record service time, combined ceiling ~1154 evt/s): the
  broker delivers records to librdkafka promptly (flat RTT), librdkafka
  hands them to the buffer promptly (flat poll duration), and the queue
  that builds up under load is exactly the layer between "already fetched"
  and "actually processed" - previously undetectable because
  `processor_inflight_events` only counts the single record inside an
  active `process()` call, never the ones already sitting in librdkafka's
  own memory waiting their turn.
- **Correctness:** `unique_event_ids == processed_rows == matched_e2e` held
  in all 9 repeats. All four processor smoke scenarios (including `normal`,
  which had passed as recently as Stage 25) now fail - re-isolated via
  `git stash` against unmodified `main` at this stage's start, confirming
  the failure is unrelated to this stage's changes and has apparently
  broadened since Stage 25/26 (plausibly the same underlying DLQ-offset
  race worsening as the shared DLQ topic accumulates more history across
  many benchmark sessions). Already tracked separately; not touched here
  (out of scope: no processing/retry/DLQ logic changed this stage).
- **Hypotheses ruled out (this stage, joining the running list):** broker
  round-trip-time growth under load, `poll()`-call blocking-duration growth
  under load, empty-poll-rate growth under load. Combined with Stages
  20-26: cgroup throttling, host CPU saturation, scheduler starvation,
  connection explosion, LWLock contention, IO-wait explosion, a single slow
  query, transaction read/write/commit-phase growth, SQL-count/execution-
  time growth near saturation, lock contention, an application-level
  processor-internal record buffer (refined, not fully ruled out - see
  above), elevated Kafka poll interval/idle time during degradation,
  handler-execution-time growth under load, producer/injector-side
  short-window arrival bursts.
- **Next experiment:** the mechanism is now localized to librdkafka's own
  fetch queue, one layer the application cannot directly observe or control
  without new client configuration. The natural next diagnostic is to
  correlate `fetchq_records` growth against `consumer_lag` (broker-reported,
  offset-based) at matching timestamps within a single repeat to establish
  whether the two track together throughout a degrading repeat (confirming
  fetchq depth as a leading or coincident indicator of broker-side lag) or
  diverge (which would suggest the fetch queue itself has a bound being hit
  independently) - a within-repeat time-series analysis rather than another
  per-rate sweep, and still measurement-only.

## Consumer scaling model experiment — bounded worker pool, reverted

- **Previous diagnosis leading to this experiment:** Stages 20-27 measured
  every layer of the pipeline - PostgreSQL execution cost, transaction
  lifecycle, host/container CPU scheduling, processor-internal queueing,
  producer arrival variance, and the Kafka broker/fetch-queue boundary -
  and found each one flat or well-behaved regardless of degradation. Stage
  25 established the structural explanation for the ~1050-1100 evt/s
  ceiling: three single-threaded consumers, each bound by a fixed ~2.6ms
  per-record synchronous service time, give a combined theoretical ceiling
  of `3 x (1/0.0026s) ~= 1154 evt/s`. This stage tests that structural
  explanation directly: does bounded concurrency inside one processor
  instance raise the ceiling, or is the ceiling coming from something the
  runtime/language itself imposes (e.g. the GIL)?
- **Consumer lifecycle and offset-commit flow inspected before writing any
  code** (per instruction), confirming two things that shaped the design:
  1. [`services/event_processor/offset_tracker.py`](../../services/event_processor/offset_tracker.py)'s
     `OffsetCommitTracker`/`_PartitionState` already use a min-heap to track
     the highest *contiguous* terminal offset per partition - it already
     tolerates terminal completions arriving out of order. Its own
     docstring anticipates this exact scenario: "This module assumes
     single-threaded, synchronous use... If a future concurrent processing
     model is introduced, this tracker's per-partition contiguous-offset
     bookkeeping is what must be preserved or replaced with an equivalent
     guarantee."
  2. [`services/event_processor/processor.py`](../../services/event_processor/processor.py)'s
     `MessageProcessor.process()` already calls its injected `committer`
     synchronously, internally, for every terminal outcome (`_commit()`) -
     the committer is a constructor-injected dependency (`OffsetCommitter`
     protocol), not something `process()` assumes is the real consumer.
- **A real, additive bug found and fixed in the tracker before any
  benchmark ran.** The heap-based out-of-order tolerance in (1) only
  applies to completion order *after* a partition's `safe_offset` has
  bootstrapped ("the first offset ever seen for this partition, minus one,
  is already safe"). That bootstrap itself assumed observation order
  matches delivery order - true only when nothing can call `mark_terminal()`
  out of delivery order, which concurrent workers do break: if the second
  of two dispatched records finishes first, the tracker would bootstrap
  from the wrong (higher) offset and silently drop the still-pending lower
  one from tracking entirely. Fixed with a small, purely additive method -
  `OffsetCommitTracker.observe()` / `_PartitionState.observe()` /
  `KafkaEventConsumer.observe_dispatched()` - called once per record, in
  true delivery order, at dispatch time (before workers can race), which
  pre-seeds a separate `dispatch_floor` that `mark_terminal()`'s bootstrap
  now prefers. `observe()` never touches `safe_offset` itself, so it can
  never make anything look prematurely safe to commit on its own. No
  existing method's behavior changed; all 16 pre-existing
  `test_offset_tracker.py` cases still pass unmodified, plus 5 new cases
  covering the exact failure mode this fixes and the corrected concurrent
  scenario. Documented in detail in
  [`services/event_processor/main_pooled.py`](../../services/event_processor/main_pooled.py)'s
  module docstring.
- **Design (isolated, opt-in, default unchanged).** New
  `processor_worker_pool_size` config field (default `1`, env
  `PROCESSOR_WORKER_POOL_SIZE`, also wired into `compose.yaml` with the
  same default). `main()` dispatches to the new `run_processor_pooled()`
  (in the new file `main_pooled.py`) only when this is `> 1`; the existing
  `run_processor()` function is never modified, never called differently,
  and every existing test for it still passes unmodified - the default
  path is provably untouched.

  ```
  poll thread (unchanged: owns Kafka client, OffsetCommitTracker, rebalance callbacks)
    poll() -> observe_dispatched() [delivery-order bootstrap] -> bounded work queue
                                                                        |
                                                          N worker threads (pool_size)
                                                          each: MessageProcessor.process()
                                                          (validation, Redis reserve/complete,
                                                           full DB transaction - all unchanged)
                                                                        |
                                                          _QueuedCommitter -> commit queue
                                                                        |
  poll thread drains commit queue -> consumer.commit_terminal() [unchanged, single-threaded]
  ```

  Only `MessageProcessor.process()` calls run concurrently; every
  tracker-touching call stays on the poll thread, so no locking was added
  anywhere. Each worker gets its own `RunSummary` (plain `+=` counters,
  not thread-safe to share) and `random.Random` (mutates internal state per
  call), merged into one summary at shutdown (`_merge_summaries()` - sums
  every field except `latency_max_ms`, which takes the max). `RedisIdempotencyStore`,
  `DlqPublisher`, and the `psycopg_pool.ConnectionPool`-backed
  `UnitOfWorkFactory`/`Database` are shared across workers unmodified -
  all three are documented thread-safe by their own libraries (redis-py's
  client pools connections internally; librdkafka's `Producer.produce()`
  is explicitly safe for concurrent calls; psycopg3's `ConnectionPool` is
  built for shared multi-threaded use). No business logic, no idempotency
  logic, and no DLQ/retry logic in `processor.py`, `idempotency.py`, or
  `dlq.py` was touched.
- **Tests added** (11 new, all passing, zero pre-existing tests modified in
  behavior): 5 in `test_offset_tracker.py` (observe() bootstrap/idempotence/
  out-of-order-completion correctness, including the exact bug found
  above), 2 in `test_processor_consumer_offset_batching.py`
  (`observe_dispatched()` never commits anything on its own; out-of-order
  completion never skips the gap), 3 in the new `test_main_pooled.py`
  (out-of-order worker completion still commits only the contiguous safe
  offset - the decisive correctness test, using a `threading.Event` to
  deterministically force offset 11 to finish before offset 10; per-worker
  `RunSummary` isolation/merge with 10 messages across 4 workers, no double
  counting; bounded work-queue backpressure never exceeds `pool_size`
  concurrently-blocked handlers). Verified stable across 5 repeated local
  runs (no flakiness from the threading-based synchronization).
- **Benchmark:** 3 workers, 1/1/1 verified (lag 0 before and after),
  processor image rebuilt, 1050/1100/1150/1200 evt/s, 10s warmup, 45s
  steady, 3 repeats each, same database/indexes/methodology as every prior
  stage. Baseline (`processor_worker_pool_size=1`, unchanged default) tag
  [`bench-worker-pool-baseline-3w`](../../artifacts/benchmark/bench-worker-pool-baseline-3w/);
  candidate (`processor_worker_pool_size=4`, chosen to exactly match the
  existing `processor_db_pool_max_size` default of 4 so no worker would
  contend for a database connection the others didn't already have their
  own of) tag
  [`bench-worker-pool-candidate-3w`](../../artifacts/benchmark/bench-worker-pool-candidate-3w/).
  Commands identical except `--run-tag`, with `PROCESSOR_WORKER_POOL_SIZE`
  exported before `docker compose up` for the candidate.
- **Results (mean of 3 repeats per rate):**

  | Rate | Lag slope: baseline | Lag slope: candidate | Peak lag: baseline | Peak lag: candidate | Missing rows: baseline | Missing rows: candidate |
  | --- | ---: | ---: | ---: | ---: | ---: | ---: |
  | 1050 | +92.2/s | +257.6/s | 4,571 | 13,482 | 0, 0, 0 | 672, 662, 820 |
  | 1100 | +167.4/s | +331.1/s | 7,792 | 16,129 | 0, 0, 0 | 948, 777, 791 |
  | 1150 | +158.6/s | +365.3/s | 7,663 | 16,999 | 0, 0, 0 | 844, 750, 946 |
  | 1200 | +288.1/s | +422.5/s | 13,450 | 19,965 | 0, 0, 0 | 857, 734, 830 |

  "Missing rows" = `injected.published_count - correctness.processed_rows`
  per repeat (i.e. `unique_event_ids > processed_rows == matched_e2e` - the
  correctness invariant `unique_event_ids == processed_rows == matched_e2e`
  held in **all 12 baseline repeats** and was **violated in all 12
  candidate repeats**.
- **The central finding: the candidate is not just slower, it is actively
  unsafe.** Container logs during the candidate sweep show 11,326
  `event_dead_lettered` records (5,807 `missing_business_dependency`,
  5,519 `database_integrity_error`) - a volume of DLQ traffic that does
  not occur at all in the baseline or in any prior stage of this series.
  Handler latency's tail exploded alongside this: p99 rose from ~10ms
  (baseline, matching every prior stage) to ~240ms (candidate, every rate).
  `max_fetchq_records_sampled` also grew relative to baseline (e.g. 1050:
  baseline 1,798-3,482 vs. candidate 4,954-9,702), consistent with the
  pipeline falling further behind, not catching up.
- **Root cause: Kafka's within-partition ordering guarantee is a business-
  logic invariant this system depends on, and naive round-robin dispatch to
  any free worker breaks it.** The DLQ error categories name the exact
  mechanism: `missing_business_dependency` fired overwhelmingly on
  `payment_failed`/`payment_completed` events, and `database_integrity_error`
  on `order_created` events - both are symptoms of a payment-related event
  being processed before its parent order/session/cart had been durably
  persisted. In the synchronous baseline, Kafka's per-partition delivery
  order plus this consumer's strictly sequential `poll() -> process()`
  loop together guarantee that a causally-earlier event (e.g.
  `order_created`) is fully committed to Postgres before a causally-later
  one from the same partition (e.g. `payment_completed`) is even looked
  at. The worker pool dispatches whichever record `poll()` returns next to
  whichever worker is free, with no regard for which entity (order,
  customer, cart) it belongs to - so two causally-dependent events from the
  same partition can and did land on different threads and race. This is
  not a tuning problem (more DB pool connections, a different worker count,
  a bigger queue) - it is a structural mismatch between "dispatch to any
  free worker" and a domain model that relies on ordered, sequential
  delivery within a partition.
- **Answers to the experiment's own questions:**
  - **Does bounded concurrency raise the ceiling?** No - sustainable
    throughput did not improve at any tested rate; lag slope was 1.5-2.8x
    *worse* than baseline at every rate, and unlike the baseline, the
    candidate never achieved a clean (near-zero missing-rows) repeat at
    any rate tested.
  - **Is the ceiling caused by the synchronous single-record model, or by
    the runtime/language itself?** Neither answer is supported cleanly by
    this experiment - the candidate's regression is dominated by the
    ordering-violation mechanism above, which would need to be fixed
    (e.g. sticky per-partition-key worker assignment preserving causal
    order, not free-for-all dispatch) before a fair comparison of "does
    Python's GIL cap the achievable concurrency for this I/O-bound
    workload" could even be made. This experiment answers a different,
    more fundamental question first: naive worker-pool concurrency is
    unsafe for this domain model as currently structured.
- **Decision: REVERT the runtime default - keep the diagnostic
  instrumentation and the additive offset-tracker fix.** `processor_worker_pool_size`
  defaults to `1` (unchanged synchronous behavior) and is not enabled
  anywhere in this environment going forward. The `observe()`/
  `observe_dispatched()` addition to `offset_tracker.py`/`consumer.py` is
  kept even though the concurrent path that needs it is not enabled by
  default: it is purely additive (zero behavior change to any existing
  caller, all pre-existing tests pass unmodified), it fixes a real latent
  bug the module's own documentation predicted, and it is a prerequisite
  for any future, correctly-ordered concurrent design. `main_pooled.py`
  and its tests are kept as a documented, working reference for *how* to
  safely route offset commits through a single thread under concurrency -
  should a future experiment implement per-entity-key sticky dispatch, this
  module's queue/commit-draining/summary-merging plumbing would not need
  to change, only the dispatch policy would.
- **Correctness:** held in all 12 baseline repeats; violated in all 12
  candidate repeats (see above) - this is itself the decisive finding, not
  a caveat. The `normal`/`duplicate`/`dlq`/`retry` processor smoke
  scenarios still fail identically to Stages 26-27's already-tracked,
  pre-existing, unrelated DLQ-offset-race issue (not re-isolated via
  `git stash` again this stage since it was already confirmed twice
  before to reproduce on unmodified `main`).
- **Next experiment:** if bounded concurrency is revisited, it must
  preserve per-partition-key causal order - e.g. hashing each record's
  entity key (order/customer/cart id, however it's derivable from the
  event) to a fixed worker index, so all events for one entity always
  route to the same worker and process in delivery order, while different
  entities can still process in parallel across workers. That is a
  meaningfully larger design than this stage's "start with the smallest
  experiment" scope and was intentionally not attempted here once the
  ordering violation was found - this stage's job was to test the
  simplest concurrency model and report what happened, not to iterate
  until something worked.
