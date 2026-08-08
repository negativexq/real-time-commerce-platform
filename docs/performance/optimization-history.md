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
  further optimization was applied.
