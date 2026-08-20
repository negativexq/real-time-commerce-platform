# Benchmark Artifact Index

High-volume `direct-saturation.json` and `injector-<rate>-<repeat>.json`
telemetry (full per-repeat/per-event sample series from
`scripts/benchmark/direct_saturation.py` and `direct_injector.py`) is
retained locally and excluded from Git via `.gitignore`. Repository
artifacts preserve compact, report-backed benchmark evidence, summaries,
EXPLAIN outputs, and other small reproducibility artifacts. Directories
committed before this policy keep their historical raw JSON in Git history
unchanged; only newly generated raw telemetry is excluded going forward.
The entries below are the experiments used by the performance-engineering
report; exploratory and intermediate directories remain available beside
them.

| Artifact | Purpose | Important result | Report section |
| --- | --- | --- | --- |
| [`bench-20260802T004243Z`](bench-20260802T004243Z/) | Initial 100 evt/s Demo API benchmark | ~49.84 evt/s observed baseline | Initial baseline |
| [`bench-after-refresh-final-20260807T114000Z`](bench-after-refresh-final-20260807T114000Z/) | Periodic/background progress-refresh stage | ~77–80 evt/s generation range | Progress refresh |
| [`bench-fixed-rate-final-20260807T151000Z`](bench-fixed-rate-final-20260807T151000Z/) | Monotonic fixed-rate pacing | ~97.93 evt/s median at 100 requested | Fixed-rate pacing |
| [`bench-tx-decomposition-200-final-20260808T030000Z`](bench-tx-decomposition-200-final-20260808T030000Z/) | PostgreSQL transaction decomposition | Payment-history SELECTs were the expensive SQL class | Transaction decomposition |
| [`bench-tx-combined-payments-20260808T080000Z`](bench-tx-combined-payments-20260808T080000Z/) | Combined recent/prior query experiment | System performance regressed; reverted | Rejected experiment |
| [`bench-tx-index-20260808T090000Z`](bench-tx-index-20260808T090000Z/) | Composite-index read experiment | Seq Scan replaced by indexed access; lookup latency fell sharply | Composite index |
| [`bench-tx-index-write-20260808T100000Z`](bench-tx-index-write-20260808T100000Z/) | Index write-side validation | Insert/transaction/WAL impact retained for review | Composite index |
| [`bench-direct-processor-20260808T150000Z`](bench-direct-processor-20260808T150000Z/) | Direct injector and isolated processor sweep | Direct injection removed the Demo path from processor measurement | Direct injector |
| [`bench-worker-scale-1w-20260808T160000Z`](bench-worker-scale-1w-20260808T160000Z/) | One-worker baseline | ~500 evt/s sustainable; 600 saturated | Scaling |
| [`bench-worker-scale-2w-resource-20260808T180000Z`](bench-worker-scale-2w-resource-20260808T180000Z/) | Two-worker controlled scaling | 2/1 assignment; non-linear improvement | Scaling |
| [`bench-worker-scale-3w-20260808T191000Z`](bench-worker-scale-3w-20260808T191000Z/) | Three-worker, three-partition sweep | ~750 evt/s sustainable; 900 saturated | Scaling |
| [`bench-worker-scale-3w-boundary-775-20260808T210000Z`](bench-worker-scale-3w-boundary-775-20260808T210000Z/) | One permitted binary-search boundary rate | 775 evt/s non-sustainable in all repeats | Final boundary |
| [`bench-batched-commit-3w-boundary`](bench-batched-commit-3w-boundary/) | Same 750/775/800 evt/s boundary re-tested after batching offset commits | 775 and 800 evt/s became sustainable (lag slope ≤+2.03/s in all 9 repeats) | Bounded batched offset commits |
| [`bench-batched-commit-3w-ceiling-broad`](bench-batched-commit-3w-ceiling-broad/) | Broad post-batching saturation sweep at 850/900/950/1000 evt/s | 850 and 900 sustainable; 950 escalating across repeats; 1000 saturated | Stage 14 - ceiling discovery |
| [`bench-batched-commit-3w-ceiling-refinement`](bench-batched-commit-3w-ceiling-refinement/) | Boundary refinement at 925 evt/s | Low/non-escalating lag slope but E2E p95 reached >1000ms in 2/3 repeats; classified as transition band, not a clean ceiling | Stage 14 - ceiling discovery |
| [`bench-info-baseline-control-3w-boundary`](bench-info-baseline-control-3w-boundary/) | Fresh INFO-level control at 900/925/950 evt/s, pre-change image | 456,181 `event_processed` lines / ~254MB stdout+stderr in the measured window | Stage 15 - logging cost isolation |
| [`bench-debug-success-log-3w-boundary`](bench-debug-success-log-3w-boundary/) | Same 900/925/950 evt/s after moving `event_processed` to DEBUG | 0 `event_processed` lines / ~7KB in the measured window; ~2-point processor CPU delta, no material PostgreSQL/throughput change | Stage 15 - logging cost isolation |
| [`bench-tx-decomposition-v2-3w-boundary`](bench-tx-decomposition-v2-3w-boundary/) | Diagnosis-only transaction/SQL-class decomposition at 900/925/950 evt/s, reusing existing production instrumentation | `fraud_context` is the largest DB-side stage (~46-59% of transaction time); `orders`/`product_views` lack the composite index `payments` has (EXPLAIN-proven 4-20x cost gap); pool contention and fraud-rule CPU ruled out | Transaction decomposition v2 |
| [`bench-orders-index-baseline-3w-boundary`](bench-orders-index-baseline-3w-boundary/) | Fresh controlled baseline (reset tables) at 900/925/950 evt/s before adding an `orders` composite index | Correctness-clean 9/9 repeats; lag slope +1.2 to +6.7/s from a clean, low-data-volume starting state | Orders composite index |
| [`bench-orders-index-3w-boundary`](bench-orders-index-3w-boundary/) | Same controlled sweep after adding `idx_orders_customer_ordered_at (customer_id, ordered_at DESC)` | Lag slope and E2E p95 improved at every rate (e.g. 900 evt/s: +3.06→+1.48/s, 230→120 ms); EXPLAIN plan changed to Index Only Scan, ~2.7x faster | Orders composite index |
| [`bench-product-views-index-baseline-3w-boundary`](bench-product-views-index-baseline-3w-boundary/) | Fresh controlled baseline (reset tables, orders index retained) at 900/925/950 evt/s before adding a `product_views` composite index | Correctness-clean 9/9 repeats; lag slope +0.56 to +4.79/s | Product-views composite index |
| [`bench-product-views-index-3w-boundary`](bench-product-views-index-3w-boundary/) | Same controlled sweep after adding `idx_product_views_customer_viewed_at (customer_id, viewed_at DESC)` | Clear win at 900 evt/s (+3.06→+1.20/s, 263→113 ms); wash at 925; noise-dominated at 950; EXPLAIN plan changed to Index Only Scan, ~3.6x faster, no write-cost regression at any rate | Product-views composite index |
| [`bench-post-index-3w-ceiling-broad`](bench-post-index-3w-ceiling-broad/) | Fresh-reset ceiling sweep at 950/1000/1050/1100 evt/s with both new indexes retained | 1000/1050 evt/s clean (3/3 repeats); 1100 evt/s non-sustainable (1/3 clean); injector kept pace at 99.4-99.9% throughout | Post-index capacity discovery |
| [`bench-post-index-3w-ceiling-refinement`](bench-post-index-3w-ceiling-refinement/) | Refinement at 1075 evt/s to bracket the transition interval | 2/3 repeats degraded (slope +10.0/+3.0/+34.8/s) - confirms transition interval ~1050-1075 evt/s | Post-index capacity discovery |

The broader reliability run
[`bench-fixed-rate-final-20260807T151000Z`](bench-fixed-rate-final-20260807T151000Z/)
also contains idempotency, retry/DLQ, outbox, lag, and verification artifacts.

The highest persisted injector artifact is in
[`bench-worker-scale-3w-20260808T191000Z`](bench-worker-scale-3w-20260808T191000Z/):
900 requested and approximately 889.4/s median actual injection. A standalone
1000-rate validation was previously recorded as approximately 975.7/s, but no
matching JSON file exists in this artifact tree; the documentation does not
treat it as artifact-verifiable capacity.
