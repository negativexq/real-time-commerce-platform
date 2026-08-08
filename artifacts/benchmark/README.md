# Benchmark Artifact Index

Raw JSON artifacts are intentionally preserved. The entries below are the
experiments used by the performance-engineering report; exploratory and
intermediate directories remain available beside them.

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

The broader reliability run
[`bench-fixed-rate-final-20260807T151000Z`](bench-fixed-rate-final-20260807T151000Z/)
also contains idempotency, retry/DLQ, outbox, lag, and verification artifacts.

The highest persisted injector artifact is in
[`bench-worker-scale-3w-20260808T191000Z`](bench-worker-scale-3w-20260808T191000Z/):
900 requested and approximately 889.4/s median actual injection. A standalone
1000-rate validation was previously recorded as approximately 975.7/s, but no
matching JSON file exists in this artifact tree; the documentation does not
treat it as artifact-verifiable capacity.
