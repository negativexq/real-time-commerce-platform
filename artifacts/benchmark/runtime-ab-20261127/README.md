# Runtime A/B: Python vs Go event processor (2026-11-27)

The unchanged `direct_saturation.py` sweep ran against each runtime in turn.
A temporary orchestration script, `scripts/benchmark/runtime_ab.sh`, did the
reset and runtime switch. It and its table generator were removed once Go
became the only processor. The per-runtime raw results are in `python/` and
`go/`.

## Setup

Each runtime started from the same state:

- Benchmark tables truncated, then `VACUUM ANALYZE`.
- A fresh topic with 3 partitions and a fresh consumer group.
- Its own Redis key prefix.
- 3 replicas with the same compose limits.

Sweep parameters:

- Rates: 1000, 1200, 1400 and 1700 evt/s.
- 2 repeats per rate.
- 10 s warmup and 45 s of measured load per repeat.
- A repeat counts as sustainable when its lag slope is at most +5/s.

Correctness: every repeat in both runtimes processed exactly the events it
injected, with 0 errors and 0 error log lines.

## Results (median of 2 repeats)

| rate | runtime | service evt/s | lag slope /s | E2E p95 ms | proc CPU% peak (3 replicas) | sustainable |
|---|---|---|---|---|---|---|
| 1200 | python | 1197 | +2.1 | 235 | 158 | 2/2 |
| 1200 | go | 1197 | +1.4 | 51 | 104 | 2/2 |
| 1400 | python | 1371 | +26.2 | 1686 | 182 | 0/2 |
| 1400 | go | 1395 | +3.0 | 89 | 124 | 2/2 |
| 1700 | python | 1477 | +220.1 | 12939 | 188 | 0/2 |
| 1700 | go | 1676 | +18.2 | 1065 | 175 | 0/2 |

- **Sustainable ceiling.** Python's ceiling is between 1200 and 1400 evt/s.
  Go held 1400 cleanly. At 1700 Go was just over the threshold, with lag
  slopes of +16.6 and +19.9/s.
- **Saturated throughput.** At 1700 evt/s, Go processed about 1676 evt/s and
  Python about 1477 evt/s, so Go was roughly 13% higher.
- **Latency at the same rate.** At 1200 evt/s, E2E p95 was 4–5x lower on Go
  (51 vs 235 ms). At 1400 evt/s it was about 19x lower (89 vs 1686 ms).
- **CPU.** At the same rate, peak processor CPU was about 30–35% lower on Go.
- **Database time.** On the warm 1000 evt/s repeat, the mean measured
  transaction time was 0.94 ms on Go and 1.68 ms on Python.

## The cold first repeat (1000 evt/s, repeat 0)

Repeat 0 was the first measurement after the truncate in both runtimes, and
in both it was not sustainable. Python processed 874 evt/s and Go 543 evt/s.
`postgres_table_deltas` shows the cause:

- **Sequential scans.** In both cold repeats PostgreSQL ran tens of
  thousands of sequential scans on every business table. Its statistics had
  been gathered on empty tables.
- **Recovery.** In repeat 1, after autoanalyze, all of those reads became
  index scans. The only exception is `refunds`, which stays small.
- **Go-only scans.** The cold Go repeat also ran sequential scans on
  `processed_events` (52k) and `fraud_evaluations` (7k). The cold Python
  repeat ran none.

The likely mechanism is how each driver uses PostgreSQL's plan cache:

- **pgx** prepares every statement on first use in a connection. After five
  executions, PostgreSQL can switch that statement to a generic plan, and
  here it chose that plan while the tables were still nearly empty.
- **psycopg3** prepares a statement only after five executions. It therefore
  keeps planning each execution against current data for longer.

Neither runtime is affected once table statistics reflect real data. This
is a property of the benchmark's truncate-and-start protocol and does not
describe either runtime's capacity. A longer warmup would remove it. On the
Go side, so would `QueryExecModeExec` or `plan_cache_mode=force_custom_plan`.

Follow-up: pgx's `cache_describe` mode fixed the cold repeat but cost about
40% more database time per transaction (see
[`runtime-ab-go-refine-20261127`](../runtime-ab-go-refine-20261127/README.md)).
The processor therefore keeps the default `cache_statement` mode.
