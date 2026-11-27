# Group commit A/B: one transaction per event vs per batch (2026-11-28)

The Go processor now persists the new events of one fetched batch in a single
PostgreSQL transaction (`PROCESSOR_BATCH_MAX_EVENTS`, default 50). This A/B ran
the same image twice, changing only that variable: `1` restores one transaction
per event, `50` is the new default.

## Setup

- Database reset to an empty schema (28 MB) before the first run.
- 6 processor replicas on a fresh 6-partition topic with its own consumer group
  and Redis key prefix.
- Docker VM with 8 CPUs. The processor, PostgreSQL, Kafka and Redis share them.
- `direct_saturation.py` at 2600, 3400, 4200 and 5000 evt/s, 1 repeat per rate,
  10 s warmup and 45 s of measured load.
- The runs were interleaved in ABBA order (1, 50, 50, 1). Each run adds about
  1.8 GB to the database, and the same setting lost up to 27% from that growth
  alone, so back-to-back runs of one setting would mostly measure the table size.

## Results (service rate, evt/s)

| Run | DB size at start | 2600 | 3400 | 4200 | 5000 |
|---|---:|---:|---:|---:|---:|
| batch=1 | 28 MB | 2189 | 2572 | 2463 | 2473 |
| batch=50 | 1.8 GB | 2561 | **3018** | 2428 | 2828 |
| batch=50 | 3.6 GB | 2475 | 2184 | 1895 | 2559 |
| batch=1 | 5.3 GB | 1757 | 1338 | 1601 | 1873 |
| **mean batch=1** | | 1973 | 1955 | 2032 | 2173 |
| **mean batch=50** | | **2518** | **2601** | **2162** | **2694** |

- **Peak.** 3018 evt/s with batch=50, against 2572 with batch=1.
- **Reproducible ceiling.** Taking each run's best rate, batch=50 averaged about
  2790 evt/s and batch=1 about 2220, so roughly +25%. About 2800 evt/s is the
  figure to quote, not the single 3018 peak.
- **Commits.** PostgreSQL commits fell from about 900–1700/s to about 40–55/s.
- **What limits it now.** Total container CPU sat at 790–917% of the 8 CPUs,
  PostgreSQL at about 270%. The ceiling is CPU on this machine.
- **Not a sustainable boundary.** Every rate here is above the ceiling, so
  the table reports saturated throughput. The closest to sustainable was
  batch=50 at 2600 evt/s on the first run, with a lag slope of +16/s.

## Round-trip batching (same day, earlier)

Before group commit, the fraud-context reads, the ledger insert and the
fraud-persistence writes were folded into pgx pipelined batches. An interleaved
old-vs-new A/B at 1400 and 1800 evt/s showed about +2%, within noise: round
trips were not the bottleneck. The change was kept because group commit builds
on the same batches.

## Unused indexes

Migration `010_drop_unused_indexes.sql` drops eight indexes that no query reads,
each a B-tree insert on the hot write path. It was applied before both A/Bs.

Correctness: every run processed exactly the events it injected. The Go unit and
persistence tests, including equivalence tests of a group against one
transaction per event, and the 7 integration tests pass.

Raw `direct-saturation-*.json` and `injector-*.json` files stay local
(gitignored).
