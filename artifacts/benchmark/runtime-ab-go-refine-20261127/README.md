# Go processor: cold-start fix check and ceiling refinement (2026-11-27)

A Go-only sweep at 1000, 1500 and 1600 evt/s ran with the same protocol as [`runtime-ab-20261127`](../runtime-ab-20261127/).
The only change was pgx's query mode: `cache_describe` (unnamed statements,
planned per execution) instead of the default `cache_statement` (named
prepared statements).

## Results

| rate | repeat | service evt/s | lag slope /s | E2E p95 ms | sustainable |
|---|---|---|---|---|---|
| 1000 | 0 (cold) | 998 | +1.9 | 67 | yes |
| 1000 | 1 | 997 | +1.6 | 43 | yes |
| 1500 | 0 | 1446 | +49.8 | 3364 | no |
| 1500 | 1 | 1459 | +39.7 | 3341 | no |
| 1600 | 0 | 1520 | +76.8 | 5772 | no |
| 1600 | 1 | 1515 | +81.1 | 4730 | no |

Correctness: every repeat processed exactly the events it injected, with 0
errors and 0 error log lines.

## Findings

1. **The cold-start problem is fixed.** The first repeat after the truncate
   now sustains 1000 evt/s with an E2E p95 of 67 ms. In `cache_statement`
   mode the same repeat processed only 543 evt/s, with an E2E p95 of 36 s.
   Sequential scans in that repeat fell sharply: `customers` from 54.7k to
   12.1k and `processed_events` from 52.3k to 2.6k.
2. **The fix costs steady-state throughput.** Planning every execution
   raises database time per transaction:

   | mode | rate / repeat | transaction ms | PG CPU% peak |
   |---|---|---|---|
   | `cache_statement` | 1000 / 1 | 0.94 | 49 |
   | `cache_describe` | 1000 / 1 | 1.32 | 87 |
   | `cache_statement` | 1400 / 0 | 1.11 | 97 |
   | `cache_describe` | 1500 / 0 | 1.55 | 168 |

   With `cache_describe`, Go saturates at about 1450–1520 evt/s. With
   `cache_statement`, it processed 1676 evt/s at 1700 evt/s offered.
   The sustainable ceiling falls from just above 1400 to roughly 1400–1450.
3. **The cold-start problem happens only when tables are nearly empty when
   statements are first prepared.** That is the benchmark's
   truncate-and-start protocol, or a brand-new database. Production tables
   with real data and statistics do not trigger it.

Caveat: the two modes ran in separate sessions, not as an interleaved A/B,
and this run started with less accumulated data. Less data should favour
`cache_describe`, yet it still came out slower. The higher transaction time
and higher PostgreSQL CPU are consistent with per-execution planning cost.

## Decision

The processor was reverted to pgx's default `cache_statement` mode. The
cold-start effect only appears while tables are nearly empty, and avoiding
it with per-execution planning cost too much steady-state throughput.
