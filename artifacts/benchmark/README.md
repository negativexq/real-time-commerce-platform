# Benchmark Artifact Index

The event processor has run as the Go service in `services/event_processor_go/`
since 2026-11-27. The earlier benchmark directories measured the Python
processor and were removed from the tree when it was retired. They include
the Demo API baselines, worker scaling, index and fraud-context
optimizations, saturation diagnoses, and the fraud-mix studies. The
performance documents still cite those directories by name.

To recover one, find the commit that deleted it and check out the tree from
the commit before:

```bash
git log --diff-filter=D --oneline -- artifacts/benchmark
git checkout <deleting-commit>^ -- artifacts/benchmark/<directory>
```

High-volume raw telemetry (`direct-saturation-*.json`, `injector-*.json`,
`postgres-diagnostics-raw-*.json`, `cpu-scheduling-raw-*.json`) is
gitignored and was only ever kept locally. Only report-backed summaries and
notes are committed.

## Current artifacts

| Artifact | Purpose | Result |
| --- | --- | --- |
| [`runtime-ab-20261127`](runtime-ab-20261127/README.md) | Python vs Go processor A/B: 3 workers, reset state, 1000–1700 evt/s | Go sustained 1400 evt/s where Python did not. It processed about 13% more at 1700 evt/s, with 4–19x lower E2E p95 and 30–35% less processor CPU. |
| [`runtime-ab-go-refine-20261127`](runtime-ab-go-refine-20261127/README.md) | Go cold-start check and ceiling refinement at 1500/1600 evt/s with pgx `cache_describe` | The cold first repeat was fixed, but database time per transaction rose about 40%. pgx stays in `cache_statement` mode. |
| [`group-commit-ab-20261128`](group-commit-ab-20261128/README.md) | One transaction per event vs one per fetched batch, 6 workers, interleaved ABBA from a reset DB, 2600–5000 evt/s | Peak 3018 vs 2572 evt/s; about 2800 vs 2220 reproducible (+25%). Commits fell from ~900–1700/s to ~40–55/s. The ceiling is now CPU. |
