# Benchmark artifacts

Compact summary reports and metrics stay in the repository. Raw per-run
outputs, including large JSON/JSONL telemetry and injector dumps, are local
only and ignored so they do not slow fresh clones.

Raw outputs are reproducible with the benchmark scripts under
`scripts/benchmark/`, including `direct_injector.py` and
`direct_saturation.py`. See [`benchmark/README.md`](benchmark/README.md) for
the retained evidence index and the performance reports for interpretation.

This applies going forward; raw files already present in Git history are not
rewritten.
