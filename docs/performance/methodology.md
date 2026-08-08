# Performance Benchmark Methodology

This document defines the measurement terms used by the performance reports.
The benchmark is local and workload-specific; it is not a production-capacity
test.

## Workload phases

Each steady-state run follows the same sequence:

1. Wait for the primary consumer group to be idle and lag-free.
2. Run a warm-up phase separately.
3. Apply a fixed-duration load at the requested rate.
4. Stop publishing and continue sampling until Kafka lag drains.
5. Validate processed records, event-ID matching, errors, and terminal state.

The scaling sweeps used 10 seconds of warm-up, 30 seconds of steady-state,
and three repeats. The final 3-worker boundary sweep used 45 seconds of
steady-state for 800, 850, and 775 evt/s.

## Rate and latency definitions

- **Requested rate:** target rate supplied to the injector or Demo API.
- **Generated/injected rate:** messages actually published to Kafka divided
  by the measured publish interval.
- **Processed rate:** terminally processed messages divided by the measured
  processing interval.
- **E2E latency:** Kafka broker publish timestamp (`CreateTime`) to
  `processed_events.processed_at`. It is not the synthetic event payload's
  `produced_at` field.
- **Handler latency:** processor handler duration measured by the processor
  histogram.
- **Transaction latency:** measured PostgreSQL transaction duration.
- **Lag slope:** change in consumer-group lag divided by elapsed steady-state
  sample time.
- **Drain time:** time from load end until the consumer group reaches the
  configured consecutive-zero-lag condition.

The direct injector uses unique event IDs and the existing event schema. It
does not call Demo Control API, `repository.add_manifest()`, progress refresh,
or the Demo API pacing loop.

## Sustainable and saturated rates

A rate is marked **sustainable** only when:

- actual injection is at least 95% of the requested rate;
- processed throughput follows injected throughput;
- lag slope is not persistently positive during steady state;
- lag drains in a reasonable bounded interval;
- E2E p95/p99 does not show runaway growth;
- errors and correctness checks remain clean.

Peak lag alone is not a saturation criterion. A transient peak that returns
to zero is different from a positive lag slope that continues throughout the
load.

For queueing analysis, arrival rate is `λ`, service rate is `μ`, and the
expected backlog growth is approximately `λ - μ`. The boundary experiments
showed this relationship directly: measured lag slope tracked the arrival
minus service difference.

## Correctness checks

The benchmark validates unique event IDs, processed-row counts, E2E timestamp
matches, terminal error counters, and post-load lag drain. The broader
reliability benchmark also validates duplicate durable side effects, retry/DLQ
behavior, and transactional outbox delivery; those artifacts are indexed in
[`artifacts/benchmark/README.md`](../../artifacts/benchmark/README.md).

## Resource environment

The documented runs used Docker Desktop on an arm64 Mac host with an 8-CPU
Docker VM. Compose did not set CPU quotas. PostgreSQL, Kafka, processor, and
worker runtime samples were collected where the relevant artifact supports
them. Resource numbers are observations from that environment, not hardware
limits or universal service characteristics.
