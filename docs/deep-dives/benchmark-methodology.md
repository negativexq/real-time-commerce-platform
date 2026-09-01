# Benchmark Methodology

The isolated benchmark measures the processor path without allowing the
interactive scenario runner to become the producer bottleneck:

```text
benchmark direct injector
        ↓
Kafka
        ↓
3 partitions
        ↓
3 processor workers
        ↓
Redis + PostgreSQL
        ↓
outbox / DLQ as required
```

This is different from the Demo full-path benchmark:

| Scope | Path | Meaning |
| --- | --- | --- |
| Demo full path | Demo Control API → Scenario Runner → Kafka → processor → persistence | Measures the interactive application and generation path end to end. |
| Isolated processor capacity | Direct injector → Kafka → processor → Redis/PostgreSQL | Measures processor saturation while the injector supplies the requested rate. |

The direct injector is benchmark-only. It uses the same event schemas and
valid journeys, but does not call the Demo Control API or its pacing and
progress-refresh loop.

## Why service rate alone is insufficient

`processed events/sec` is not capacity by itself. A processor can appear to
process 1,200 events/sec while receiving 1,300 events/sec; the backlog is
still growing.

The benchmark therefore considers requested and actual injection rate, actual
service rate, Kafka lag slope, peak lag, end-of-load lag, drain behavior, E2E
matching, and correctness.

```text
arrival ≈ service
lag slope ≈ 0
→ bounded / near-line-rate

arrival > service
persistent positive lag slope
→ backlog grows
→ transition / degraded
```

The established runs use a separate warm-up, fixed-duration steady state,
post-load drain, and repeated observations around candidate boundaries. The
historical isolated results used three workers, three partitions, and a 1/1/1
assignment; methodology details and phase durations are recorded in the
[performance methodology](../performance/methodology.md).

## Correctness is a gate

A run is only useful as capacity evidence when the higher rate does not come
from dropping or weakening work. Accepted runs check:

- unique injected event IDs match durable `processed_events` rows and E2E matches;
- no unexpected DLQ, dependency, or database-integrity errors;
- source Kafka lag drains to zero;
- pending outbox work drains to zero;
- fraud evaluations match the count of eligible events;
- the 0% profile generates exactly zero eligible events.

The principle is simple: **throughput without correctness is not capacity**.

## Workload-sensitive result

The local study shows that the observed capacity region changes with the
amount of fraud-path work:

| Fraud-eligible share | Highest near-line-rate observation | Transition candidate |
| ---: | ---: | ---: |
| ~42.8% | ~1075 evt/s | ~1100 |
| ~20% | ~1200 evt/s | ~1300 |
| ~10% | ~1400 evt/s | ~1500–1600 |
| ~5% | ~1500–1600 evt/s | Boundary not fully resolved |
| 0% | ~1600 evt/s | ~1700 |

These are conservative observations, not a production SLA. The mechanism is
not a single multiplier: total cost includes base event handling plus, for
eligible events, fraud-context reads, rule evaluation, and fraud persistence.
PostgreSQL work, event mix, partition distribution, and fixed processor,
Kafka, and Redis overhead also contribute.

```text
event base cost
+ optional fraud-context reads
+ rule evaluation
+ database persistence
+ Kafka/Redis/processor overhead
```

That is why events/sec alone is not enough to define capacity.

## Benchmark limitations

These measurements run in a local Docker environment on a specific host and
are workload-, state-, and topology-dependent; they are not a production SLA
or universal platform limit. Some profile groups were executed after separate
environment resets, so the table is best treated as a measured
workload-sensitivity study rather than a perfectly controlled single-state
authoritative capacity matrix. Several exact transition boundaries remain
broad.

### Interview takeaway

The benchmark finds the point where service can no longer keep up with
arrival while correctness remains intact—not simply the highest events/sec
number observed.

## Related

- [Fraud-Eligible Workload Profiles](fraud-workload-profiles.md)
- [Scenario & Journey Generation](scenario-generation.md)
- [Fraud Decision Strategy](fraud-decision-strategy.md)
- [Performance methodology](../performance/methodology.md)
- [Optimization history](../performance/optimization-history.md)
- [Benchmark artifacts](../../artifacts/benchmark/README.md)
