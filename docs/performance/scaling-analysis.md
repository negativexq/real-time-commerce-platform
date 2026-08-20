# Scaling Analysis: One, Two, and Three Workers

This analysis covers the isolated `Kafka → event_processor → persistence`
pipeline using the direct Kafka injector. It is not the Demo Control API full
path.

## Kafka topology

`commerce.events` has three partitions and the consumer group is
`commerce-event-processor-v1`.

- One worker: all three partitions assigned to one consumer.
- Two workers: 2/1 partition assignment.
- Three workers: 1/1/1 partition assignment.

## Aggregate throughput

| Rate | One worker | Two workers | Three workers |
| ---: | ---: | ---: | ---: |
| 400 evt/s | ~395.5/s | ~401.0/s | ~397.3/s |
| 500 evt/s | ~496.6/s | ~499.6/s | ~495.4/s |
| 600 evt/s | ~502.0/s | ~584.5/s | ~594.2/s |
| 750 evt/s | ~560.4/s | ~660.6/s | ~742.2/s |
| 900 evt/s | — | — | ~888.6/s, positive lag slope |

The one-worker artifact is
[`bench-worker-scale-1w-20260808T160000Z`](../../artifacts/benchmark/bench-worker-scale-1w-20260808T160000Z/).
The resource-complete two-worker artifact is
[`bench-worker-scale-2w-resource-20260808T180000Z`](../../artifacts/benchmark/bench-worker-scale-2w-resource-20260808T180000Z/).
The three-worker sweep is
[`bench-worker-scale-3w-20260808T191000Z`](../../artifacts/benchmark/bench-worker-scale-3w-20260808T191000Z/).

## Lag and latency

| Rate | 1-worker lag slope | 2-worker lag slope | 3-worker lag slope | 3-worker E2E p95 |
| ---: | ---: | ---: | ---: | ---: |
| 600 | +90.7/s | +10.4/s | +0.3/s | ~185 ms |
| 750 | +179.2/s | +80.3/s | +0.7/s | ~954 ms |
| 900 | — | — | positive in all repeats | ~1.28 s |

The 3-worker boundary artifact then tested 800 and 850 evt/s. 800 was
unstable: two repeats were near zero slope, one was +47.4/s. 850 was clearly
non-sustainable, with two repeats at +185.6 and +283.3/s. The one permitted
binary-search rate, 775 evt/s, was non-sustainable in all three repeats:
approximately +52.9, +86.8, and +93.4/s.

## Worker balance and resource observations

The three-worker assignment removed the 2/1 imbalance. Worker CPU was balanced
in the boundary runs; at 900 evt/s the three workers were approximately
51.7%, 50.9%, and 52.2% in the recorded runtime snapshots. The aggregate
processor metrics did not expose a worker label for processed-event counts,
so this report does not invent per-worker throughput numbers.

PostgreSQL CPU rose with three-worker load: approximately 52% at 600 evt/s,
61% at 750 evt/s, and 71% at 900 evt/s. Transaction p95 remained near 4.8 ms
through the 900-evt/s sweep, while saturation runs showed higher handler and
queueing/E2E latency. Kafka resource samples were variable and are reported
as observations rather than a single proven Kafka-only bottleneck.

## Scaling factors

At the overloaded rates where scaling is meaningful:

- 1 → 2 at 600 evt/s: `584.5 / 502.0 ≈ 1.16x`;
- 1 → 3 at 600 evt/s: `594.2 / 502.0 ≈ 1.18x`;
- 1 → 2 at 750 evt/s: `660.6 / 560.4 ≈ 1.18x`;
- 1 → 3 at 750 evt/s: `742.2 / 560.4 ≈ 1.32x`.

Using confirmed sustainable ceilings, one worker was ~500 evt/s and three
workers ~750 evt/s: a 1.5x throughput factor, or approximately 50% of ideal
three-worker linear efficiency. The result supports horizontal scaling, but
also shows shared persistence/broker costs and the importance of partition
assignment.

## Conclusion

The 2/1 assignment was a real contributor to the two-worker result, but not
the sole bottleneck. With 1/1/1 assignment, three workers reached ~750 evt/s
sustainably; the transition to saturation occurred between 750 and 775 evt/s.

**Update:** this ceiling was a property of the per-event synchronous offset
commit in place at the time of this sweep, not of horizontal scaling itself.
The "Bounded batched offset commits" entry in
[`optimization-history.md`](optimization-history.md) re-tested 750/775/800
evt/s with the same 3-worker/3-partition topology after batching offset
commits, and 775/800 evt/s became clearly sustainable (near-zero lag slope
in all repeats, versus non-sustainable/unstable here).

**Second update:** the "Post-batching capacity discovery" entry in
[`optimization-history.md`](optimization-history.md) located the new
ceiling with the same 1/1/1 topology unchanged: **900 evt/s is the highest
artifact-backed sustainable rate** (up from 750 evt/s, ~20% higher).
Repeatable saturation now begins in the **900-950 evt/s** band. PostgreSQL
CPU and WAL full-page-image rate rose sharply toward 1000 evt/s while
processor CPU stayed moderate, making PostgreSQL the strongest
evidence-based hypothesis for the next bottleneck - see that entry for the
full measurement.
