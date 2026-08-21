# Design Decisions

Why the event processor is built the way it is - each section follows
**Problem → Considered alternatives → Decision → Trade-off**. These are
the actual decisions embedded in this repository's code and its
performance-engineering history (`docs/performance-report.md`,
`docs/performance/optimization-history.md`), not an idealized design.
See [`event-lifecycle.svg`](event-lifecycle.svg) and
[`event-processing-sequence.svg`](event-processing-sequence.svg) for the
mechanics these decisions produce.

## Why Redis + PostgreSQL idempotency?

**Problem.** A Kafka consumer using at-least-once delivery can receive the
same record more than once (rebalance, redelivery after a slow commit,
retry after a transient failure). Reprocessing it must never re-run
business writes, fraud evaluation, or the outbox insert a second time.

**Considered alternatives.**
- *Redis only.* Fast (`reserve`/`complete` are single atomic Lua-script
  round trips - see `services/event_processor/idempotency.py`), and
  naturally handles cross-instance coordination when multiple processor
  containers share a consumer group. But Redis in this deployment is
  reconstructible coordination state, not the system of record: a
  restart, an eviction, or a TTL expiry under load can erase an in-flight
  reservation with no fallback. Relying on it alone means a lost
  reservation could let a duplicate re-run business writes.
- *PostgreSQL only.* The `processed_events` ledger (a `SELECT` by
  `event_id` before every write - `persistence/repositories/processed_events.py`)
  is durable and is already required inside the transaction regardless,
  since it is also the Kafka-coordinate uniqueness guard. But checking it
  for *every* record, including the overwhelming majority that are not
  duplicates, means paying a database round trip (and, worse, opening a
  connection from the pool) on the hot path for a check Redis can usually
  answer without touching the database at all.

**Decision.** Both, deliberately layered: Redis `reserve()` is the fast
path that also coordinates *active processing* across instances (a
`PROCESSING` lease with a bounded TTL prevents two consumers from
racing on the same event while a rebalance is in flight); the
`processed_events` ledger is the durable fallback that is authoritative
whenever Redis cannot answer - see the recovery timeline in
[`failure-recovery.svg`](failure-recovery.svg), where a crash after DB
commit but before Redis `complete()` leaves Redis in the wrong state
entirely, and the ledger is what actually prevents the re-run.

**Trade-off.** Two independent things to reason about instead of one, and
a brief window where Redis says `PROCESSING` while the ledger already has
the row (closed by the ledger check, not by Redis, so it is safe - just
not free of complexity). The alternative extremes are worse: pure Redis
sacrifices durability, pure PostgreSQL sacrifices most of the fast path's
speed advantage since a legitimate first-time event doesn't get to skip
the row-existence check either way.

## Why at-least-once instead of exactly-once?

**Problem.** Kafka's own delivery guarantee is fundamentally at-least-once
for a consumer that commits offsets after processing (the only safe
order - committing before processing risks silent data loss on a crash).
Exactly-once semantics across an external system like PostgreSQL would
require either Kafka transactions coordinated with a two-phase commit
into PostgreSQL (not something Kafka natively offers past its own log)
or an idempotent-consumer pattern that makes duplicates harmless instead
of trying to prevent them from occurring.

**Considered alternatives.**
- *Chase true exactly-once* via distributed transactions or Kafka's
  transactional producer/consumer APIs bridged into PostgreSQL. Adds a
  second coordination protocol, a second set of failure modes, and
  couples the consumer's commit timing to a cross-system transaction
  manager this project does not otherwise need.
- *Accept at-least-once with no duplicate protection* and let the
  business tables absorb duplicate writes. Directly violates the
  correctness this system exists to provide (a duplicate `order_created`
  or `payment_completed` would corrupt state, not just log noise).

**Decision.** Accept Kafka's native at-least-once delivery and make the
*consumer* idempotent instead of trying to make delivery exactly-once.
The offset for a record is only committed after: the PostgreSQL
transaction commits, **and** Redis `complete()` succeeds (see
`processor.py`'s `_commit()` call sites) - so a duplicate delivery is
expected and normal, not an edge case, and every write path is built to
tolerate it (`ON CONFLICT DO NOTHING`, rowcount guards, the ledger
digest check).

**Trade-off.** Every business repository has to carry duplicate-safety
logic (see `docs/performance/optimization-history.md`'s persistence
inventory) instead of trusting the platform to prevent duplicates
outright. In exchange, the system has no dependency on a distributed
transaction coordinator, and the failure mode under a crash is
well-understood and independently testable (see
[`failure-recovery.svg`](failure-recovery.svg)) rather than dependent on
a cross-system protocol's own correctness.

## Why transactional outbox?

**Problem.** A fraud alert that should be published to
`commerce.fraud-alerts` is *derived from* a database write (the fraud
evaluation). If the alert is published to Kafka directly from application
code right after the database write, there is an unavoidable window
where one succeeds and the other doesn't - a crash between "commit the
evaluation" and "publish to Kafka" either loses the alert entirely or
(if publish happens first) publishes an alert for a decision that never
actually got persisted.

**Considered alternatives.**
- *Publish to Kafka directly after commit,* in the same code path. Cannot
  be made atomic with the database commit - there are always two
  separate network calls with a crash window between them, however small.
- *Best-effort publish with manual reconciliation.* Pushes the atomicity
  problem onto an out-of-band process instead of solving it structurally.

**Decision.** Write the outbox row (`fraud_outbox`) using the *same
cursor, inside the same PostgreSQL transaction* as the `fraud_alerts`
row it documents (`fraud/repository.py`'s `FraudRepository.persist()`) -
see the transaction box in [`event-lifecycle.svg`](event-lifecycle.svg).
The row either commits atomically with the alert it represents, or
neither does. A separate publisher process (`fraud-outbox-publisher`)
polls the `fraud_outbox` table independently and marks rows published;
if it crashes mid-publish, the row is simply still `PENDING` and gets
picked up again - the publish step itself doesn't need to be atomic with
anything, because the *durability* of "this alert needs to be published"
already was.

**Trade-off.** Publishing is not immediate - there is a polling delay
between commit and actual Kafka delivery, and a second service
(`fraud-outbox-publisher`) exists purely to drain this table. That
latency and operational surface is the price paid to eliminate the
lost-or-duplicated-alert failure mode entirely, rather than mitigating it
with retries and reconciliation.

## Why partition-scoped ordering?

**Problem.** Business events for one entity are causally dependent:
`order_created` must be durable before `payment_completed` for that
order is processed, which must be durable before a `refund_requested`
against that payment. Something has to guarantee that dependent events
are handled in the order they actually happened.

**Considered alternatives.**
- *Global ordering* (a single Kafka partition, or an external sequencer
  ensuring one global order across all events). Removes any doubt about
  ordering, but caps consumer parallelism at one consumer for the entire
  topic - the exact ceiling Stage 20-27's diagnostics spent significant
  effort tracing the causes of, before Stage 28 confirmed that breaking
  sequential processing (even *within* one partition) directly causes
  ordering violations.
- *No ordering guarantee, reconcile after the fact.* Would require every
  business write to tolerate arriving out of causal order (e.g. a payment
  arriving before its order), which this schema's foreign-key-shaped
  validation (`MissingBusinessDependencyError` guards throughout the
  repositories) is deliberately not designed to do.

**Decision.** Rely on Kafka's actual guarantee - ordering *within* one
partition - and route causally-related events for the same entity to the
same partition via the producer's partition key (customer/entity-scoped),
so a single-threaded, strictly sequential consumer per partition is
sufficient to preserve the dependency chain. This is why
Stage 28's worker-pool experiment (dispatching records from one
partition to multiple threads) broke correctness: it violated the exact
assumption this decision depends on. Stage 29 later confirmed the
*safe* way to add consumer-side parallelism - more partitions, each with
its own dedicated single-threaded consumer - preserves this guarantee,
even though it did not raise the throughput ceiling on this benchmark
host.

**Trade-off.** Throughput is capped by the per-partition, single-threaded
service rate of the slowest consumer, and cannot be raised by adding
concurrency *within* a partition without redesigning how work is
dispatched (per-entity-key sticky assignment, not attempted in this
repository). Global ordering was rejected because it makes this ceiling
worse, not better; the chosen design accepts a partition-scoped ceiling
in exchange for keeping every write's dependency-ordering guarantee
simple and structurally enforced rather than reconciled after the fact.

## Why no persistence batching?

Stage 30's conclusion, in full in
`docs/performance/optimization-history.md#persistence-batching-feasibility--code-path-analysis-only-no-implementation`.

**Problem.** Reducing per-event SQL call count (batching several events'
writes into fewer statements) looked like a plausible way to reduce
per-event database cost.

**Considered alternatives.** Bulk-inserting `processed_events` ledger
rows across several events; batching business-repository writes across
several events; batching `fraud_outbox` rows across several events;
batching the fraud-context read queries across events sharing a
customer.

**Decision.** Reject all four, for distinct reasons rooted in the
existing correctness model, not a shared "batching is hard" excuse:
- the **per-event transaction boundary** is what makes the durable
  idempotency check, the digest-conflict guard, and the 1:1
  offset-safety relationship (one event, one commit, one safe offset
  advance) correct - a batch introduces partial-row-failure bookkeeping
  that does not exist today;
- **business writes carry `exists()` dependency checks, `FOR UPDATE`
  row locks, and totals validation** against the *previous* event's
  already-durable state - batching (or reordering) these reintroduces
  the exact ordering hazard Stage 28 already demonstrated, just moved
  from concurrent threads to a batched SQL boundary;
- **`fraud_outbox` atomicity** is defined as "commits with the specific
  business effect it documents" - cross-event outbox batching breaks
  that atomicity boundary, which is the entire mechanism that makes the
  outbox pattern correct (see "Why transactional outbox?" above);
- **fraud-context read batching** is only possible in the narrow case of
  several events sharing a customer, which arbitrary adjacent events
  don't guarantee, and Stage 29 did not show PostgreSQL saturation
  sufficient to justify the added buffering/state complexity.

**Trade-off.** Per-event SQL call count (3-7 for a typical event, 14-23
for a fraud-eligible one - code-derived counts, not benchmark
measurements) is not reduced. The trade accepted here is: keep every
correctness guarantee already established, and leave a plausible-looking
throughput lever unpulled, rather than implement a batching mechanism
that would need to re-derive per-row idempotency, retry, and atomicity
guarantees the current design gets from the transaction boundary itself.

## Why worker scaling has limits?

Stage 28's rejection and Stage 29/31's results, in full in
`docs/performance/optimization-history.md`.

**Problem.** The isolated pipeline's throughput ceiling (~1050 evt/s at 3
partitions/3 workers) looked like it should be raisable by adding more
consumer concurrency.

**Considered alternatives, each tried and its result recorded:**
- **Intra-partition worker-pool concurrency** (Stage 28): a bounded
  thread pool behind one consumer, dispatching records from the same
  partition to multiple worker threads. Rejected - correctness was
  violated in all 12 candidate repeats (662-948 missing rows per
  repeat, 11,326 events dead-lettered), because dispatching to "any free
  worker" breaks the within-partition ordering guarantee described above
  ("Why partition-scoped ordering?"). This is a correctness risk, not a
  tuning problem - more database connections or a different worker count
  would not have fixed it.
- **Kafka-native partition scaling** (Stage 29): more partitions, each
  with its own dedicated single-threaded consumer (safe - no ordering
  guarantee touched). Correctness held cleanly in all 12 repeats even at
  2.2x the requested rate, but mean service rate stayed pinned in the
  same ~977-1,233 evt/s band whether 3 or 6 partitions/workers were used -
  scaling partitions did not raise the ceiling on this benchmark host.
- **Container CPU budget** (Stage 31): doubling the CPU quota available
  to each of 6 processor containers (0.5 → 1.0 CPU each). Produced no
  meaningful throughput improvement, and the containers' actual CPU
  usage barely changed between the two configurations (~250-370% used
  in both, regardless of a 300% vs. 600% aggregate ceiling) -
  `nr_throttled` was negligible (~0.45% of periods) even at the tighter
  configuration. Container-level CPU quota was ruled out as the
  constraint.

**Decision.** Do not add worker-pool concurrency (unsafe), keep
partition-scaling available as a *correctness-safe* lever even though it
alone did not move throughput on this host, and stop pursuing local CPU-
budget tuning once the container-quota hypothesis was weakened. The
default runtime stays single-threaded-per-partition
(`processor_worker_pool_size=1`); the pooled-consumer code from Stage 28
is retained only as a documented reference for a future, correctly
ordered (per-entity-key sticky dispatch) redesign, not as something
enabled anywhere.

**Trade-off.** The ~1050 evt/s isolated-pipeline ceiling on this local
benchmark host is treated as the honestly-measured, multiply-cross-
checked capacity for this environment - not a number this repository's
diagnostics found a safe way to raise further. The remaining unresolved
variable (host/VM-wide CPU contention, as opposed to per-container quota)
was left untested because this environment does not expose a
reproducible way to change Docker Desktop's VM CPU allocation (see Stage
31's write-up) - a real gap in what could be verified here, stated as
such rather than assumed either way.
