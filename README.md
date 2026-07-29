# Real-Time Commerce Platform

A lightweight, event-driven commerce platform intended to demonstrate
realistic customer journeys and reliable event processing on a local Apple
Silicon development machine.

## Current status

Sprints 0 through 6 are completed. Sprint 7 is current and adds transactional
PostgreSQL persistence, typed repositories, versioned migrations, durable
event idempotency, and exact monetary/refund checks to the Kafka processor.
Fraud scoring is not implemented.

Sprint 3 shared event contracts and canonical serialization are completed and
remain the generator's only schema source.

The envelope, payloads, registry, versioning policy, and partition-key guidance
are documented in [Event contracts](docs/event-contracts.md).
Generator operation and boundaries are documented in
[Event generator](docs/event-generator.md).
Persona and anomaly semantics are documented in
[Stateful personas and controlled anomalies](docs/personas-and-anomalies.md).
Processor behavior and failure semantics are documented in
[Kafka event processor](docs/event-processor.md).
The schema, migrations, transaction boundary, and recovery behavior are
documented in [PostgreSQL persistence](docs/postgresql-persistence.md).

## Requirements

- Python 3.12 or newer
- GNU Make
- Docker Desktop with Docker Compose

## Setup

Create and activate a virtual environment, then install the development tools:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Copy the safe local development defaults:

```bash
cp .env.example .env
```

Never store production or real credentials in this repository.

## Sprint 7 architecture

```text
Host / Compose clients
    │
    ├── localhost:29092 / kafka:9092 ── Kafka KRaft broker
    │                                      ├── kafka-init
    │                                      └── Kafka UI on localhost:8080
    │
    ├── generator profile ─────────────── event-generator
    │                                      ├── in-memory customer state
    │                                      ├── persona strategy registry
    │                                      └── publishes commerce.events
    ├── processor profile ─────────────── event-processor
    │                                      ├── validates shared contracts
    │                                      ├── Redis idempotency leases
    │                                      ├── one PostgreSQL transaction/event
    │                                      ├── typed business repositories
    │                                      └── commerce.events.dlq
    │
    ├── localhost:5432 / postgres:5432 ─ PostgreSQL system of record
    │                                      └── durable named volume
    │
    └── localhost:6379 / redis:6379 ───── Redis operational state
                                           └── append-only named volume
```

All published ports bind to `127.0.0.1`. Services communicate through the
existing Compose network.

## Event generator quick start

The default stack stays infrastructure-only. Build and publish a deterministic
five-journey sample:

```bash
make up
make generator-build
make generator-sample
```

Run continuously, inspect status/logs, or stop only the generator:

```bash
make generator-up
make generator-status
make generator-logs
make generator-down
```

Open Kafka UI at <http://localhost:8080>, select the local cluster, open
`commerce.events`, and use the Messages tab to inspect generated events.

## Event processor quick start

The processor is profile-gated and does not start with the default stack:

```bash
make processor-build
make processor-up
make processor-status
make processor-logs
```

Publish and process bounded valid data, demonstrate completed duplicate
suppression, or route malformed records to the DLQ:

```bash
make processor-sample
make processor-smoke
make processor-duplicate-smoke
make processor-dlq-smoke
make processor-retry-smoke
```

A valid record is parsed through the shared registry, atomically reserved by
`event_id`, committed to PostgreSQL as a ledger plus business effects, marked
completed in Redis, and then committed in Kafka. PostgreSQL detects identical
redelivery independently and repairs Redis completion without repeating effects.
An invalid record is committed only after its deterministic DLQ record has a
confirmed Kafka delivery. This is at-least-once processing, not exactly once.

Run deterministic persona and anomaly demonstrations:

```bash
make generator-normal
make generator-suspicious
make generator-bot
make generator-takeover
make generator-anomalies
```

Suspicious and account-takeover patterns are synthetic behavior, not fraud
classification. The persistence processor does not implement a fraud engine.

## Kafka

Kafka runs as one combined broker/controller node in KRaft mode, which stores
cluster metadata without ZooKeeper. This resource-conscious topology is for
local development, not production high availability.

- Host clients: `localhost:29092`
- Compose clients: `kafka:9092`
- Kafka UI: <http://localhost:8080>

The unexposed KRaft controller listener uses port 9093 inside Compose.
Automatic topic creation is disabled.

| Topic | Partitions | Replicas | Cleanup | Purpose |
| --- | ---: | ---: | --- | --- |
| `commerce.events` | 3 | 1 | delete | Future commerce event stream |
| `commerce.events.dlq` | 1 | 1 | delete | Processor invalid/exhausted dead letters |
| `commerce.fraud.alerts` | 3 | 1 | delete | Future explainable fraud alerts |

Ordering exists only within a partition, not globally.

## PostgreSQL

PostgreSQL is the durable system of record. The event processor persists every
valid new event and its business effects in one explicit transaction before
Redis completion and Kafka offset commit.

- Host connection: `localhost:5432`
- Compose connection: `postgres:5432`
- Encoding: UTF-8
- Database timezone: UTC

Core Sprint 7 tables:

| Table | Purpose |
| --- | --- |
| `processed_events` | Idempotently records processed event envelopes |
| `customers`, `sessions` | Durable customer and session parents |
| `product_views` | Immutable event-level views |
| `carts`, `cart_items` | Latest cart state and exact items |
| `orders`, `payments`, `refunds` | Exact commerce outcomes |
| `fraud_alerts` | Stores explainable fraud decisions and scores |
| `dead_letter_events` | Stores failed records and transport context |

The schema includes indexes for common event, decision, correlation, and time
lookups. Check constraints enforce positive event versions, fraud scores from
0 through 100, and decisions of `APPROVE`, `REVIEW`, or `BLOCK`.

Initialization SQL in `database/init/` runs only for an empty volume. Ordered
SQL migrations evolve existing volumes with checksums, advisory locking, and
transactional application.

Apply and inspect migrations, then safely inspect estimated row counts:

```bash
make db-migrate
make db-migration-status
make db-schema-check
make db-counts
```

## Redis

Redis is only for temporary operational state such as duplicate detection,
recent customer activity, failed-payment counters, device activity, and fraud
blacklist lookups. Important records must not exist only in Redis; future
applications must tolerate expiration and eviction and reconstruct state from
durable sources.

- Host connection: `localhost:6379`
- Compose connection: `redis:6379`
- Data limit: 256 MiB
- Container memory limit: 384 MiB
- Persistence: append-only file with `appendfsync everysec`
- Eviction policy: `allkeys-lru`

`allkeys-lru` evicts the least recently used keys when Redis reaches its data
limit. This is appropriate because every Redis key is temporary and
reconstructible. Append-only persistence improves local restart continuity but
does not make Redis the system of record.

## Environment variables

`.env.example` supplies safe local defaults:

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_HOST_PORT` | `29092` | Kafka host listener |
| `KAFKA_UI_PORT` | `8080` | Kafka UI |
| `POSTGRES_HOST_PORT` | `5432` | PostgreSQL host listener |
| `POSTGRES_DB` | `commerce` | Local database |
| `POSTGRES_USER` | `commerce` | Local database user |
| `POSTGRES_PASSWORD` | `commerce_local_dev` | Local-only password |
| `REDIS_HOST_PORT` | `6379` | Redis host listener |

## Start and stop

```bash
make compose-config
make up
make ps
```

`make up` starts the full current stack and waits on readiness-aware
dependencies where required. Stop all containers while preserving every named
volume:

```bash
make down
```

`make clean` has the same non-destructive volume behavior.

## Smoke tests and inspection

No locally installed Kafka, PostgreSQL, or Redis CLI is required:

```bash
make kafka-topics
make kafka-describe
make kafka-smoke
make postgres-tables
make storage-status
make storage-smoke
make persistence-smoke
make persistence-duplicate-smoke
make persistence-recovery-smoke
make persistence-dependency-smoke
make persistence-refund-smoke
```

The storage smoke test checks both health endpoints and all required tables. It
inserts, reads, and deletes a temporary PostgreSQL row using generated UUIDs
and UTC-aware timestamps, then sets, reads, and deletes a temporary Redis key
with a TTL.

Interactive tools:

```bash
make postgres-shell
make redis-cli
```

## Persistent volumes

| Volume | Contents |
| --- | --- |
| `real-time-commerce-platform-kafka-data` | Kafka metadata and messages |
| `real-time-commerce-platform-postgres-data` | Durable PostgreSQL data |
| `real-time-commerce-platform-redis-data` | Redis append-only data |

`make down` and `make clean` preserve all three volumes.
`make clean-volumes` prints a destructive warning and deletes all three.

## Development commands

```bash
make lint             # Run Ruff linting
make format           # Format Python files with Ruff
make format-check     # Verify formatting
make type-check       # Run mypy
make test             # Run pytest
make check            # Run all Python checks
make compose-config   # Validate Compose configuration
make up               # Start the full stack
make down             # Stop containers and preserve volumes
make logs             # Follow Kafka service logs
make ps               # Show Compose services
make kafka-topics     # List Kafka topics
make kafka-describe   # Describe Kafka topics
make kafka-smoke      # Run Kafka publish/consume smoke test
make postgres-shell   # Open psql
make postgres-tables  # List PostgreSQL tables
make redis-cli        # Open redis-cli
make storage-smoke    # Test PostgreSQL and Redis
make storage-status   # Show storage service status
make storage-logs     # Follow PostgreSQL and Redis logs
make generator-build  # Build the event-generator image
make generator-up     # Start continuous generation
make generator-down   # Remove only the generator service
make generator-logs   # Follow structured generator logs
make generator-run    # Run continuous generation interactively
make generator-sample # Publish five deterministic journeys
make generator-status # Show generator profile status
make generator-smoke  # Run bounded producer end-to-end validation
make generator-personas # Show personas and configured weights
make generator-normal # Publish a deterministic normal sample
make generator-suspicious # Publish suspicious synthetic behavior
make generator-bot # Publish a bounded bot sample
make generator-takeover # Demonstrate prior history then takeover
make generator-anomalies # Publish all controlled anomaly types
make generator-persona-smoke # Validate persona/state patterns
make generator-anomaly-smoke # Validate raw anomaly records
make processor-build  # Build the event-processor image
make processor-up     # Start continuous processing
make processor-down   # Remove only the processor
make processor-run    # Run interactively
make processor-smoke  # Validate normal bounded processing
make processor-duplicate-smoke # Prove completed duplicate suppression
make processor-dlq-smoke # Validate malformed-record DLQ handling
make processor-retry-smoke # Validate bounded retry then success
make clean            # Stop containers and preserve volumes
make clean-volumes    # Delete all persisted local data
```

## Troubleshooting

```bash
docker compose ps -a
docker compose logs kafka kafka-init kafka-ui
docker compose logs postgres
docker compose logs redis
make storage-status
make postgres-tables
make compose-config
```

If a host port is occupied, override its corresponding variable in `.env`.
PostgreSQL initialization scripts only run against an empty volume; use
`make clean-volumes` only when intentionally discarding all Kafka, PostgreSQL,
and Redis data.

## Apple Silicon

The pinned Kafka, Kafka UI, PostgreSQL, and Redis images publish Linux ARM64
variants and run natively on Apple Silicon. Kafka’s heap, Kafka UI’s heap, and
Redis memory are conservatively limited for a 16 GB M2 MacBook Air.

## Repository layout

```text
database/init/       PostgreSQL first-run initialization SQL
database/migrations/ Ordered, checksum-verified schema migrations
docs/           Contract and architecture documentation
infrastructure/ Kafka infrastructure scripts
scripts/        Container-backed smoke tests
shared/         Shared Python code and versioned event schemas
tests/          Cross-service test suites
```

## Roadmap

Later sprints may introduce fraud classification, Prometheus, and Grafana.
They are outside Sprint 7.
