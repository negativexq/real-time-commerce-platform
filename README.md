# Real-Time Commerce Platform

A lightweight, event-driven commerce platform intended to demonstrate
realistic customer journeys and reliable event processing on a local
Apple Silicon development machine.

## Current status

Sprint 1 provides local Kafka infrastructure: a single-node KRaft broker,
explicit topic initialization, and Kafka UI. No application services,
databases, event generators, processors, fraud consumers, or business logic
exist yet.

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

No real credentials should be stored in the repository. To create local
configuration, copy the example environment file:

```bash
cp .env.example .env
```

## Sprint 1 architecture

```text
Host clients ── localhost:29092 ──┐
                                  │
Compose clients ── kafka:9092 ─── Kafka (broker + KRaft controller)
                                  │
                                  ├── kafka-init (one-shot topic creation)
Browser ── localhost:8080 ─────── Kafka UI
```

KRaft stores Kafka metadata in Kafka's own quorum instead of ZooKeeper. This
local setup combines the broker and controller roles in one node to minimize
resource use. It is suitable for development, not production or
high-availability use.

Kafka has two advertised client listeners:

- Containers in the Compose network use `kafka:9092`.
- Tools running on the host use `localhost:29092`.

The KRaft controller listener uses port 9093 only inside the Compose network
and is not exposed to the host. Automatic topic creation is disabled.

## Start and stop

Copy the local defaults, then start the stack:

```bash
cp .env.example .env
make compose-config
make up
make ps
```

Kafka UI is available only from the local machine at
<http://localhost:8080>. Stop containers without deleting Kafka data:

```bash
make down
```

## Kafka topics

The `kafka-init` one-shot service waits for the broker, creates missing topics
idempotently, verifies them, and exits successfully.

| Topic | Partitions | Replicas | Cleanup | Purpose |
| --- | ---: | ---: | --- | --- |
| `commerce.events` | 3 | 1 | delete | Future commerce event stream |
| `commerce.events.dlq` | 1 | 1 | delete | Future invalid-event dead letters |
| `commerce.fraud.alerts` | 3 | 1 | delete | Future explainable fraud alerts |

Three partitions permit key-based parallelism while preserving ordering only
within each partition. The DLQ has one partition because Sprint 1 favors a
simple inspection path over throughput.

Inspect and smoke-test Kafka without locally installed Kafka CLI tools:

```bash
make kafka-topics
make kafka-describe
make kafka-smoke
```

The smoke test checks broker connectivity and all required topics, publishes a
JSON event with timezone-aware UTC timestamps, and consumes that exact event
with a bounded timeout.

## Persistent data

Kafka data is stored in the named Docker volume
`real-time-commerce-platform-kafka-data`. `make down` and `make clean` preserve
it. The destructive `make clean-volumes` command prints a warning and deletes
the volume.

## Development commands

```bash
make lint          # Run Ruff linting
make format        # Format Python files with Ruff
make format-check  # Verify formatting without changing files
make type-check    # Run mypy
make test          # Run pytest
make check         # Run all Python quality checks
make compose-config # Validate Docker Compose configuration
make up             # Start Kafka, initialize topics, and start Kafka UI
make down           # Stop the stack and preserve Kafka data
make logs           # Follow Kafka, initializer, and UI logs
make ps             # Show Compose service status
make kafka-topics   # List topics with Kafka's containerized CLI
make kafka-describe # Describe all topics
make kafka-smoke    # Run the containerized Kafka smoke test
make clean          # Stop containers and preserve Kafka data
make clean-volumes  # Stop containers and delete persisted Kafka data
```

## Troubleshooting

```bash
docker compose ps
docker compose logs kafka
docker compose logs kafka-init
docker compose logs kafka-ui
make compose-config
```

If port 29092 or 8080 is already occupied, change `KAFKA_HOST_PORT` or
`KAFKA_UI_PORT` in `.env`. After a deliberate reset, use
`make clean-volumes`; this permanently removes local Kafka messages and
metadata.

## Apple Silicon

The pinned Kafka and Kafka UI images publish Linux ARM64 variants and run
natively on Apple Silicon. The broker heap is capped at 512 MiB and Kafka UI
at 256 MiB to keep the Sprint 1 stack lightweight on a 16 GB M2 MacBook Air.
Docker Desktop must be running before Compose commands are used.

## Repository layout

```text
services/        Future application services
shared/          Shared Python code and event schemas
database/        Future database initialization and migrations
infrastructure/  Future local infrastructure configuration
tests/           Cross-service test suites
docs/            Architecture decisions and project documentation
```

## Roadmap

Later sprints may introduce PostgreSQL, Redis, Python producers and consumers,
Prometheus, and Grafana. These components are outside Sprint 1.
