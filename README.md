# Real-Time Commerce Platform

A lightweight, event-driven commerce platform intended to demonstrate
realistic customer journeys and reliable event processing on a local
Apple Silicon development machine.

## Current status

Sprint 0 establishes the repository and Python tooling foundation. Application
services and infrastructure are intentionally not implemented yet.

## Requirements

- Python 3.12 or newer
- GNU Make
- Docker Desktop (required in a later sprint)

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

## Development commands

```bash
make lint          # Run Ruff linting
make format        # Format Python files with Ruff
make format-check  # Verify formatting without changing files
make type-check    # Run mypy
make test          # Run pytest
make check         # Run all Python quality checks
make compose-config
```

`make compose-config` is reserved for validating Docker Compose configuration.
It will become usable when Compose infrastructure is introduced in a later
sprint.

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

Later sprints may introduce Kafka in KRaft mode, PostgreSQL, Redis, Python
producers and consumers, Prometheus, Grafana, and Docker Compose orchestration.
These components are outside Sprint 0.
