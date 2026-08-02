"""Host-side connection settings and fixed benchmark parameters.

These deliberately point at the *host*-exposed ports from ``compose.yaml``
(``127.0.0.1:*``) since every benchmark module runs on the host, not inside a
container. Values are overridable via environment variables so the benchmark
stays reproducible without editing code.
"""

import hashlib
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    run_tag: str
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: _env("BENCH_KAFKA_BOOTSTRAP", "localhost:29092")
    )
    redis_url: str = field(
        default_factory=lambda: _env("BENCH_REDIS_URL", "redis://localhost:6379/0")
    )
    postgres_dsn: str = field(
        default_factory=lambda: _env(
            "BENCH_POSTGRES_DSN",
            "postgresql://commerce:commerce_local_dev@localhost:5432/commerce",
        )
    )
    prometheus_url: str = field(
        default_factory=lambda: _env("BENCH_PROMETHEUS_URL", "http://localhost:9090")
    )
    demo_api_base_url: str = field(
        default_factory=lambda: _env("BENCH_DEMO_API_URL", "http://127.0.0.1:8082")
    )
    compose_project: str = field(
        default_factory=lambda: _env(
            "BENCH_COMPOSE_PROJECT", "real-time-commerce-platform"
        )
    )
    primary_consumer_group: str = "commerce-event-processor-v1"
    events_topic: str = "commerce.events"
    dlq_topic: str = "commerce.events.dlq"
    artifacts_dir: str = field(
        default_factory=lambda: _env("BENCH_ARTIFACTS_DIR", "artifacts/benchmark")
    )

    def phase_dir(self) -> str:
        return os.path.join(self.artifacts_dir, self.run_tag)

    def apply_process_env(self) -> None:
        """Point the real ProcessorConfig/GeneratorConfig at host-exposed ports."""
        os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", self.kafka_bootstrap_servers)
        os.environ.setdefault("REDIS_URL", self.redis_url)
        os.environ.setdefault("POSTGRES_DSN", self.postgres_dsn)
        os.environ.setdefault("PROCESSOR_LOG_LEVEL", "WARNING")


def derive_seed(run_tag: str, *parts: str) -> int:
    """Deterministic-but-collision-resistant seed for a given run_tag/phase.

    Every generator/journey seed used elsewhere in this repo's own tooling
    (Makefile, scripts/*-smoke.py) reuses small fixed constants like 42, 43,
    5150, 6200 - against a long-lived shared demo stack, reusing one of
    those same seeds makes the generator produce event_ids identical to
    events already processed in a *previous* session (SeededUuidFactory is
    deterministic), which corrupts latency/throughput measurements via
    idempotency short-circuiting on stale rows. Hashing run_tag with the
    phase name guarantees a fresh, wide-range seed per benchmark run.
    """
    digest = hashlib.sha256(":".join((run_tag, *parts)).encode()).hexdigest()
    return int(digest[:12], 16) % 2_000_000_000 + 1


def default_run_tag() -> str:
    return datetime.now(UTC).strftime("bench-%Y%m%dT%H%M%SZ")


def load_config(run_tag: str | None = None) -> BenchmarkConfig:
    return BenchmarkConfig(run_tag=run_tag or _env("BENCH_RUN_TAG", default_run_tag()))
