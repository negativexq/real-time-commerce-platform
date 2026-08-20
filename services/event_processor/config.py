"""Validated environment and CLI configuration for the event processor."""

import argparse
import os
from collections.abc import Mapping, Sequence
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

NonBlank = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
OffsetReset = Literal["earliest", "latest", "error"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class ProcessorConfig(BaseModel):
    """Runtime settings with conservative local defaults."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kafka_bootstrap_servers: NonBlank = "kafka:9092"
    processor_input_topic: NonBlank = "commerce.events"
    processor_dlq_topic: NonBlank = "commerce.events.dlq"
    processor_consumer_group: NonBlank = "commerce-event-processor-v1"
    processor_client_id: NonBlank = "commerce-event-processor"
    processor_auto_offset_reset: OffsetReset = "earliest"
    processor_session_timeout_ms: int = Field(default=10_000, ge=6_000, le=300_000)
    processor_heartbeat_interval_ms: int = Field(default=3_000, gt=0)
    processor_max_poll_interval_ms: int = Field(default=300_000, ge=10_000)
    processor_poll_timeout_seconds: float = Field(default=1.0, gt=0, le=30)
    processor_idle_timeout_seconds: float = Field(default=10.0, gt=0, le=3_600)
    processor_max_processing_attempts: int = Field(default=3, gt=0, le=20)
    processor_retry_initial_backoff_ms: int = Field(default=100, ge=0, le=60_000)
    processor_retry_max_backoff_ms: int = Field(default=2_000, ge=0, le=300_000)
    processor_retry_multiplier: float = Field(default=2.0, ge=1, le=10)
    processor_retry_jitter_ratio: float = Field(default=0.1, ge=0, le=1)
    processor_idempotency_processing_ttl_seconds: int = Field(default=60, gt=0)
    processor_idempotency_completed_ttl_seconds: int = Field(default=604_800, gt=0)
    processor_idempotency_key_prefix: NonBlank = "commerce:processor:v1:event"
    processor_redis_socket_timeout_seconds: float = Field(default=2.0, gt=0, le=60)
    processor_redis_connect_timeout_seconds: float = Field(default=2.0, gt=0, le=60)
    processor_dlq_max_payload_bytes: int = Field(default=65_536, gt=0)
    processor_dlq_delivery_timeout_ms: int = Field(default=10_000, gt=0)
    processor_shutdown_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    processor_log_level: LogLevel = "INFO"
    redis_url: NonBlank = "redis://redis:6379/0"
    postgres_dsn: NonBlank = (
        "postgresql://commerce:commerce_local_dev@postgres:5432/commerce"
    )
    processor_db_pool_min_size: int = Field(default=1, gt=0, le=20)
    processor_db_pool_max_size: int = Field(default=4, gt=0, le=50)
    processor_db_connect_timeout_seconds: int = Field(default=5, gt=0, le=120)
    processor_db_acquire_timeout_seconds: float = Field(default=5.0, gt=0, le=120)
    processor_db_statement_timeout_ms: int = Field(default=5_000, gt=0)
    processor_db_max_idle_seconds: float = Field(default=300.0, gt=0)
    processor_db_healthcheck_interval_seconds: float = Field(default=30.0, gt=0)
    processor_db_startup_attempts: int = Field(default=5, gt=0, le=100)
    processor_db_startup_backoff_seconds: float = Field(default=1.0, ge=0, le=60)
    processor_required_schema_version: int = Field(default=4, gt=0)
    processor_persist_raw_event_json: bool = True
    processor_db_log_slow_query_ms: int = Field(default=250, gt=0)
    processor_max_messages: int | None = Field(default=None, gt=0)
    processor_from_beginning: bool = False
    processor_offset_commit_batch_size: int = Field(default=50, gt=0, le=10_000)
    processor_offset_commit_interval_ms: int = Field(default=100, gt=0, le=60_000)
    processor_worker_pool_size: int = Field(default=1, ge=1, le=64)

    @model_validator(mode="after")
    def validate_relationships(self) -> Self:
        """Reject timing and TTL relationships that undermine correctness."""
        if self.processor_heartbeat_interval_ms >= self.processor_session_timeout_ms:
            raise ValueError("heartbeat interval must be less than session timeout")
        if (
            self.processor_retry_initial_backoff_ms
            > self.processor_retry_max_backoff_ms
        ):
            raise ValueError("initial retry backoff must not exceed maximum")
        if (
            self.processor_idempotency_completed_ttl_seconds
            <= self.processor_idempotency_processing_ttl_seconds
        ):
            raise ValueError("completed TTL must be greater than processing TTL")
        if self.processor_db_pool_max_size < self.processor_db_pool_min_size:
            raise ValueError("database pool maximum must be at least its minimum")
        return self

    @classmethod
    def from_environment(
        cls, environment: Mapping[str, str] | None = None
    ) -> "ProcessorConfig":
        """Load only documented environment variables."""
        source = os.environ if environment is None else environment
        names = {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka_bootstrap_servers",
            "PROCESSOR_INPUT_TOPIC": "processor_input_topic",
            "PROCESSOR_DLQ_TOPIC": "processor_dlq_topic",
            "PROCESSOR_CONSUMER_GROUP": "processor_consumer_group",
            "PROCESSOR_CLIENT_ID": "processor_client_id",
            "PROCESSOR_AUTO_OFFSET_RESET": "processor_auto_offset_reset",
            "PROCESSOR_SESSION_TIMEOUT_MS": "processor_session_timeout_ms",
            "PROCESSOR_HEARTBEAT_INTERVAL_MS": "processor_heartbeat_interval_ms",
            "PROCESSOR_MAX_POLL_INTERVAL_MS": "processor_max_poll_interval_ms",
            "PROCESSOR_POLL_TIMEOUT_SECONDS": "processor_poll_timeout_seconds",
            "PROCESSOR_IDLE_TIMEOUT_SECONDS": "processor_idle_timeout_seconds",
            "PROCESSOR_MAX_PROCESSING_ATTEMPTS": "processor_max_processing_attempts",
            "PROCESSOR_RETRY_INITIAL_BACKOFF_MS": (
                "processor_retry_initial_backoff_ms"
            ),
            "PROCESSOR_RETRY_MAX_BACKOFF_MS": "processor_retry_max_backoff_ms",
            "PROCESSOR_RETRY_MULTIPLIER": "processor_retry_multiplier",
            "PROCESSOR_RETRY_JITTER_RATIO": "processor_retry_jitter_ratio",
            "PROCESSOR_IDEMPOTENCY_PROCESSING_TTL_SECONDS": (
                "processor_idempotency_processing_ttl_seconds"
            ),
            "PROCESSOR_IDEMPOTENCY_COMPLETED_TTL_SECONDS": (
                "processor_idempotency_completed_ttl_seconds"
            ),
            "PROCESSOR_IDEMPOTENCY_KEY_PREFIX": ("processor_idempotency_key_prefix"),
            "PROCESSOR_REDIS_SOCKET_TIMEOUT_SECONDS": (
                "processor_redis_socket_timeout_seconds"
            ),
            "PROCESSOR_REDIS_CONNECT_TIMEOUT_SECONDS": (
                "processor_redis_connect_timeout_seconds"
            ),
            "PROCESSOR_DLQ_MAX_PAYLOAD_BYTES": "processor_dlq_max_payload_bytes",
            "PROCESSOR_DLQ_DELIVERY_TIMEOUT_MS": ("processor_dlq_delivery_timeout_ms"),
            "PROCESSOR_SHUTDOWN_TIMEOUT_SECONDS": (
                "processor_shutdown_timeout_seconds"
            ),
            "PROCESSOR_LOG_LEVEL": "processor_log_level",
            "REDIS_URL": "redis_url",
            "POSTGRES_DSN": "postgres_dsn",
            "PROCESSOR_DB_POOL_MIN_SIZE": "processor_db_pool_min_size",
            "PROCESSOR_DB_POOL_MAX_SIZE": "processor_db_pool_max_size",
            "PROCESSOR_DB_CONNECT_TIMEOUT_SECONDS": (
                "processor_db_connect_timeout_seconds"
            ),
            "PROCESSOR_DB_ACQUIRE_TIMEOUT_SECONDS": (
                "processor_db_acquire_timeout_seconds"
            ),
            "PROCESSOR_DB_STATEMENT_TIMEOUT_MS": "processor_db_statement_timeout_ms",
            "PROCESSOR_DB_MAX_IDLE_SECONDS": "processor_db_max_idle_seconds",
            "PROCESSOR_DB_HEALTHCHECK_INTERVAL_SECONDS": (
                "processor_db_healthcheck_interval_seconds"
            ),
            "PROCESSOR_DB_STARTUP_ATTEMPTS": "processor_db_startup_attempts",
            "PROCESSOR_DB_STARTUP_BACKOFF_SECONDS": (
                "processor_db_startup_backoff_seconds"
            ),
            "PROCESSOR_REQUIRED_SCHEMA_VERSION": ("processor_required_schema_version"),
            "PROCESSOR_PERSIST_RAW_EVENT_JSON": "processor_persist_raw_event_json",
            "PROCESSOR_DB_LOG_SLOW_QUERY_MS": "processor_db_log_slow_query_ms",
            "PROCESSOR_OFFSET_COMMIT_BATCH_SIZE": (
                "processor_offset_commit_batch_size"
            ),
            "PROCESSOR_OFFSET_COMMIT_INTERVAL_MS": (
                "processor_offset_commit_interval_ms"
            ),
            "PROCESSOR_WORKER_POOL_SIZE": "processor_worker_pool_size",
        }
        return cls.model_validate(
            {field: source[name] for name, field in names.items() if name in source}
        )


def parse_config(
    arguments: Sequence[str] | None = None,
    environment: Mapping[str, str] | None = None,
) -> ProcessorConfig:
    """Apply explicit CLI values over validated environment settings."""
    base = ProcessorConfig.from_environment(environment)
    parser = argparse.ArgumentParser(description="Process commerce Kafka events")
    parser.add_argument("--max-messages", type=int)
    parser.add_argument("--idle-timeout", type=float)
    parser.add_argument(
        "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    )
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--group-id")
    parsed = parser.parse_args(arguments)
    overrides = {
        key: value
        for key, value in {
            "processor_max_messages": parsed.max_messages,
            "processor_idle_timeout_seconds": parsed.idle_timeout,
            "processor_log_level": parsed.log_level,
            "processor_consumer_group": parsed.group_id,
        }.items()
        if value is not None
    }
    if parsed.from_beginning:
        overrides["processor_from_beginning"] = True
        overrides["processor_auto_offset_reset"] = "earliest"
    return ProcessorConfig.model_validate({**base.model_dump(), **overrides})
