"""Outbox publisher configuration.

Reads the same environment variables, with the same defaults and bounds,
that the publisher used to borrow from the processor's configuration.
"""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from shared.observability import MetricsConfig

NonBlank = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


def _select(source: Mapping[str, str], names: Mapping[str, str]) -> dict[str, str]:
    return {field: source[env] for env, field in names.items() if env in source}


class ConnectionSettings(BaseModel):
    """Kafka and PostgreSQL endpoints plus the Kafka delivery timeout."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kafka_bootstrap_servers: NonBlank = "kafka:9092"
    postgres_dsn: NonBlank = (
        "postgresql://commerce:commerce_local_dev@postgres:5432/commerce"
    )
    delivery_timeout_ms: int = Field(default=10_000, gt=0)

    @classmethod
    def from_environment(cls, source: Mapping[str, str]) -> "ConnectionSettings":
        return cls.model_validate(
            _select(
                source,
                {
                    "KAFKA_BOOTSTRAP_SERVERS": "kafka_bootstrap_servers",
                    "POSTGRES_DSN": "postgres_dsn",
                    "PROCESSOR_DLQ_DELIVERY_TIMEOUT_MS": "delivery_timeout_ms",
                },
            )
        )


class OutboxSettings(BaseModel):
    """Claim, batching, retry, and backoff bounds for the fraud outbox."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    fraud_alert_topic: NonBlank = "commerce.fraud-alerts"
    fraud_outbox_enabled: bool = True
    fraud_outbox_batch_size: int = Field(default=20, gt=0, le=1_000)
    fraud_outbox_poll_interval_ms: int = Field(default=500, gt=0)
    fraud_outbox_max_attempts: int = Field(default=10, gt=0, le=100)
    fraud_outbox_initial_backoff_ms: int = Field(default=500, ge=0)
    fraud_outbox_max_backoff_ms: int = Field(default=30_000, ge=0)
    fraud_outbox_claim_ttl_seconds: int = Field(default=30, gt=0)

    @model_validator(mode="after")
    def validate_relationships(self) -> Self:
        if self.fraud_outbox_initial_backoff_ms > self.fraud_outbox_max_backoff_ms:
            raise ValueError("initial outbox backoff cannot exceed maximum")
        return self

    @classmethod
    def from_environment(cls, source: Mapping[str, str]) -> "OutboxSettings":
        return cls.model_validate(
            _select(source, {name.upper(): name for name in cls.model_fields})
        )


@dataclass(frozen=True, slots=True)
class OutboxConfig:
    connection: ConnectionSettings
    outbox: OutboxSettings
    metrics: MetricsConfig

    @classmethod
    def from_environment(
        cls, environment: Mapping[str, str] | None = None
    ) -> "OutboxConfig":
        source = os.environ if environment is None else environment
        return cls(
            ConnectionSettings.from_environment(source),
            OutboxSettings.from_environment(source),
            MetricsConfig.from_environment(
                "fraud-outbox-publisher",
                9103,
                source,
                port_environment_name="FRAUD_OUTBOX_METRICS_PORT",
            ),
        )
