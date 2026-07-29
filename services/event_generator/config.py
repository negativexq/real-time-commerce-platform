"""Validated event-generator configuration."""

import os
from collections.abc import Mapping
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

NonBlank = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
CompressionType = Literal["gzip", "snappy", "lz4", "zstd", "none"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class GeneratorConfig(BaseModel):
    """Runtime and producer settings with safe local defaults."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kafka_bootstrap_servers: NonBlank = "kafka:9092"
    kafka_events_topic: NonBlank = "commerce.events"
    kafka_client_id: NonBlank = "event-generator"
    kafka_compression_type: CompressionType = "lz4"
    kafka_linger_ms: int = Field(default=20, ge=0, le=5_000)
    kafka_batch_size: int = Field(default=65_536, ge=1_024, le=10_485_760)
    kafka_delivery_timeout_ms: int = Field(default=30_000, ge=1_000, le=300_000)
    kafka_request_timeout_ms: int = Field(default=10_000, ge=1_000, le=120_000)
    generator_rate_per_second: float = Field(default=1.0, gt=0, le=1_000)
    generator_max_product_views: int = Field(default=3, gt=0, le=100)
    generator_add_to_cart_probability: float = Field(default=0.55, ge=0, le=1)
    generator_checkout_probability: float = Field(default=0.70, ge=0, le=1)
    generator_payment_success_probability: float = Field(default=0.85, ge=0, le=1)
    generator_refund_probability: float = Field(default=0.05, ge=0, le=1)
    generator_seed: int | None = None
    generator_log_level: LogLevel = "INFO"
    generator_flush_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    generator_journeys: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_kafka_timeouts(self) -> Self:
        """Delivery timeout must not be shorter than request timeout."""
        if self.kafka_delivery_timeout_ms < self.kafka_request_timeout_ms:
            raise ValueError(
                "kafka_delivery_timeout_ms must be at least kafka_request_timeout_ms"
            )
        return self

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> "GeneratorConfig":
        """Load known environment variables and validate their values."""
        source = os.environ if environment is None else environment
        names = {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka_bootstrap_servers",
            "KAFKA_EVENTS_TOPIC": "kafka_events_topic",
            "KAFKA_CLIENT_ID": "kafka_client_id",
            "KAFKA_COMPRESSION_TYPE": "kafka_compression_type",
            "KAFKA_LINGER_MS": "kafka_linger_ms",
            "KAFKA_BATCH_SIZE": "kafka_batch_size",
            "KAFKA_DELIVERY_TIMEOUT_MS": "kafka_delivery_timeout_ms",
            "KAFKA_REQUEST_TIMEOUT_MS": "kafka_request_timeout_ms",
            "GENERATOR_RATE_PER_SECOND": "generator_rate_per_second",
            "GENERATOR_MAX_PRODUCT_VIEWS": "generator_max_product_views",
            "GENERATOR_ADD_TO_CART_PROBABILITY": ("generator_add_to_cart_probability"),
            "GENERATOR_CHECKOUT_PROBABILITY": "generator_checkout_probability",
            "GENERATOR_PAYMENT_SUCCESS_PROBABILITY": (
                "generator_payment_success_probability"
            ),
            "GENERATOR_REFUND_PROBABILITY": "generator_refund_probability",
            "GENERATOR_SEED": "generator_seed",
            "GENERATOR_LOG_LEVEL": "generator_log_level",
            "GENERATOR_FLUSH_TIMEOUT_SECONDS": "generator_flush_timeout_seconds",
            "GENERATOR_JOURNEYS": "generator_journeys",
        }
        optional_numbers = {"GENERATOR_SEED", "GENERATOR_JOURNEYS"}
        values: dict[str, str] = {}
        for environment_name, field_name in names.items():
            if environment_name not in source:
                continue
            value = source[environment_name]
            if environment_name in optional_numbers and value == "":
                continue
            values[field_name] = value
        return cls.model_validate(values)
