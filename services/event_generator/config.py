"""Validated event-generator configuration."""

import os
from collections.abc import Mapping
from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from shared.commerce_common.enums import CustomerPersona

NonBlank = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
CompressionType = Literal["gzip", "snappy", "lz4", "zstd", "none"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
DEFAULT_PERSONA_WEIGHTS = (
    "normal=0.50,indecisive=0.15,discount_hunter=0.15,"
    "suspicious=0.10,bot=0.05,account_takeover=0.05"
)


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
    generator_max_product_views: int = Field(default=20, gt=0, le=100)
    generator_add_to_cart_probability: float = Field(default=0.55, ge=0, le=1)
    generator_checkout_probability: float = Field(default=0.70, ge=0, le=1)
    generator_payment_success_probability: float = Field(default=0.85, ge=0, le=1)
    generator_refund_probability: float = Field(default=0.05, ge=0, le=1)
    generator_seed: int | None = None
    generator_log_level: LogLevel = "INFO"
    generator_flush_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    generator_journeys: int | None = Field(default=None, gt=0)
    generator_customer_pool_size: int = Field(default=100, gt=0, le=100_000)
    generator_new_customer_probability: float = Field(default=0.35, ge=0, le=1)
    generator_persona_weights: str = DEFAULT_PERSONA_WEIGHTS
    generator_persona: CustomerPersona | None = None
    generator_stateful_mode: bool = True
    generator_max_payment_attempts: int = Field(default=3, ge=1, le=10)
    generator_payment_retry_probability: float = Field(default=0.65, ge=0, le=1)
    generator_anomalies_enabled: bool = False
    generator_duplicate_event_probability: float = Field(default=0, ge=0, le=1)
    generator_malformed_json_probability: float = Field(default=0, ge=0, le=1)
    generator_missing_field_probability: float = Field(default=0, ge=0, le=1)
    generator_unknown_event_type_probability: float = Field(default=0, ge=0, le=1)
    generator_late_event_probability: float = Field(default=0, ge=0, le=1)
    generator_out_of_order_probability: float = Field(default=0, ge=0, le=1)
    generator_payload_mismatch_probability: float = Field(default=0, ge=0, le=1)
    generator_max_late_event_seconds: int = Field(default=86_400, gt=0, le=2_592_000)
    generator_max_anomalies_per_journey: int = Field(default=2, ge=0, le=20)

    @field_validator("generator_persona_weights")
    @classmethod
    def validate_persona_weights(cls, value: str) -> str:
        """Validate the documented comma-separated persona weight format."""
        parse_persona_weights(value)
        return value

    @model_validator(mode="after")
    def validate_kafka_timeouts(self) -> Self:
        """Delivery timeout must not be shorter than request timeout."""
        if self.kafka_delivery_timeout_ms < self.kafka_request_timeout_ms:
            raise ValueError(
                "kafka_delivery_timeout_ms must be at least kafka_request_timeout_ms"
            )
        return self

    @property
    def persona_weights(self) -> dict[CustomerPersona, float]:
        """Return normalized configured persona weights."""
        return parse_persona_weights(self.generator_persona_weights)

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
            "GENERATOR_CUSTOMER_POOL_SIZE": "generator_customer_pool_size",
            "GENERATOR_NEW_CUSTOMER_PROBABILITY": (
                "generator_new_customer_probability"
            ),
            "GENERATOR_PERSONA_WEIGHTS": "generator_persona_weights",
            "GENERATOR_STATEFUL_MODE": "generator_stateful_mode",
            "GENERATOR_MAX_PAYMENT_ATTEMPTS": "generator_max_payment_attempts",
            "GENERATOR_PAYMENT_RETRY_PROBABILITY": (
                "generator_payment_retry_probability"
            ),
            "GENERATOR_ANOMALIES_ENABLED": "generator_anomalies_enabled",
            "GENERATOR_DUPLICATE_EVENT_PROBABILITY": (
                "generator_duplicate_event_probability"
            ),
            "GENERATOR_MALFORMED_JSON_PROBABILITY": (
                "generator_malformed_json_probability"
            ),
            "GENERATOR_MISSING_FIELD_PROBABILITY": (
                "generator_missing_field_probability"
            ),
            "GENERATOR_UNKNOWN_EVENT_TYPE_PROBABILITY": (
                "generator_unknown_event_type_probability"
            ),
            "GENERATOR_LATE_EVENT_PROBABILITY": "generator_late_event_probability",
            "GENERATOR_OUT_OF_ORDER_PROBABILITY": (
                "generator_out_of_order_probability"
            ),
            "GENERATOR_PAYLOAD_MISMATCH_PROBABILITY": (
                "generator_payload_mismatch_probability"
            ),
            "GENERATOR_MAX_LATE_EVENT_SECONDS": ("generator_max_late_event_seconds"),
            "GENERATOR_MAX_ANOMALIES_PER_JOURNEY": (
                "generator_max_anomalies_per_journey"
            ),
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


def parse_persona_weights(value: str) -> dict[CustomerPersona, float]:
    """Parse and normalize persona weights without accepting unknown names."""
    weights: dict[CustomerPersona, float] = {}
    try:
        entries = [entry.strip() for entry in value.split(",") if entry.strip()]
        for entry in entries:
            name, raw_weight = (part.strip() for part in entry.split("=", 1))
            persona = CustomerPersona(name)
            if persona in weights:
                raise ValueError(f"duplicate persona weight: {name}")
            weight = float(raw_weight)
            if weight < 0:
                raise ValueError("persona weights must be non-negative")
            weights[persona] = weight
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid persona weights: {value!r}") from exc
    if set(weights) != set(CustomerPersona):
        missing = sorted(
            persona.value for persona in set(CustomerPersona) - set(weights)
        )
        raise ValueError(
            f"persona weights must include every persona; missing {missing}"
        )
    total = sum(weights.values())
    if total <= 0:
        raise ValueError("at least one persona weight must be positive")
    return {persona: weight / total for persona, weight in weights.items()}
