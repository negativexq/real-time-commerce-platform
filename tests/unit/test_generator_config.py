"""Unit tests for event-generator configuration and CLI precedence."""

import pytest
from pydantic import ValidationError

from services.event_generator.config import GeneratorConfig
from services.event_generator.main import parse_config


def test_configuration_defaults() -> None:
    """Defaults target internal Kafka and conservative producer settings."""
    config = GeneratorConfig()

    assert config.kafka_bootstrap_servers == "kafka:9092"
    assert config.kafka_events_topic == "commerce.events"
    assert config.kafka_compression_type == "lz4"
    assert config.generator_rate_per_second == 1.0
    assert config.generator_journeys is None


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("GENERATOR_RATE_PER_SECOND", "0"),
        ("GENERATOR_MAX_PRODUCT_VIEWS", "0"),
        ("GENERATOR_ADD_TO_CART_PROBABILITY", "1.01"),
        ("GENERATOR_CHECKOUT_PROBABILITY", "-0.01"),
        ("GENERATOR_FLUSH_TIMEOUT_SECONDS", "0"),
        ("KAFKA_EVENTS_TOPIC", "   "),
        ("KAFKA_BOOTSTRAP_SERVERS", ""),
        ("KAFKA_COMPRESSION_TYPE", "brotli"),
        ("KAFKA_LINGER_MS", "5001"),
        ("KAFKA_BATCH_SIZE", "10"),
    ],
)
def test_configuration_rejects_invalid_environment(name: str, value: str) -> None:
    """Invalid ranges and blank/unsupported strings fail validation."""
    with pytest.raises(ValidationError):
        GeneratorConfig.from_environment({name: value})


def test_cli_overrides_environment() -> None:
    """Explicit CLI values take precedence over environment defaults."""
    config = parse_config(
        ["--journeys", "10", "--rate", "2.5", "--seed", "42", "--log-level", "DEBUG"],
        {
            "GENERATOR_JOURNEYS": "3",
            "GENERATOR_RATE_PER_SECOND": "1.5",
            "GENERATOR_SEED": "7",
            "GENERATOR_LOG_LEVEL": "INFO",
        },
    )

    assert config.generator_journeys == 10
    assert config.generator_rate_per_second == 2.5
    assert config.generator_seed == 42
    assert config.generator_log_level == "DEBUG"


def test_delivery_timeout_must_cover_request_timeout() -> None:
    """Kafka timeout settings reject an impossible ordering."""
    with pytest.raises(ValidationError, match="at least"):
        GeneratorConfig(
            kafka_delivery_timeout_ms=5_000,
            kafka_request_timeout_ms=10_000,
        )
