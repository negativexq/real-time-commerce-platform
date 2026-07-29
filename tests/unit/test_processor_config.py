"""Processor configuration and CLI precedence tests."""

import pytest
from pydantic import ValidationError

from services.event_processor.config import ProcessorConfig, parse_config


def test_processor_defaults_are_safe() -> None:
    config = ProcessorConfig()
    assert config.processor_input_topic == "commerce.events"
    assert config.processor_dlq_topic == "commerce.events.dlq"
    assert config.processor_auto_offset_reset == "earliest"
    assert config.processor_max_processing_attempts == 3
    assert config.processor_max_messages is None


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("PROCESSOR_INPUT_TOPIC", " "),
        ("PROCESSOR_CONSUMER_GROUP", ""),
        ("PROCESSOR_AUTO_OFFSET_RESET", "middle"),
        ("PROCESSOR_MAX_PROCESSING_ATTEMPTS", "0"),
        ("PROCESSOR_RETRY_JITTER_RATIO", "1.1"),
        ("PROCESSOR_DLQ_MAX_PAYLOAD_BYTES", "0"),
        ("PROCESSOR_REDIS_SOCKET_TIMEOUT_SECONDS", "0"),
    ],
)
def test_processor_rejects_invalid_environment(name: str, value: str) -> None:
    with pytest.raises(ValidationError):
        ProcessorConfig.from_environment({name: value})


def test_processor_rejects_invalid_timing_and_ttls() -> None:
    with pytest.raises(ValidationError, match="heartbeat"):
        ProcessorConfig(
            processor_session_timeout_ms=6_000,
            processor_heartbeat_interval_ms=6_000,
        )
    with pytest.raises(ValidationError, match="completed TTL"):
        ProcessorConfig(
            processor_idempotency_processing_ttl_seconds=60,
            processor_idempotency_completed_ttl_seconds=60,
        )
    with pytest.raises(ValidationError, match="backoff"):
        ProcessorConfig(
            processor_retry_initial_backoff_ms=2_000,
            processor_retry_max_backoff_ms=100,
        )


def test_processor_cli_overrides_environment() -> None:
    config = parse_config(
        [
            "--max-messages",
            "20",
            "--idle-timeout",
            "5",
            "--log-level",
            "DEBUG",
            "--from-beginning",
            "--group-id",
            "test-group",
        ],
        {
            "PROCESSOR_IDLE_TIMEOUT_SECONDS": "30",
            "PROCESSOR_CONSUMER_GROUP": "environment-group",
        },
    )
    assert config.processor_max_messages == 20
    assert config.processor_idle_timeout_seconds == 5
    assert config.processor_log_level == "DEBUG"
    assert config.processor_from_beginning
    assert config.processor_consumer_group == "test-group"
