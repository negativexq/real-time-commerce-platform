"""Sprint 7 database configuration and credential-safe endpoint tests."""

import pytest
from pydantic import ValidationError

from services.event_processor.config import ProcessorConfig
from services.event_processor.persistence.database import safe_postgres_endpoint


def test_database_defaults_are_bounded() -> None:
    config = ProcessorConfig()
    assert config.processor_db_pool_min_size == 1
    assert config.processor_db_pool_max_size == 4
    assert config.processor_db_statement_timeout_ms == 5_000
    assert config.processor_required_schema_version == 4
    assert config.processor_persist_raw_event_json


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("POSTGRES_DSN", ""),
        ("PROCESSOR_DB_POOL_MIN_SIZE", "0"),
        ("PROCESSOR_DB_POOL_MAX_SIZE", "0"),
        ("PROCESSOR_DB_CONNECT_TIMEOUT_SECONDS", "0"),
        ("PROCESSOR_DB_ACQUIRE_TIMEOUT_SECONDS", "0"),
        ("PROCESSOR_DB_STATEMENT_TIMEOUT_MS", "0"),
        ("PROCESSOR_DB_STARTUP_ATTEMPTS", "0"),
    ],
)
def test_invalid_database_environment_is_rejected(name: str, value: str) -> None:
    with pytest.raises(ValidationError):
        ProcessorConfig.from_environment({name: value})


def test_pool_maximum_must_cover_minimum() -> None:
    with pytest.raises(ValidationError, match="maximum"):
        ProcessorConfig(
            processor_db_pool_min_size=5,
            processor_db_pool_max_size=4,
        )


def test_postgres_endpoint_never_contains_credentials() -> None:
    endpoint = safe_postgres_endpoint(
        "postgresql://commerce:super-secret@postgres:5432/commerce"
    )
    assert endpoint == "postgresql://postgres:5432/commerce"
    assert "commerce:" not in endpoint
    assert "super-secret" not in endpoint
