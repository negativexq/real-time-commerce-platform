"""Validated environment configuration for the local control plane."""

import os
from collections.abc import Mapping

from pydantic import BaseModel, Field, field_validator


class DemoConfig(BaseModel):
    postgres_dsn: str = (
        "postgresql://commerce:commerce_local_dev@postgres:5432/commerce"
    )
    redis_url: str = "redis://redis:6379/0"
    kafka_bootstrap_servers: str = "kafka:9092"
    demo_max_concurrent_runs: int = Field(2, ge=1, le=10)
    demo_run_timeout_seconds: int = Field(3600, ge=1, le=3600)
    demo_run_stop_timeout_seconds: int = Field(15, ge=1, le=60)
    demo_progress_refresh_interval_ms: int = Field(1000, ge=250, le=10000)
    demo_max_event_count: int = Field(100000, ge=1, le=100000)
    demo_max_events_per_second: int = Field(1000, ge=1, le=1000)
    demo_max_duration_seconds: int = Field(3600, ge=1, le=3600)
    demo_allowed_origins: tuple[str, ...] = ("http://localhost:3003",)
    demo_api_token_enabled: bool = False
    demo_api_token: str = ""
    demo_history_page_size: int = Field(20, ge=1, le=100)
    demo_history_max_page_size: int = Field(100, ge=1, le=100)
    demo_health_cache_seconds: int = Field(5, ge=1, le=60)
    demo_prometheus_timeout_seconds: float = Field(3, gt=0, le=10)
    demo_sse_heartbeat_seconds: int = Field(10, ge=2, le=60)
    prometheus_url: str = "http://prometheus:9090"
    grafana_url: str = "http://localhost:3002"
    kafka_ui_url: str = "http://localhost:8080"

    @field_validator("demo_api_token")
    @classmethod
    def validate_token(cls, value: str) -> str:
        if value and len(value) < 16:
            raise ValueError("DEMO_API_TOKEN must contain at least 16 characters")
        return value

    @classmethod
    def from_environment(
        cls, environment: Mapping[str, str] | None = None
    ) -> "DemoConfig":
        source = os.environ if environment is None else environment
        values: dict[str, object] = {}
        for name, field in cls.model_fields.items():
            key = name.upper()
            if key not in source:
                continue
            value: object = source[key]
            if name == "demo_allowed_origins":
                value = tuple(
                    item.strip() for item in str(value).split(",") if item.strip()
                )
            values[field.alias or name] = value
        return cls.model_validate(values)
