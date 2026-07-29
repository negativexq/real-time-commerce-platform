"""Validated, service-aware observability configuration."""

import re
from collections.abc import Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator

_METRIC_NAME = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")


class MetricsConfig(BaseModel):
    """Common metrics and recoverable-health settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = Field(default=9100, ge=1, le=65535)
    path: str = "/metrics"
    namespace: str = "commerce"
    service_name: str
    max_poll_staleness_seconds: float = Field(default=30, gt=0)
    max_success_staleness_seconds: float = Field(default=300, gt=0)
    max_outbox_staleness_seconds: float = Field(default=120, gt=0)
    refresh_interval_seconds: float = Field(default=15, gt=0, le=300)

    @field_validator("namespace")
    @classmethod
    def valid_namespace(cls, value: str) -> str:
        if not _METRIC_NAME.fullmatch(value):
            raise ValueError("metrics namespace must be a Prometheus identifier")
        return value

    @field_validator("path")
    @classmethod
    def valid_path(cls, value: str) -> str:
        if not value.startswith("/") or value == "/":
            raise ValueError("metrics path must be an absolute non-root path")
        return value

    @classmethod
    def from_environment(
        cls,
        service_name: str,
        default_port: int,
        environment: Mapping[str, str],
        *,
        port_environment_name: str,
    ) -> "MetricsConfig":
        values: dict[str, object] = {
            "service_name": environment.get("METRICS_SERVICE_NAME") or service_name,
            "port": environment.get(port_environment_name, default_port),
        }
        names = {
            "METRICS_ENABLED": "enabled",
            "METRICS_HOST": "host",
            "METRICS_PORT": "port",
            "METRICS_PATH": "path",
            "METRICS_NAMESPACE": "namespace",
            "HEALTH_MAX_POLL_STALENESS_SECONDS": "max_poll_staleness_seconds",
            "HEALTH_MAX_SUCCESS_STALENESS_SECONDS": "max_success_staleness_seconds",
            "HEALTH_MAX_OUTBOX_STALENESS_SECONDS": "max_outbox_staleness_seconds",
            "METRICS_REFRESH_INTERVAL_SECONDS": "refresh_interval_seconds",
        }
        values.update(
            {
                field: environment[name]
                for name, field in names.items()
                if name in environment
            }
        )
        return cls.model_validate(values)
