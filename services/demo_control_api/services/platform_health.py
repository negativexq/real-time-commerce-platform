"""Briefly cached dependency health checks."""

import asyncio
from datetime import UTC, datetime
from time import monotonic
from typing import Any

import httpx
import psycopg
import redis.asyncio as redis
from confluent_kafka.admin import AdminClient  # type: ignore[import-untyped]

from services.demo_control_api.config import DemoConfig


class PlatformHealth:
    def __init__(self, config: DemoConfig) -> None:
        self.config = config
        self._cached: tuple[float, dict[str, Any]] | None = None
        self._lock = asyncio.Lock()

    async def get(self) -> dict[str, Any]:
        async with self._lock:
            if (
                self._cached
                and monotonic() - self._cached[0]
                < self.config.demo_health_cache_seconds
            ):
                return self._cached[1]
            services = await self._check()
            state = (
                "HEALTHY"
                if all(item["state"] == "HEALTHY" for item in services)
                else "DEGRADED"
            )
            result = {
                "overall": state,
                "checked_at": datetime.now(UTC).isoformat(),
                "services": services,
            }
            self._cached = (monotonic(), result)
            return result

    async def _check(self) -> list[dict[str, str]]:
        results: list[dict[str, str]] = []
        try:
            await asyncio.to_thread(self._postgres)
            results.append({"name": "PostgreSQL", "state": "HEALTHY"})
        except Exception:
            results.append({"name": "PostgreSQL", "state": "UNHEALTHY"})
        try:
            client = redis.from_url(  # type: ignore[no-untyped-call]
                self.config.redis_url, socket_timeout=2
            )
            await client.ping()
            await client.aclose()
            results.append({"name": "Redis", "state": "HEALTHY"})
        except Exception:
            results.append({"name": "Redis", "state": "UNHEALTHY"})
        try:
            await asyncio.to_thread(self._kafka)
            results.append({"name": "Kafka", "state": "HEALTHY"})
        except Exception:
            results.append({"name": "Kafka", "state": "UNHEALTHY"})
        urls = {
            "Prometheus": f"{self.config.prometheus_url}/-/ready",
            "Grafana": self.config.grafana_url.replace("localhost", "grafana")
            + "/api/health",
            "Kafka UI": self.config.kafka_ui_url.replace("localhost", "kafka-ui"),
        }
        async with httpx.AsyncClient(timeout=2) as client:
            for name, url in urls.items():
                try:
                    response = await client.get(url)
                    state = "HEALTHY" if response.is_success else "UNHEALTHY"
                except httpx.HTTPError:
                    state = "UNKNOWN"
                results.append({"name": name, "state": state})
        for name in (
            "Event Processor",
            "Fraud Outbox Publisher",
            "Kafka Exporter",
            "PostgreSQL Exporter",
            "Redis Exporter",
        ):
            results.append({"name": name, "state": "UNKNOWN"})
        results.append({"name": "Demo Control API", "state": "HEALTHY"})
        return results

    def _postgres(self) -> None:
        with psycopg.connect(self.config.postgres_dsn, connect_timeout=2) as connection:
            connection.execute("SELECT 1")

    def _kafka(self) -> None:
        AdminClient(
            {"bootstrap.servers": self.config.kafka_bootstrap_servers}
        ).list_topics(timeout=2)
