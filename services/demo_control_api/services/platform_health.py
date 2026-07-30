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

MONITORED_STATES = {"HEALTHY", "DEGRADED", "UNHEALTHY"}


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
            monitored = [item for item in services if item["state"] in MONITORED_STATES]
            state = (
                "DEGRADED"
                if any(item["state"] != "HEALTHY" for item in monitored)
                else "HEALTHY"
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
        required_urls = {
            "Event Processor": self.config.processor_health_url,
            "Fraud Outbox Publisher": self.config.outbox_health_url,
            "Prometheus": f"{self.config.prometheus_url}/-/ready",
            "Grafana": self.config.grafana_health_url,
            "Kafka UI": self.config.kafka_ui_health_url,
        }
        async with httpx.AsyncClient(timeout=2) as client:
            try:
                response = await client.get(self.config.generator_health_url)
                generator_state = "HEALTHY" if response.is_success else "UNHEALTHY"
            except httpx.HTTPError:
                generator_state = "NOT_MONITORED"
            results.append({"name": "Generator", "state": generator_state})
            results.append({"name": "Fraud Engine", "state": "NOT_MONITORED"})
            for name, url in required_urls.items():
                try:
                    response = await client.get(url)
                    state = "HEALTHY" if response.is_success else "UNHEALTHY"
                except httpx.HTTPError:
                    state = "UNHEALTHY"
                results.append({"name": name, "state": state})
        for name in (
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
