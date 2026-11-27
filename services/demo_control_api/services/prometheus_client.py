"""Restricted predefined Prometheus queries."""

import asyncio
from collections.abc import Awaitable, Callable
from math import inf, isfinite
from time import monotonic
from typing import Any

import httpx

QUERIES = {
    "processed_rate": 'sum(rate(commerce_processor_events_terminal_total{result="processed"}[2m]))',
    "average_latency_seconds": (
        "sum(rate(commerce_processor_event_processing_duration_seconds_sum[2m]))"
        " / "
        "sum(rate(commerce_processor_event_processing_duration_seconds_count[2m]))"
    ),
    "duplicate_rate": 'sum(rate(commerce_processor_events_terminal_total{result="duplicate"}[5m]))',
    "dlq_rate": 'sum(rate(commerce_processor_events_terminal_total{result="dlq"}[5m]))',
    "consumer_lag": (
        'sum(kafka_consumergroup_lag{consumergroup="commerce-event-processor-v1"})'
    ),
    "p95_latency": "commerce:processor_latency_seconds:p95",
    "database_success_rate": 'sum(rate(commerce_database_transactions_total{result="committed"}[5m]))',
    "fraud_alert_rate": "sum(rate(commerce_fraud_alerts_created_total[5m]))",
    "outbox_pending": 'sum(commerce_outbox_rows{status="pending"})',
}

PRESENCE_QUERIES = {
    "processed_rate": "count(commerce_processor_events_terminal_total)",
    "average_latency_seconds": (
        "count(commerce_processor_event_processing_duration_seconds_count)"
    ),
}


SEVERITY_ORDER = {"critical": 0, "warning": 1}
STATE_ORDER = {"firing": 0, "pending": 1}


def _alert_item(alert: dict[str, Any]) -> dict[str, Any]:
    labels = alert.get("labels") or {}
    annotations = alert.get("annotations") or {}
    return {
        "name": str(labels.get("alertname", "Unnamed alert")),
        "state": str(alert.get("state", "firing")),
        "severity": str(labels.get("severity", "warning")),
        "summary": str(annotations.get("summary", "")),
        "description": str(annotations.get("description", "")),
        "source": labels.get("job"),
        "active_at": alert.get("activeAt"),
    }


class TtlCache:
    """One cached value, recomputed at most once per `ttl` seconds.

    Concurrent callers wait for the same refresh instead of each running the
    queries, so ten open tabs cost Prometheus what one does.
    """

    def __init__(self, ttl: float) -> None:
        self.ttl = ttl
        self._value: dict[str, Any] | None = None
        self._at = -inf
        self._lock = asyncio.Lock()

    def _fresh(self) -> bool:
        return self._value is not None and monotonic() - self._at < self.ttl

    async def get(
        self, compute: Callable[[], Awaitable[dict[str, Any]]]
    ) -> dict[str, Any]:
        if self._fresh():
            assert self._value is not None
            return self._value
        async with self._lock:
            if not self._fresh():
                self._value = await compute()
                self._at = monotonic()
            assert self._value is not None
            return self._value


class RestrictedPrometheusClient:
    def __init__(
        self,
        url: str,
        timeout: float,
        summary_ttl: float = 0,
        alerts_ttl: float = 0,
    ) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout
        self._summary_cache = TtlCache(summary_ttl)
        self._alerts_cache = TtlCache(alerts_ttl)

    async def summary(self) -> dict[str, Any]:
        return await self._summary_cache.get(self._summary)

    async def alerts(self) -> dict[str, Any]:
        return await self._alerts_cache.get(self._alerts)

    async def _summary(self) -> dict[str, Any]:
        values: dict[str, float | None] = {}
        degraded = False
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for name, query in QUERIES.items():
                try:
                    values[name] = await self._query_metric(client, name, query)
                except (httpx.HTTPError, KeyError, TypeError, ValueError):
                    degraded = True
                    values[name] = None
        return {
            "status": "degraded" if degraded else "available",
            "scope": "platform_wide_prometheus",
            "values": values,
        }

    async def _alerts(self) -> dict[str, Any]:
        """Active Prometheus alerts, firing and most severe first.

        Inactive rules are not listed by Prometheus, so an empty list with
        status "available" means nothing is wrong.
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(f"{self.url}/api/v1/alerts")
                response.raise_for_status()
                raw = response.json()["data"]["alerts"]
            items = [_alert_item(alert) for alert in raw if isinstance(alert, dict)]
        except (httpx.HTTPError, KeyError, TypeError, ValueError):
            return {"status": "unavailable", "items": [], "firing": 0}
        items.sort(
            key=lambda item: (
                STATE_ORDER.get(item["state"], 2),
                SEVERITY_ORDER.get(item["severity"], 2),
                item["name"],
            )
        )
        firing = sum(1 for item in items if item["state"] == "firing")
        return {"status": "available", "items": items, "firing": firing}

    async def _query(self, client: Any, query: str) -> float | None:
        response = await client.get(f"{self.url}/api/v1/query", params={"query": query})
        response.raise_for_status()
        data = response.json()["data"]
        result = data.get("result")
        if data.get("resultType") == "scalar":
            if not isinstance(result, list) or len(result) < 2:
                return None
            value = float(result[1])
        else:
            if not isinstance(result, list) or not result:
                return None
            value = float(result[0]["value"][1])
        return value if isfinite(value) else None

    async def _query_metric(self, client: Any, name: str, query: str) -> float | None:
        value = await self._query(client, query)
        if value is not None or name not in PRESENCE_QUERIES:
            return value
        present = await self._query(client, PRESENCE_QUERIES[name])
        return 0.0 if present is not None and present > 0 else None
