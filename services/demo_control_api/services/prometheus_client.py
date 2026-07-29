"""Restricted predefined Prometheus queries."""

from math import isfinite
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


class RestrictedPrometheusClient:
    def __init__(self, url: str, timeout: float) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout

    async def summary(self) -> dict[str, Any]:
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
