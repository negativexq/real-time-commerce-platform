"""Restricted predefined Prometheus queries."""

from typing import Any

import httpx

QUERIES = {
    "processed_rate": 'sum(rate(commerce_processor_events_terminal_total{result="processed"}[5m]))',
    "duplicate_rate": 'sum(rate(commerce_processor_events_terminal_total{result="duplicate"}[5m]))',
    "dlq_rate": 'sum(rate(commerce_processor_events_terminal_total{result="dlq"}[5m]))',
    "consumer_lag": "sum(kafka_consumergroup_lag)",
    "p95_latency": "commerce:processor_latency_seconds:p95",
    "database_success_rate": 'sum(rate(commerce_database_transactions_total{result="committed"}[5m]))',
    "fraud_alert_rate": "sum(rate(commerce_fraud_alerts_created_total[5m]))",
    "outbox_pending": 'sum(commerce_outbox_rows{status="pending"})',
}


class RestrictedPrometheusClient:
    def __init__(self, url: str, timeout: float) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout

    async def summary(self) -> dict[str, Any]:
        values: dict[str, float | None] = {}
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                for name, query in QUERIES.items():
                    response = await client.get(
                        f"{self.url}/api/v1/query", params={"query": query}
                    )
                    response.raise_for_status()
                    result = response.json().get("data", {}).get("result", [])
                    values[name] = float(result[0]["value"][1]) if result else None
            return {
                "status": "available",
                "scope": "platform_wide_prometheus",
                "values": values,
            }
        except (httpx.HTTPError, KeyError, TypeError, ValueError):
            return {
                "status": "degraded",
                "scope": "platform_wide_prometheus",
                "values": values,
            }
