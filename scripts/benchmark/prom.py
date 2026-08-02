"""Minimal Prometheus HTTP API client for arbitrary PromQL queries.

Separate from ``services.demo_control_api.services.prometheus_client``
because that client only exposes a fixed allow-listed query set for the API;
the benchmark needs ad-hoc instant/range/quantile queries.
"""

from math import isfinite
from typing import Any

import httpx


class PrometheusClient:
    def __init__(self, url: str, timeout: float = 10.0) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout

    def instant(self, query: str, time: float | None = None) -> float | None:
        params: dict[str, Any] = {"query": query}
        if time is not None:
            params["time"] = time
        response = httpx.get(
            f"{self.url}/api/v1/query", params=params, timeout=self.timeout
        )
        response.raise_for_status()
        payload = response.json()["data"]
        result = payload.get("result")
        if payload.get("resultType") == "scalar":
            if not isinstance(result, list) or len(result) < 2:
                return None
            value = float(result[1])
        else:
            if not isinstance(result, list) or not result:
                return None
            value = float(result[0]["value"][1])
        return value if isfinite(value) else None

    def range(
        self, query: str, start: float, end: float, step: str = "5s"
    ) -> list[tuple[float, float]]:
        response = httpx.get(
            f"{self.url}/api/v1/query_range",
            params={"query": query, "start": start, "end": end, "step": step},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()["data"]
        result = payload.get("result")
        if not isinstance(result, list) or not result:
            return []
        series = result[0]["values"]
        points: list[tuple[float, float]] = []
        for ts, value in series:
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            if isfinite(numeric):
                points.append((float(ts), numeric))
        return points

    def quantile(
        self,
        metric_bucket: str,
        quantile: float,
        window_seconds: int,
        label_selector: str = "",
    ) -> float | None:
        """label_selector, if given, is a PromQL label matcher like
        '{route_template="/api/v1/runs"}' applied to the bucket metric.
        The 'le' label is always preserved for histogram_quantile."""
        query = (
            f"histogram_quantile({quantile}, "
            f"sum(rate({metric_bucket}{label_selector}[{window_seconds}s])) by (le))"
        )
        return self.instant(query)
