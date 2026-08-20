"""Low-overhead SQL timing and statement classification for one transaction."""

from time import perf_counter
from typing import Any

import psycopg

from shared.observability.metrics import ApplicationMetrics


class InstrumentedConnection:
    """Transparent connection facade that times bounded SQL categories."""

    def __init__(
        self, connection: psycopg.Connection[Any], metrics: ApplicationMetrics
    ):
        self._connection = connection
        self.metrics = metrics
        self.phase = "other"

    def set_phase(self, phase: str) -> None:
        self.phase = phase

    def cursor(self, *args: Any, **kwargs: Any) -> "InstrumentedCursor":
        return InstrumentedCursor(self._connection.cursor(*args, **kwargs), self)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


class InstrumentedCursor:
    def __init__(self, cursor: Any, connection: InstrumentedConnection):
        self._cursor = cursor
        self._connection = connection

    def execute(self, query: Any, params: Any = None, *args: Any, **kwargs: Any) -> Any:
        started = perf_counter()
        try:
            if params is None:
                return self._cursor.execute(query, *args, **kwargs)
            return self._cursor.execute(query, params, *args, **kwargs)
        finally:
            self._observe(query, perf_counter() - started)

    def executemany(
        self, query: Any, params_seq: Any, *args: Any, **kwargs: Any
    ) -> Any:
        started = perf_counter()
        try:
            return self._cursor.executemany(query, params_seq, *args, **kwargs)
        finally:
            self._observe(query, perf_counter() - started)

    def _observe(self, query: Any, duration: float) -> None:
        operation, kind = _classify(str(query), self._connection.phase)
        self._connection.metrics.database_sql_duration.labels(operation, kind).observe(
            duration
        )
        self._connection.metrics.database_sql_statement_count.labels(
            operation, kind
        ).inc()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    def __enter__(self) -> "InstrumentedCursor":
        self._cursor.__enter__()
        return self

    def __exit__(self, *args: Any) -> Any:
        return self._cursor.__exit__(*args)


def _classify(query: str, phase: str) -> tuple[str, str]:
    normalized = " ".join(query.lower().split())
    kind = (
        "select"
        if normalized.startswith("select")
        else "insert"
        if normalized.startswith("insert")
        else "update"
        if normalized.startswith("update")
        else "delete"
        if normalized.startswith("delete")
        else "other"
    )
    if "processed_events" in normalized:
        return (
            "processed_events_insert" if kind == "insert" else "processed_events_select"
        ), kind
    if "fraud_evaluations" in normalized:
        return (
            "fraud_evaluation_write" if kind == "insert" else "fraud_evaluation_select"
        ), kind
    if "fraud_alerts" in normalized:
        return "fraud_alert_write", kind
    if "fraud_outbox" in normalized:
        return "outbox_insert", kind
    if phase == "fraud_context":
        if kind != "select":
            return "fraud_context_other", kind
        if "home_country" in normalized and "billing_country" in normalized:
            return "fraud_context_customer_order", kind
        if "home_country" in normalized:
            return "fraud_context_customer", kind
        if "select session_id from orders" in normalized:
            return "fraud_context_order_session", kind
        if "started_at from sessions" in normalized:
            return "fraud_context_session", kind
        if "ordered_at, total" in normalized:
            return "fraud_context_order", kind
        if "requested_at, amount from refunds" in normalized:
            return "fraud_context_refunds", kind
        if "count(*)" in normalized and "product_views" in normalized:
            return "fraud_context_product_views", kind
        if "count(*)" in normalized and "orders" in normalized:
            return "fraud_context_recent_orders", kind
        if "p.amount - coalesce" in normalized:
            return "fraud_context_refund_facts", kind
        if "attempted_at <=" in normalized:
            return "fraud_context_recent_payments", kind
        if "attempted_at <" in normalized:
            return "fraud_context_prior_payments", kind
        return "fraud_context_other", kind
    if phase == "business":
        for table in (
            "customers",
            "sessions",
            "product_views",
            "carts",
            "cart_items",
            "orders",
            "payments",
            "refunds",
        ):
            if table in normalized:
                return "business_" + table, kind
        return "business_other", kind
    return "other_sql", kind
