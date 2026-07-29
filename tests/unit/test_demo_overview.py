"""Overview data-contract tests."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from services.demo_control_api.models.runs import DemoRun, RunStatus
from services.demo_control_api.models.scenarios import RunCreate, ScenarioType
from services.demo_control_api.services.overview import (
    build_fraud_summary,
    select_overview_run,
)
from services.demo_control_api.services.prometheus_client import (
    PRESENCE_QUERIES,
    QUERIES,
    RestrictedPrometheusClient,
)


def run(status: RunStatus, created_at: datetime, **counts: int) -> DemoRun:
    run_id = uuid4()
    return DemoRun(
        run_id=run_id,
        scenario_type=ScenarioType.TAKEOVER,
        status=status,
        requested_event_count=20,
        requested_duration_seconds=None,
        requested_events_per_second=10,
        seed=7,
        parameters=RunCreate(
            scenario_type=ScenarioType.TAKEOVER,
            event_count=20,
            events_per_second=10,
            seed=7,
        ),
        test_scope=f"demo:{run_id}",
        created_at=created_at,
        updated_at=created_at,
        **counts,
    )


def vector(value: float | None) -> dict[str, Any]:
    result = [] if value is None else [{"metric": {}, "value": [1, str(value)]}]
    return {"data": {"resultType": "vector", "result": result}}


def scalar(value: float) -> dict[str, Any]:
    return {"data": {"resultType": "scalar", "result": [1, str(value)]}}


class Response:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self.payload


class Client:
    def __init__(self, responses: dict[str, dict[str, Any]]) -> None:
        self.responses = responses

    async def get(self, _url: str, *, params: dict[str, str]) -> Response:
        return Response(self.responses.get(params["query"], vector(0)))


@pytest.mark.asyncio
async def test_available_counter_without_recent_events_returns_zero() -> None:
    client = Client(
        {
            QUERIES["processed_rate"]: vector(None),
            PRESENCE_QUERIES["processed_rate"]: vector(10),
        }
    )
    value = await RestrictedPrometheusClient("http://prometheus", 1)._query_metric(
        client, "processed_rate", QUERIES["processed_rate"]
    )
    assert value == 0.0


@pytest.mark.asyncio
async def test_unavailable_counter_remains_null() -> None:
    missing = Client(
        {
            QUERIES["processed_rate"]: vector(None),
            PRESENCE_QUERIES["processed_rate"]: vector(None),
        }
    )
    assert (
        await RestrictedPrometheusClient("http://prometheus", 1)._query_metric(
            missing, "processed_rate", QUERIES["processed_rate"]
        )
        is None
    )


@pytest.mark.asyncio
async def test_prometheus_scalar_and_vector_results_are_supported() -> None:
    query = "test_query"
    prometheus = RestrictedPrometheusClient("http://prometheus", 1)
    assert await prometheus._query(Client({query: vector(2.5)}), query) == 2.5
    assert await prometheus._query(Client({query: scalar(3.5)}), query) == 3.5


def test_average_latency_uses_histogram_sum_and_count() -> None:
    query = QUERIES["average_latency_seconds"]
    assert "event_processing_duration_seconds_sum" in query
    assert "event_processing_duration_seconds_count" in query
    assert " / " in query


def test_selected_run_prefers_active_then_latest_completed() -> None:
    now = datetime.now(UTC)
    latest_completed = run(RunStatus.COMPLETED, now)
    older_active = run(RunStatus.RUNNING, now - timedelta(hours=1))
    assert select_overview_run([latest_completed, older_active]) == older_active
    older_completed = run(RunStatus.COMPLETED, now - timedelta(hours=2))
    assert select_overview_run([older_completed, latest_completed]) == latest_completed


def test_fraud_summary_counts_share_one_run_and_total_is_consistent() -> None:
    selected = run(
        RunStatus.RUNNING,
        datetime.now(UTC),
        approve_count=8,
        review_count=2,
        block_count=3,
        fraud_alert_count=3,
    )
    summary = build_fraud_summary(selected)
    assert summary.scope == "ACTIVE_RUN"
    assert summary.approve_count == 8
    assert summary.review_count == 2
    assert summary.block_count == 3
    assert summary.fraud_alert_count == 3
    assert summary.total_decisions == 13
