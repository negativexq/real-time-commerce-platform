"""Sprint 10 bounded domain tests without live dependencies."""

import asyncio
import random
from collections import Counter
from unittest.mock import AsyncMock

import httpx
import pytest
from pydantic import ValidationError

from services.demo_control_api.config import DemoConfig
from services.demo_control_api.models.runs import RunStatus, transition_allowed
from services.demo_control_api.models.scenarios import RunCreate, ScenarioType
from services.demo_control_api.services.dashboard_catalog import dashboard_catalog
from services.demo_control_api.services.platform_health import PlatformHealth
from services.demo_control_api.services.prometheus_client import (
    QUERIES,
    RestrictedPrometheusClient,
)
from services.demo_control_api.services.scenario_catalog import SCENARIOS
from services.demo_control_api.services.scenario_runner import build_generator_config
from services.event_generator.generator import SeededUuidFactory, SyntheticGenerator
from services.event_generator.journey import JourneyBuilder, SystemClock


def test_catalog_is_complete_and_allow_listed() -> None:
    assert set(SCENARIOS) == set(ScenarioType)
    with pytest.raises(ValueError):
        RunCreate.model_validate({"scenario_type": "arbitrary.module.Class"})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_count", 0),
        ("event_count", 100001),
        ("events_per_second", 0),
        ("events_per_second", 1001),
        ("duration_seconds", 3601),
    ],
)
def test_parameter_bounds(field: str, value: int) -> None:
    with pytest.raises(ValidationError):
        RunCreate.model_validate({"scenario_type": "normal_customer", field: value})


def test_mixed_distribution_must_total_100() -> None:
    with pytest.raises(ValidationError, match="total 100"):
        RunCreate.model_validate(
            {
                "scenario_type": ScenarioType.MIXED,
                "persona_distribution": {"normal": 99},
            }
        )
    request = RunCreate.model_validate(
        {
            "scenario_type": ScenarioType.MIXED,
            "persona_distribution": {"normal": 100},
        }
    )
    assert sum(request.persona_distribution.values()) == 100  # type: ignore[union-attr]


def test_duration_rate_must_be_feasible() -> None:
    with pytest.raises(ValidationError, match="exceeds"):
        RunCreate.model_validate(
            {
                "scenario_type": ScenarioType.NORMAL,
                "event_count": 11,
                "duration_seconds": 1,
                "events_per_second": 10,
            }
        )


def test_state_machine_allows_only_documented_edges() -> None:
    assert transition_allowed(RunStatus.PENDING, RunStatus.STARTING)
    assert transition_allowed(RunStatus.RUNNING, RunStatus.STOP_REQUESTED)
    assert transition_allowed(RunStatus.STOP_REQUESTED, RunStatus.STOPPED)
    assert not transition_allowed(RunStatus.PENDING, RunStatus.COMPLETED)
    assert not transition_allowed(RunStatus.COMPLETED, RunStatus.RUNNING)


def test_configuration_rejects_short_enabled_token_material() -> None:
    with pytest.raises(ValidationError):
        DemoConfig.model_validate({"demo_api_token": "short"})


def test_prometheus_and_dashboard_inputs_are_fixed() -> None:
    assert "run_id" not in " ".join(QUERIES.values())
    assert len(dashboard_catalog("http://grafana:3000")) == 7


@pytest.mark.asyncio
async def test_unmonitored_services_do_not_degrade_platform() -> None:
    health = PlatformHealth(DemoConfig.model_validate({}))
    health._check = AsyncMock(  # type: ignore[method-assign]
        return_value=[
            {"name": "Event Processor", "state": "HEALTHY"},
            {"name": "Generator", "state": "NOT_MONITORED"},
            {"name": "Fraud Engine", "state": "UNKNOWN"},
        ]
    )
    assert (await health.get())["overall"] == "HEALTHY"


@pytest.mark.asyncio
async def test_unhealthy_monitored_service_degrades_platform() -> None:
    health = PlatformHealth(DemoConfig.model_validate({}))
    health._check = AsyncMock(  # type: ignore[method-assign]
        return_value=[
            {"name": "Event Processor", "state": "HEALTHY"},
            {"name": "Fraud Outbox Publisher", "state": "UNHEALTHY"},
            {"name": "Fraud Engine", "state": "NOT_MONITORED"},
        ]
    )
    assert (await health.get())["overall"] == "DEGRADED"


@pytest.mark.asyncio
async def test_exporter_health_comes_from_prometheus_up() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/query"
        result = [
            {"metric": {"job": "kafka-exporter"}, "value": [0, "1"]},
            {"metric": {"job": "redis-exporter"}, "value": [0, "0"]},
        ]
        return httpx.Response(200, json={"data": {"result": result}})

    health = PlatformHealth(DemoConfig.model_validate({}))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        states = {
            item["name"]: item["state"] for item in await health._exporters(client)
        }
    assert states == {
        "Kafka Exporter": "HEALTHY",
        "PostgreSQL Exporter": "UNKNOWN",
        "Redis Exporter": "UNHEALTHY",
    }


@pytest.mark.asyncio
async def test_exporter_health_is_unknown_when_prometheus_fails() -> None:
    health = PlatformHealth(DemoConfig.model_validate({}))
    transport = httpx.MockTransport(lambda _: httpx.Response(503))
    async with httpx.AsyncClient(transport=transport) as client:
        states = {item["state"] for item in await health._exporters(client)}
    assert states == {"UNKNOWN"}


@pytest.mark.asyncio
async def test_alerts_list_firing_and_critical_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/alerts"
        alerts = [
            {
                "labels": {"alertname": "HighConsumerLag", "severity": "warning"},
                "annotations": {"summary": "Lag"},
                "state": "firing",
                "activeAt": "2026-11-27T11:00:00Z",
            },
            {
                "labels": {"alertname": "DLQSinkBehind", "severity": "warning"},
                "annotations": {},
                "state": "pending",
            },
            {
                "labels": {
                    "alertname": "DLQSinkDown",
                    "severity": "critical",
                    "job": "dlq-sink",
                },
                "annotations": {"summary": "DLQ sink is down"},
                "state": "firing",
            },
        ]
        return httpx.Response(200, json={"data": {"alerts": alerts}})

    _mock_transport(monkeypatch, handler)
    result = await RestrictedPrometheusClient("http://prometheus:9090", 1).alerts()
    assert result["status"] == "available"
    assert result["firing"] == 2
    assert [item["name"] for item in result["items"]] == [
        "DLQSinkDown",
        "HighConsumerLag",
        "DLQSinkBehind",
    ]
    assert result["items"][0]["source"] == "dlq-sink"


@pytest.mark.asyncio
async def test_alerts_report_unavailable_when_prometheus_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_transport(monkeypatch, lambda _: httpx.Response(503))
    result = await RestrictedPrometheusClient("http://prometheus:9090", 1).alerts()
    assert result == {"status": "unavailable", "items": [], "firing": 0}


@pytest.mark.asyncio
async def test_alerts_are_cached_for_every_caller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"data": {"alerts": []}})

    _mock_transport(monkeypatch, handler)
    client = RestrictedPrometheusClient("http://prometheus:9090", 1, alerts_ttl=60)
    results = await asyncio.gather(*(client.alerts() for _ in range(10)))
    assert calls == 1
    assert all(result["status"] == "available" for result in results)

    uncached = RestrictedPrometheusClient("http://prometheus:9090", 1)
    await uncached.alerts()
    await uncached.alerts()
    assert calls == 3


def _mock_transport(monkeypatch: pytest.MonkeyPatch, handler: object) -> None:
    real = httpx.AsyncClient

    def client(*args: object, **kwargs: object) -> httpx.AsyncClient:
        kwargs["transport"] = httpx.MockTransport(handler)  # type: ignore[arg-type]
        return real(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(httpx, "AsyncClient", client)


def test_internal_service_health_urls_use_container_ports() -> None:
    config = DemoConfig.model_validate({})
    assert config.processor_health_url == "http://event-processor:9101/health"
    assert config.outbox_health_url == "http://fraud-outbox-publisher:9103/health"
    assert config.grafana_health_url == "http://grafana:3000/api/health"


MIXED_REQUEST = {
    "scenario_type": "mixed_traffic",
    "event_count": 4000,
    "events_per_second": 40,
    "seed": 7,
    "persona_distribution": {
        "normal": 50,
        "suspicious": 20,
        "bot": 10,
        "account_takeover": 10,
        "discount_hunter": 5,
        "indecisive": 5,
    },
}


def test_mixed_traffic_uses_the_requested_persona_mix() -> None:
    # Regression: pinning add-to-cart/checkout to 1 made the generator ignore
    # persona weights, so mixed runs were all normal customers and never
    # produced REVIEW or BLOCK decisions.
    config = build_generator_config(
        RunCreate.model_validate(MIXED_REQUEST), DemoConfig.model_validate({})
    )
    builder = JourneyBuilder(
        config,
        SyntheticGenerator(random.Random(7), SeededUuidFactory(7)),
        SystemClock(),
    )
    chosen = Counter(builder._choose_persona().value for _ in range(1000))
    assert chosen["suspicious"] > 120
    assert chosen["bot"] > 40
    assert chosen["normal"] < 750


@pytest.mark.parametrize(
    ("scenario", "add_to_cart", "refund"),
    [("suspicious_payment", 1, 0), ("refund_abuse", 1, 1)],
)
def test_single_persona_scenarios_still_force_checkout(
    scenario: str, add_to_cart: float, refund: float
) -> None:
    config = build_generator_config(
        RunCreate.model_validate({"scenario_type": scenario}),
        DemoConfig.model_validate({}),
    )
    assert config.generator_add_to_cart_probability == add_to_cart
    assert config.generator_checkout_probability == 1
    assert config.generator_refund_probability == refund
