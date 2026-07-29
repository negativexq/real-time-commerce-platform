"""Sprint 10 bounded domain tests without live dependencies."""

import pytest
from pydantic import ValidationError

from services.demo_control_api.config import DemoConfig
from services.demo_control_api.models.runs import RunStatus, transition_allowed
from services.demo_control_api.models.scenarios import RunCreate, ScenarioType
from services.demo_control_api.services.dashboard_catalog import dashboard_catalog
from services.demo_control_api.services.prometheus_client import QUERIES
from services.demo_control_api.services.scenario_catalog import SCENARIOS


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
