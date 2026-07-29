"""Run state and response models."""

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field

from services.demo_control_api.models.scenarios import RunCreate, ScenarioType


class RunStatus(StrEnum):
    PENDING = "PENDING"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOP_REQUESTED = "STOP_REQUESTED"
    STOPPED = "STOPPED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


TERMINAL_STATUSES = frozenset(
    {RunStatus.STOPPED, RunStatus.COMPLETED, RunStatus.FAILED}
)
TRANSITIONS = {
    RunStatus.PENDING: frozenset({RunStatus.STARTING}),
    RunStatus.STARTING: frozenset({RunStatus.RUNNING, RunStatus.FAILED}),
    RunStatus.RUNNING: frozenset(
        {RunStatus.COMPLETED, RunStatus.STOP_REQUESTED, RunStatus.FAILED}
    ),
    RunStatus.STOP_REQUESTED: frozenset({RunStatus.STOPPED, RunStatus.FAILED}),
}


def transition_allowed(current: RunStatus, target: RunStatus) -> bool:
    return target in TRANSITIONS.get(current, frozenset())


class DemoRun(BaseModel):
    run_id: UUID
    scenario_type: ScenarioType
    status: RunStatus
    requested_event_count: int
    requested_duration_seconds: int | None
    requested_events_per_second: float
    seed: int
    parameters: RunCreate
    test_scope: str
    generated_event_count: int = 0
    processed_event_count: int = 0
    duplicate_count: int = 0
    dlq_count: int = 0
    approve_count: int = 0
    review_count: int = 0
    block_count: int = 0
    fraud_alert_count: int = 0
    outbox_published_count: int = 0
    status_message: str | None = None
    error_category: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    stopped_at: datetime | None = None
    updated_at: datetime


class Page(BaseModel):
    items: list[DemoRun]
    page: int = Field(ge=1)
    page_size: int = Field(ge=1)
    total: int = Field(ge=0)


class RunSummary(BaseModel):
    run: DemoRun
    validation_failures: int
    postgres_committed_events: int
    fraud_evaluations: int
    outbox_pending: int
    outbox_published: int
    duration_seconds: float | None
    effective_event_rate: float | None
    values_scope: str = "exact_run_specific_postgresql"
