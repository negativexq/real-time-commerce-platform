"""Overview-specific read models."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, computed_field

from services.demo_control_api.models.runs import RunStatus
from services.demo_control_api.models.scenarios import ScenarioType

OverviewScope = Literal["ACTIVE_RUN", "LATEST_COMPLETED_RUN"]


class OverviewFraudSummary(BaseModel):
    run_id: UUID | None = None
    scenario_type: ScenarioType | None = None
    run_status: RunStatus | None = None
    scope: OverviewScope | None = None
    approve_count: int = 0
    review_count: int = 0
    block_count: int = 0
    fraud_alert_count: int = 0

    @computed_field  # type: ignore[prop-decorator]
    @property
    def total_decisions(self) -> int:
        return self.approve_count + self.review_count + self.block_count
