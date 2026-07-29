"""Overview selection and run-scoped fraud read model."""

from collections.abc import Sequence

from services.demo_control_api.models.overview import (
    OverviewFraudSummary,
    OverviewScope,
)
from services.demo_control_api.models.runs import DemoRun, RunStatus

ACTIVE_RUN_STATUSES = {
    RunStatus.PENDING,
    RunStatus.STARTING,
    RunStatus.RUNNING,
    RunStatus.STOP_REQUESTED,
}


def select_overview_run(runs: Sequence[DemoRun]) -> DemoRun | None:
    active = [run for run in runs if run.status in ACTIVE_RUN_STATUSES]
    if active:
        return max(active, key=lambda run: (run.created_at, str(run.run_id)))
    completed = [run for run in runs if run.status is RunStatus.COMPLETED]
    return (
        max(completed, key=lambda run: (run.created_at, str(run.run_id)))
        if completed
        else None
    )


def build_fraud_summary(
    run: DemoRun | None, scope: OverviewScope | None = None
) -> OverviewFraudSummary:
    if run is None:
        return OverviewFraudSummary()
    selected_scope = scope or (
        "ACTIVE_RUN" if run.status in ACTIVE_RUN_STATUSES else "LATEST_COMPLETED_RUN"
    )
    return OverviewFraudSummary(
        run_id=run.run_id,
        scenario_type=run.scenario_type,
        run_status=run.status,
        scope=selected_scope,
        approve_count=run.approve_count,
        review_count=run.review_count,
        block_count=run.block_count,
        fraud_alert_count=run.fraud_alert_count,
    )
