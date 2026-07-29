"""FastAPI application and stable /api/v1 routes."""

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from time import perf_counter
from typing import Any
from uuid import UUID

import psycopg
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from prometheus_client import make_asgi_app
from psycopg.rows import dict_row

from services.demo_control_api.config import DemoConfig
from services.demo_control_api.metrics import (
    EVENTS_REQUESTED,
    REQUEST_DURATION,
    REQUESTS,
    SSE_CONNECTIONS,
)
from services.demo_control_api.models.runs import (
    TERMINAL_STATUSES,
    DemoRun,
    Page,
    RunStatus,
)
from services.demo_control_api.models.scenarios import (
    RunCreate,
    ScenarioDefinition,
    ScenarioType,
)
from services.demo_control_api.repositories.demo_runs import (
    DemoRunRepository,
    InvalidTransitionError,
)
from services.demo_control_api.services.dashboard_catalog import dashboard_catalog
from services.demo_control_api.services.platform_health import PlatformHealth
from services.demo_control_api.services.prometheus_client import (
    RestrictedPrometheusClient,
)
from services.demo_control_api.services.scenario_catalog import SCENARIOS
from services.demo_control_api.services.scenario_runner import (
    ConcurrencyLimitError,
    ScenarioRunner,
)

config = DemoConfig.from_environment()
repository = DemoRunRepository(config.postgres_dsn)
runner = ScenarioRunner(config, repository)
health_service = PlatformHealth(config)
prometheus = RestrictedPrometheusClient(
    config.prometheus_url, config.demo_prometheus_timeout_seconds
)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    await asyncio.to_thread(repository.reconcile)
    yield


app = FastAPI(
    title="Commerce Demo Control API",
    version="1.0.0",
    description="Bounded local scenario control plane.",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(config.demo_allowed_origins),
    allow_credentials=False,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)
app.mount("/metrics", make_asgi_app())


@app.middleware("http")
async def boundaries(request: Request, call_next: Any) -> Any:
    if int(request.headers.get("content-length", "0")) > 32768:
        return JSONResponse({"detail": "request body exceeds 32 KiB"}, 413)
    started = perf_counter()
    response = await call_next(request)
    route = request.scope.get("route")
    template = getattr(route, "path", "unmatched")
    REQUESTS.labels(template, request.method, f"{response.status_code // 100}xx").inc()
    REQUEST_DURATION.labels(template, request.method).observe(perf_counter() - started)
    return response


def token_guard(authorization: str | None = Header(None)) -> None:
    if config.demo_api_token_enabled:
        if not config.demo_api_token:
            raise HTTPException(503, "API token is enabled but not configured")
        if authorization != f"Bearer {config.demo_api_token}":
            raise HTTPException(401, "invalid API token")


def require_run(run_id: UUID) -> DemoRun:
    run = repository.get(run_id)
    if run is None:
        raise HTTPException(404, "run not found")
    return run


@app.get("/api/v1/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v1/ready")
def ready() -> dict[str, str]:
    try:
        with psycopg.connect(config.postgres_dsn, connect_timeout=2) as connection:
            connection.execute("SELECT 1")
        return {"status": "ready"}
    except psycopg.Error as exc:
        raise HTTPException(503, "PostgreSQL is unavailable") from exc


@app.get("/api/v1/scenarios", response_model=list[ScenarioDefinition])
def scenarios() -> list[ScenarioDefinition]:
    return list(SCENARIOS.values())


@app.get("/api/v1/scenarios/{scenario_type}", response_model=ScenarioDefinition)
def scenario(scenario_type: ScenarioType) -> ScenarioDefinition:
    return SCENARIOS[scenario_type]


@app.post(
    "/api/v1/runs",
    response_model=DemoRun,
    status_code=201,
    dependencies=[Depends(token_guard)],
)
async def create_run(request: RunCreate) -> DemoRun:
    if (
        request.event_count > config.demo_max_event_count
        or request.events_per_second > config.demo_max_events_per_second
    ):
        raise HTTPException(422, "configured server limit exceeded")
    run = repository.create(request)
    EVENTS_REQUESTED.labels(request.scenario_type.value).inc(request.event_count)
    try:
        return await runner.start(run)
    except ConcurrencyLimitError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.get("/api/v1/runs", response_model=Page)
def runs(page: int = Query(1, ge=1), page_size: int | None = Query(None, ge=1)) -> Page:
    size = page_size or config.demo_history_page_size
    if size > config.demo_history_max_page_size:
        raise HTTPException(422, "page_size exceeds configured maximum")
    return repository.list(page, size)


@app.get("/api/v1/runs/{run_id}", response_model=DemoRun)
def run_detail(run_id: UUID) -> DemoRun:
    return require_run(run_id)


@app.post(
    "/api/v1/runs/{run_id}/stop",
    response_model=DemoRun,
    dependencies=[Depends(token_guard)],
)
async def stop_run(run_id: UUID) -> DemoRun:
    return await runner.stop(require_run(run_id))


@app.post(
    "/api/v1/runs/{run_id}/retry",
    response_model=DemoRun,
    status_code=201,
    dependencies=[Depends(token_guard)],
)
async def retry_run(run_id: UUID) -> DemoRun:
    old = require_run(run_id)
    if old.status not in {RunStatus.FAILED, RunStatus.STOPPED}:
        raise HTTPException(409, "only failed or stopped runs can be retried")
    new = repository.create(old.parameters)
    return await runner.start(new)


@app.get("/api/v1/runs/{run_id}/summary")
def run_summary(run_id: UUID) -> Any:
    require_run(run_id)
    return repository.summary(run_id)


@app.get("/api/v1/runs/{run_id}/timeline")
def timeline(run_id: UUID) -> dict[str, Any]:
    run = require_run(run_id)
    events = [
        {"type": "created", "at": run.created_at},
        {"type": "status", "status": run.status, "at": run.updated_at},
    ]
    return {"items": events[-100:]}


@app.get("/api/v1/runs/{run_id}/stream")
async def stream(run_id: UUID, request: Request) -> StreamingResponse:
    require_run(run_id)

    async def events() -> AsyncIterator[str]:
        SSE_CONNECTIONS.inc()
        try:
            last = ""
            while True:
                if await request.is_disconnected():
                    return
                run = await asyncio.to_thread(repository.refresh, run_id)
                payload = run.model_dump(mode="json")
                encoded = json.dumps(payload, separators=(",", ":"))
                if encoded != last:
                    yield f"event: progress\ndata: {encoded}\n\n"
                    last = encoded
                if run.status in TERMINAL_STATUSES:
                    yield f"event: completed\ndata: {json.dumps({'run_id': str(run_id), 'status': run.status})}\n\n"
                    return
                yield "event: heartbeat\ndata: {}\n\n"
                await asyncio.sleep(config.demo_progress_refresh_interval_ms / 1000)
        finally:
            SSE_CONNECTIONS.dec()

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/v1/platform/health")
async def platform_health() -> dict[str, Any]:
    return await health_service.get()


@app.get("/api/v1/platform/metrics/summary")
async def metrics_summary() -> dict[str, Any]:
    return await prometheus.summary()


@app.get("/api/v1/platform/topics")
def topics() -> dict[str, Any]:
    return {
        "items": [
            {"name": "commerce.events", "partitions": 3},
            {"name": "commerce.events.dlq", "partitions": 1},
            {"name": "commerce.fraud-alerts", "partitions": 3},
        ]
    }


@app.get("/api/v1/platform/services")
async def services() -> dict[str, Any]:
    return await health_service.get()


def db_list(query: str, parameters: tuple[object, ...]) -> list[dict[str, Any]]:
    with psycopg.connect(config.postgres_dsn, row_factory=dict_row) as connection:
        return [dict(row) for row in connection.execute(query, parameters).fetchall()]


@app.get("/api/v1/fraud/alerts")
def fraud_alerts(page_size: int = Query(20, ge=1, le=100)) -> dict[str, Any]:
    return {
        "items": db_list(
            """SELECT alert_id, source_event_id, score, decision, severity, status, reason_codes, created_at,
      (SELECT run_id FROM demo_run_event_manifest WHERE event_id=fraud_alerts.source_event_id ORDER BY created_at DESC LIMIT 1) run_id,
      (SELECT status FROM fraud_outbox WHERE aggregate_id=fraud_alerts.alert_id LIMIT 1) outbox_status
      FROM fraud_alerts ORDER BY created_at DESC LIMIT %s""",
            (page_size,),
        )
    }


@app.get("/api/v1/fraud/evaluations")
def fraud_evaluations(page_size: int = Query(20, ge=1, le=100)) -> dict[str, Any]:
    return {
        "items": db_list(
            "SELECT evaluation_id, source_event_id, total_score, decision, severity, matched_rule_count, evaluated_at FROM fraud_evaluations ORDER BY evaluated_at DESC LIMIT %s",
            (page_size,),
        )
    }


@app.get("/api/v1/fraud/alerts/{alert_id}")
def fraud_alert(alert_id: UUID) -> dict[str, Any]:
    rows = db_list(
        "SELECT alert_id, source_event_id, score, decision, severity, status, reason_codes, created_at FROM fraud_alerts WHERE alert_id=%s",
        (alert_id,),
    )
    if not rows:
        raise HTTPException(404, "alert not found")
    return rows[0]


@app.get("/api/v1/dlq")
def dlq(page_size: int = Query(20, ge=1, le=100)) -> dict[str, Any]:
    return {
        "items": db_list(
            "SELECT id, original_topic, original_partition, original_offset, error_type, left(error_message, 300) error_message, failed_at FROM dead_letter_events ORDER BY failed_at DESC LIMIT %s",
            (page_size,),
        )
    }


@app.get("/api/v1/dlq/{event_id}")
def dlq_detail(event_id: int) -> dict[str, Any]:
    rows = db_list(
        "SELECT id, original_topic, original_partition, original_offset, error_type, left(error_message, 300) error_message, failed_at FROM dead_letter_events WHERE id=%s",
        (event_id,),
    )
    if not rows:
        raise HTTPException(404, "DLQ record not found")
    return rows[0]


@app.get("/api/v1/dashboards")
def dashboards() -> list[dict[str, str]]:
    return dashboard_catalog(config.grafana_url)


@app.delete("/api/v1/runs/{run_id}/test-data", dependencies=[Depends(token_guard)])
def cleanup_run(run_id: UUID) -> dict[str, int]:
    try:
        return repository.cleanup(run_id)
    except InvalidTransitionError as exc:
        raise HTTPException(409, str(exc)) from exc
