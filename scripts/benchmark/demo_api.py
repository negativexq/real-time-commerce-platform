"""Thin client for the demo control API, modeled on ``scripts/demo-run.py``.

Reused by every benchmark phase that should exercise the real production
pipeline (primary consumer group ``commerce-event-processor-v1``) through the
existing, allow-listed scenario runner instead of hand-rolling Kafka
producers.
"""

import time
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(frozen=True, slots=True)
class RunResult:
    run_id: str
    status: str
    started_at: str | None
    completed_at: str | None
    detail: dict[str, Any]
    summary: dict[str, Any]


class DemoApiClient:
    def __init__(self, base_url: str, timeout: float = 15.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str) -> dict[str, Any]:
        response = httpx.get(f"{self.base_url}{path}", timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        response = httpx.post(f"{self.base_url}{path}", json=body, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def create_run(self, body: dict[str, Any], max_retries: int = 5) -> str:
        # DEMO_MAX_CONCURRENT_RUNS briefly counts a just-finished run as
        # still active for a moment after its status flips to a terminal
        # state (the asyncio task hasn't been popped from ScenarioRunner's
        # tracking dict yet), so a 409 right after the previous phase
        # finishes is a transient race, not a real capacity problem.
        last_error: httpx.HTTPStatusError | None = None
        for attempt in range(max_retries):
            try:
                run = self._post("/api/v1/runs", body)
                return str(run["run_id"])
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 409:
                    raise
                last_error = exc
                time.sleep(min(2 ** attempt, 10))
        assert last_error is not None
        raise last_error

    def wait_for_terminal(self, run_id: str, timeout: float) -> RunResult:
        deadline = time.monotonic() + timeout
        detail: dict[str, Any] = {}
        while time.monotonic() < deadline:
            detail = self._get(f"/api/v1/runs/{run_id}")
            if detail["status"] in {"COMPLETED", "STOPPED", "FAILED"}:
                summary = self._get(f"/api/v1/runs/{run_id}/summary")
                return RunResult(
                    run_id=run_id,
                    status=detail["status"],
                    started_at=detail.get("started_at"),
                    completed_at=detail.get("completed_at"),
                    detail=detail,
                    summary=summary,
                )
            time.sleep(1)
        raise TimeoutError(f"run {run_id} did not reach a terminal state in time")

    def run_scenario(
        self, body: dict[str, Any], timeout: float = 300.0
    ) -> RunResult:
        run_id = self.create_run(body)
        return self.wait_for_terminal(run_id, timeout)

    def stop_run(self, run_id: str) -> None:
        httpx.post(
            f"{self.base_url}/api/v1/runs/{run_id}/stop", timeout=self.timeout
        ).raise_for_status()

    def stop_and_wait(self, run_id: str, timeout: float = 30.0) -> None:
        """Best-effort: request a stop and wait briefly for it to land, so a
        slow/stuck run never keeps occupying a DEMO_MAX_CONCURRENT_RUNS slot
        for the rest of the benchmark."""
        try:
            self.stop_run(run_id)
            self.wait_for_terminal(run_id, timeout)
        except (httpx.HTTPStatusError, TimeoutError):
            pass

    def cleanup_test_data(self, run_id: str) -> None:
        httpx.delete(
            f"{self.base_url}/api/v1/runs/{run_id}/test-data", timeout=self.timeout
        ).raise_for_status()

    def health(self) -> dict[str, Any]:
        return self._get("/api/v1/health")

    def platform_health(self) -> dict[str, Any]:
        return self._get("/api/v1/platform/health")
