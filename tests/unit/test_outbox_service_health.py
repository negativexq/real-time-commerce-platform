"""The outbox publisher reports database health from its own claims."""

from types import SimpleNamespace
from typing import Any

import psycopg
import pytest
from prometheus_client import CollectorRegistry

from services.fraud_outbox_publisher.service import OutboxService, OutboxSummary
from shared.observability.metrics import ApplicationMetrics


class StopAfterClaim:
    """Claims nothing once, then stops the loop."""

    recovered_claims = 0

    def __init__(self, service: OutboxService, error: Exception | None = None) -> None:
        self.service = service
        self.error = error

    def claim(self, _config: Any) -> list[Any]:
        self.service.running = False
        if self.error is not None:
            raise self.error
        return []

    def status_snapshot(self) -> tuple[dict[str, int], float]:
        return {}, 0.0


def _service(
    error: Exception | None = None,
) -> tuple[OutboxService, ApplicationMetrics, CollectorRegistry]:
    registry = CollectorRegistry()
    metrics = ApplicationMetrics("fraud-outbox-publisher", registry=registry)
    service = OutboxService.__new__(OutboxService)
    service.config = SimpleNamespace(  # type: ignore[assignment]
        outbox=SimpleNamespace(fraud_outbox_poll_interval_ms=0),
        metrics=SimpleNamespace(
            refresh_interval_seconds=3600, max_outbox_staleness_seconds=120
        ),
    )
    service.repository = StopAfterClaim(service, error)  # type: ignore[assignment]
    service.summary = OutboxSummary()
    service.running = True
    service.metrics = metrics
    service._last_refresh = float("inf")
    service._healthy = True
    return service, metrics, registry


def _database_healthy(registry: CollectorRegistry) -> float | None:
    return registry.get_sample_value("commerce_database_healthy")


def test_successful_claim_marks_database_healthy() -> None:
    service, _metrics, registry = _service()
    assert _database_healthy(registry) == 0
    service.run()
    assert _database_healthy(registry) == 1


def test_database_error_marks_database_unhealthy() -> None:
    service, metrics, registry = _service(psycopg.OperationalError("gone"))
    metrics.database_healthy.set(1)
    with pytest.raises(psycopg.OperationalError):
        service.run()
    assert _database_healthy(registry) == 0
