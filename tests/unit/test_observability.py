"""Stable metric contracts, isolated registries, endpoint, and configuration."""

import urllib.request

import pytest
from prometheus_client import CollectorRegistry, generate_latest
from pydantic import ValidationError

from shared.observability import ApplicationMetrics, MetricsConfig, MetricsServer


def config(**changes: object) -> MetricsConfig:
    return MetricsConfig.model_validate(
        {"service_name": "test-service", "port": 19199, **changes}
    )


def test_metric_names_types_and_bounded_labels_are_stable() -> None:
    metrics = ApplicationMetrics("processor", registry=CollectorRegistry())
    names = {family.name: family.type for family in metrics.registry.collect()}
    assert names["commerce_processor_events_received"] == "counter"
    assert names["commerce_processor_event_processing_duration_seconds"] == "histogram"
    assert names["commerce_service_healthy"] == "gauge"
    text = generate_latest(metrics.registry).decode()
    assert all(
        sensitive not in text
        for sensitive in ("customer_id", "event_id", "correlation_id")
    )


def test_isolated_registries_avoid_duplicate_registration() -> None:
    first = ApplicationMetrics("processor")
    second = ApplicationMetrics("processor")
    first.processor_events_received.labels("order_created", "commerce.events").inc()
    assert b'event_type="order_created"' in generate_latest(first.registry)
    assert b'event_type="order_created"' not in generate_latest(second.registry)


@pytest.mark.parametrize(
    "changes",
    [
        {"port": 0},
        {"namespace": "not-valid!"},
        {"path": "metrics"},
        {"refresh_interval_seconds": 0},
    ],
)
def test_invalid_metrics_configuration_is_rejected(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        config(**changes)


def test_service_specific_port_and_disabled_metrics() -> None:
    loaded = MetricsConfig.from_environment(
        "event-processor",
        9101,
        {"METRICS_ENABLED": "false", "PROCESSOR_METRICS_PORT": "9201"},
        port_environment_name="PROCESSOR_METRICS_PORT",
    )
    assert not loaded.enabled
    assert loaded.port == 9201


def test_metrics_endpoint_returns_prometheus_text() -> None:
    metrics = ApplicationMetrics("test-service")
    server = MetricsServer(config(), metrics.registry)
    try:
        server.start()
    except PermissionError:
        pytest.skip("test sandbox does not permit binding a loopback socket")
    try:
        response = urllib.request.urlopen("http://127.0.0.1:19199/metrics", timeout=2)
        assert response.headers["Content-Type"].startswith("text/plain")
        assert b"commerce_service_up" in response.read()
    finally:
        server.stop()
