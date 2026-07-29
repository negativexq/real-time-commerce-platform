"""Bounded-label API metrics."""

from prometheus_client import Counter, Gauge, Histogram

REQUESTS = Counter(
    "commerce_demo_api_requests_total",
    "API requests",
    ("route_template", "method", "status_class"),
)
REQUEST_DURATION = Histogram(
    "commerce_demo_api_request_duration_seconds",
    "API request duration",
    ("route_template", "method"),
)
RUNS = Counter(
    "commerce_demo_runs_total", "Terminal runs", ("scenario", "terminal_status")
)
ACTIVE_RUNS = Gauge("commerce_demo_active_runs", "Active runs")
RUN_DURATION = Histogram(
    "commerce_demo_run_duration_seconds",
    "Run duration",
    ("scenario", "terminal_status"),
)
SSE_CONNECTIONS = Gauge("commerce_demo_sse_connections", "SSE connections")
EVENTS_REQUESTED = Counter(
    "commerce_demo_scenario_events_requested_total",
    "Requested scenario events",
    ("scenario",),
)
