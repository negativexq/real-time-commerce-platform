"""Git-provisioned Grafana dashboard metadata."""

DASHBOARDS = [
    (
        "Platform Overview",
        "Portfolio-level health and throughput.",
        "commerce-platform-overview",
        "overview",
    ),
    (
        "Kafka Streaming",
        "Topics, rates, partitions, and lag.",
        "commerce-kafka-streaming",
        "streaming",
    ),
    (
        "Processor",
        "Validation, idempotency, and terminal outcomes.",
        "commerce-processor",
        "processing",
    ),
    (
        "Persistence",
        "PostgreSQL transactions and pool health.",
        "commerce-persistence",
        "storage",
    ),
    ("Fraud", "Decisions, scores, rules, and alerts.", "commerce-fraud", "fraud"),
    ("Outbox", "Durable alert publication state.", "commerce-outbox", "fraud"),
    (
        "Infrastructure",
        "Exporter and dependency health.",
        "commerce-infrastructure",
        "infrastructure",
    ),
]


def dashboard_catalog(grafana_url: str) -> list[dict[str, str]]:
    return [
        {
            "title": title,
            "description": description,
            "uid": uid,
            "category": category,
            "url": f"{grafana_url.rstrip('/')}/d/{uid}",
        }
        for title, description, uid, category in DASHBOARDS
    ]
