"""Consumer-group lag via ``kafka-consumer-groups.sh`` inside the running
``kafka`` container (used as a cross-check against the Prometheus
``kafka_consumergroup_lag`` metric, and as the only source for lag on
benchmark-isolated consumer groups the kafka-exporter isn't scraping)."""

import subprocess
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PartitionLag:
    topic: str
    partition: int
    current_offset: int | None
    log_end_offset: int | None
    lag: int | None


def describe_group(compose_project: str, group: str) -> list[PartitionLag]:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            compose_project,
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--describe",
            "--group",
            group,
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    rows: list[PartitionLag] = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 6 or parts[0] != group:
            continue
        try:
            partition = int(parts[2])
            current = int(parts[3]) if parts[3] != "-" else None
            log_end = int(parts[4]) if parts[4] != "-" else None
            lag = int(parts[5]) if parts[5] != "-" else None
        except ValueError:
            continue
        rows.append(PartitionLag(parts[1], partition, current, log_end, lag))
    return rows


def total_lag(compose_project: str, group: str) -> int:
    rows = describe_group(compose_project, group)
    return sum(row.lag for row in rows if row.lag is not None)
