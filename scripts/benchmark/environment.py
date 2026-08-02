"""Capture machine/Docker/container/topic/config facts for the report.

Every value here is read from the live system at run time — nothing is
hardcoded — so re-running the benchmark on a different machine or after
config changes reflects reality.
"""

import json
import platform
import subprocess
from typing import Any

import redis as redis_lib

from scripts.benchmark.config import BenchmarkConfig
from scripts.benchmark.pg import query_one


def _run(cmd: list[str], timeout: int = 20) -> str:
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout, check=False
    )
    return result.stdout.strip()


def _docker_info(config: BenchmarkConfig) -> dict[str, Any]:
    raw = _run(
        [
            "docker",
            "info",
            "--format",
            "{{.ServerVersion}}|{{.NCPU}}|{{.MemTotal}}|{{.OperatingSystem}}"
            "|{{.KernelVersion}}",
        ]
    )
    parts = raw.split("|") if raw else []
    version = _run(["docker", "version", "--format", "{{.Server.Version}}"])
    return {
        "docker_server_version": version or (parts[0] if parts else None),
        "docker_vm_cpus": int(parts[1])
        if len(parts) > 1 and parts[1].isdigit()
        else None,
        "docker_vm_mem_bytes": int(parts[2])
        if len(parts) > 2 and parts[2].isdigit()
        else None,
        "docker_vm_os": parts[3] if len(parts) > 3 else None,
        "docker_vm_kernel": parts[4] if len(parts) > 4 else None,
    }


def _container_mem_limits(config: BenchmarkConfig) -> dict[str, str | None]:
    raw = _run(
        [
            "docker",
            "compose",
            "-p",
            config.compose_project,
            "config",
            "--format",
            "json",
        ]
    )
    limits: dict[str, str | None] = {}
    try:
        parsed = json.loads(raw)
        for name, service in parsed.get("services", {}).items():
            limits[name] = service.get("mem_limit")
    except (json.JSONDecodeError, AttributeError):
        pass
    return limits


def _container_cpu_counts(config: BenchmarkConfig) -> dict[str, int]:
    raw = _run(
        [
            "docker",
            "compose",
            "-p",
            config.compose_project,
            "ps",
            "--format",
            "{{.Service}}",
        ]
    )
    counts: dict[str, int] = {}
    for name in raw.splitlines():
        name = name.strip()
        if name:
            counts[name] = counts.get(name, 0) + 1
    return counts


def _topic_partitions(config: BenchmarkConfig, topic: str) -> int | None:
    raw = _run(
        [
            "docker",
            "compose",
            "-p",
            config.compose_project,
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--describe",
            "--topic",
            topic,
        ]
    )
    count = 0
    for line in raw.splitlines():
        if line.strip().startswith("Topic:") and "Partition:" in line:
            count += 1
    return count or None


def _redis_config(config: BenchmarkConfig) -> dict[str, Any]:
    try:
        client = redis_lib.Redis.from_url(config.redis_url, socket_timeout=3)
        maxmemory = client.config_get("maxmemory")
        policy = client.config_get("maxmemory-policy")
        info = client.info("server")
        client.close()
        return {
            "maxmemory": maxmemory.get("maxmemory"),
            "maxmemory_policy": policy.get("maxmemory-policy"),
            "redis_version": info.get("redis_version"),
        }
    except Exception as exc:  # noqa: BLE001 - best-effort environment capture
        return {"error": str(exc)}


def _postgres_info(config: BenchmarkConfig) -> dict[str, Any]:
    try:
        row = query_one(config.postgres_dsn, "SELECT version() AS version")
        return {"version": row["version"] if row else None}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


def main() -> int:
    import argparse

    from scripts.benchmark.artifacts import phase_path, write_json
    from scripts.benchmark.config import load_config

    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    args = parser.parse_args()
    config = load_config(args.run_tag)
    payload = capture(config)
    out_path = phase_path(config.phase_dir(), "environment")
    write_json(out_path, payload)
    print(f"wrote {out_path}")
    return 0


def capture(config: BenchmarkConfig) -> dict[str, Any]:
    return {
        "host": {
            "machine": platform.machine(),
            "platform": platform.platform(),
            "processor": platform.processor(),
            "python_version": platform.python_version(),
        },
        "docker": _docker_info(config),
        "container_mem_limits": _container_mem_limits(config),
        "container_instance_counts": _container_cpu_counts(config),
        "kafka_topics": {
            config.events_topic: _topic_partitions(config, config.events_topic),
            config.dlq_topic: _topic_partitions(config, config.dlq_topic),
        },
        "redis": _redis_config(config),
        "postgres": _postgres_info(config),
        "compose_project": config.compose_project,
    }


if __name__ == "__main__":
    import sys

    sys.exit(main())
