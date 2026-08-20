"""External CPU-scheduling diagnostic sampler.

Distinguishes "PostgreSQL is genuinely spending CPU on aggregate query
work" from "PostgreSQL/processor workers are being delayed by container
CPU throttling, host CPU saturation, or scheduler pressure". Reads only
already-available Linux/container counters (cgroup v2 `cpu.stat`,
`/proc/stat`, `/proc/loadavg`, `/proc/pressure/cpu`, `/proc/<pid>/stat`,
`/proc/<pid>/status`) via `docker exec`, plus `docker stats` for
container-level CPU%, plus a trivial fixed-cost `SELECT 1` probe over its
own diagnostic connection. It never touches the processor hot path, adds
no processor/database logic, and changes no configuration.

Environment note: this repository runs on Docker Desktop for macOS, whose
containers share one Linux VM kernel - so `/proc/stat`, `/proc/loadavg`,
and `/proc/pressure/cpu` read from *any* container reflect the whole VM,
not that container alone, and are read once per tick (from the postgres
container) rather than once per container. `docker exec` itself measured
~150-350ms per invocation in this environment (confirmed by timing before
choosing the sampling interval below) - far from negligible - so per-tick
container reads are dispatched concurrently, and `docker stats` (which has
an intrinsic ~2s sampling window of its own) runs on its own, coarser
cadence layered into the same loop rather than every tick.
"""

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any

import psycopg

from scripts.benchmark.config import load_config

# cpu.max: "<quota> <period>" in microseconds, or "max <period>" if unlimited.
CGROUP_TARGETS = ("postgres",)  # extended with event-processor-N at runtime


def parse_cgroup_cpu_stat(text: str) -> dict[str, int]:
    """Parse cgroup v2 cpu.stat key/value lines into an int dict."""
    result: dict[str, int] = {}
    for line in text.strip().splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                result[parts[0]] = int(parts[1])
            except ValueError:
                continue
    return result


def parse_cgroup_cpu_max(text: str) -> dict[str, int | None]:
    """Parse cgroup v2 cpu.max ("<quota|max> <period>")."""
    parts = text.strip().split()
    if len(parts) != 2:
        return {"quota_usec": None, "period_usec": None}
    quota, period = parts
    return {
        "quota_usec": None if quota == "max" else int(quota),
        "period_usec": int(period),
    }


def cgroup_throttle_delta(
    before: dict[str, int], after: dict[str, int]
) -> dict[str, float | int | None]:
    """Derive throttling evidence between two cpu.stat snapshots."""
    periods_delta = after.get("nr_periods", 0) - before.get("nr_periods", 0)
    throttled_delta = after.get("nr_throttled", 0) - before.get("nr_throttled", 0)
    throttled_usec_delta = after.get("throttled_usec", 0) - before.get(
        "throttled_usec", 0
    )
    ratio = (throttled_delta / periods_delta) if periods_delta > 0 else None
    return {
        "periods_delta": periods_delta,
        "throttled_periods_delta": throttled_delta,
        "throttled_usec_delta": throttled_usec_delta,
        "throttled_period_ratio": ratio,
    }


def parse_proc_stat_cpu_line(line: str) -> dict[str, int]:
    """Parse one '/proc/stat' cpu/cpuN line: user nice system idle iowait
    irq softirq steal guest guest_nice (trailing fields may be absent)."""
    fields = (
        "user",
        "nice",
        "system",
        "idle",
        "iowait",
        "irq",
        "softirq",
        "steal",
        "guest",
        "guest_nice",
    )
    parts = line.split()
    values = parts[1:]
    return {
        name: int(value)
        for name, value in zip(fields, values, strict=False)
        if value.isdigit()
    }


def cpu_percent_from_tick_delta(
    before: dict[str, int], after: dict[str, int]
) -> float | None:
    """Utilization % between two /proc/stat cpu-line snapshots (any core or
    the aggregate 'cpu' line - same field shape either way)."""
    before_total = sum(before.values())
    after_total = sum(after.values())
    total_delta = after_total - before_total
    if total_delta <= 0:
        return None
    idle_delta = after.get("idle", 0) - before.get("idle", 0)
    return max(0.0, min(100.0, (1 - idle_delta / total_delta) * 100))


def parse_proc_stat_global(text: str) -> dict[str, Any]:
    """Parse the whole '/proc/stat' file: aggregate + per-core ticks plus
    ctxt/procs_running/procs_blocked."""
    result: dict[str, Any] = {"cpu": {}, "cores": {}}
    for line in text.strip().splitlines():
        if line.startswith("cpu "):
            result["cpu"] = parse_proc_stat_cpu_line(line)
        elif line.startswith("cpu") and line[3].isdigit():
            name = line.split()[0]
            result["cores"][name] = parse_proc_stat_cpu_line(line)
        elif line.startswith("ctxt "):
            result["ctxt"] = int(line.split()[1])
        elif line.startswith("procs_running "):
            result["procs_running"] = int(line.split()[1])
        elif line.startswith("procs_blocked "):
            result["procs_blocked"] = int(line.split()[1])
    return result


def parse_loadavg(text: str) -> dict[str, float]:
    parts = text.strip().split()
    if len(parts) < 3:
        return {"load1": 0.0, "load5": 0.0, "load15": 0.0}
    return {
        "load1": float(parts[0]),
        "load5": float(parts[1]),
        "load15": float(parts[2]),
    }


def parse_psi_cpu(text: str) -> dict[str, float | None]:
    """Parse '/proc/pressure/cpu' 'some'/'full' avg10/avg60/avg300 lines.
    Returns None fields if PSI is unavailable (caller passes "")."""
    result: dict[str, float | None] = {
        "some_avg10": None,
        "some_avg60": None,
        "some_avg300": None,
        "full_avg10": None,
        "full_avg60": None,
        "full_avg300": None,
    }
    for line in text.strip().splitlines():
        parts = line.split()
        if not parts:
            continue
        kind = parts[0]
        if kind not in ("some", "full"):
            continue
        for field in parts[1:]:
            if "=" not in field:
                continue
            key, value = field.split("=", 1)
            if key in ("avg10", "avg60", "avg300"):
                result[f"{kind}_{key}"] = float(value)
    return result


def parse_proc_pid_stat(text: str) -> dict[str, int]:
    """utime/stime (fields 14/15, clock ticks) from '/proc/<pid>/stat'.
    The comm field can contain spaces/parens, so split after the closing
    ')' rather than trusting whitespace-splitting from the start."""
    closing = text.rfind(")")
    if closing == -1:
        return {"utime_ticks": 0, "stime_ticks": 0}
    fields = text[closing + 1 :].split()
    # fields[0] is state (field 3); utime is field 14 -> fields[11].
    if len(fields) < 13:
        return {"utime_ticks": 0, "stime_ticks": 0}
    return {"utime_ticks": int(fields[11]), "stime_ticks": int(fields[12])}


def parse_proc_pid_status_ctxt(text: str) -> dict[str, int]:
    result = {"voluntary_ctxt_switches": 0, "nonvoluntary_ctxt_switches": 0}
    for line in text.splitlines():
        if line.startswith("voluntary_ctxt_switches:"):
            result["voluntary_ctxt_switches"] = int(line.split()[1])
        elif line.startswith("nonvoluntary_ctxt_switches:"):
            result["nonvoluntary_ctxt_switches"] = int(line.split()[1])
    return result


def raw_artifact_filename(label: str) -> str:
    return f"cpu-scheduling-raw-{label}.json"


def summary_artifact_filename(label: str) -> str:
    return f"cpu-scheduling-summary-{label}.json"


def raw_artifact_path(phase_dir: str, label: str) -> Path:
    return Path(phase_dir) / raw_artifact_filename(label)


def summary_artifact_path(phase_dir: str, label: str) -> Path:
    return Path(phase_dir) / summary_artifact_filename(label)


DEEP_SCRIPT = (
    "cat /sys/fs/cgroup/cpu.stat; echo ---MAX---; cat /sys/fs/cgroup/cpu.max; "
    "echo ---PROCS---; "
    "for p in /proc/[0-9]*; do "
    "  c=$(cat $p/comm 2>/dev/null); "
    '  if [ "$c" = "postgres" ] || [ "$p" = "/proc/1" ]; then '
    "    echo PID $(basename $p); cat $p/stat 2>/dev/null; "
    "    echo ---STATUS---; cat $p/status 2>/dev/null | grep ctxt; "
    "    echo ---END---; "
    "  fi; "
    "done"
)

VM_SCRIPT = (
    "cat /proc/loadavg; echo ---STAT---; cat /proc/stat; "
    "echo ---PSI---; cat /proc/pressure/cpu 2>/dev/null"
)


def _docker_exec(container: str, script: str) -> str:
    result = subprocess.run(
        ["docker", "exec", container, "sh", "-c", script],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    return result.stdout


def _parse_deep_output(text: str) -> dict[str, Any]:
    sections = text.split("---MAX---")
    cpu_stat = parse_cgroup_cpu_stat(sections[0]) if sections else {}
    rest = sections[1] if len(sections) > 1 else ""
    max_part, _, procs_part = rest.partition("---PROCS---")
    cpu_max = parse_cgroup_cpu_max(max_part)
    utime_total = stime_total = voluntary_total = nonvoluntary_total = 0
    process_count = 0
    for block in procs_part.split("---END---"):
        if "PID" not in block:
            continue
        stat_part, _, status_part = block.partition("---STATUS---")
        stat_text = stat_part.split("\n", 1)[-1] if "\n" in stat_part else ""
        stat = parse_proc_pid_stat(stat_text)
        status = parse_proc_pid_status_ctxt(status_part)
        utime_total += stat["utime_ticks"]
        stime_total += stat["stime_ticks"]
        voluntary_total += status["voluntary_ctxt_switches"]
        nonvoluntary_total += status["nonvoluntary_ctxt_switches"]
        process_count += 1
    return {
        "cpu_stat": cpu_stat,
        "cpu_max": cpu_max,
        "process_count": process_count,
        "utime_ticks": utime_total,
        "stime_ticks": stime_total,
        "voluntary_ctxt_switches": voluntary_total,
        "nonvoluntary_ctxt_switches": nonvoluntary_total,
    }


def _parse_vm_output(text: str) -> dict[str, Any]:
    loadavg_part, _, rest = text.partition("---STAT---")
    stat_part, _, psi_part = rest.partition("---PSI---")
    return {
        "loadavg": parse_loadavg(loadavg_part),
        "proc_stat": parse_proc_stat_global(stat_part),
        "psi": parse_psi_cpu(psi_part) if psi_part.strip() else None,
    }


def _container_cpu_percent(
    project: str, wanted_suffixes: tuple[str, ...]
) -> dict[str, dict[str, float | None]]:
    result = subprocess.run(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.Name}}|{{.CPUPerc}}|{{.MemPerc}}",
        ],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    wanted = {f"{project}-{suffix}" for suffix in wanted_suffixes}
    snapshots: dict[str, dict[str, float | None]] = {}
    for line in result.stdout.splitlines():
        parts = line.split("|")
        if len(parts) != 3:
            continue
        name = parts[0]
        if name not in wanted and not name.startswith(f"{project}-event-processor-"):
            continue
        cpu = parts[1].rstrip("%").strip()
        memory = parts[2].rstrip("%").strip()
        snapshots[name] = {
            "cpu_percent": float(cpu) if cpu else None,
            "memory_percent": float(memory) if memory else None,
        }
    return snapshots


def probe_once(connection: psycopg.Connection[tuple[object, ...]]) -> float | None:
    """Fixed-cost 'SELECT 1' round trip in milliseconds, or None on error.

    Limitation: this measures connection dispatch + trivial round-trip
    latency (network + backend wakeup + reply), not PostgreSQL CPU cost in
    isolation - a rising value is consistent with scheduler/dispatch delay
    but does not, by itself, prove it.
    """
    try:
        started = time.monotonic()
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        return (time.monotonic() - started) * 1000
    except Exception:
        return None


def sample_tick(
    project: str,
    processor_containers: list[str],
    probe_connection: psycopg.Connection[tuple[object, ...]],
    phase: str | None,
) -> dict[str, Any]:
    containers = ["postgres-1", *processor_containers]
    procs = {
        name: subprocess.Popen(
            ["docker", "exec", f"{project}-{name}", "sh", "-c", DEEP_SCRIPT],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        for name in containers
    }
    vm_proc = subprocess.Popen(
        ["docker", "exec", f"{project}-postgres-1", "sh", "-c", VM_SCRIPT],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    probe_ms = probe_once(probe_connection)
    deep: dict[str, Any] = {}
    for name, proc in procs.items():
        stdout, _ = proc.communicate(timeout=10)
        deep[name] = _parse_deep_output(stdout)
    vm_stdout, _ = vm_proc.communicate(timeout=10)
    return {
        "t": time.time(),
        "phase": phase,
        "probe_latency_ms": probe_ms,
        "deep": deep,
        "vm": _parse_vm_output(vm_stdout),
    }


def probe_percentiles(latencies_ms: list[float]) -> dict[str, float | int | None]:
    """p50/p95/p99/max over successful probe samples; error_count is the
    caller's responsibility to pass separately since this only sees the
    successful (non-None) latencies."""
    if not latencies_ms:
        return {"p50": None, "p95": None, "p99": None, "max": None, "count": 0}
    ordered = sorted(latencies_ms)
    n = len(ordered)

    def _pct(p: float) -> float:
        index = min(n - 1, int(round(p * (n - 1))))
        return ordered[index]

    return {
        "p50": _pct(0.50),
        "p95": _pct(0.95),
        "p99": _pct(0.99),
        "max": ordered[-1],
        "count": n,
    }


def _cgroup_series(
    samples: list[dict[str, Any]], container: str
) -> list[dict[str, int]]:
    return [
        sample["deep"][container]["cpu_stat"]
        for sample in samples
        if container in sample.get("deep", {}) and sample["deep"][container]["cpu_stat"]
    ]


def container_throttle_summary(
    samples: list[dict[str, Any]], container: str
) -> dict[str, float | int | None]:
    """Cumulative throttling evidence across a whole run: first vs last
    cpu.stat snapshot for one container (cpu.stat counters are monotonic
    cumulative, so first-vs-last already gives the full-run delta)."""
    series = _cgroup_series(samples, container)
    if len(series) < 2:
        return {
            "periods_delta": None,
            "throttled_periods_delta": None,
            "throttled_usec_delta": None,
            "throttled_period_ratio": None,
        }
    return cgroup_throttle_delta(series[0], series[-1])


def _vm_cpu_series(
    samples: list[dict[str, Any]], core: str | None
) -> list[dict[str, int]]:
    result = []
    for sample in samples:
        vm = sample.get("vm") or {}
        proc_stat = vm.get("proc_stat") or {}
        ticks = proc_stat.get("cores", {}).get(core) if core else proc_stat.get("cpu")
        if ticks:
            result.append(ticks)
    return result


def busiest_core_summary(samples: list[dict[str, Any]]) -> dict[str, Any]:
    """Per-core utilization % between consecutive ticks; returns the
    single busiest core's mean/max across the whole run, to surface
    one-core saturation hidden inside a moderate aggregate figure."""
    core_names: set[str] = set()
    for sample in samples:
        proc_stat = (sample.get("vm") or {}).get("proc_stat") or {}
        core_names.update(proc_stat.get("cores", {}).keys())
    best: tuple[str, float, float] | None = None
    for core in sorted(core_names):
        series = _vm_cpu_series(samples, core)
        percents = [
            value
            for value in (
                cpu_percent_from_tick_delta(series[i], series[i + 1])
                for i in range(len(series) - 1)
            )
            if value is not None
        ]
        if not percents:
            continue
        mean_percent = sum(percents) / len(percents)
        max_percent = max(percents)
        if best is None or max_percent > best[2]:
            best = (core, mean_percent, max_percent)
    if best is None:
        return {"core": None, "mean_percent": None, "max_percent": None}
    return {"core": best[0], "mean_percent": best[1], "max_percent": best[2]}


def summarize_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    probe_latencies = [
        s["probe_latency_ms"] for s in samples if s.get("probe_latency_ms") is not None
    ]
    probe_errors = sum(1 for s in samples if s.get("probe_latency_ms") is None)

    container_names: set[str] = set()
    for sample in samples:
        container_names.update((sample.get("deep") or {}).keys())

    throttling = {
        name: container_throttle_summary(samples, name)
        for name in sorted(container_names)
    }

    def _vm(sample: dict[str, Any]) -> dict[str, Any]:
        return dict(sample.get("vm") or {})

    loadavgs: list[dict[str, Any]] = [
        _vm(sample)["loadavg"] for sample in samples if _vm(sample).get("loadavg")
    ]
    load1_values = [load["load1"] for load in loadavgs]

    psi_values: list[dict[str, Any]] = [
        _vm(sample)["psi"] for sample in samples if _vm(sample).get("psi") is not None
    ]
    psi_some_avg10 = [
        p["some_avg10"] for p in psi_values if p.get("some_avg10") is not None
    ]

    ctxt_series = [
        (sample.get("vm") or {}).get("proc_stat", {}).get("ctxt")
        for sample in samples
        if (sample.get("vm") or {}).get("proc_stat", {}).get("ctxt") is not None
    ]
    ctxt_delta = (ctxt_series[-1] - ctxt_series[0]) if len(ctxt_series) >= 2 else None
    duration = (samples[-1]["t"] - samples[0]["t"]) if len(samples) >= 2 else None
    ctxt_per_second = (
        ctxt_delta / duration if ctxt_delta is not None and duration else None
    )

    container_cpu_values: dict[str, list[float]] = {}
    for sample in samples:
        for name, stats in (sample.get("containers") or {}).items():
            cpu_percent = stats.get("cpu_percent")
            if cpu_percent is not None:
                container_cpu_values.setdefault(name, []).append(cpu_percent)
    container_cpu_summary = {
        name: {
            "avg": sum(values) / len(values) if values else None,
            "max": max(values) if values else None,
        }
        for name, values in container_cpu_values.items()
    }

    return {
        "sample_count": len(samples),
        "probe_latency_ms": {
            **probe_percentiles(probe_latencies),
            "error_count": probe_errors,
        },
        "container_cpu_percent": container_cpu_summary,
        "cgroup_throttling": throttling,
        "psi_cpu_some_avg10_mean": (
            sum(psi_some_avg10) / len(psi_some_avg10) if psi_some_avg10 else None
        ),
        "psi_available": bool(psi_some_avg10),
        "load1_avg": sum(load1_values) / len(load1_values) if load1_values else None,
        "load1_max": max(load1_values) if load1_values else None,
        "ctxt_per_second": ctxt_per_second,
        "busiest_core": busiest_core_summary(samples),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--duration-seconds", type=float, required=True)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--stats-every", type=int, default=3)
    parser.add_argument("--processor-containers", default="event-processor-1")
    args = parser.parse_args()

    config = load_config(args.run_tag)
    project = config.compose_project
    processor_containers = [
        name.strip() for name in args.processor_containers.split(",") if name.strip()
    ]

    samples: list[dict[str, Any]] = []
    deadline = time.monotonic() + args.duration_seconds
    tick_index = 0
    with psycopg.connect(
        config.postgres_dsn,
        autocommit=True,
        application_name="cpu-scheduling-diagnostics",
    ) as probe_connection:
        while time.monotonic() < deadline:
            tick_started = time.monotonic()
            sample = sample_tick(
                project, processor_containers, probe_connection, phase=None
            )
            if tick_index % args.stats_every == 0:
                sample["containers"] = _container_cpu_percent(
                    project,
                    tuple(f"{name}" for name in ("postgres-1", "kafka-1", "redis-1")),
                )
            samples.append(sample)
            tick_index += 1
            elapsed = time.monotonic() - tick_started
            time.sleep(max(0.0, args.interval_seconds - elapsed))

    metadata = {
        "run_tag": args.run_tag,
        "label": args.label,
        "interval_seconds": args.interval_seconds,
        "stats_every": args.stats_every,
        "processor_containers": processor_containers,
        "sample_count": len(samples),
    }
    phase_dir = config.phase_dir()
    Path(phase_dir).mkdir(parents=True, exist_ok=True)

    raw_path = raw_artifact_path(phase_dir, args.label)
    raw_path.write_text(json.dumps({**metadata, "samples": samples}, indent=2))
    print(f"wrote {raw_path} ({len(samples)} samples)")

    summary_path = summary_artifact_path(phase_dir, args.label)
    summary_path.write_text(
        json.dumps({**metadata, "summary": summarize_samples(samples)}, indent=2)
    )
    print(f"wrote {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
