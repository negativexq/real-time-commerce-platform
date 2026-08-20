from scripts.benchmark.cpu_scheduling_diagnostics import (
    busiest_core_summary,
    cgroup_throttle_delta,
    container_throttle_summary,
    cpu_percent_from_tick_delta,
    parse_cgroup_cpu_max,
    parse_cgroup_cpu_stat,
    parse_loadavg,
    parse_proc_pid_stat,
    parse_proc_pid_status_ctxt,
    parse_proc_stat_cpu_line,
    parse_proc_stat_global,
    parse_psi_cpu,
    probe_percentiles,
    raw_artifact_filename,
    raw_artifact_path,
    summarize_samples,
    summary_artifact_filename,
)


def test_parse_cgroup_cpu_stat() -> None:
    text = (
        "usage_usec 506108\n"
        "user_usec 309347\n"
        "system_usec 196761\n"
        "nr_periods 0\n"
        "nr_throttled 0\n"
        "throttled_usec 0\n"
    )
    result = parse_cgroup_cpu_stat(text)
    assert result == {
        "usage_usec": 506108,
        "user_usec": 309347,
        "system_usec": 196761,
        "nr_periods": 0,
        "nr_throttled": 0,
        "throttled_usec": 0,
    }


def test_parse_cgroup_cpu_max_unlimited() -> None:
    assert parse_cgroup_cpu_max("max 100000\n") == {
        "quota_usec": None,
        "period_usec": 100000,
    }


def test_parse_cgroup_cpu_max_limited() -> None:
    assert parse_cgroup_cpu_max("50000 100000\n") == {
        "quota_usec": 50000,
        "period_usec": 100000,
    }


def test_cgroup_throttle_delta_no_throttling() -> None:
    before = {"nr_periods": 100, "nr_throttled": 0, "throttled_usec": 0}
    after = {"nr_periods": 220, "nr_throttled": 0, "throttled_usec": 0}
    delta = cgroup_throttle_delta(before, after)
    assert delta["periods_delta"] == 120
    assert delta["throttled_periods_delta"] == 0
    assert delta["throttled_period_ratio"] == 0.0


def test_cgroup_throttle_delta_with_throttling() -> None:
    before = {"nr_periods": 100, "nr_throttled": 5, "throttled_usec": 20000}
    after = {"nr_periods": 200, "nr_throttled": 55, "throttled_usec": 320000}
    delta = cgroup_throttle_delta(before, after)
    assert delta["periods_delta"] == 100
    assert delta["throttled_periods_delta"] == 50
    assert delta["throttled_usec_delta"] == 300000
    assert delta["throttled_period_ratio"] == 0.5


def test_cgroup_throttle_delta_zero_periods_is_none_ratio() -> None:
    delta = cgroup_throttle_delta(
        {"nr_periods": 10, "nr_throttled": 0}, {"nr_periods": 10, "nr_throttled": 0}
    )
    assert delta["periods_delta"] == 0
    assert delta["throttled_period_ratio"] is None


def test_parse_proc_stat_cpu_line() -> None:
    line = "cpu0 704 0 176 9825 101 0 70 0 0 0"
    result = parse_proc_stat_cpu_line(line)
    assert result["user"] == 704
    assert result["system"] == 176
    assert result["idle"] == 9825
    assert result["steal"] == 0


def test_cpu_percent_from_tick_delta_idle_core() -> None:
    before = parse_proc_stat_cpu_line("cpu0 100 0 10 9800 5 0 1 0 0 0")
    after = parse_proc_stat_cpu_line("cpu0 100 0 10 9900 5 0 1 0 0 0")
    percent = cpu_percent_from_tick_delta(before, after)
    assert percent is not None
    assert percent < 5.0


def test_cpu_percent_from_tick_delta_busy_core() -> None:
    before = parse_proc_stat_cpu_line("cpu0 100 0 10 9800 5 0 1 0 0 0")
    after = parse_proc_stat_cpu_line("cpu0 200 0 100 9800 5 0 1 0 0 0")
    percent = cpu_percent_from_tick_delta(before, after)
    assert percent is not None
    assert percent > 90.0


def test_cpu_percent_from_tick_delta_no_elapsed_time() -> None:
    line = parse_proc_stat_cpu_line("cpu0 100 0 10 9800 5 0 1 0 0 0")
    assert cpu_percent_from_tick_delta(line, line) is None


def test_parse_proc_stat_global_extracts_ctxt_and_procs() -> None:
    text = (
        "cpu  5092 0 1138 80116 631 0 113 0 0 0\n"
        "cpu0 704 0 176 9825 101 0 70 0 0 0\n"
        "cpu1 749 0 151 9883 102 0 18 0 0 0\n"
        "ctxt 1578224\n"
        "btime 1787227887\n"
        "processes 10877\n"
        "procs_running 5\n"
        "procs_blocked 0\n"
    )
    result = parse_proc_stat_global(text)
    assert result["ctxt"] == 1578224
    assert result["procs_running"] == 5
    assert result["procs_blocked"] == 0
    assert set(result["cores"].keys()) == {"cpu0", "cpu1"}
    assert result["cpu"]["user"] == 5092


def test_parse_loadavg() -> None:
    assert parse_loadavg("0.53 0.19 0.07 3/693 129\n") == {
        "load1": 0.53,
        "load5": 0.19,
        "load15": 0.07,
    }


def test_parse_loadavg_malformed_returns_zeros() -> None:
    assert parse_loadavg("") == {"load1": 0.0, "load5": 0.0, "load15": 0.0}


def test_parse_psi_cpu_available() -> None:
    text = (
        "some avg10=0.08 avg60=0.21 avg300=0.09 total=706069\n"
        "full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
    )
    result = parse_psi_cpu(text)
    assert result["some_avg10"] == 0.08
    assert result["full_avg300"] == 0.00


def test_parse_psi_cpu_unavailable_keeps_none() -> None:
    result = parse_psi_cpu("")
    assert result["some_avg10"] is None
    assert result["full_avg10"] is None


def test_parse_proc_pid_stat_handles_parens_in_comm() -> None:
    # comm field can contain spaces/parens (e.g. "(some proc)"); utime/stime
    # must be located relative to the last ')', not by naive whitespace split.
    text = "1 (postgres: main) S 0 1 1 0 -1 4194560 " + " ".join(
        str(n) for n in range(3, 30)
    )
    result = parse_proc_pid_stat(text)
    assert result["utime_ticks"] >= 0
    assert result["stime_ticks"] >= 0


def test_parse_proc_pid_stat_too_short_returns_zeros() -> None:
    assert parse_proc_pid_stat("1 (sh) S 0") == {"utime_ticks": 0, "stime_ticks": 0}


def test_parse_proc_pid_status_ctxt() -> None:
    text = (
        "Name:\tpostgres\n"
        "voluntary_ctxt_switches:\t488\n"
        "nonvoluntary_ctxt_switches:\t12\n"
    )
    assert parse_proc_pid_status_ctxt(text) == {
        "voluntary_ctxt_switches": 488,
        "nonvoluntary_ctxt_switches": 12,
    }


def test_raw_and_summary_filenames_differ_and_are_label_scoped() -> None:
    assert raw_artifact_filename("1050") != summary_artifact_filename("1050")
    assert raw_artifact_filename("1050") == "cpu-scheduling-raw-1050.json"
    paths = {raw_artifact_path("phase", label) for label in ("1050", "1075", "1100")}
    assert len(paths) == 3


def test_probe_percentiles_empty() -> None:
    result = probe_percentiles([])
    assert result["count"] == 0
    assert result["p50"] is None


def test_probe_percentiles_basic() -> None:
    result = probe_percentiles([1.0, 2.0, 3.0, 4.0, 100.0])
    assert result["count"] == 5
    assert result["max"] == 100.0
    assert result["p50"] == 3.0


def test_container_throttle_summary_from_samples() -> None:
    samples = [
        {"deep": {"postgres": {"cpu_stat": {"nr_periods": 10, "nr_throttled": 0}}}},
        {"deep": {"postgres": {"cpu_stat": {"nr_periods": 50, "nr_throttled": 20}}}},
    ]
    result = container_throttle_summary(samples, "postgres")
    assert result["periods_delta"] == 40
    assert result["throttled_periods_delta"] == 20
    assert result["throttled_period_ratio"] == 0.5


def test_container_throttle_summary_insufficient_samples() -> None:
    result = container_throttle_summary([], "postgres")
    assert result["periods_delta"] is None


def test_busiest_core_summary_finds_hottest_core() -> None:
    # cpu0 stays mostly idle between ticks; cpu1's idle counter barely moves
    # while its user time rises sharply - a single hot core hidden inside a
    # multi-core aggregate, which is exactly the pattern this helper exists
    # to surface.
    samples = [
        {
            "vm": {
                "proc_stat": {
                    "cores": {
                        "cpu0": {"user": 100, "idle": 9800},
                        "cpu1": {"user": 100, "idle": 9800},
                    }
                }
            }
        },
        {
            "vm": {
                "proc_stat": {
                    "cores": {
                        "cpu0": {"user": 150, "idle": 9850},
                        "cpu1": {"user": 9900, "idle": 9801},
                    }
                }
            }
        },
    ]
    result = busiest_core_summary(samples)
    assert result["core"] == "cpu1"
    assert result["max_percent"] is not None
    assert result["max_percent"] > 90.0


def test_summarize_samples_reports_missing_psi_honestly() -> None:
    samples = [
        {
            "t": 0.0,
            "probe_latency_ms": 0.5,
            "deep": {},
            "vm": {"loadavg": {"load1": 0.1}, "proc_stat": {"ctxt": 100}, "psi": None},
        },
        {
            "t": 1.0,
            "probe_latency_ms": 0.6,
            "deep": {},
            "vm": {"loadavg": {"load1": 0.2}, "proc_stat": {"ctxt": 150}, "psi": None},
        },
    ]
    summary = summarize_samples(samples)
    assert summary["psi_available"] is False
    assert summary["psi_cpu_some_avg10_mean"] is None
    assert summary["ctxt_per_second"] == 50.0
    assert summary["probe_latency_ms"]["count"] == 2
