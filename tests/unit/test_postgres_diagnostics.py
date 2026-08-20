from scripts.benchmark.postgres_diagnostics import (
    classify_query,
    raw_artifact_filename,
    raw_artifact_path,
    summarize_samples,
    summary_artifact_filename,
)


def test_raw_and_summary_filenames_differ_for_the_same_label() -> None:
    assert raw_artifact_filename("1050") != summary_artifact_filename("1050")
    assert raw_artifact_filename("1050") == "postgres-diagnostics-raw-1050.json"
    assert summary_artifact_filename("1050") == "postgres-diagnostics-summary-1050.json"


def test_different_labels_produce_different_raw_paths() -> None:
    """Same overwrite class of bug as direct_saturation.py: three sequential
    single-rate invocations under one --run-tag must not collide."""
    phase_dir = "artifacts/benchmark/bench-example"
    paths = {raw_artifact_path(phase_dir, label) for label in ("1050", "1075", "1100")}
    assert len(paths) == 3


def test_raw_filename_never_matches_summary_filename_pattern() -> None:
    for label in ("1050", "1075", "1100", "smoke"):
        assert "summary" not in raw_artifact_filename(label)
        assert "raw" not in summary_artifact_filename(label)


def test_classify_query_buckets_by_table_and_kind() -> None:
    assert classify_query("SELECT home_country FROM customers WHERE ...") == (
        "customers_select"
    )
    assert (
        classify_query("INSERT INTO orders (order_id, ...) VALUES (...)")
        == "orders_insert"
    )
    assert classify_query(None) == "none"
    assert classify_query("") == "none"


def test_classify_query_never_returns_raw_text() -> None:
    query = "SELECT email_hash FROM customers WHERE customer_id = '<secret>'"
    result = classify_query(query)
    assert "<secret>" not in result
    assert "email_hash" not in result


def test_classify_query_falls_back_to_other() -> None:
    assert classify_query("VACUUM ANALYZE") == "other_other"
    assert classify_query("BEGIN") == "other_begin"


def _sample(
    *,
    t: float,
    active: int = 1,
    waiting: int = 0,
    idle_in_txn: int = 0,
    active_txns: int = 1,
    blocked: int = 0,
    wait_event_type_counts: dict[str, int] | None = None,
    wait_event_counts: dict[str, int] | None = None,
    query_class_active_counts: dict[str, int] | None = None,
    locks_waiting_by_mode: dict[str, int] | None = None,
    xact_commit: int = 0,
    xact_rollback: int = 0,
) -> dict[str, object]:
    return {
        "t": t,
        "active": active,
        "idle": 0,
        "idle_in_txn": idle_in_txn,
        "waiting": waiting,
        "blocked": blocked,
        "active_txns": active_txns,
        "longest_active_query_age_s": 0.1,
        "longest_txn_age_s": 0.2,
        "wait_event_type_counts": wait_event_type_counts or {},
        "wait_event_counts": wait_event_counts or {},
        "query_class_active_counts": query_class_active_counts or {},
        "backend_type_counts": {},
        "locks_granted_by_mode": {},
        "locks_waiting_by_mode": locks_waiting_by_mode or {},
        "xact_commit": xact_commit,
        "xact_rollback": xact_rollback,
    }


def test_summarize_samples_empty_series() -> None:
    summary = summarize_samples([])
    assert summary["sample_count"] == 0
    assert summary["active_avg"] is None
    assert summary["wait_event_type_totals"] == {}


def test_summarize_samples_excludes_idle_periods_by_default() -> None:
    samples = [
        _sample(t=0, active=0),
        _sample(t=1, active=5),
        _sample(t=2, active=3),
    ]
    summary = summarize_samples(samples)
    assert summary["sample_count"] == 3
    assert summary["considered_count"] == 2
    assert summary["active_avg"] == 4
    assert summary["active_max"] == 5


def test_summarize_samples_can_include_idle_periods() -> None:
    samples = [_sample(t=0, active=0), _sample(t=1, active=4)]
    summary = summarize_samples(samples, active_only=False)
    assert summary["considered_count"] == 2
    assert summary["active_avg"] == 2


def test_summarize_samples_aggregates_wait_events_and_transactions_per_second() -> None:
    samples = [
        _sample(
            t=0,
            wait_event_type_counts={"Lock": 1},
            wait_event_counts={"transactionid": 1},
            xact_commit=1000,
            xact_rollback=0,
        ),
        _sample(
            t=10,
            wait_event_type_counts={"Lock": 2, "IO": 1},
            wait_event_counts={"transactionid": 2, "DataFileRead": 1},
            xact_commit=1500,
            xact_rollback=5,
        ),
    ]
    summary = summarize_samples(samples)
    assert summary["wait_event_type_totals"] == {"Lock": 3, "IO": 1}
    assert summary["wait_event_totals"]["transactionid"] == 3
    assert summary["transactions_per_second"] == (500 + 5) / 10


def test_summarize_samples_tracks_max_waiting_locks() -> None:
    samples = [
        _sample(t=0, locks_waiting_by_mode={"RowExclusiveLock": 1}),
        _sample(t=1, locks_waiting_by_mode={"RowExclusiveLock": 4, "ShareLock": 1}),
    ]
    summary = summarize_samples(samples)
    assert summary["locks_waiting_by_mode_max"] == {
        "RowExclusiveLock": 4,
        "ShareLock": 1,
    }
