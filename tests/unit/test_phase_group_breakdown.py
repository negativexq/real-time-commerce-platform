from scripts.benchmark.direct_saturation import phase_group_breakdown


def _breakdown(**overrides: float | None) -> dict[str, dict[str, float | None]]:
    base: dict[str, float | None] = {
        "transaction_total": 2.0,
        "pool_acquire": 0.02,
        "connection_release": 0.01,
        "processed_events_insert": 0.15,
        "business_persistence": 0.30,
        "fraud_context": 0.90,
        "fraud_persistence": 0.35,
        "commit": 0.25,
    }
    base.update(overrides)
    return {key: {"avg": value} for key, value in base.items()}


def test_read_phase_is_fraud_context_only() -> None:
    result = phase_group_breakdown(_breakdown())
    assert result["read_phase_ms"] == 0.90


def test_write_phase_sums_all_write_stages() -> None:
    result = phase_group_breakdown(_breakdown())
    assert result["write_phase_ms"] == 0.15 + 0.30 + 0.35


def test_commit_and_pool_stages_pass_through() -> None:
    result = phase_group_breakdown(_breakdown())
    assert result["commit_ms"] == 0.25
    assert result["pool_acquire_ms"] == 0.02
    assert result["connection_release_ms"] == 0.01


def test_unattributed_is_the_residual_against_transaction_total() -> None:
    result = phase_group_breakdown(_breakdown())
    known = 0.90 + (0.15 + 0.30 + 0.35) + 0.25 + 0.02 + 0.01
    assert result["unattributed_ms"] == 2.0 - known


def test_non_fraud_event_has_no_read_phase_but_write_phase_survives() -> None:
    # fraud_context/fraud_persistence never fire for non-fraud-eligible
    # events - Prometheus would report None for those stages in a window
    # where only non-fraud traffic ran.
    result = phase_group_breakdown(
        _breakdown(fraud_context=None, fraud_persistence=None)
    )
    assert result["read_phase_ms"] is None
    assert result["write_phase_ms"] == 0.15 + 0.30


def test_missing_transaction_total_yields_no_unattributed_estimate() -> None:
    result = phase_group_breakdown(_breakdown(transaction_total=None))
    assert result["unattributed_ms"] is None


def test_completely_empty_breakdown_returns_all_none() -> None:
    result = phase_group_breakdown({})
    assert all(value is None for value in result.values())


def test_unknown_keys_in_breakdown_are_ignored() -> None:
    breakdown = _breakdown()
    breakdown["fraud_context_recent_payments"] = {"avg": 999.0}
    result = phase_group_breakdown(breakdown)
    assert result["read_phase_ms"] == 0.90
