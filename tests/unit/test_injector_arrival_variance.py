import pytest

from scripts.benchmark.direct_injector import (
    arrival_variance_summary,
    inter_arrival_gaps,
    sliding_window_rates,
)


def test_inter_arrival_gaps_are_consecutive_differences() -> None:
    assert inter_arrival_gaps([0.0, 0.1, 0.35, 0.4]) == pytest.approx([0.1, 0.25, 0.05])


def test_inter_arrival_gaps_empty_and_single_are_empty() -> None:
    assert inter_arrival_gaps([]) == []
    assert inter_arrival_gaps([1.0]) == []


def test_sliding_window_rates_even_spacing_matches_requested_rate() -> None:
    # 10 events evenly spaced 100ms apart over 1s -> 10 evt/s in each 100ms bin.
    timestamps = [i * 0.1 for i in range(10)]
    rates = sliding_window_rates(timestamps, 0.1)
    assert all(rate == 10.0 for rate in rates)


def test_sliding_window_rates_detects_a_burst() -> None:
    # 9 events packed into the first 100ms, then a long gap, then 1 more.
    timestamps = [i * 0.01 for i in range(9)] + [1.0]
    rates = sliding_window_rates(timestamps, 0.1)
    assert rates[0] == 90.0
    assert max(rates[1:]) < 90.0


def test_sliding_window_rates_empty_input() -> None:
    assert sliding_window_rates([], 0.1) == []


def test_arrival_variance_summary_reports_gap_percentiles_and_window_rates() -> None:
    timestamps = [i * 0.1 for i in range(20)]
    summary = arrival_variance_summary(timestamps)
    assert summary["inter_arrival_gap_ms"]["p50"] == pytest.approx(100.0)
    assert summary["inter_arrival_gap_ms"]["max"] == pytest.approx(100.0)
    assert summary["window_rate_1s"]["max"] == 10.0


def test_arrival_variance_summary_empty_timestamps_has_no_gaps() -> None:
    summary = arrival_variance_summary([])
    assert summary["inter_arrival_gap_ms"]["max"] is None
    assert summary["window_rate_100ms"]["window_count"] == 0
