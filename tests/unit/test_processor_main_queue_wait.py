from datetime import UTC, datetime, timedelta

from services.event_processor.main import queue_wait_seconds


def test_returns_none_when_broker_supplied_no_timestamp() -> None:
    assert queue_wait_seconds(None, datetime.now(UTC)) is None


def test_computes_elapsed_seconds_between_produced_and_observed() -> None:
    produced = datetime(2026, 1, 1, tzinfo=UTC)
    observed = produced + timedelta(milliseconds=250)
    assert queue_wait_seconds(produced, observed) == 0.25


def test_zero_wait_when_produced_and_observed_match() -> None:
    now = datetime.now(UTC)
    assert queue_wait_seconds(now, now) == 0.0


def test_negative_wait_from_clock_skew_is_dropped() -> None:
    produced = datetime.now(UTC)
    observed = produced - timedelta(milliseconds=5)
    assert queue_wait_seconds(produced, observed) is None
