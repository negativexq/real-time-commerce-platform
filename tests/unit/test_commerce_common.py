"""Unit tests for shared commerce utilities."""

from datetime import UTC

from shared.commerce_common import utc_now


def test_utc_now_returns_timezone_aware_utc_datetime() -> None:
    """The shared clock must never produce a naive timestamp."""
    timestamp = utc_now()

    assert timestamp.tzinfo is UTC
    assert timestamp.utcoffset() is not None
