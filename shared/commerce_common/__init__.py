"""Common utilities shared by commerce platform services."""

from datetime import UTC, datetime


def utc_now() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(UTC)


__all__ = ["utc_now"]
