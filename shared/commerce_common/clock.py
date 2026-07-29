"""UTC clock and timestamp contract types."""

from datetime import UTC, datetime
from typing import Annotated

from pydantic import AfterValidator, AwareDatetime, PlainSerializer


def utc_now() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(UTC)


def normalize_utc(value: datetime) -> datetime:
    """Normalize an aware datetime to UTC."""
    return value.astimezone(UTC)


def serialize_utc(value: datetime) -> str:
    """Serialize a UTC datetime with an explicit ``Z`` suffix."""
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


UtcDateTime = Annotated[
    AwareDatetime,
    AfterValidator(normalize_utc),
    PlainSerializer(serialize_utc, return_type=str, when_used="json"),
]

__all__ = ["UtcDateTime", "normalize_utc", "serialize_utc", "utc_now"]
