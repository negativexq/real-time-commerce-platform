"""Decimal contract types with lossless JSON serialization."""

from decimal import Decimal
from typing import Annotated

from pydantic import Field, PlainSerializer


def decimal_to_string(value: Decimal) -> str:
    """Serialize a Decimal without converting it through binary float."""
    return str(value)


DecimalString = PlainSerializer(
    decimal_to_string,
    return_type=str,
    when_used="json",
)
NonNegativeMoney = Annotated[Decimal, Field(ge=Decimal("0")), DecimalString]
PositiveMoney = Annotated[Decimal, Field(gt=Decimal("0")), DecimalString]
FraudScore = Annotated[
    Decimal,
    Field(ge=Decimal("0"), le=Decimal("100")),
    DecimalString,
]

__all__ = [
    "FraudScore",
    "NonNegativeMoney",
    "PositiveMoney",
    "decimal_to_string",
]
