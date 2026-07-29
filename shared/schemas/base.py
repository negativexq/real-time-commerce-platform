"""Strict base types for all event contracts."""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, StringConstraints


class ContractModel(BaseModel):
    """Base model that rejects coercion and undocumented fields."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


NonEmptyString = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, strict=True),
]
CountryCode = Annotated[
    str,
    StringConstraints(pattern=r"^[A-Z]{2}$", strict=True),
]

__all__ = ["ContractModel", "CountryCode", "NonEmptyString"]
