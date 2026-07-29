"""Scenario catalog and bounded request models."""

from enum import StrEnum

from pydantic import BaseModel, Field, model_validator


class ScenarioType(StrEnum):
    NORMAL = "normal_customer"
    SUSPICIOUS = "suspicious_payment"
    TAKEOVER = "account_takeover"
    BOT = "bot_checkout"
    REFUND = "refund_abuse"
    DUPLICATE = "duplicate_delivery"
    MALFORMED = "malformed_event"
    MIXED = "mixed_traffic"


class ScenarioDefinition(BaseModel):
    scenario_type: ScenarioType
    title: str
    purpose: str
    expected_outcome: str
    transaction_configurable: bool = False


class RunCreate(BaseModel):
    scenario_type: ScenarioType
    event_count: int = Field(500, ge=1, le=100000)
    duration_seconds: int | None = Field(None, ge=1, le=3600)
    events_per_second: int = Field(20, ge=1, le=1000)
    seed: int = 42
    anomaly_rate: float = Field(0, ge=0, le=0.25)
    duplicate_rate: float = Field(0, ge=0, le=0.25)
    malformed_rate: float = Field(0, ge=0, le=0.25)
    persona_distribution: dict[str, int] | None = None
    transaction_enabled: bool = True
    cleanup_after_completion: bool = False
    notes: str | None = Field(None, max_length=500)
    malformed_case: str = Field(
        "malformed_json",
        pattern=r"^(malformed_json|missing_field|unknown_event_type|payload_mismatch)$",
    )

    @model_validator(mode="after")
    def validate_relationships(self) -> "RunCreate":
        if self.duration_seconds is not None:
            capacity = self.duration_seconds * self.events_per_second
            if self.event_count > capacity:
                raise ValueError(
                    "event_count exceeds duration_seconds × events_per_second"
                )
        if self.scenario_type is ScenarioType.MIXED:
            if (
                not self.persona_distribution
                or sum(self.persona_distribution.values()) != 100
            ):
                raise ValueError("mixed persona percentages must total 100")
            allowed = {
                "normal",
                "suspicious",
                "bot",
                "account_takeover",
                "discount_hunter",
                "indecisive",
            }
            if set(self.persona_distribution) - allowed or any(
                value < 0 or value > 100 for value in self.persona_distribution.values()
            ):
                raise ValueError("mixed persona distribution contains invalid values")
        elif self.persona_distribution is not None:
            raise ValueError("persona_distribution is only valid for mixed_traffic")
        return self
