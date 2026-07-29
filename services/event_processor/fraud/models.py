"""Typed internal fraud models."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID

type JsonValue = None | bool | int | str | list[JsonValue] | dict[str, JsonValue]


class FraudDecision(StrEnum):
    APPROVE = "APPROVE"
    REVIEW = "REVIEW"
    BLOCK = "BLOCK"


class FraudSeverity(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True, slots=True)
class FraudRuleResult:
    rule_id: str
    rule_version: str
    matched: bool
    score: int
    severity: FraudSeverity
    reason_code: str
    explanation: str
    evidence: dict[str, JsonValue]
    evaluated_at: datetime


@dataclass(frozen=True, slots=True)
class FraudEvaluation:
    evaluation_id: UUID
    source_event_id: UUID
    customer_id: UUID
    order_id: UUID | None
    payment_id: UUID | None
    total_score: int
    decision: FraudDecision
    severity: FraudSeverity
    rule_results: tuple[FraudRuleResult, ...]
    evaluated_at: datetime
    engine_version: str
    ruleset_version: str

    @property
    def matched_rules(self) -> tuple[FraudRuleResult, ...]:
        return tuple(result for result in self.rule_results if result.matched)
