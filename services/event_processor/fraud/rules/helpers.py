"""Safe bounded rule-result helpers."""

from services.event_processor.fraud.context import FraudContext
from services.event_processor.fraud.models import (
    FraudRuleResult,
    FraudSeverity,
    JsonValue,
)


def result(
    context: FraudContext,
    rule_id: str,
    matched: bool,
    score: int,
    severity: FraudSeverity,
    reason_code: str,
    explanation: str,
    evidence: dict[str, JsonValue],
) -> FraudRuleResult:
    return FraudRuleResult(
        rule_id,
        "1.0",
        matched,
        score if matched else 0,
        severity if matched else FraudSeverity.LOW,
        reason_code,
        explanation[:256],
        evidence,
        context.event_time,
    )
