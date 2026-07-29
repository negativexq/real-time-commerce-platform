"""Transparent score, decision, and severity aggregation."""

from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.models import (
    FraudDecision,
    FraudRuleResult,
    FraudSeverity,
)

SEVERITY_ORDER = {
    FraudSeverity.LOW: 0,
    FraudSeverity.MEDIUM: 1,
    FraudSeverity.HIGH: 2,
    FraudSeverity.CRITICAL: 3,
}


def aggregate(
    results: tuple[FraudRuleResult, ...], config: FraudConfig
) -> tuple[int, FraudDecision, FraudSeverity]:
    if any(result.score < 0 for result in results):
        raise ValueError("fraud rules cannot return negative scores")
    matched = tuple(result for result in results if result.matched)
    score = min(100, sum(result.score for result in matched))
    if score >= config.fraud_block_threshold:
        decision = FraudDecision.BLOCK
        minimum = FraudSeverity.HIGH
    elif score >= config.fraud_review_threshold:
        decision = FraudDecision.REVIEW
        minimum = FraudSeverity.MEDIUM
    else:
        decision = FraudDecision.APPROVE
        minimum = FraudSeverity.LOW
    severity = max(
        (result.severity for result in matched),
        default=minimum,
        key=SEVERITY_ORDER.__getitem__,
    )
    if SEVERITY_ORDER[severity] < SEVERITY_ORDER[minimum]:
        severity = minimum
    return score, decision, severity
