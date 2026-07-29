"""Shared fraud-rule protocol and result construction."""

from typing import Protocol

from services.event_processor.fraud.context import FraudContext
from services.event_processor.fraud.models import FraudRuleResult
from shared.commerce_common.enums import EventType


class FraudRule(Protocol):
    rule_id: str
    rule_version: str
    supported_event_types: frozenset[EventType]
    maximum_score: int

    def evaluate(self, context: FraudContext) -> FraudRuleResult: ...
