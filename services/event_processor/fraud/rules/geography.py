"""Synthetic country mismatch rule; no geolocation is performed."""

from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.context import FraudContext
from services.event_processor.fraud.models import FraudRuleResult, FraudSeverity
from services.event_processor.fraud.rules.helpers import result
from shared.commerce_common.enums import EventType


class CountryMismatchRule:
    rule_id = "country_mismatch"
    rule_version = "1.0"
    supported_event_types = frozenset(
        {EventType.ORDER_CREATED, EventType.PAYMENT_FAILED, EventType.PAYMENT_COMPLETED}
    )

    def __init__(self, config: FraudConfig) -> None:
        self.config = config
        self.maximum_score = min(
            100,
            config.fraud_country_mismatch_score + config.fraud_multi_country_score,
        )

    def evaluate(self, context: FraudContext) -> FraudRuleResult:
        mismatch = context.country_mismatch
        multiple = context.recent_distinct_countries > 1
        score = self.config.fraud_country_mismatch_score * int(
            mismatch
        ) + self.config.fraud_multi_country_score * int(multiple)
        return result(
            context,
            self.rule_id,
            mismatch or multiple,
            min(score, self.maximum_score),
            FraudSeverity.HIGH if multiple else FraudSeverity.MEDIUM,
            "COUNTRY_MISMATCH",
            "Synthetic country indicators differ from established activity.",
            {
                "home_match": not mismatch,
                "recent_country_count": context.recent_distinct_countries,
            },
        )
