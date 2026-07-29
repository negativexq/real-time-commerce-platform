"""New-device rule."""

from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.context import FraudContext
from services.event_processor.fraud.models import FraudRuleResult, FraudSeverity
from services.event_processor.fraud.rules.helpers import result
from shared.commerce_common.enums import EventType


class NewDeviceRule:
    rule_id = "new_device"
    rule_version = "1.0"
    supported_event_types = frozenset(
        {EventType.PAYMENT_FAILED, EventType.PAYMENT_COMPLETED}
    )

    def __init__(self, config: FraudConfig) -> None:
        self.config = config
        self.maximum_score = config.fraud_new_device_score

    def evaluate(self, context: FraudContext) -> FraudRuleResult:
        matched = (
            context.lifetime_successful_payments
            >= self.config.fraud_min_history_for_device_rule
            and context.is_new_device
        )
        return result(
            context,
            self.rule_id,
            matched,
            self.maximum_score,
            FraudSeverity.MEDIUM,
            "NEW_DEVICE",
            "An established account used an unrecognized device indicator.",
            {
                "established_history": context.established_history,
                "known_device": not context.is_new_device,
            },
        )
