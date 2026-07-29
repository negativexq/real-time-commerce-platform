"""Pure deterministic fraud rule orchestration."""

from uuid import UUID, uuid5

from services.event_processor.errors import (
    FraudRuleConfigurationError,
    PermanentProcessingError,
)
from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.context import FraudContext
from services.event_processor.fraud.models import FraudEvaluation
from services.event_processor.fraud.registry import build_rule_registry
from services.event_processor.fraud.rules.base import FraudRule
from services.event_processor.fraud.scoring import aggregate

EVALUATION_NAMESPACE = UUID("af5fe7db-8df2-4e57-b506-39fc223b6e9c")


def deterministic_evaluation_id(source_event_id: UUID, ruleset_version: str) -> UUID:
    return uuid5(EVALUATION_NAMESPACE, f"{source_event_id}:{ruleset_version}")


class FraudEngine:
    def __init__(
        self, config: FraudConfig, rules: tuple[FraudRule, ...] | None = None
    ) -> None:
        self.config = config
        self.rules = rules if rules is not None else build_rule_registry(config)

    def evaluate(self, context: FraudContext) -> FraudEvaluation:
        results = []
        for rule in self.rules:
            if context.event_type not in rule.supported_event_types:
                continue
            try:
                evaluated = rule.evaluate(context)
            except FraudRuleConfigurationError:
                raise
            except Exception as exc:
                raise PermanentProcessingError(
                    f"fraud rule failed: {rule.rule_id}"
                ) from exc
            if evaluated.score > rule.maximum_score:
                raise FraudRuleConfigurationError(
                    f"fraud rule exceeded maximum: {rule.rule_id}"
                )
            results.append(evaluated)
        frozen = tuple(results)
        score, decision, severity = aggregate(frozen, self.config)
        return FraudEvaluation(
            deterministic_evaluation_id(
                context.source_event_id, self.config.fraud_ruleset_version
            ),
            context.source_event_id,
            context.customer_id,
            context.order_id,
            context.payment_id,
            score,
            decision,
            severity,
            frozen,
            context.event_time,
            self.config.fraud_engine_version,
            self.config.fraud_ruleset_version,
        )
