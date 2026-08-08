"""Atomic fraud evaluation, alert, and outbox persistence."""

import json
from time import perf_counter

import psycopg

from services.event_processor.errors import FraudEvaluationIntegrityError
from services.event_processor.fraud.config import FraudConfig
from services.event_processor.fraud.models import FraudDecision, FraudEvaluation
from services.event_processor.fraud.publisher import build_alert_message
from shared.observability.metrics import ApplicationMetrics
from shared.schemas import EventEnvelope, canonical_json
from shared.schemas.base import ContractModel


class FraudRepository:
    def __init__(
        self,
        connection: psycopg.Connection[tuple[object, ...]],
        config: FraudConfig,
        metrics: ApplicationMetrics | None = None,
    ) -> None:
        self.connection = connection
        self.config = config
        self.metrics = metrics

    def persist(
        self,
        evaluation: FraudEvaluation,
        source_event: EventEnvelope[ContractModel],
    ) -> dict[str, int]:
        rule_results = [
            {
                "rule_id": item.rule_id,
                "rule_version": item.rule_version,
                "matched": item.matched,
                "score": item.score,
                "severity": item.severity.value,
                "reason_code": item.reason_code,
                "explanation": item.explanation,
                "evidence": item.evidence,
                "evaluated_at": item.evaluated_at.isoformat(),
            }
            for item in evaluation.rule_results
        ]
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT total_score, decision, severity, engine_version,
                       ruleset_version, rule_results
                FROM fraud_evaluations WHERE source_event_id = %s
                """,
                (evaluation.source_event_id,),
            )
            existing = cursor.fetchone()
            expected = (
                evaluation.total_score,
                evaluation.decision.value,
                evaluation.severity.value,
                evaluation.engine_version,
                evaluation.ruleset_version,
                rule_results,
            )
            if existing is not None:
                actual = (*existing[:5], existing[5])
                if actual != expected:
                    raise FraudEvaluationIntegrityError(
                        "source event has conflicting deterministic fraud evaluation"
                    )
                return {"fraud_evaluations": 0}
            cursor.execute(
                """
                INSERT INTO fraud_evaluations (
                    evaluation_id, source_event_id, customer_id, order_id,
                    payment_id, total_score, decision, severity, engine_version,
                    ruleset_version, matched_rule_count, rule_results, evaluated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s
                )
                """,
                (
                    evaluation.evaluation_id,
                    evaluation.source_event_id,
                    evaluation.customer_id,
                    evaluation.order_id,
                    evaluation.payment_id,
                    evaluation.total_score,
                    evaluation.decision.value,
                    evaluation.severity.value,
                    evaluation.engine_version,
                    evaluation.ruleset_version,
                    len(evaluation.matched_rules),
                    json.dumps(rule_results, separators=(",", ":"), sort_keys=True),
                    evaluation.evaluated_at,
                ),
            )
            rows = {"fraud_evaluations": 1}
            if evaluation.decision is FraudDecision.APPROVE:
                return rows
            message = build_alert_message(evaluation, source_event)
            reason_codes = [item.reason_code for item in evaluation.matched_rules]
            cursor.execute(
                """
                INSERT INTO fraud_alerts (
                    alert_id, evaluation_id, source_event_id, customer_id,
                    order_id, payment_id, score, decision, severity, status,
                    reason_codes, alert_event_id, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, 'OPEN',
                    %s::jsonb, %s, %s, %s
                )
                """,
                (
                    message.alert_id,
                    evaluation.evaluation_id,
                    evaluation.source_event_id,
                    evaluation.customer_id,
                    evaluation.order_id,
                    evaluation.payment_id,
                    evaluation.total_score,
                    evaluation.decision.value,
                    evaluation.severity.value,
                    json.dumps(reason_codes),
                    message.alert_event_id,
                    evaluation.evaluated_at,
                    evaluation.evaluated_at,
                ),
            )
            rows["fraud_alerts"] = 1
            if self.config.fraud_outbox_enabled:
                outbox_started = perf_counter()
                cursor.execute(
                    """
                    INSERT INTO fraud_outbox (
                        outbox_id, aggregate_type, aggregate_id, event_id,
                        event_type, event_version, topic, message_key,
                        headers_json, payload_json, payload_bytes, status,
                        available_at
                    ) VALUES (
                        %s, 'fraud_alert', %s, %s, %s, %s, %s, %s,
                        %s::jsonb, %s::jsonb, %s, 'PENDING', CURRENT_TIMESTAMP
                    )
                    """,
                    (
                        message.outbox_id,
                        message.alert_id,
                        message.alert_event_id,
                        message.event.event_type.value,
                        message.event.event_version,
                        self.config.fraud_alert_topic,
                        message.key,
                        json.dumps(
                            [[name, value.decode()] for name, value in message.headers]
                        ),
                        canonical_json(message.event),
                        message.payload_bytes,
                    ),
                )
                if self.metrics is not None:
                    self.metrics.outbox_write_duration.observe(
                        perf_counter() - outbox_started
                    )
                rows["fraud_outbox"] = 1
            return rows
