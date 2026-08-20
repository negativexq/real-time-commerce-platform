"""Stable, bounded Prometheus metric families for the commerce services."""

from time import time

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class ApplicationMetrics:
    """All application metrics registered against an isolated registry."""

    def __init__(
        self,
        service: str,
        namespace: str = "commerce",
        registry: CollectorRegistry | None = None,
    ) -> None:
        self.service = service
        self.registry = registry or CollectorRegistry()

        def counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Counter:
            return Counter(
                name,
                doc,
                labels,
                namespace=namespace,
                registry=self.registry,
            )

        def gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Gauge:
            return Gauge(
                name,
                doc,
                labels,
                namespace=namespace,
                registry=self.registry,
            )

        def histogram(
            name: str,
            doc: str,
            labels: tuple[str, ...] = (),
            buckets: tuple[float, ...] | None = None,
        ) -> Histogram:
            return Histogram(
                name,
                doc,
                labels,
                namespace=namespace,
                registry=self.registry,
                buckets=buckets or Histogram.DEFAULT_BUCKETS,
            )

        self.service_up = gauge(
            "service_up", "Application process is running.", ("service",)
        )
        self.service_healthy = gauge(
            "service_healthy", "Application health evaluation.", ("service",)
        )
        self.service_last_success = gauge(
            "service_last_success_timestamp_seconds",
            "Unix timestamp of last successful work.",
            ("service",),
        )
        self.service_start = gauge(
            "service_start_time_seconds", "Application start time.", ("service",)
        )
        self.service_up.labels(service).set(1)
        self.service_healthy.labels(service).set(1)
        self.service_start.labels(service).set(time())

        self.processor_events_received = counter(
            "processor_events_received",
            "Source records received.",
            ("event_type", "topic"),
        )
        self.processor_events_terminal = counter(
            "processor_events_terminal",
            "Terminal source outcomes.",
            ("event_type", "result"),
        )
        self.processor_validation_failures = counter(
            "processor_validation_failures", "Validation failures.", ("category",)
        )
        self.processor_retries = counter(
            "processor_retries", "Bounded processing retries.", ("category", "outcome")
        )
        self.processor_dlq_published = counter(
            "processor_dlq_published",
            "Confirmed DLQ publications.",
            ("error_category",),
        )
        self.processor_offset_commits = counter(
            "processor_offset_commits",
            "Batched manual offset commit call outcomes (one per Kafka "
            "commit() call, not per event).",
            ("result",),
        )
        self.processor_offset_commit_calls = counter(
            "processor_offset_commit_calls",
            "Batched offset commit calls by trigger.",
            ("trigger",),
        )
        self.processor_offset_commit_batch_records = histogram(
            "processor_offset_commit_batch_records",
            "Terminal records represented by each batched offset commit call.",
            buckets=(1, 5, 10, 25, 50, 100, 250, 500),
        )
        self.processor_rebalances = counter(
            "processor_rebalances", "Consumer rebalance actions.", ("action",)
        )
        self.processor_shutdowns = counter(
            "processor_shutdowns", "Processor shutdown outcomes.", ("result",)
        )
        self.processor_processing_duration = histogram(
            "processor_event_processing_duration_seconds",
            "Record receipt through terminal handling.",
            ("event_type", "result"),
        )
        self.processor_poll_to_handler_duration = histogram(
            "processor_poll_to_handler_duration_seconds",
            "Kafka poll through dispatch into the processor handler.",
        )
        self.processor_loop_gap_duration = histogram(
            "processor_loop_gap_duration_seconds",
            "Time from one terminal process return to the next Kafka poll.",
        )
        self.processor_validation_duration = histogram(
            "processor_validation_duration_seconds", "Contract validation latency."
        )
        self.processor_redis_duration = histogram(
            "processor_redis_duration_seconds",
            "Redis path latency.",
            ("operation", "result"),
        )
        self.processor_database_duration = histogram(
            "processor_database_duration_seconds",
            "Processor database latency.",
            ("operation", "result"),
        )
        self.processor_dlq_duration = histogram(
            "processor_dlq_publish_duration_seconds", "DLQ confirmation latency."
        )
        self.processor_offset_duration = histogram(
            "processor_offset_commit_duration_seconds",
            "Synchronous batched offset commit call latency.",
        )
        self.processor_inflight = gauge(
            "processor_inflight_events", "Current in-flight records."
        )
        self.processor_assigned = gauge(
            "processor_assigned_partitions", "Currently assigned source partitions."
        )
        self.processor_last_success = gauge(
            "processor_last_success_timestamp_seconds",
            "Last successful terminal source record.",
        )
        self.processor_last_poll = gauge(
            "processor_last_poll_timestamp_seconds", "Last Kafka poll timestamp."
        )
        self.processor_healthy = gauge(
            "processor_healthy", "Processor recoverable health."
        )

        self.idempotency_operations = counter(
            "idempotency_operations",
            "Redis idempotency outcomes.",
            ("operation", "result"),
        )
        self.idempotency_reconciliations = counter(
            "idempotency_reconciliations",
            "Redis/PostgreSQL reconciliation.",
            ("result",),
        )
        self.idempotency_duration = histogram(
            "idempotency_operation_duration_seconds",
            "Redis idempotency latency.",
            ("operation", "result"),
        )
        self.idempotency_processing_keys = gauge(
            "idempotency_processing_keys", "Bounded sampled processing keys."
        )
        self.idempotency_completed_keys = gauge(
            "idempotency_completed_keys", "Bounded sampled completed keys."
        )

        self.database_transactions = counter(
            "database_transactions", "Application database transactions.", ("result",)
        )
        self.database_rows_written = counter(
            "database_rows_written",
            "Rows written by bounded table name.",
            ("table", "operation"),
        )
        self.database_pool_acquisitions = counter(
            "database_pool_acquisitions", "Pool acquisitions.", ("result",)
        )
        self.database_slow_operations = counter(
            "database_slow_operations",
            "Slow operations over configured threshold.",
            ("operation",),
        )
        self.database_transaction_duration = histogram(
            "database_transaction_duration_seconds", "Transaction latency.", ("result",)
        )
        self.database_pool_acquire_duration = histogram(
            "database_pool_acquire_duration_seconds", "Connection acquisition latency."
        )
        self.database_operation_duration = histogram(
            "database_operation_duration_seconds",
            "Database operation latency.",
            ("operation", "table", "result"),
        )
        self.database_sql_duration = histogram(
            "database_sql_duration_seconds",
            "Individual SQL round-trip latency by bounded operation.",
            ("operation", "statement_kind"),
        )
        self.database_sql_statement_count = counter(
            "database_sql_statement_count",
            "Individual SQL statements by bounded operation.",
            ("operation", "statement_kind"),
        )
        self.database_stage_duration = histogram(
            "database_stage_duration_seconds",
            "Measured transaction stage latency.",
            ("stage",),
        )
        self.database_connection_release_duration = histogram(
            "database_connection_release_duration_seconds",
            "Connection context release latency.",
        )
        self.database_pool_connections = gauge(
            "database_pool_connections", "Connection pool state.", ("state",)
        )
        self.database_healthy = gauge(
            "database_healthy", "Application database health."
        )

        self.fraud_evaluations = counter(
            "fraud_evaluations",
            "Fraud evaluations.",
            ("event_type", "decision", "severity"),
        )
        self.fraud_rule_matches = counter(
            "fraud_rule_matches", "Matched fraud rules.", ("rule_id", "severity")
        )
        self.fraud_rule_evaluations = counter(
            "fraud_rule_evaluations", "Rule outcomes.", ("rule_id", "result")
        )
        self.fraud_alerts_created = counter(
            "fraud_alerts_created", "Durable fraud alerts.", ("decision", "severity")
        )
        self.fraud_context_failures = counter(
            "fraud_context_failures", "Fraud context failures.", ("category",)
        )
        self.fraud_integrity_failures = counter(
            "fraud_integrity_failures", "Deterministic fraud integrity failures."
        )
        self.fraud_evaluation_duration = histogram(
            "fraud_evaluation_duration_seconds",
            "Fraud evaluation latency.",
            ("event_type", "decision"),
        )
        self.fraud_context_duration = histogram(
            "fraud_context_build_duration_seconds", "Fraud context build latency."
        )
        self.fraud_rule_duration = histogram(
            "fraud_rule_duration_seconds",
            "Individual rule latency.",
            ("rule_id", "result"),
        )
        self.fraud_score = histogram(
            "fraud_score",
            "Synthetic rule-based score distribution.",
            buckets=(0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100),
        )
        self.fraud_last_evaluation = gauge(
            "fraud_last_evaluation_timestamp_seconds",
            "Last fraud evaluation timestamp.",
        )

        self.outbox_claims = counter(
            "outbox_claims", "Outbox claim outcomes.", ("result",)
        )
        self.outbox_publications = counter(
            "outbox_publications", "Outbox publication outcomes.", ("result",)
        )
        self.outbox_recovered_claims = counter(
            "outbox_recovered_claims", "Expired publishing claims recovered."
        )
        self.outbox_delivery_attempts = counter(
            "outbox_delivery_attempts", "Kafka delivery attempts.", ("result",)
        )
        self.outbox_claim_duration = histogram(
            "outbox_claim_duration_seconds", "Outbox claim transaction latency."
        )
        self.outbox_publish_duration = histogram(
            "outbox_publish_duration_seconds", "Outbox publish latency.", ("result",)
        )
        self.outbox_write_duration = histogram(
            "outbox_write_duration_seconds",
            "Transactional fraud outbox row write latency.",
        )
        self.outbox_batch_duration = histogram(
            "outbox_batch_duration_seconds", "Outbox batch latency."
        )
        self.outbox_rows = gauge(
            "outbox_rows", "Cached outbox rows by status.", ("status",)
        )
        self.outbox_oldest_pending_age = gauge(
            "outbox_oldest_pending_age_seconds", "Age of oldest pending row."
        )
        self.outbox_last_success = gauge(
            "outbox_last_success_timestamp_seconds",
            "Last confirmed publication timestamp.",
        )
        self.outbox_healthy = gauge(
            "outbox_healthy", "Outbox publisher recoverable health."
        )

        self.generator_events_generated = counter(
            "generator_events_generated",
            "Synthetic events generated.",
            ("event_type", "persona"),
        )
        self.generator_events_published = counter(
            "generator_events_published",
            "Synthetic publish outcomes.",
            ("event_type", "result"),
        )
        self.generator_anomalies = counter(
            "generator_anomalies_injected", "Controlled anomalies.", ("anomaly_type",)
        )
        self.generator_journeys = counter(
            "generator_persona_journeys", "Synthetic journeys.", ("persona", "result")
        )
        self.generator_publish_duration = histogram(
            "generator_publish_duration_seconds", "Producer queue latency.", ("result",)
        )
        self.generator_journey_duration = histogram(
            "generator_journey_duration_seconds",
            "Journey construction latency.",
            ("persona",),
        )
        self.generator_active_customers = gauge(
            "generator_active_customers", "Process-local synthetic customer count."
        )
        self.generator_rate = gauge(
            "generator_generation_rate_per_second", "Configured generation rate."
        )
        self.generator_healthy = gauge(
            "generator_healthy", "Generator recoverable health."
        )

    def success(self) -> None:
        now = time()
        self.service_last_success.labels(self.service).set(now)
        self.service_healthy.labels(self.service).set(1)
