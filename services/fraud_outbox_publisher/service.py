"""Bounded polling loop independent from source consumption."""

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter, sleep, time

import psycopg

from services.fraud_outbox_publisher.config import OutboxConfig
from services.fraud_outbox_publisher.publisher import AlertPublisher
from services.fraud_outbox_publisher.repository import OutboxRepository
from shared.observability.metrics import ApplicationMetrics


@dataclass(slots=True)
class OutboxSummary:
    rows_claimed: int = 0
    alerts_published: int = 0
    publish_retries: int = 0
    permanent_failures: int = 0


class OutboxService:
    def __init__(
        self, config: OutboxConfig, metrics: ApplicationMetrics | None = None
    ) -> None:
        self.config = config
        self.connection = psycopg.connect(config.processor.postgres_dsn)
        self.repository = OutboxRepository(self.connection)
        self.publisher = AlertPublisher(config)
        self.summary = OutboxSummary()
        self.running = True
        self.metrics = metrics
        self._last_refresh = 0.0

    def run(self) -> None:
        heartbeat = Path("/tmp/fraud-outbox-healthy")
        while self.running:
            batch_started = perf_counter()
            heartbeat.touch()
            claim_started = perf_counter()
            records = self.repository.claim(self.config.fraud)
            if self.metrics is not None:
                self.metrics.outbox_claim_duration.observe(
                    perf_counter() - claim_started
                )
                self.metrics.outbox_claims.labels(
                    "claimed" if records else "empty"
                ).inc(len(records) or 1)
                self.metrics.outbox_recovered_claims.inc(
                    self.repository.recovered_claims
                )
            self.summary.rows_claimed += len(records)
            for record in records:
                publish_started = perf_counter()
                try:
                    self.publisher.publish(record)
                    self.repository.published(record)
                    self.summary.alerts_published += 1
                    if self.metrics is not None:
                        self.metrics.outbox_publications.labels("published").inc()
                        self.metrics.outbox_delivery_attempts.labels("success").inc()
                        self.metrics.outbox_publish_duration.labels(
                            "published"
                        ).observe(perf_counter() - publish_started)
                        self.metrics.outbox_last_success.set_to_current_time()
                        self.metrics.success()
                except Exception as exc:
                    self.repository.failed(record, exc, self.config.fraud)
                    if self.metrics is not None:
                        result = (
                            "failed"
                            if record.attempts
                            >= self.config.fraud.fraud_outbox_max_attempts
                            else "retry"
                        )
                        self.metrics.outbox_publications.labels(result).inc()
                        self.metrics.outbox_delivery_attempts.labels("failed").inc()
                        self.metrics.outbox_publish_duration.labels(result).observe(
                            perf_counter() - publish_started
                        )
                    if record.attempts >= self.config.fraud.fraud_outbox_max_attempts:
                        self.summary.permanent_failures += 1
                    else:
                        self.summary.publish_retries += 1
            if not records:
                sleep(self.config.fraud.fraud_outbox_poll_interval_ms / 1_000)
            if self.metrics is not None:
                self.metrics.outbox_batch_duration.observe(
                    perf_counter() - batch_started
                )
                if (
                    time() - self._last_refresh
                    >= self.config.metrics.refresh_interval_seconds
                ):
                    counts, age = self.repository.status_snapshot()
                    for status in ("pending", "publishing", "published", "failed"):
                        if status != "published":
                            self.metrics.outbox_rows.labels(status).set(
                                counts.get(status, 0)
                            )
                    self.metrics.outbox_oldest_pending_age.set(age)
                    healthy = age <= self.config.metrics.max_outbox_staleness_seconds
                    self.metrics.outbox_healthy.set(int(healthy))
                    self.metrics.service_healthy.labels(self.metrics.service).set(
                        int(healthy)
                    )
                    self._last_refresh = time()

    def stop(self) -> None:
        self.running = False

    def close(self) -> None:
        self.connection.close()
