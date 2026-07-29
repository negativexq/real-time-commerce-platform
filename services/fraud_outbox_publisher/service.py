"""Bounded polling loop independent from source consumption."""

from dataclasses import dataclass
from pathlib import Path
from time import sleep

import psycopg

from services.fraud_outbox_publisher.config import OutboxConfig
from services.fraud_outbox_publisher.publisher import AlertPublisher
from services.fraud_outbox_publisher.repository import OutboxRepository


@dataclass(slots=True)
class OutboxSummary:
    rows_claimed: int = 0
    alerts_published: int = 0
    publish_retries: int = 0
    permanent_failures: int = 0


class OutboxService:
    def __init__(self, config: OutboxConfig) -> None:
        self.config = config
        self.connection = psycopg.connect(config.processor.postgres_dsn)
        self.repository = OutboxRepository(self.connection)
        self.publisher = AlertPublisher(config)
        self.summary = OutboxSummary()
        self.running = True

    def run(self) -> None:
        heartbeat = Path("/tmp/fraud-outbox-healthy")
        while self.running:
            heartbeat.touch()
            records = self.repository.claim(self.config.fraud)
            self.summary.rows_claimed += len(records)
            for record in records:
                try:
                    self.publisher.publish(record)
                    self.repository.published(record)
                    self.summary.alerts_published += 1
                except Exception as exc:
                    self.repository.failed(record, exc, self.config.fraud)
                    if record.attempts >= self.config.fraud.fraud_outbox_max_attempts:
                        self.summary.permanent_failures += 1
                    else:
                        self.summary.publish_retries += 1
            if not records:
                sleep(self.config.fraud.fraud_outbox_poll_interval_ms / 1_000)

    def stop(self) -> None:
        self.running = False

    def close(self) -> None:
        self.connection.close()
