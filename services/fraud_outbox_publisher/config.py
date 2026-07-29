"""Outbox configuration assembled from existing validated settings."""

import os
from dataclasses import dataclass

from services.event_processor.config import ProcessorConfig
from services.event_processor.fraud.config import FraudConfig
from shared.observability import MetricsConfig


@dataclass(frozen=True, slots=True)
class OutboxConfig:
    processor: ProcessorConfig
    fraud: FraudConfig
    metrics: MetricsConfig

    @classmethod
    def from_environment(cls) -> "OutboxConfig":
        return cls(
            ProcessorConfig.from_environment(),
            FraudConfig.from_environment(),
            MetricsConfig.from_environment(
                "fraud-outbox-publisher",
                9103,
                os.environ,
                port_environment_name="FRAUD_OUTBOX_METRICS_PORT",
            ),
        )
