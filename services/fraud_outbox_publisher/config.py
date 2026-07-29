"""Outbox configuration assembled from existing validated settings."""

from dataclasses import dataclass

from services.event_processor.config import ProcessorConfig
from services.event_processor.fraud.config import FraudConfig


@dataclass(frozen=True, slots=True)
class OutboxConfig:
    processor: ProcessorConfig
    fraud: FraudConfig

    @classmethod
    def from_environment(cls) -> "OutboxConfig":
        return cls(ProcessorConfig.from_environment(), FraudConfig.from_environment())
