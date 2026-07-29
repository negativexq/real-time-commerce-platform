"""Validated fraud-engine configuration."""

import os
from collections.abc import Mapping
from decimal import Decimal
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

NonBlank = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
DEFAULT_RULES = (
    "high_amount,payment_velocity,failed_payment_burst,new_device,"
    "country_mismatch,rapid_checkout,account_takeover_composite,"
    "refund_abuse,bot_checkout,payment_amount_mismatch"
)


class FraudConfig(BaseModel):
    """Safe deterministic defaults for local synthetic scoring."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    fraud_engine_enabled: bool = True
    fraud_engine_version: NonBlank = "1.0.0"
    fraud_ruleset_version: NonBlank = "2026-08"
    fraud_enabled_rules: NonBlank = DEFAULT_RULES
    fraud_review_threshold: int = Field(default=30, ge=0, le=100)
    fraud_block_threshold: int = Field(default=60, ge=0, le=100)
    fraud_history_lookback_days: int = Field(default=30, gt=0, le=365)
    fraud_max_history_records: int = Field(default=500, gt=0, le=10_000)
    fraud_high_amount_threshold: Decimal = Field(default=Decimal("5000"), ge=0)
    fraud_amount_multiplier_threshold: Decimal = Field(default=Decimal("3"), gt=1)
    fraud_high_amount_score: int = Field(default=25, ge=0, le=100)
    fraud_amount_deviation_score: int = Field(default=20, ge=0, le=100)
    fraud_velocity_max_attempts: int = Field(default=3, gt=0)
    fraud_velocity_window_seconds: int = Field(default=120, gt=0)
    fraud_velocity_score: int = Field(default=20, ge=0, le=100)
    fraud_failed_payment_threshold: int = Field(default=3, gt=0)
    fraud_failed_payment_window_seconds: int = Field(default=300, gt=0)
    fraud_failed_payment_score: int = Field(default=25, ge=0, le=100)
    fraud_new_device_score: int = Field(default=15, ge=0, le=100)
    fraud_min_history_for_device_rule: int = Field(default=2, gt=0)
    fraud_country_mismatch_score: int = Field(default=10, ge=0, le=100)
    fraud_multi_country_score: int = Field(default=10, ge=0, le=100)
    fraud_country_window_seconds: int = Field(default=600, gt=0)
    fraud_rapid_checkout_seconds: int = Field(default=15, gt=0)
    fraud_rapid_checkout_score: int = Field(default=15, ge=0, le=100)
    fraud_account_takeover_score: int = Field(default=60, ge=0, le=100)
    fraud_account_takeover_min_signals: int = Field(default=4, ge=2, le=5)
    fraud_refund_rate_threshold: Decimal = Field(default=Decimal("0.5"), ge=0, le=1)
    fraud_rapid_refund_seconds: int = Field(default=600, gt=0)
    fraud_refund_abuse_score: int = Field(default=35, ge=0, le=100)
    fraud_bot_view_threshold: int = Field(default=12, gt=0)
    fraud_bot_checkout_score: int = Field(default=30, ge=0, le=100)
    fraud_amount_mismatch_score: int = Field(default=100, ge=0, le=100)
    fraud_alert_topic: NonBlank = "commerce.fraud-alerts"
    fraud_outbox_enabled: bool = True
    fraud_outbox_batch_size: int = Field(default=20, gt=0, le=1_000)
    fraud_outbox_poll_interval_ms: int = Field(default=500, gt=0)
    fraud_outbox_max_attempts: int = Field(default=10, gt=0, le=100)
    fraud_outbox_initial_backoff_ms: int = Field(default=500, ge=0)
    fraud_outbox_max_backoff_ms: int = Field(default=30_000, ge=0)
    fraud_outbox_claim_ttl_seconds: int = Field(default=30, gt=0)

    @model_validator(mode="after")
    def validate_relationships(self) -> Self:
        if not 0 <= self.fraud_review_threshold < self.fraud_block_threshold <= 100:
            raise ValueError("require 0 <= review threshold < block threshold <= 100")
        if self.fraud_outbox_initial_backoff_ms > self.fraud_outbox_max_backoff_ms:
            raise ValueError("initial outbox backoff cannot exceed maximum")
        if len(set(self.enabled_rule_ids)) != len(self.enabled_rule_ids):
            raise ValueError("enabled fraud rules must be unique")
        return self

    @property
    def enabled_rule_ids(self) -> tuple[str, ...]:
        return tuple(
            item.strip() for item in self.fraud_enabled_rules.split(",") if item.strip()
        )

    @classmethod
    def from_environment(
        cls, environment: Mapping[str, str] | None = None
    ) -> "FraudConfig":
        source = os.environ if environment is None else environment
        fields = cls.model_fields
        values = {
            name.lower(): value
            for name, value in source.items()
            if name.lower() in fields and name.startswith("FRAUD_")
        }
        return cls.model_validate(values)
