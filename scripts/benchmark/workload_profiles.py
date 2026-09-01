"""Benchmark-only workload profiles built from complete JourneyBuilder journeys."""

from dataclasses import dataclass
from datetime import UTC, datetime
from random import Random

from scripts.benchmark.config import derive_seed
from services.event_generator.anomalies import valid_message
from services.event_generator.config import GeneratorConfig
from services.event_generator.generator import SeededUuidFactory, SyntheticGenerator
from services.event_generator.journey import JourneyBuilder
from services.event_generator.messages import PublishableMessage


class _DeterministicClock:
    def now(self) -> datetime:
        return datetime(2026, 1, 1, tzinfo=UTC)


PROFILE_NAMES = (
    "baseline",
    "fraud_eligible_20",
    "fraud_eligible_10",
    "fraud_eligible_5",
    "fraud_eligible_0",
)
PROFILE_TARGETS: dict[str, float | None] = {
    "baseline": None,
    "fraud_eligible_20": 0.20,
    "fraud_eligible_10": 0.10,
    "fraud_eligible_5": 0.05,
    "fraud_eligible_0": 0.0,
}

# Calibrated from a deterministic 100,003-event sample with the same
# JourneyBuilder settings as the historical injector. These values change
# only benchmark input probability; JourneyBuilder owns all references.
PROFILE_CHECKOUT_PROBABILITIES = {
    "fraud_eligible_20": 0.336,
    "fraud_eligible_10": 0.150,
    "fraud_eligible_5": 0.070,
    "fraud_eligible_0": 0.0,
}
FRAUD_ELIGIBLE_EVENT_TYPES = frozenset(
    {
        "checkout_started",
        "order_created",
        "payment_completed",
        "payment_failed",
        "refund_requested",
    }
)


@dataclass(frozen=True, slots=True)
class PreparedWorkload:
    """Messages and complete journey boundaries for one controlled run."""

    messages: list[PublishableMessage]
    component_ranges: list[tuple[int, int]]
    requested_fraud_eligible_share: float


def _generator_config(
    bootstrap: str, client_id: str, checkout_probability: float
) -> GeneratorConfig:
    return GeneratorConfig(
        kafka_bootstrap_servers=bootstrap,
        kafka_client_id=client_id,
        generator_add_to_cart_probability=1,
        generator_checkout_probability=checkout_probability,
        generator_refund_probability=0,
        generator_anomalies_enabled=False,
    )


def prepare_controlled_workload(
    *, bootstrap: str, seed: int, target_count: int, profile: str, client_id: str
) -> PreparedWorkload:
    """Build complete valid journeys at a calibrated checkout probability."""
    checkout_probability = PROFILE_CHECKOUT_PROBABILITIES.get(profile)
    target = PROFILE_TARGETS.get(profile)
    if checkout_probability is None or target is None:
        raise ValueError(f"unsupported controlled workload profile: {profile}")

    # Keep the workload random stream identical to the historical generator.
    # Only UUIDs are namespaced so sequential profiles cannot reuse deterministic
    # event IDs and collide with the durable idempotency ledger.
    uuid_seed = derive_seed(str(seed), "workload-profile-uuid", profile)
    random_source = Random(seed)
    synthetic = SyntheticGenerator(random_source, SeededUuidFactory(uuid_seed))
    config = _generator_config(bootstrap, client_id, checkout_probability)
    builder = JourneyBuilder(config, synthetic, _DeterministicClock())

    messages: list[PublishableMessage] = []
    component_ranges: list[tuple[int, int]] = []
    while len(messages) < target_count:
        start = len(messages)
        journey = builder.build()
        component = [valid_message(event) for event in journey.events]
        messages.extend(component)
        component_ranges.append((start, len(messages)))

    return PreparedWorkload(messages, component_ranges, target)


def event_type_counts(messages: list[PublishableMessage]) -> dict[str, int]:
    """Count event types in published-message metadata."""
    counts: dict[str, int] = {}
    for message in messages:
        counts[message.event_type] = counts.get(message.event_type, 0) + 1
    return counts


def calculate_fraud_eligible_share(event_type_counts: dict[str, int]) -> float:
    """Return the incoming event share entering the fraud-eligible path."""
    total = sum(event_type_counts.values())
    eligible = sum(
        count
        for event_type, count in event_type_counts.items()
        if event_type in FRAUD_ELIGIBLE_EVENT_TYPES
    )
    return eligible / total if total else 0.0
