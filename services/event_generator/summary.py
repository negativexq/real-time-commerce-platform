"""Deterministic in-memory run metrics without Prometheus."""

from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal

from services.event_generator.journey import JourneyResult
from services.event_generator.messages import AnomalyType
from shared.commerce_common.enums import CustomerPersona, EventType
from shared.schemas import PaymentCompletedPayload


@dataclass(slots=True)
class RunSummary:
    """Aggregate generation outcomes for logs and tests."""

    journeys_per_persona: Counter[CustomerPersona] = field(default_factory=Counter)
    customers_per_persona: Counter[CustomerPersona] = field(default_factory=Counter)
    events_per_type: Counter[EventType] = field(default_factory=Counter)
    anomalies_per_type: Counter[AnomalyType] = field(default_factory=Counter)
    new_journeys: int = 0
    returning_journeys: int = 0
    payment_successes: int = 0
    payment_failures: int = 0
    refunds: int = 0
    event_total: int = 0
    journey_total: int = 0
    total_logical_commerce_value: Decimal = Decimal("0")
    _seen_customers: set[str] = field(default_factory=set)

    @property
    def average_events_per_journey(self) -> Decimal:
        """Return exact average event count."""
        if not self.journey_total:
            return Decimal("0")
        return Decimal(self.event_total) / Decimal(self.journey_total)

    def as_log(self) -> dict[str, object]:
        """Return JSON-renderable deterministic aggregate fields."""
        return {
            "journeys_generated": self.journey_total,
            "unique_customers": len(self._seen_customers),
            "persona_counts": {
                key.value: self.journeys_per_persona[key]
                for key in sorted(
                    self.journeys_per_persona, key=lambda item: item.value
                )
            },
            "valid_messages": self.event_total,
            "anomalous_messages": sum(self.anomalies_per_type.values()),
            "duplicate_messages": self.anomalies_per_type[AnomalyType.DUPLICATE],
            "payment_successes": self.payment_successes,
            "payment_failures": self.payment_failures,
            "refunds": self.refunds,
            "average_events_per_journey": str(
                self.average_events_per_journey.quantize(Decimal("0.001"))
            ),
            "total_logical_commerce_value": str(self.total_logical_commerce_value),
        }

    def record_journey(
        self,
        journey: JourneyResult,
        anomaly_types: list[AnomalyType],
    ) -> None:
        """Update every aggregate from one immutable journey result."""
        self.journey_total += 1
        self.journeys_per_persona[journey.persona] += 1
        customer_key = str(journey.customer_id)
        if customer_key not in self._seen_customers:
            self._seen_customers.add(customer_key)
            self.customers_per_persona[journey.persona] += 1
        if journey.returning_customer:
            self.returning_journeys += 1
        else:
            self.new_journeys += 1
        for event in journey.events:
            self.events_per_type[event.event_type] += 1
            self.event_total += 1
            if event.event_type is EventType.PAYMENT_COMPLETED:
                self.payment_successes += 1
                self.total_logical_commerce_value += (
                    event.payload.amount
                    if isinstance(event.payload, PaymentCompletedPayload)
                    else Decimal("0")
                )
            elif event.event_type is EventType.PAYMENT_FAILED:
                self.payment_failures += 1
            elif event.event_type is EventType.REFUND_REQUESTED:
                self.refunds += 1
        self.anomalies_per_type.update(anomaly_types)
