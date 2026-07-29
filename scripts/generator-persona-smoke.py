"""Bounded persona/state smoke test executed inside the generator image."""

import random
from datetime import UTC, datetime, timedelta

from services.event_generator.config import GeneratorConfig
from services.event_generator.generator import SeededUuidFactory, SyntheticGenerator
from services.event_generator.journey import JourneyBuilder
from services.event_generator.producer import KafkaEventProducer
from shared.commerce_common.enums import CustomerPersona, EventType
from shared.schemas import parse_event


class SmokeClock:
    """Deterministic logical-time anchor."""

    def __init__(self) -> None:
        self.current = datetime(2026, 6, 1, tzinfo=UTC)

    def now(self) -> datetime:
        value = self.current
        self.current += timedelta(seconds=1)
        return value


def builder(persona: CustomerPersona, seed: int) -> JourneyBuilder:
    config = GeneratorConfig.from_environment().model_copy(
        update={
            "generator_persona": persona,
            "generator_seed": seed,
            "generator_new_customer_probability": 0.0,
            "generator_anomalies_enabled": False,
        }
    )
    synthetic = SyntheticGenerator(random.Random(seed), SeededUuidFactory(seed))
    return JourneyBuilder(config, synthetic, SmokeClock())


def main() -> int:
    """Validate all persona patterns and publish only typed events."""
    results = {}
    for index, persona in enumerate(CustomerPersona):
        persona_builder = builder(persona, 700 + index)
        journeys = [persona_builder.build(), persona_builder.build()]
        results[persona] = journeys

    normal = results[CustomerPersona.NORMAL]
    if normal[0].customer_id != normal[1].customer_id:
        raise AssertionError("normal returning identity was not stable")
    if EventType.USER_REGISTERED in {event.event_type for event in normal[1].events}:
        raise AssertionError("returning normal customer registered twice")

    indecisive_views = sum(
        event.event_type is EventType.PRODUCT_VIEWED
        for event in results[CustomerPersona.INDECISIVE][0].events
    )
    normal_views = sum(
        event.event_type is EventType.PRODUCT_VIEWED for event in normal[0].events
    )
    if indecisive_views <= normal_views:
        raise AssertionError("indecisive persona did not browse more than normal")

    bot = results[CustomerPersona.BOT][0]
    bot_views = sum(
        event.event_type is EventType.PRODUCT_VIEWED for event in bot.events
    )
    if not 8 <= bot_views <= 20:
        raise AssertionError("bot browsing burst was not bounded")

    history, takeover = results[CustomerPersona.ACCOUNT_TAKEOVER]
    if history.persona is not CustomerPersona.NORMAL:
        raise AssertionError("takeover did not establish normal prior history")
    if takeover.customer_id != history.customer_id or not takeover.returning_customer:
        raise AssertionError("takeover did not reuse the established customer")

    producer = KafkaEventProducer(GeneratorConfig.from_environment())
    event_count = 0
    for journeys in results.values():
        for journey in journeys:
            for event in journey.events:
                parse_event(event.model_dump_json())
                producer.publish(event)
                event_count += 1
    producer.flush()
    print(
        "Persona smoke test passed: "
        f"{len(results)} personas, {event_count} valid messages."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
