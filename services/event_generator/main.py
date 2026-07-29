"""Event-generator CLI and graceful application lifecycle."""

import argparse
import random
import signal
import sys
from collections.abc import Sequence
from pathlib import Path
from threading import Event
from time import perf_counter
from types import FrameType

from services.event_generator.config import GeneratorConfig
from services.event_generator.generator import (
    SeededUuidFactory,
    SyntheticGenerator,
    UuidFactory,
)
from services.event_generator.journey import JourneyBuilder, SystemClock
from services.event_generator.logging import configure_logging, get_logger
from services.event_generator.producer import KafkaEventProducer


class ShutdownController:
    """Signal-safe stop request shared with the generation loop."""

    def __init__(self) -> None:
        self._event = Event()

    @property
    def requested(self) -> bool:
        """Whether shutdown has been requested."""
        return self._event.is_set()

    def request(self) -> None:
        """Request a graceful shutdown."""
        self._event.set()

    def wait(self, timeout: float) -> bool:
        """Wait interruptibly between journeys."""
        return self._event.wait(timeout)


def parse_config(
    arguments: Sequence[str] | None = None,
    environment: dict[str, str] | None = None,
) -> GeneratorConfig:
    """Load environment defaults and apply explicit CLI overrides."""
    base = GeneratorConfig.from_environment(environment)
    parser = argparse.ArgumentParser(description="Publish synthetic commerce journeys")
    parser.add_argument("--journeys", type=int)
    parser.add_argument("--rate", type=float)
    parser.add_argument("--seed", type=int)
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    parsed = parser.parse_args(arguments)
    overrides = {
        name: value
        for name, value in {
            "generator_journeys": parsed.journeys,
            "generator_rate_per_second": parsed.rate,
            "generator_seed": parsed.seed,
            "generator_log_level": parsed.log_level,
        }.items()
        if value is not None
    }
    return GeneratorConfig.model_validate({**base.model_dump(), **overrides})


def build_journey_builder(config: GeneratorConfig) -> JourneyBuilder:
    """Construct production generation dependencies."""
    random_source = random.Random(config.generator_seed)
    uuid_factory = (
        SeededUuidFactory(config.generator_seed)
        if config.generator_seed is not None
        else UuidFactory()
    )
    return JourneyBuilder(
        config,
        SyntheticGenerator(random_source, uuid_factory),
        SystemClock(),
    )


def run_generation(
    config: GeneratorConfig,
    builder: JourneyBuilder,
    producer: KafkaEventProducer,
    shutdown: ShutdownController,
) -> int:
    """Run finite or continuous generation and perform a bounded final flush."""
    logger = get_logger()
    finite = config.generator_journeys is not None
    target = config.generator_journeys
    generated = 0
    interval = min(1.0 / config.generator_rate_per_second, 60.0)

    logger.info(
        "generator_started",
        kafka_bootstrap_servers=config.kafka_bootstrap_servers,
        topic=config.kafka_events_topic,
        mode="finite" if finite else "continuous",
        rate=config.generator_rate_per_second,
        seed_configured=config.generator_seed is not None,
        client_id=config.kafka_client_id,
    )

    while not shutdown.requested and (target is None or generated < target):
        started = perf_counter()
        journey = builder.build()
        for event in journey.events:
            producer.publish(event)
        producer.poll(0)
        generated += 1
        logger.info(
            "journey_generated",
            correlation_id=str(journey.correlation_id),
            customer_id=str(journey.customer_id),
            event_count=len(journey.events),
            terminal_event_type=journey.terminal_event_type.value,
            generation_duration_ms=round((perf_counter() - started) * 1_000, 3),
        )
        if target is None or generated < target:
            shutdown.wait(interval)

    logger.info("generator_stopping", journeys_generated=generated)
    producer.flush()
    logger.info("generator_stopped", journeys_generated=generated, undelivered=0)
    return 0


def main(arguments: Sequence[str] | None = None) -> int:
    """CLI entry point."""
    configure_logging("INFO")
    try:
        config = parse_config(arguments)
        configure_logging(config.generator_log_level)
        shutdown = ShutdownController()

        def handle_signal(signum: int, frame: FrameType | None) -> None:
            del frame
            get_logger().info("shutdown_requested", signal=signum)
            shutdown.request()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
        ready_path = Path("/tmp/event-generator-ready")
        ready_path.touch()
        try:
            return run_generation(
                config,
                build_journey_builder(config),
                KafkaEventProducer(config),
                shutdown,
            )
        finally:
            ready_path.unlink(missing_ok=True)
    except Exception:
        get_logger().exception("generator_failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
