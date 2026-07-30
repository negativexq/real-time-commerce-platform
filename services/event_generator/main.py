"""Event-generator CLI and graceful application lifecycle."""

import argparse
import os
import random
import signal
import sys
from collections.abc import Sequence
from pathlib import Path
from threading import Event
from time import perf_counter
from types import FrameType

from services.event_generator.anomalies import AnomalyInjector
from services.event_generator.config import GeneratorConfig
from services.event_generator.generator import (
    SeededUuidFactory,
    SyntheticGenerator,
    UuidFactory,
)
from services.event_generator.journey import JourneyBuilder, SystemClock
from services.event_generator.logging import configure_logging, get_logger
from services.event_generator.producer import KafkaEventProducer
from services.event_generator.summary import RunSummary
from shared.commerce_common.enums import CustomerPersona
from shared.observability import ApplicationMetrics, MetricsConfig, MetricsServer


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
    parser.add_argument(
        "--persona", choices=[persona.value for persona in CustomerPersona]
    )
    parser.add_argument("--persona-mix")
    parser.add_argument(
        "--stateful", action=argparse.BooleanOptionalAction, default=None
    )
    parser.add_argument("--anomalies", action="store_true", default=None)
    parser.add_argument("--customers", type=int)
    parsed = parser.parse_args(arguments)
    overrides = {
        name: value
        for name, value in {
            "generator_journeys": parsed.journeys,
            "generator_rate_per_second": parsed.rate,
            "generator_seed": parsed.seed,
            "generator_log_level": parsed.log_level,
            "generator_persona": parsed.persona,
            "generator_persona_weights": parsed.persona_mix,
            "generator_stateful_mode": parsed.stateful,
            "generator_anomalies_enabled": parsed.anomalies,
            "generator_customer_pool_size": parsed.customers,
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
    metrics: ApplicationMetrics | None = None,
) -> int:
    """Run finite or continuous generation and perform a bounded final flush."""
    logger = get_logger()
    finite = config.generator_journeys is not None
    target = config.generator_journeys
    generated = 0
    interval = min(1.0 / config.generator_rate_per_second, 60.0)
    injector = AnomalyInjector(config, random.Random(config.generator_seed))
    summary = RunSummary()
    if metrics is not None:
        metrics.generator_rate.set(config.generator_rate_per_second)

    logger.info(
        "generator_started",
        kafka_bootstrap_servers=config.kafka_bootstrap_servers,
        topic=config.kafka_events_topic,
        mode="finite" if finite else "continuous",
        rate=config.generator_rate_per_second,
        seed_configured=config.generator_seed is not None,
        client_id=config.kafka_client_id,
        stateful=config.generator_stateful_mode,
        anomalies_enabled=config.generator_anomalies_enabled,
        persona=config.generator_persona,
    )

    while not shutdown.requested and (target is None or generated < target):
        started = perf_counter()
        journey = builder.build()
        messages = injector.prepare(journey.events)
        for position, message in enumerate(messages):
            producer.publish_message(message)
            if message.anomaly_type is not None:
                if metrics is not None:
                    metrics.generator_anomalies.labels(message.anomaly_type.value).inc()
                logger.warning(
                    "synthetic_anomaly_published",
                    anomaly_type=message.anomaly_type.value,
                    original_event_id=(
                        str(message.event_id) if message.event_id is not None else None
                    ),
                    event_type=message.event_type,
                    correlation_id=(
                        str(message.correlation_id)
                        if message.correlation_id is not None
                        else None
                    ),
                    topic=config.kafka_events_topic,
                    key=message.key.decode(),
                    sequence_position=position,
                )
        producer.poll(0)
        generated += 1
        summary.record_journey(
            journey,
            [
                message.anomaly_type
                for message in messages
                if message.anomaly_type is not None
            ],
        )
        if metrics is not None:
            persona = journey.persona.value
            metrics.generator_journeys.labels(persona, "generated").inc()
            metrics.generator_journey_duration.labels(persona).observe(
                perf_counter() - started
            )
            for event in journey.events:
                metrics.generator_events_generated.labels(
                    event.event_type.value, persona
                ).inc()
            metrics.generator_active_customers.set(
                sum(summary.customers_per_persona.values())
            )
            metrics.generator_healthy.set(1)
            metrics.success()
        logger.info(
            "journey_generated",
            correlation_id=str(journey.correlation_id),
            customer_id=str(journey.customer_id),
            event_count=len(journey.events),
            terminal_event_type=journey.terminal_event_type.value,
            generation_duration_ms=round((perf_counter() - started) * 1_000, 3),
            persona=journey.persona.value,
            returning_customer=journey.returning_customer,
            customer_lifetime_journeys=journey.customer_lifetime_journeys,
            logical_journey_duration_ms=journey.logical_journey_duration_ms,
            payment_attempt_count=journey.payment_attempt_count,
            anomaly_count=sum(message.anomaly_type is not None for message in messages),
        )
        if target is None or generated < target:
            shutdown.wait(interval)

    logger.info("generator_stopping", **summary.as_log())
    producer.flush()
    logger.info(
        "generator_stopped",
        **summary.as_log(),
        delivery_failures=producer.delivery_failures,
        undelivered_messages=0,
    )
    return 0


def main(arguments: Sequence[str] | None = None) -> int:
    """CLI entry point."""
    configure_logging("INFO")
    try:
        config = parse_config(arguments)
        configure_logging(config.generator_log_level)
        shutdown = ShutdownController()
        metrics_config = MetricsConfig.from_environment(
            "event-generator",
            9102,
            os.environ,
            port_environment_name="GENERATOR_METRICS_PORT",
        )
        metrics = ApplicationMetrics(
            metrics_config.service_name, metrics_config.namespace
        )
        ready_path = Path("/tmp/event-generator-ready")
        metrics_server = MetricsServer(
            metrics_config, metrics.registry, ready_path.exists
        )
        try:
            metrics_server.start()
        except OSError:
            get_logger().exception("metrics_server_failed")
            metrics.generator_healthy.set(0)

        def handle_signal(signum: int, frame: FrameType | None) -> None:
            del frame
            get_logger().info("shutdown_requested", signal=signum)
            shutdown.request()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
        ready_path.touch()
        try:
            code = run_generation(
                config,
                build_journey_builder(config),
                KafkaEventProducer(config, metrics=metrics),
                shutdown,
                metrics,
            )
            metrics_server.stop()
            return code
        finally:
            ready_path.unlink(missing_ok=True)
    except Exception:
        get_logger().exception("generator_failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
