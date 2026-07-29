"""Bounded end-to-end generator smoke test executed inside the service image."""

from collections import defaultdict
from datetime import datetime
from time import monotonic
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition  # type: ignore[import-untyped]

from services.event_generator.config import GeneratorConfig
from services.event_generator.generator import SeededUuidFactory, SyntheticGenerator
from services.event_generator.journey import JourneyBuilder, SystemClock
from services.event_generator.logging import configure_logging
from services.event_generator.producer import KafkaEventProducer
from shared.commerce_common.enums import EventType
from shared.schemas import parse_event
from shared.schemas.base import ContractModel

REQUIRED_HEADERS = {
    "event_id",
    "event_type",
    "event_version",
    "correlation_id",
    "source",
    "content_type",
}


def customer_id(payload: ContractModel) -> str:
    """Read the required journey customer identifier."""
    value = getattr(payload, "customer_id", None)
    if value is None:
        raise AssertionError("generated payload omitted customer_id")
    return str(value)


def main() -> int:
    """Generate, isolate, consume, and validate two complete journeys."""
    config = GeneratorConfig.from_environment().model_copy(
        update={
            "generator_add_to_cart_probability": 1.0,
            "generator_checkout_probability": 1.0,
            "generator_payment_success_probability": 1.0,
            "generator_refund_probability": 0.0,
            "generator_seed": 4242,
            "generator_journeys": 2,
        }
    )
    configure_logging(config.generator_log_level)
    consumer = Consumer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "group.id": f"generator-smoke-{uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )
    metadata = consumer.list_topics(config.kafka_events_topic, timeout=5)
    topic_metadata = metadata.topics.get(config.kafka_events_topic)
    if topic_metadata is None or topic_metadata.error is not None:
        raise RuntimeError("Kafka topic is unavailable")

    starts: list[TopicPartition] = []
    for partition in sorted(topic_metadata.partitions):
        topic_partition = TopicPartition(config.kafka_events_topic, partition)
        _, high = consumer.get_watermark_offsets(topic_partition, timeout=5)
        starts.append(TopicPartition(config.kafka_events_topic, partition, high))

    import random

    builder = JourneyBuilder(
        config,
        SyntheticGenerator(random.Random(4242), SeededUuidFactory(4242)),
        SystemClock(),
    )
    journeys = [builder.build() for _ in range(2)]
    producer = KafkaEventProducer(config)
    for journey in journeys:
        for event in journey.events:
            producer.publish(event)
    producer.flush()

    expected_count = sum(len(journey.events) for journey in journeys)
    expected_correlations = {str(journey.correlation_id) for journey in journeys}
    consumer.assign(starts)
    received: dict[str, list[tuple[datetime, EventType, str]]] = defaultdict(list)
    deadline = monotonic() + 20
    count = 0
    while count < expected_count and monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is None:
            continue
        if message.error() is not None:
            raise RuntimeError(f"Kafka consume failed: {message.error()}")
        event = parse_event(message.value())
        correlation_id = str(event.correlation_id)
        if correlation_id not in expected_correlations:
            continue
        if message.key() is None:
            raise AssertionError("generated Kafka message has no key")
        if message.key().decode() != customer_id(event.payload):
            raise AssertionError("Kafka key does not match customer_id")
        headers = {name for name, _ in message.headers() or []}
        if not REQUIRED_HEADERS.issubset(headers):
            raise AssertionError("generated Kafka message is missing headers")
        received[correlation_id].append(
            (event.event_time, event.event_type, customer_id(event.payload))
        )
        count += 1
    consumer.close()

    if count != expected_count:
        raise AssertionError(
            f"expected {expected_count} isolated messages, consumed {count}"
        )
    for correlation_id in expected_correlations:
        events = received[correlation_id]
        if len(events) < 7:
            raise AssertionError("journey emitted fewer than seven full-path events")
        if [event_type for _, event_type, _ in events[:2]] != [
            EventType.USER_REGISTERED,
            EventType.SESSION_STARTED,
        ]:
            raise AssertionError("journey does not begin with registration and session")
        timestamps = [timestamp for timestamp, _, _ in events]
        if timestamps != sorted(timestamps):
            raise AssertionError("journey timestamps are not non-decreasing")
        if len({customer for _, _, customer in events}) != 1:
            raise AssertionError("journey customer IDs are inconsistent")

    print(
        f"Generator smoke test passed: {len(journeys)} journeys, "
        f"{expected_count} valid isolated messages."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
