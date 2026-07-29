"""Publish and validate representative controlled anomaly records."""

import random
from time import monotonic
from uuid import uuid4

from confluent_kafka import (  # type: ignore[import-untyped]
    Consumer,
    Message,
    TopicPartition,
)

from services.event_generator.anomalies import AnomalyInjector
from services.event_generator.config import GeneratorConfig
from services.event_generator.generator import SeededUuidFactory, SyntheticGenerator
from services.event_generator.journey import JourneyBuilder, SystemClock
from services.event_generator.messages import AnomalyType
from services.event_generator.producer import KafkaEventProducer
from shared.commerce_common.enums import CustomerPersona
from shared.schemas import parse_event


def main() -> int:
    """Isolate, publish, consume, and distinguish every anomaly type."""
    config = GeneratorConfig.from_environment().model_copy(
        update={
            "generator_persona": CustomerPersona.NORMAL,
            "generator_seed": 5150,
            "generator_add_to_cart_probability": 1.0,
            "generator_checkout_probability": 1.0,
            "generator_payment_success_probability": 1.0,
            "generator_anomalies_enabled": True,
            "generator_duplicate_event_probability": 1.0,
            "generator_malformed_json_probability": 1.0,
            "generator_missing_field_probability": 1.0,
            "generator_unknown_event_type_probability": 1.0,
            "generator_late_event_probability": 1.0,
            "generator_out_of_order_probability": 1.0,
            "generator_payload_mismatch_probability": 1.0,
            "generator_max_anomalies_per_journey": 7,
        }
    )
    consumer = Consumer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "group.id": f"anomaly-smoke-{uuid4()}",
            "enable.auto.commit": False,
        }
    )
    metadata = consumer.list_topics(config.kafka_events_topic, timeout=5)
    topic = metadata.topics.get(config.kafka_events_topic)
    if topic is None or topic.error is not None:
        raise RuntimeError("Kafka topic is unavailable")
    starts = []
    for partition in sorted(topic.partitions):
        tp = TopicPartition(config.kafka_events_topic, partition)
        _, high = consumer.get_watermark_offsets(tp, timeout=5)
        starts.append(TopicPartition(config.kafka_events_topic, partition, high))

    synthetic = SyntheticGenerator(random.Random(5150), SeededUuidFactory(5150))
    journey = JourneyBuilder(config, synthetic, SystemClock()).build()
    messages = AnomalyInjector(config, random.Random(5150)).prepare(journey.events)
    producer = KafkaEventProducer(config)
    for message in messages:
        producer.publish_message(message)
    producer.flush()

    consumer.assign(starts)
    received: list[Message] = []
    deadline = monotonic() + 25
    while len(received) < len(messages) and monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is None:
            continue
        if message.error() is not None:
            raise RuntimeError(f"Kafka consume failed: {message.error()}")
        if message.key() == messages[0].key:
            received.append(message)
    consumer.close()
    if len(received) != len(messages):
        raise AssertionError(
            f"expected {len(messages)} isolated records, got {len(received)}"
        )

    found: set[AnomalyType] = set()
    duplicate_values: list[bytes] = []
    valid_count = 0
    for message in received:
        headers = dict(message.headers() or [])
        raw_kind = headers.get("synthetic_anomaly")
        if raw_kind is None:
            parse_event(message.value())
            valid_count += 1
            continue
        kind = AnomalyType(raw_kind.decode())
        found.add(kind)
        if kind is AnomalyType.DUPLICATE:
            duplicate_values.append(message.value())
        elif kind is AnomalyType.LATE_EVENT:
            parse_event(message.value())
        elif kind is not AnomalyType.OUT_OF_ORDER:
            try:
                parse_event(message.value())
            except ValueError:
                pass
            else:
                raise AssertionError(f"{kind.value} unexpectedly parsed")

    if found != set(AnomalyType):
        raise AssertionError(f"missing anomaly types: {set(AnomalyType) - found}")
    if not duplicate_values or duplicate_values[0] not in [
        message.value() for message in received
    ]:
        raise AssertionError("duplicate bytes were not preserved")
    if valid_count == 0:
        raise AssertionError("normal valid records did not remain parseable")
    print(
        "Anomaly smoke test passed: "
        f"{len(received)} isolated records, {len(found)} anomaly types."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
