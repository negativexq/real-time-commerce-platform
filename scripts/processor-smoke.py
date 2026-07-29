"""Bounded container-backed processor smoke scenarios."""

import json
import random
import sys
from datetime import UTC, datetime
from time import monotonic
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition  # type: ignore[import-untyped]
from redis import Redis

from services.event_generator.config import GeneratorConfig
from services.event_generator.generator import SeededUuidFactory, SyntheticGenerator
from services.event_generator.journey import JourneyBuilder
from services.event_generator.producer import KafkaEventProducer
from services.event_processor.config import ProcessorConfig
from services.event_processor.consumer import KafkaEventConsumer
from services.event_processor.dlq import DlqPublisher
from services.event_processor.errors import RetryableProcessingError
from services.event_processor.handler import AuditEventHandler, EventHandler
from services.event_processor.idempotency import RedisIdempotencyStore
from services.event_processor.models import ProcessingOutcome, RunSummary
from services.event_processor.processor import MessageProcessor
from shared.commerce_common.enums import CustomerPersona, EventType
from shared.schemas import EventEnvelope
from shared.schemas.base import ContractModel


class SmokeClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class FailThenSucceed:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0

    def handle(self, event: EventEnvelope[ContractModel], context: object) -> None:
        del event, context
        self.calls += 1
        if self.calls <= self.failures:
            raise RetryableProcessingError("controlled smoke retry")


class AlwaysFail:
    def handle(self, event: EventEnvelope[ContractModel], context: object) -> None:
        del event, context
        raise RetryableProcessingError("controlled exhausted smoke retry")


def high_offsets(consumer: Consumer, topic: str) -> int:
    metadata = consumer.list_topics(topic, timeout=5).topics[topic]
    return sum(
        consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=5)[1]
        for partition in metadata.partitions
    )


def main() -> int:
    scenario = sys.argv[1] if len(sys.argv) > 1 else "normal"
    if scenario not in {"normal", "duplicate", "dlq", "retry"}:
        raise ValueError(f"unknown scenario: {scenario}")
    identity = uuid4().hex
    prefix = f"commerce:processor:test:{identity}"
    group = f"processor-{scenario}-smoke-{identity}"
    config = ProcessorConfig.from_environment().model_copy(
        update={
            "processor_consumer_group": group,
            "processor_client_id": f"processor-smoke-{identity}",
            "processor_auto_offset_reset": "latest",
            "processor_idempotency_key_prefix": prefix,
            "processor_max_processing_attempts": 3,
            "processor_retry_initial_backoff_ms": 0,
            "processor_retry_max_backoff_ms": 0,
        }
    )
    raw_consumer = Consumer(KafkaEventConsumer.kafka_config(config))
    consumer = KafkaEventConsumer(config, raw_consumer)
    consumer.subscribe()
    assignment_deadline = monotonic() + 10
    while not raw_consumer.assignment() and monotonic() < assignment_deadline:
        consumer.poll()
    if not raw_consumer.assignment():
        raise RuntimeError("consumer assignment timed out")

    dlq_before = high_offsets(raw_consumer, config.processor_dlq_topic)
    generator_config = GeneratorConfig.from_environment().model_copy(
        update={
            "generator_seed": 6200,
            "generator_persona": CustomerPersona.NORMAL,
            "generator_add_to_cart_probability": 1,
            "generator_checkout_probability": 1,
            "generator_payment_success_probability": 1,
            "generator_refund_probability": 0,
        }
    )
    builder = JourneyBuilder(
        generator_config,
        SyntheticGenerator(random.Random(6200), SeededUuidFactory(6200)),
        SmokeClock(),
    )
    journey = builder.build()
    event = journey.events[0]
    exhausted_event = journey.events[1]
    producer = KafkaEventProducer(generator_config)
    if scenario == "dlq":
        from services.event_generator.anomalies import valid_message
        from services.event_generator.messages import PublishableMessage

        valid = valid_message(event)
        decoded = json.loads(valid.value)
        variants: list[tuple[str, bytes, bytes, list[tuple[str, bytes]]]] = []
        variants.append(("malformed_json", b"{", valid.key, valid.headers))
        missing = dict(decoded)
        missing.pop("event_id")
        variants.append(
            ("missing_field", json.dumps(missing).encode(), valid.key, valid.headers)
        )
        unknown = dict(decoded)
        unknown["event_type"] = "synthetic_unknown_event"
        variants.append(
            (
                "unknown_event_type",
                json.dumps(unknown).encode(),
                valid.key,
                valid.headers,
            )
        )
        mismatch = dict(decoded)
        mismatch["payload"] = {}
        variants.append(
            (
                "payload_mismatch",
                json.dumps(mismatch).encode(),
                valid.key,
                valid.headers,
            )
        )
        bad_version = dict(decoded)
        bad_version["event_version"] = 2
        variants.append(
            (
                "unsupported_event_version",
                json.dumps(bad_version).encode(),
                valid.key,
                valid.headers,
            )
        )
        bad_headers = [
            (name, b"different" if name == "source" else value)
            for name, value in valid.headers
        ]
        variants.append(("header_body_mismatch", valid.value, valid.key, bad_headers))
        variants.append(("key_body_mismatch", valid.value, b"wrong", valid.headers))
        for anomaly, value, key, headers in variants:
            producer.publish_message(
                PublishableMessage(
                    value,
                    key,
                    [*headers, ("synthetic_anomaly", anomaly.encode())],
                    valid.event_id,
                    valid.event_type,
                    valid.correlation_id,
                )
            )
    else:
        producer.publish(event)
        if scenario == "duplicate":
            producer.publish(event)
        elif scenario == "retry":
            producer.publish(exhausted_event)
    producer.flush()

    store = RedisIdempotencyStore(config)
    dlq = DlqPublisher(config)
    summary = RunSummary()
    retry_handler = FailThenSucceed(2)
    audit = AuditEventHandler()
    handlers: dict[EventType, EventHandler] = {kind: audit for kind in EventType}
    if scenario == "retry":
        handlers[event.event_type] = retry_handler
        handlers[exhausted_event.event_type] = AlwaysFail()
    processor = MessageProcessor(
        config,
        store,
        dlq,
        consumer,
        handlers,
        summary,
        processor_instance_id=identity,
        wait=lambda seconds: None,
    )
    expected = {"duplicate": 2, "dlq": 7, "retry": 2}.get(scenario, 1)
    outcomes: list[ProcessingOutcome] = []
    deadline = monotonic() + 20
    while len(outcomes) < expected and monotonic() < deadline:
        record = consumer.poll()
        if record is not None:
            outcomes.append(processor.process(record))
    if len(outcomes) != expected or not all(item.terminal for item in outcomes):
        raise AssertionError("processor did not terminally handle expected records")

    redis = Redis.from_url(config.redis_url, decode_responses=True)
    keys = list(redis.scan_iter(match=f"{prefix}:*", count=100))
    if scenario != "dlq" and (
        len(keys) != 1
        or any('"status":"completed"' not in str(redis.get(key)) for key in keys)
    ):
        raise AssertionError("event idempotency state did not reach completed")
    if scenario == "duplicate" and summary.duplicate_records != 1:
        raise AssertionError("duplicate handler suppression was not observed")
    if scenario == "retry" and (
        retry_handler.calls != 3
        or summary.retries != 4
        or summary.retry_exhausted != 1
        or summary.dlq_records != 1
    ):
        raise AssertionError("retry success and exhaustion were not both observed")
    dlq_after = high_offsets(raw_consumer, config.processor_dlq_topic)
    if scenario == "dlq" and (
        dlq_after - dlq_before != 7 or len(summary.validation_failures) != 6
    ):
        raise AssertionError("representative poison records did not all reach DLQ")
    if scenario not in {"dlq", "retry"} and dlq_after != dlq_before:
        raise AssertionError("valid smoke record unexpectedly reached DLQ")
    if scenario == "retry" and dlq_after - dlq_before != 1:
        raise AssertionError("exhausted retry did not produce exactly one DLQ record")

    if keys:
        redis.delete(*keys)
    redis.close()
    dlq.close()
    store.close()
    consumer.close()
    print(f"Processor {scenario} smoke passed: {summary.as_log()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
