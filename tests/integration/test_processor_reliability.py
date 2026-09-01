"""Real Kafka/Redis/PostgreSQL checks for processor reliability boundaries."""

import json
import random
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from time import monotonic
from typing import Any, cast
from uuid import UUID, uuid4

import psycopg
import pytest
from confluent_kafka import (  # type: ignore[import-untyped]
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[import-untyped]
from redis import Redis
from redis.exceptions import RedisError

from scripts.benchmark.config import load_config
from services.event_processor.config import ProcessorConfig
from services.event_processor.consumer import KafkaEventConsumer
from services.event_processor.dlq import DlqPublisher
from services.event_processor.idempotency import RedisIdempotencyStore
from services.event_processor.models import (
    ProcessingOutcome,
    ProcessingStatus,
    RunSummary,
)
from services.event_processor.persistence.database import Database
from services.event_processor.persistence.handlers import default_persistence_registry
from services.event_processor.persistence.unit_of_work import UnitOfWorkFactory
from services.event_processor.processor import MessageProcessor
from shared.commerce_common.enums import Currency, CustomerPersona, EventType
from shared.kafka_metadata import event_message_headers, event_message_key
from shared.schemas import EventEnvelope, OrderCreatedPayload, UserRegisteredPayload

pytestmark = pytest.mark.integration

POLL_DEADLINE_SECONDS = 15.0


@dataclass(frozen=True, slots=True)
class Stack:
    kafka_bootstrap_servers: str
    redis_url: str
    postgres_dsn: str
    admin: AdminClient


@dataclass(frozen=True, slots=True)
class Topics:
    input: str
    dlq: str


@pytest.fixture
def stack() -> Iterator[Stack]:
    """Use the compose stack when available; preserve local no-Docker skips."""
    benchmark = load_config("processor-reliability-integration")
    redis_client = Redis.from_url(
        benchmark.redis_url,
        socket_connect_timeout=2,
        socket_timeout=2,
    )
    admin = AdminClient(
        {
            "bootstrap.servers": benchmark.kafka_bootstrap_servers,
            "socket.timeout.ms": 2_000,
        }
    )
    try:
        with (
            psycopg.connect(benchmark.postgres_dsn, connect_timeout=2) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute("SELECT 1")
            assert cursor.fetchone() == (1,)
        redis_client.ping()
        admin.list_topics(timeout=2)
    except (psycopg.Error, RedisError, KafkaException) as exc:
        redis_client.close()
        pytest.skip(f"integration dependencies are unavailable: {exc}")

    try:
        yield Stack(
            benchmark.kafka_bootstrap_servers,
            benchmark.redis_url,
            benchmark.postgres_dsn,
            admin,
        )
    finally:
        redis_client.close()


@pytest.fixture
def topics(stack: Stack) -> Iterator[Topics]:
    """Create and remove both topics for each test, including on failure."""
    prefix = f"processor-reliability-{uuid4().hex}"
    topics = Topics(f"{prefix}.input", f"{prefix}.dlq")
    futures = stack.admin.create_topics(
        [
            NewTopic(topics.input, num_partitions=1, replication_factor=1),
            NewTopic(topics.dlq, num_partitions=1, replication_factor=1),
        ],
        request_timeout=10,
    )
    for future in futures.values():
        future.result(10)

    try:
        yield topics
    finally:
        futures = stack.admin.delete_topics(
            [topics.input, topics.dlq], operation_timeout=10
        )
        for future in futures.values():
            future.result(10)


def _processor_config(stack: Stack, topics: Topics, group_id: str) -> ProcessorConfig:
    return ProcessorConfig(
        kafka_bootstrap_servers=stack.kafka_bootstrap_servers,
        processor_input_topic=topics.input,
        processor_dlq_topic=topics.dlq,
        processor_consumer_group=group_id,
        processor_client_id=f"{group_id}-client",
        processor_auto_offset_reset="earliest",
        processor_poll_timeout_seconds=0.2,
        processor_max_processing_attempts=3,
        processor_retry_initial_backoff_ms=0,
        processor_retry_max_backoff_ms=0,
        processor_retry_jitter_ratio=0,
        processor_idempotency_key_prefix=f"commerce:processor:integration:{group_id}",
        redis_url=stack.redis_url,
        postgres_dsn=stack.postgres_dsn,
        processor_db_pool_min_size=1,
        processor_db_pool_max_size=2,
        processor_offset_commit_batch_size=1,
    )


@contextmanager
def _running_processor(
    config: ProcessorConfig,
) -> Iterator[tuple[KafkaEventConsumer, MessageProcessor]]:
    consumer = KafkaEventConsumer(config)
    idempotency = RedisIdempotencyStore(config)
    dlq = DlqPublisher(config)
    database = Database(config)
    try:
        database.open()
        consumer.subscribe()
        processor = MessageProcessor(
            config,
            idempotency,
            dlq,
            consumer,
            default_persistence_registry(),
            RunSummary(),
            processor_instance_id=f"integration-{uuid4().hex}",
            persistence=UnitOfWorkFactory(database, config),
            wait=lambda _seconds: None,
            rng=random.Random(0),
        )
        yield consumer, processor
    finally:
        try:
            consumer.flush_pending()
        finally:
            consumer.close()
            dlq.close()
            idempotency.close()
            database.close()


def _publish_event(config: ProcessorConfig, event: EventEnvelope[Any]) -> None:
    producer = Producer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "client.id": f"{config.processor_client_id}-test-producer",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    producer.produce(
        config.processor_input_topic,
        key=event_message_key(event),
        value=event.model_dump_json().encode(),
        headers=event_message_headers(event),
    )
    assert producer.flush(10) == 0


def _process_until(
    consumer: KafkaEventConsumer,
    processor: MessageProcessor,
    expected_count: int,
) -> list[ProcessingOutcome]:
    deadline = monotonic() + POLL_DEADLINE_SECONDS
    outcomes: list[ProcessingOutcome] = []
    while len(outcomes) < expected_count and monotonic() < deadline:
        message = consumer.poll()
        if message is None:
            consumer.maybe_flush_idle()
            continue
        outcomes.append(processor.process(message))
    consumer.flush_pending()
    assert len(outcomes) == expected_count, (
        f"processed {len(outcomes)} of {expected_count} records before deadline"
    )
    return outcomes


def _committed_offset(config: ProcessorConfig) -> int:
    observer = Consumer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "group.id": config.processor_consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        committed = observer.committed(
            [TopicPartition(config.processor_input_topic, 0)], 5
        )
        offset = committed[0].offset
        assert offset >= 0
        return int(offset)
    finally:
        observer.close()


def _consume_dlq(config: ProcessorConfig) -> dict[str, Any]:
    observer = Consumer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "group.id": f"{config.processor_consumer_group}-dlq-observer",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    observer.subscribe([config.processor_dlq_topic])
    deadline = monotonic() + POLL_DEADLINE_SECONDS
    try:
        while monotonic() < deadline:
            message = observer.poll(0.2)
            if message is None:
                continue
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(error)
            assert message.value() is not None
            return cast(dict[str, Any], json.loads(message.value()))
    finally:
        observer.close()
    pytest.fail("no DLQ record arrived before deadline")


def _delete_idempotency_state(
    stack: Stack, config: ProcessorConfig, event_id: UUID
) -> None:
    redis_client = Redis.from_url(stack.redis_url)
    try:
        assert redis_client.delete(
            f"{config.processor_idempotency_key_prefix}:{event_id}"
        ) in (0, 1)
    finally:
        redis_client.close()


def _delete_rows(
    stack: Stack,
    event_id: UUID,
    *,
    customer_id: UUID | None = None,
    order_id: UUID | None = None,
) -> None:
    with (
        psycopg.connect(stack.postgres_dsn, autocommit=True) as connection,
        connection.cursor() as cursor,
    ):
        if order_id is not None:
            cursor.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))
        if customer_id is not None:
            cursor.execute(
                "DELETE FROM customers WHERE customer_id = %s", (customer_id,)
            )
        cursor.execute("DELETE FROM processed_events WHERE event_id = %s", (event_id,))


def _user_registered_event(
    event_id: UUID, customer_id: UUID
) -> EventEnvelope[UserRegisteredPayload]:
    now = datetime.now(UTC)
    return EventEnvelope(
        event_id=event_id,
        event_type=EventType.USER_REGISTERED,
        event_version=1,
        event_time=now,
        produced_at=now,
        source="integration:processor-reliability",
        correlation_id=uuid4(),
        payload=UserRegisteredPayload(
            customer_id=customer_id,
            email_hash=f"integration-{uuid4().hex}",
            country_code="TR",
            persona=CustomerPersona.NORMAL,
            registered_at=now,
        ),
    )


def _missing_cart_order_event(event_id: UUID) -> EventEnvelope[OrderCreatedPayload]:
    now = datetime.now(UTC)
    return EventEnvelope(
        event_id=event_id,
        event_type=EventType.ORDER_CREATED,
        event_version=1,
        event_time=now,
        produced_at=now,
        source="integration:processor-reliability",
        correlation_id=uuid4(),
        payload=OrderCreatedPayload(
            order_id=uuid4(),
            customer_id=uuid4(),
            session_id=uuid4(),
            cart_id=uuid4(),
            item_count=1,
            subtotal=Decimal("10.00"),
            discount_amount=Decimal("0.00"),
            total_amount=Decimal("10.00"),
            currency=Currency.TRY,
            shipping_country_code="TR",
            billing_country_code="TR",
            created_at=now,
        ),
    )


def test_duplicate_event_persists_business_effect_once(
    stack: Stack, topics: Topics
) -> None:
    event_id = uuid4()
    customer_id = uuid4()
    config = _processor_config(stack, topics, f"processor-idempotency-{uuid4().hex}")
    event = _user_registered_event(event_id, customer_id)
    try:
        _publish_event(config, event)
        _publish_event(config, event)
        with _running_processor(config) as (consumer, processor):
            outcomes = _process_until(consumer, processor, expected_count=2)

        assert [outcome.status for outcome in outcomes] == [
            ProcessingStatus.PROCESSED,
            ProcessingStatus.DUPLICATE,
        ]
        assert _committed_offset(config) >= 2
        with (
            psycopg.connect(stack.postgres_dsn) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = %s",
                (event_id,),
            )
            assert cursor.fetchone() == (1,)
            cursor.execute(
                "SELECT COUNT(*) FROM customers WHERE customer_id = %s",
                (customer_id,),
            )
            assert cursor.fetchone() == (1,)
    finally:
        _delete_idempotency_state(stack, config, event_id)
        _delete_rows(stack, event_id, customer_id=customer_id)


def test_exhausted_processing_routes_to_dlq_before_source_commit(
    stack: Stack, topics: Topics
) -> None:
    event_id = uuid4()
    event = _missing_cart_order_event(event_id)
    config = _processor_config(stack, topics, f"processor-dlq-{uuid4().hex}")
    try:
        _publish_event(config, event)
        with _running_processor(config) as (consumer, processor):
            outcomes = _process_until(consumer, processor, expected_count=1)

        assert outcomes[0].status is ProcessingStatus.DLQ
        assert outcomes[0].attempts == config.processor_max_processing_attempts
        record = _consume_dlq(config)
        assert record["original_event_id"] == str(event_id)
        assert record["error_category"] == "missing_business_dependency"
        assert record["processing_attempts"] == config.processor_max_processing_attempts
        assert _committed_offset(config) >= 1
        with (
            psycopg.connect(stack.postgres_dsn) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = %s",
                (event_id,),
            )
            assert cursor.fetchone() == (0,)
            cursor.execute(
                "SELECT COUNT(*) FROM orders WHERE created_event_id = %s",
                (event_id,),
            )
            assert cursor.fetchone() == (0,)
    finally:
        _delete_idempotency_state(stack, config, event_id)
        _delete_rows(stack, event_id, order_id=event.payload.order_id)
