"""Black-box reliability checks for the Go event processor.

Each test publishes to fresh topics, runs the real ``event-processor`` image
in finite mode against the compose stack's Kafka, Redis and PostgreSQL, then
asserts through the database, the DLQ topic, the committed offsets and the
processor's own ``processor_stopped`` run summary.
"""

import json
import os
import shutil
import subprocess
from collections.abc import Iterator
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


MAX_PROCESSING_ATTEMPTS = 3
PROCESSOR_RUN_TIMEOUT_SECONDS = 180


@dataclass(frozen=True, slots=True)
class RunConfig:
    """What one finite Go processor run reads and writes."""

    kafka_bootstrap_servers: str
    input_topic: str
    dlq_topic: str
    consumer_group: str
    idempotency_key_prefix: str


def _run_config(stack: Stack, topics: Topics, group_id: str) -> RunConfig:
    return RunConfig(
        stack.kafka_bootstrap_servers,
        topics.input,
        topics.dlq,
        group_id,
        f"commerce:processor:integration:{group_id}",
    )


def _run_go_processor(config: RunConfig, expected_count: int) -> dict[str, Any]:
    """Run the Go processor container until it has handled expected_count
    records and return its processor_stopped summary."""
    if shutil.which("docker") is None:
        pytest.skip("docker is required to run the Go processor")
    overrides = {
        "PROCESSOR_INPUT_TOPIC": config.input_topic,
        "PROCESSOR_DLQ_TOPIC": config.dlq_topic,
        "PROCESSOR_CONSUMER_GROUP": config.consumer_group,
        "PROCESSOR_CLIENT_ID": f"{config.consumer_group}-client",
        "PROCESSOR_IDEMPOTENCY_KEY_PREFIX": config.idempotency_key_prefix,
        "PROCESSOR_POLL_TIMEOUT_SECONDS": "0.2",
        "PROCESSOR_MAX_PROCESSING_ATTEMPTS": str(MAX_PROCESSING_ATTEMPTS),
        "PROCESSOR_RETRY_INITIAL_BACKOFF_MS": "0",
        "PROCESSOR_RETRY_MAX_BACKOFF_MS": "0",
        "PROCESSOR_RETRY_JITTER_RATIO": "0",
        "PROCESSOR_OFFSET_COMMIT_BATCH_SIZE": "1",
        "PROCESSOR_DB_POOL_MAX_SIZE": "2",
        "METRICS_ENABLED": "false",
    }
    command = [
        "docker",
        "compose",
        "--profile",
        "processor",
        "run",
        "--rm",
        "--no-deps",
        "-T",
    ]
    for name, value in overrides.items():
        command += ["-e", f"{name}={value}"]
    command += [
        os.environ.get("PROCESSOR_INTEGRATION_SERVICE", "event-processor"),
        "--max-messages",
        str(expected_count),
        "--idle-timeout",
        "15",
    ]
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=PROCESSOR_RUN_TIMEOUT_SECONDS,
        check=False,
    )
    for line in completed.stdout.splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and record.get("event") == "processor_stopped":
            return cast(dict[str, Any], record)
    pytest.fail(
        f"Go processor exited {completed.returncode} without a run summary\n"
        f"stdout:\n{completed.stdout[-4000:]}\nstderr:\n{completed.stderr[-4000:]}"
    )


def _publish_event(config: RunConfig, event: EventEnvelope[Any]) -> None:
    producer = Producer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "client.id": f"{config.consumer_group}-test-producer",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    producer.produce(
        config.input_topic,
        key=event_message_key(event),
        value=event.model_dump_json().encode(),
        headers=event_message_headers(event),
    )
    assert producer.flush(10) == 0


def _committed_offset(config: RunConfig) -> int:
    observer = Consumer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "group.id": config.consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        committed = observer.committed([TopicPartition(config.input_topic, 0)], 5)
        offset = committed[0].offset
        assert offset >= 0
        return int(offset)
    finally:
        observer.close()


def _consume_dlq(config: RunConfig) -> dict[str, Any]:
    observer = Consumer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "group.id": f"{config.consumer_group}-dlq-observer",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    observer.subscribe([config.dlq_topic])
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


def _delete_idempotency_state(stack: Stack, config: RunConfig, event_id: UUID) -> None:
    redis_client = Redis.from_url(stack.redis_url)
    try:
        assert redis_client.delete(f"{config.idempotency_key_prefix}:{event_id}") in (
            0,
            1,
        )
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
    config = _run_config(stack, topics, f"processor-idempotency-{uuid4().hex}")
    event = _user_registered_event(event_id, customer_id)
    try:
        _publish_event(config, event)
        _publish_event(config, event)
        summary = _run_go_processor(config, expected_count=2)

        assert summary["processed_records"] == 1
        assert summary["duplicate_records"] == 1
        assert summary["unresolved_records"] == 0
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
    config = _run_config(stack, topics, f"processor-dlq-{uuid4().hex}")
    try:
        _publish_event(config, event)
        summary = _run_go_processor(config, expected_count=1)

        assert summary["dlq_records"] == 1
        assert summary["retry_exhausted"] == 1
        assert summary["missing_dependency_errors"] == 1
        assert summary["unresolved_records"] == 0
        record = _consume_dlq(config)
        assert record["original_event_id"] == str(event_id)
        assert record["error_category"] == "missing_business_dependency"
        assert record["processing_attempts"] == MAX_PROCESSING_ATTEMPTS
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
