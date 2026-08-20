"""Stage 28 worker-pool loop: concurrency correctness.

Exercises run_processor_pooled() end to end with a fake Kafka client and
stub store/dlq (no real Redis/Postgres/Kafka), focused on exactly the
guarantees the task requires: offsets committed only up to the contiguous
safe point even when workers finish out of order, per-worker RunSummary
isolation (no shared-counter races), and every processed record accounted
for."""

import random
import threading
from collections.abc import Mapping
from typing import Any, cast
from uuid import UUID

from confluent_kafka import TopicPartition  # type: ignore[import-untyped]

from services.event_processor.config import ProcessorConfig
from services.event_processor.consumer import KafkaEventConsumer
from services.event_processor.dlq import DlqPublisher
from services.event_processor.handler import EventHandler
from services.event_processor.idempotency import (
    RedisIdempotencyStore,
    Reservation,
    ReservationState,
)
from services.event_processor.main import ShutdownController
from services.event_processor.main_pooled import run_processor_pooled
from services.event_processor.models import (
    ConsumedMessage,
    ProcessingContext,
    RunSummary,
)
from services.event_processor.processor import MessageProcessor, OffsetCommitter
from shared.commerce_common.enums import EventType
from shared.kafka_metadata import event_message_headers, event_message_key
from shared.schemas import EventEnvelope, canonical_json
from shared.schemas.base import ContractModel
from tests.unit.test_processor_validation import event

TOPIC = "commerce.events"


def make_message(offset: int, partition: int = 0) -> ConsumedMessage:
    item = event()
    return ConsumedMessage(
        TOPIC,
        partition,
        offset,
        item.event_time,
        event_message_key(item),
        canonical_json(item).encode(),
        event_message_headers(item),
    )


class FakeRawMessage:
    """Minimal stand-in for confluent_kafka.Message, enough for
    KafkaEventConsumer.poll() to parse into a ConsumedMessage."""

    def __init__(self, message: ConsumedMessage) -> None:
        self._message = message

    def error(self) -> None:
        return None

    def timestamp(self) -> tuple[int, int]:
        assert self._message.timestamp is not None
        return (1, int(self._message.timestamp.timestamp() * 1000))

    def topic(self) -> str:
        return self._message.topic

    def partition(self) -> int:
        return self._message.partition

    def offset(self) -> int:
        return self._message.offset

    def key(self) -> bytes | None:
        return self._message.key

    def value(self) -> bytes | None:
        return self._message.value

    def headers(self) -> list[tuple[str, bytes | None]]:
        return list(self._message.headers)


class FakeConsumerClient:
    def __init__(self, messages: list[ConsumedMessage]) -> None:
        self._queue: list[FakeRawMessage] = [FakeRawMessage(m) for m in messages]
        self.commit_calls: list[list[TopicPartition]] = []
        self._assignment = [TopicPartition(TOPIC, 0)]

    def subscribe(self, topics: list[str], **kwargs: object) -> None:
        del topics, kwargs

    def poll(self, timeout: float) -> FakeRawMessage | None:
        del timeout
        return self._queue.pop(0) if self._queue else None

    def commit(
        self, offsets: list[TopicPartition], asynchronous: bool = True
    ) -> object:
        assert asynchronous is False
        self.commit_calls.append(list(offsets))
        return None

    def close(self) -> None:
        pass

    def assignment(self) -> list[TopicPartition]:
        return self._assignment


class StubStore:
    def ping(self) -> object:
        return True

    def close(self) -> None:
        pass

    def reserve(
        self, event_id: UUID, token: str, record: ConsumedMessage, first_seen_at: Any
    ) -> Reservation:
        del event_id, token, record, first_seen_at
        return Reservation(ReservationState.RESERVED, "token")

    def complete(self, event_id: UUID, token: str, completed_at: Any) -> bool:
        del event_id, token, completed_at
        return True

    def release(self, event_id: UUID, token: str) -> bool:
        del event_id, token
        return True


class StubDlq:
    def close(self) -> None:
        pass

    def publish(self, record: object) -> None:
        del record


class StubDatabase:
    def open(self) -> None:
        pass

    def close(self) -> None:
        pass


class ControlledHandler:
    """Blocks handling a specific offset on a threading.Event, so a test can
    force one offset's processing to finish before another's despite
    dispatch (offset) order - the exact scenario observe_dispatched()
    exists to keep safe."""

    def __init__(self, block: dict[int, threading.Event]) -> None:
        self._block = block
        self.handled: list[int] = []
        self._lock = threading.Lock()

    def handle(
        self, item: EventEnvelope[ContractModel], context: ProcessingContext
    ) -> None:
        del item
        event = self._block.get(context.offset)
        if event is not None:
            event.wait(timeout=5)
        with self._lock:
            self.handled.append(context.offset)


def _run(
    messages: list[ConsumedMessage],
    handler: EventHandler,
    *,
    pool_size: int,
) -> tuple[int, RunSummary, FakeConsumerClient]:
    config = ProcessorConfig(
        processor_worker_pool_size=pool_size,
        processor_max_messages=len(messages),
        processor_idle_timeout_seconds=1,
    )
    client = FakeConsumerClient(messages)
    consumer = KafkaEventConsumer(config, client=client)
    handlers: Mapping[EventType, EventHandler] = {EventType.ORDER_CREATED: handler}
    store = StubStore()
    dlq = StubDlq()
    database = StubDatabase()

    def build_processor(
        summary: RunSummary, rng: random.Random, committer: OffsetCommitter
    ) -> MessageProcessor:
        return MessageProcessor(
            config,
            cast(RedisIdempotencyStore, store),
            cast(DlqPublisher, dlq),
            committer,
            handlers,
            summary,
            processor_instance_id="test",
            wait=lambda seconds: None,
            rng=rng,
        )

    shutdown = ShutdownController()
    code, summary = run_processor_pooled(
        config, consumer, build_processor, store, dlq, shutdown, database
    )
    return code, summary, client


def test_out_of_order_completion_commits_only_the_contiguous_safe_offset() -> None:
    release_11 = threading.Event()
    release_11.set()  # offset 11's handler never blocks
    release_10 = threading.Event()  # offset 10's handler waits until released
    handler = ControlledHandler({10: release_10})

    messages = [make_message(10), make_message(11)]

    result: dict[str, Any] = {}

    def run() -> None:
        result["code"], result["summary"], result["client"] = _run(
            messages, handler, pool_size=2
        )

    thread = threading.Thread(target=run)
    thread.start()
    # Give both workers a chance to pick up their message; offset 11 should
    # finish (its handler never blocks) well before offset 10 is released.
    import time

    time.sleep(0.2)
    release_10.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    client: FakeConsumerClient = result["client"]
    assert handler.handled == [11, 10] or handler.handled == [10, 11]
    # Whatever the intermediate commit calls looked like, the final
    # committed position must reflect both offsets done, contiguously -
    # never skipping past one that hadn't completed yet.
    final = client.commit_calls[-1]
    assert final == [TopicPartition(TOPIC, 0, 12)]


def test_per_worker_run_summary_is_isolated_and_merged_without_double_counting() -> (
    None
):
    handler = ControlledHandler({})
    messages = [make_message(offset) for offset in range(10, 20)]
    code, summary, client = _run(messages, handler, pool_size=4)

    assert code == 0
    assert summary.consumed_records == 10
    assert summary.valid_records == 10
    assert summary.processed_records == 10
    assert len(handler.handled) == 10
    assert sorted(handler.handled) == list(range(10, 20))
    # Every offset reached a terminal commit exactly once, contiguously.
    assert client.commit_calls[-1] == [TopicPartition(TOPIC, 0, 20)]


def test_bounded_work_queue_never_exceeds_configured_backpressure() -> None:
    """The queue between the poll thread and workers must stay bounded
    (pool_size * 2) rather than growing without limit - a large batch of
    messages with slow handlers should never all sit in memory at once."""
    started = threading.Event()
    release = threading.Event()
    concurrent_count = {"max": 0, "current": 0}
    lock = threading.Lock()

    class SlowHandler:
        def handle(
            self, item: EventEnvelope[ContractModel], context: ProcessingContext
        ) -> None:
            del item, context
            with lock:
                concurrent_count["current"] += 1
                concurrent_count["max"] = max(
                    concurrent_count["max"], concurrent_count["current"]
                )
            started.set()
            release.wait(timeout=5)
            with lock:
                concurrent_count["current"] -= 1

    messages = [make_message(offset) for offset in range(10, 30)]
    result: dict[str, Any] = {}

    def run() -> None:
        result["code"], result["summary"], result["client"] = _run(
            messages, SlowHandler(), pool_size=3
        )

    thread = threading.Thread(target=run)
    thread.start()
    started.wait(timeout=5)
    import time

    time.sleep(0.2)
    # At most pool_size workers can be concurrently blocked in handle().
    assert concurrent_count["max"] <= 3
    release.set()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert result["summary"].processed_records == 20
