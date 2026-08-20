"""KafkaEventConsumer batched-commit integration: revoke, shutdown, and the
revoked-partition commit guard, exercised through the real consumer/tracker
wiring with a fake Kafka client."""

import pytest
from confluent_kafka import TopicPartition  # type: ignore[import-untyped]

from services.event_processor.config import ProcessorConfig
from services.event_processor.consumer import KafkaEventConsumer
from services.event_processor.errors import FatalInfrastructureError
from services.event_processor.models import ConsumedMessage

TOPIC = "commerce.events"


class FakeKafkaClient:
    def __init__(self) -> None:
        self.commit_calls: list[list[TopicPartition]] = []
        self._assignment: list[TopicPartition] = []

    def subscribe(self, topics: list[str], **kwargs: object) -> None:
        del topics, kwargs

    def poll(self, timeout: float) -> None:
        del timeout
        return None

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


def make_consumer(
    batch_size: int = 50, interval_ms: int = 60_000
) -> tuple[KafkaEventConsumer, FakeKafkaClient]:
    config = ProcessorConfig(
        processor_offset_commit_batch_size=batch_size,
        processor_offset_commit_interval_ms=interval_ms,
    )
    client = FakeKafkaClient()
    consumer = KafkaEventConsumer(config, client=client)
    return consumer, client


def message(offset: int, partition: int = 0) -> ConsumedMessage:
    return ConsumedMessage(TOPIC, partition, offset, None, b"key", b"value", [])


def test_poll_records_empty_poll_and_duration_metrics() -> None:
    from shared.observability.metrics import ApplicationMetrics

    metrics = ApplicationMetrics("test-processor")
    config = ProcessorConfig()
    client = FakeKafkaClient()
    consumer = KafkaEventConsumer(config, client=client, metrics=metrics)

    result = consumer.poll()

    assert result is None
    assert (
        metrics.registry.get_sample_value("commerce_processor_empty_polls_total", {})
        == 1
    )
    assert (
        metrics.registry.get_sample_value(
            "commerce_processor_poll_duration_seconds_count", {}
        )
        == 1
    )


def test_commit_terminal_batches_below_threshold() -> None:
    consumer, client = make_consumer(batch_size=5, interval_ms=60_000)
    consumer.commit_terminal(message(1))
    consumer.commit_terminal(message(2))
    assert client.commit_calls == []


def test_commit_terminal_flushes_at_batch_size() -> None:
    consumer, client = make_consumer(batch_size=2, interval_ms=60_000)
    consumer.commit_terminal(message(1))
    consumer.commit_terminal(message(2))
    assert len(client.commit_calls) == 1
    committed = client.commit_calls[0]
    assert len(committed) == 1
    assert (committed[0].topic, committed[0].partition, committed[0].offset) == (
        TOPIC,
        0,
        3,
    )


def test_revoke_flushes_only_revoked_partitions_and_guards_further_commits() -> None:
    consumer, client = make_consumer(batch_size=1_000, interval_ms=60_000)
    consumer.commit_terminal(message(10, partition=0))
    consumer.commit_terminal(message(20, partition=1))
    assert client.commit_calls == []

    consumer.on_revoke(client, [TopicPartition(TOPIC, 0, 0)])

    assert len(client.commit_calls) == 1
    committed = client.commit_calls[0]
    assert len(committed) == 1
    assert (committed[0].topic, committed[0].partition, committed[0].offset) == (
        TOPIC,
        0,
        11,
    )

    # Partition 1 was never revoked - it is still tracked and can still
    # accumulate/commit normally.
    consumer.commit_terminal(message(21, partition=1))
    consumer.flush_pending("shutdown")
    assert len(client.commit_calls) == 2
    second = client.commit_calls[1]
    assert (second[0].topic, second[0].partition, second[0].offset) == (TOPIC, 1, 22)

    # Partition 0 was revoked - committing for it must be refused, never
    # silently accepted or routed to another worker's ownership.
    with pytest.raises(FatalInfrastructureError):
        consumer.commit_terminal(message(11, partition=0))


def test_shutdown_synchronously_flushes_pending_offsets() -> None:
    consumer, client = make_consumer(batch_size=1_000, interval_ms=60_000)
    consumer.commit_terminal(message(1))
    consumer.commit_terminal(message(2))
    assert client.commit_calls == []
    consumer.flush_pending("shutdown")
    assert len(client.commit_calls) == 1
    committed = client.commit_calls[0]
    assert (committed[0].topic, committed[0].partition, committed[0].offset) == (
        TOPIC,
        0,
        3,
    )


def test_revoked_partition_rejects_commit_before_any_batching() -> None:
    consumer, client = make_consumer()
    consumer.on_revoke(client, [TopicPartition(TOPIC, 0, 0)])
    with pytest.raises(FatalInfrastructureError):
        consumer.commit_terminal(message(1, partition=0))


def test_idle_poll_still_flushes_a_partial_batch_after_the_interval() -> None:
    """A partial batch below the batch-size threshold must not wait forever
    for a new message to arrive: once the stream goes idle, the time
    threshold still has to fire via the idle-poll hook, or the last few
    terminal records of a bounded run would never be committed."""
    import time

    consumer, client = make_consumer(batch_size=1_000, interval_ms=1)
    consumer.commit_terminal(message(1))
    assert client.commit_calls == []
    time.sleep(0.01)
    consumer.maybe_flush_idle()
    assert len(client.commit_calls) == 1
    committed = client.commit_calls[0]
    assert (committed[0].topic, committed[0].partition, committed[0].offset) == (
        TOPIC,
        0,
        2,
    )
