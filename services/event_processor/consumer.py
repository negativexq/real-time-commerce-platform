"""Confluent Kafka polling, rebalance callbacks, and manual offset commits."""

import json
from datetime import UTC, datetime
from time import perf_counter
from typing import Any, Protocol

from confluent_kafka import (  # type: ignore[import-untyped]
    Consumer,
    KafkaError,
    Message,
    TopicPartition,
)

from services.event_processor.config import ProcessorConfig
from services.event_processor.errors import FatalInfrastructureError
from services.event_processor.logging import get_logger
from services.event_processor.models import ConsumedMessage
from services.event_processor.offset_tracker import OffsetCommitTracker, PartitionKey
from shared.observability.metrics import ApplicationMetrics


def fetchq_records_total(stats: dict[str, Any]) -> int:
    """Sum of librdkafka's per-partition fetch-queue depth: records already
    fetched from the broker and buffered locally, not yet returned by
    poll(). A queue that stays near zero means the broker/network is the
    pacing factor; a queue that grows means records are arriving from the
    broker faster than poll()/process() drains them - a buffer this
    codebase's own Python loop cannot see."""
    total = 0
    for topic in stats.get("topics", {}).values():
        for partition in topic.get("partitions", {}).values():
            count = partition.get("fetchq_cnt")
            if isinstance(count, int):
                total += count
    return total


def broker_rtt_avg_ms(stats: dict[str, Any]) -> float | None:
    """Average broker round-trip time (ms) across brokers that have
    reported at least one sample this interval, or None if none have."""
    values: list[float] = [
        float(broker["rtt"]["avg"]) / 1000
        for broker in stats.get("brokers", {}).values()
        if isinstance(broker.get("rtt"), dict) and broker["rtt"].get("cnt", 0) > 0
    ]
    if not values:
        return None
    return sum(values) / len(values)


class ConsumerClient(Protocol):
    def subscribe(self, topics: list[str], **kwargs: object) -> None: ...

    def poll(self, timeout: float) -> Message | None: ...

    def commit(
        self, offsets: list[TopicPartition], asynchronous: bool = True
    ) -> object: ...

    def close(self) -> None: ...

    def assignment(self) -> list[TopicPartition]: ...


class KafkaEventConsumer:
    """Synchronous consumer with explicit next-offset commits."""

    def __init__(
        self,
        config: ProcessorConfig,
        client: ConsumerClient | None = None,
        metrics: ApplicationMetrics | None = None,
    ) -> None:
        self._config = config
        self._client = client or Consumer(self._real_client_config(config, metrics))
        self._logger = get_logger()
        self._revoked: set[tuple[str, int]] = set()
        self._metrics = metrics
        self._tracker = OffsetCommitTracker(
            config.processor_offset_commit_batch_size,
            config.processor_offset_commit_interval_ms / 1_000,
            self._commit_offsets,
            metrics=metrics,
        )

    @staticmethod
    def kafka_config(config: ProcessorConfig) -> dict[str, object]:
        return {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "group.id": config.processor_consumer_group,
            "client.id": config.processor_client_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": config.processor_auto_offset_reset,
            "isolation.level": "read_committed",
            "session.timeout.ms": config.processor_session_timeout_ms,
            "heartbeat.interval.ms": config.processor_heartbeat_interval_ms,
            "max.poll.interval.ms": config.processor_max_poll_interval_ms,
            "partition.assignment.strategy": "cooperative-sticky",
        }

    def _real_client_config(
        self, config: ProcessorConfig, metrics: ApplicationMetrics | None
    ) -> dict[str, object]:
        """kafka_config() plus librdkafka's own statistics callback, wired
        only for the real client this instance constructs - diagnostic only,
        reads client-internal fetch-queue/broker-RTT stats the library
        already computes, changes no processing behavior."""
        client_config = dict(self.kafka_config(config))

        def on_stats(payload: str) -> None:
            if metrics is None:
                return
            stats = json.loads(payload)
            metrics.processor_consumer_fetchq_records.set(fetchq_records_total(stats))
            rtt = broker_rtt_avg_ms(stats)
            if rtt is not None:
                metrics.processor_consumer_broker_rtt_ms.set(rtt)

        client_config["statistics.interval.ms"] = 1000
        client_config["stats_cb"] = on_stats
        return client_config

    def subscribe(self) -> None:
        self._client.subscribe(
            [self._config.processor_input_topic],
            on_assign=self.on_assign,
            on_revoke=self.on_revoke,
        )

    @property
    def has_assignment(self) -> bool:
        """Whether the group currently owns at least one partition."""
        return bool(self._client.assignment())

    def on_assign(
        self, consumer: ConsumerClient, partitions: list[TopicPartition]
    ) -> None:
        del consumer
        for partition in partitions:
            self._revoked.discard((partition.topic, partition.partition))
        self._logger.info(
            "partitions_assigned",
            partitions=[
                {"topic": item.topic, "partition": item.partition}
                for item in partitions
            ],
        )
        if self._metrics is not None:
            self._metrics.processor_rebalances.labels("assigned").inc()
            self._metrics.processor_assigned.set(len(partitions))

    def on_revoke(
        self, consumer: ConsumerClient, partitions: list[TopicPartition]
    ) -> None:
        del consumer
        keys: list[PartitionKey] = [
            (partition.topic, partition.partition) for partition in partitions
        ]
        # Synchronously flush any safe pending offsets for exactly the
        # partitions being revoked before ownership is relinquished. A
        # commit failure here must not be retried after the callback
        # returns - the partition may already belong to another worker by
        # then - so it is logged/metriced and the partition's tracker state
        # is dropped either way.
        try:
            self._tracker.flush_partitions(keys, "rebalance")
        except Exception:
            self._logger.warning(
                "offset_flush_on_revoke_failed",
                partitions=[{"topic": t, "partition": p} for t, p in keys],
            )
        finally:
            for key in keys:
                self._tracker.drop_partition(key)
        self._revoked.update(keys)
        self._logger.info(
            "partitions_revoked",
            partitions=[
                {"topic": item.topic, "partition": item.partition}
                for item in partitions
            ],
        )
        if self._metrics is not None:
            self._metrics.processor_rebalances.labels("revoked").inc()
            self._metrics.processor_assigned.set(0)

    def poll(self) -> ConsumedMessage | None:
        poll_started = perf_counter()
        raw = self._client.poll(self._config.processor_poll_timeout_seconds)
        if self._metrics is not None:
            self._metrics.processor_last_poll.set_to_current_time()
            self._metrics.processor_poll_duration.observe(perf_counter() - poll_started)
        if raw is None:
            if self._metrics is not None:
                self._metrics.processor_empty_polls.inc()
            return None
        error = raw.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                return None
            raise FatalInfrastructureError(f"Kafka consume failed: {error}")
        timestamp_type, timestamp_ms = raw.timestamp()
        del timestamp_type
        timestamp = (
            datetime.fromtimestamp(timestamp_ms / 1_000, tz=UTC)
            if timestamp_ms is not None and timestamp_ms >= 0
            else None
        )
        return ConsumedMessage(
            raw.topic(),
            raw.partition(),
            raw.offset(),
            timestamp,
            raw.key(),
            raw.value(),
            raw.headers() or [],
        )

    def commit_terminal(self, message: ConsumedMessage) -> None:
        """Record a terminal record's offset as safe to commit, then flush
        if the batch-size or time threshold has been reached. Never issues
        an immediate per-record Kafka commit; see OffsetCommitTracker."""
        if (message.topic, message.partition) in self._revoked:
            raise FatalInfrastructureError("partition was revoked before commit")
        self._tracker.mark_terminal(message.topic, message.partition, message.offset)
        self._tracker.maybe_flush()

    def _commit_offsets(self, offsets: dict[PartitionKey, int]) -> None:
        """Perform the actual synchronous Kafka commit for a batch of
        partitions. Kept synchronous (not asynchronous=True) so a failure is
        detected immediately and the tracker's pending state is preserved
        rather than silently discarded."""
        self._client.commit(
            offsets=[
                TopicPartition(topic, partition, next_offset)
                for (topic, partition), next_offset in offsets.items()
            ],
            asynchronous=False,
        )
        self._logger.debug(
            "source_offsets_committed",
            commits=[
                {"topic": topic, "partition": partition, "next_offset": next_offset}
                for (topic, partition), next_offset in offsets.items()
            ],
        )

    def maybe_flush_idle(self) -> None:
        """Check the time threshold even when no new terminal record has
        arrived to trigger it. commit_terminal() only checks thresholds when
        called, so once the input stream goes idle (poll() keeps returning
        None) a partial batch below the batch-size threshold would otherwise
        never reach its time-based flush - the interval guarantee must hold
        on wall-clock time, not on message arrival. Call this once per idle
        poll iteration in the main loop."""
        self._tracker.maybe_flush()

    def flush_pending(self, reason: str = "shutdown") -> None:
        """Synchronously flush every tracked partition's safe pending
        offsets. Must be called before close() during graceful shutdown."""
        self._tracker.flush_all(reason)

    def close(self) -> None:
        self._client.close()
