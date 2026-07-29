"""Confluent Kafka polling, rebalance callbacks, and manual offset commits."""

from datetime import UTC, datetime
from typing import Protocol

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
        self, config: ProcessorConfig, client: ConsumerClient | None = None
    ) -> None:
        self._config = config
        self._client = client or Consumer(self.kafka_config(config))
        self._logger = get_logger()
        self._revoked: set[tuple[str, int]] = set()

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

    def on_revoke(
        self, consumer: ConsumerClient, partitions: list[TopicPartition]
    ) -> None:
        del consumer
        self._revoked.update(
            (partition.topic, partition.partition) for partition in partitions
        )
        self._logger.info(
            "partitions_revoked",
            partitions=[
                {"topic": item.topic, "partition": item.partition}
                for item in partitions
            ],
        )

    def poll(self) -> ConsumedMessage | None:
        raw = self._client.poll(self._config.processor_poll_timeout_seconds)
        if raw is None:
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
        if (message.topic, message.partition) in self._revoked:
            raise FatalInfrastructureError("partition was revoked before commit")
        next_offset = message.offset + 1
        self._client.commit(
            offsets=[TopicPartition(message.topic, message.partition, next_offset)],
            asynchronous=False,
        )
        self._logger.debug(
            "source_offset_committed",
            topic=message.topic,
            partition=message.partition,
            processed_offset=message.offset,
            committed_next_offset=next_offset,
        )

    def close(self) -> None:
        self._client.close()
