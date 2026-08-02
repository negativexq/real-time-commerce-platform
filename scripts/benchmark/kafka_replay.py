"""Read real Kafka broker publish timestamps for a bounded offset range.

The event payload's own ``produced_at`` field is a synthetic journey
timestamp (event-generator can backdate/simulate journeys), not the actual
wall-clock publish time, so it cannot be used for true end-to-end latency.
The Kafka broker's per-message timestamp (CreateTime, set by the producer at
actual send time) is the real signal. This module uses a throwaway,
non-group consumer (manual partition assignment, no offset commits, no
interference with any consumer group) to read it for a bounded offset
window and correlate it with event_id via the ``event_id`` Kafka header
every producer already attaches (shared/kafka_metadata.py).
"""

import json
import time
import uuid
from typing import Any

from confluent_kafka import Consumer, TopicPartition  # type: ignore[import-untyped]


def topic_watermarks(bootstrap_servers: str, topic: str) -> dict[int, int]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"benchmark-watermark-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=10).topics[topic]
        result = {}
        for partition in metadata.partitions:
            _, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=10
            )
            result[partition] = high
        return result
    finally:
        consumer.close()


def read_publish_timestamps(
    bootstrap_servers: str,
    topic: str,
    start_offsets: dict[int, int],
    end_offsets: dict[int, int],
    wanted_event_ids: set[str],
    timeout_seconds: float = 60.0,
) -> dict[str, int]:
    """Return {event_id: kafka_broker_timestamp_ms} for wanted_event_ids."""
    if not wanted_event_ids:
        return {}
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"benchmark-replay-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    partitions = [
        TopicPartition(topic, partition, start_offsets.get(partition, 0))
        for partition in end_offsets
        if end_offsets.get(partition, 0) > start_offsets.get(partition, 0)
    ]
    if not partitions:
        consumer.close()
        return {}
    consumer.assign(partitions)
    found: dict[str, int] = {}
    remaining = {tp.partition: end_offsets[tp.partition] for tp in partitions}
    deadline = time.monotonic() + timeout_seconds
    try:
        while remaining and time.monotonic() < deadline and len(found) < len(
            wanted_event_ids
        ):
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            partition = msg.partition()
            if partition in remaining and msg.offset() >= remaining[partition] - 1:
                remaining.pop(partition, None)
            headers: dict[str, Any] = dict(msg.headers() or [])
            event_id_raw = headers.get("event_id")
            if event_id_raw is None:
                continue
            event_id = (
                event_id_raw.decode() if isinstance(event_id_raw, bytes) else event_id_raw
            )
            if event_id in wanted_event_ids and event_id not in found:
                # First-write-wins: if the same event_id was genuinely
                # published more than once (observed even outside the
                # explicit duplicate_delivery scenario - see
                # docs/performance-report.md Limitations), the earliest
                # publish on the wire is the one the idempotency layer
                # actually processed, so it is the correct latency anchor.
                _, ts_ms = msg.timestamp()
                found[event_id] = ts_ms
    finally:
        consumer.close()
    return found


def read_records(
    bootstrap_servers: str,
    topic: str,
    start_offsets: dict[int, int],
    end_offsets: dict[int, int],
    *,
    decode_json: bool = True,
    header_filter: dict[str, str] | None = None,
    timeout_seconds: float = 60.0,
) -> list[dict[str, Any]]:
    """Read every record in [start_offsets, end_offsets) for a topic, decoding
    the value as JSON. Optionally keep only records whose headers match
    header_filter exactly (used to scope DLQ records to one benchmark's
    isolated consumer_group header)."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"benchmark-replay-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    partitions = [
        TopicPartition(topic, partition, start_offsets.get(partition, 0))
        for partition in end_offsets
        if end_offsets.get(partition, 0) > start_offsets.get(partition, 0)
    ]
    if not partitions:
        consumer.close()
        return []
    consumer.assign(partitions)
    remaining = {tp.partition: end_offsets[tp.partition] for tp in partitions}
    records: list[dict[str, Any]] = []
    deadline = time.monotonic() + timeout_seconds
    try:
        while remaining and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            partition = msg.partition()
            if partition in remaining and msg.offset() >= remaining[partition] - 1:
                remaining.pop(partition, None)
            headers: dict[str, Any] = {
                key: (value.decode() if isinstance(value, bytes) else value)
                for key, value in dict(msg.headers() or []).items()
            }
            if header_filter and any(
                headers.get(key) != value for key, value in header_filter.items()
            ):
                continue
            record: dict[str, Any] = {"headers": headers, "offset": msg.offset()}
            if decode_json and msg.value():
                try:
                    record["value"] = json.loads(msg.value())
                except json.JSONDecodeError:
                    record["value"] = None
                    record["value_raw_bytes"] = len(msg.value())
            records.append(record)
    finally:
        consumer.close()
    return records
