"""Benchmark-only direct Kafka injector for the existing commerce event schema."""

import argparse
import json
import time
from collections.abc import Callable
from pathlib import Path
from random import Random
from time import perf_counter
from typing import Any

from confluent_kafka import Producer  # type: ignore[import-untyped]

from scripts.benchmark.config import derive_seed, load_config
from scripts.benchmark.stats import percentiles
from services.event_generator.anomalies import valid_message
from services.event_generator.config import GeneratorConfig
from services.event_generator.generator import SeededUuidFactory, SyntheticGenerator
from services.event_generator.journey import JourneyBuilder, SystemClock
from services.event_generator.logging import configure_logging
from services.event_generator.messages import PublishableMessage
from services.event_generator.producer import KafkaEventProducer


class DirectProducer:
    """Benchmark-only producer using the application producer settings."""

    def __init__(self, config: GeneratorConfig) -> None:
        producer_config = KafkaEventProducer.kafka_config(config)
        producer_config["broker.address.family"] = "v4"
        self._topic = config.kafka_events_topic
        self._client = Producer(producer_config)
        self.delivery_failures = 0

    def publish_message(self, message: PublishableMessage) -> None:
        def on_delivery(error: object | None, _: object) -> None:
            if error is not None:
                self.delivery_failures += 1

        for attempt in range(6):
            try:
                self._client.produce(
                    self._topic,
                    key=message.key,
                    value=message.value,
                    headers=message.headers,
                    on_delivery=on_delivery,
                )
                self._client.poll(0)
                return
            except BufferError:
                if attempt == 5:
                    raise RuntimeError("direct producer queue remained full") from None
                self._client.poll(0.1)

    def flush(self) -> None:
        remaining = self._client.flush(30)
        if remaining or self.delivery_failures:
            raise RuntimeError(
                f"direct producer delivery failed: remaining={remaining}, "
                f"failures={self.delivery_failures}"
            )

    def close(self) -> None:
        self._client.flush(30)


def inter_arrival_gaps(timestamps: list[float]) -> list[float]:
    """Gaps (seconds) between consecutive publish timestamps, in send order."""
    return [b - a for a, b in zip(timestamps, timestamps[1:], strict=False)]


def sliding_window_rates(timestamps: list[float], window_seconds: float) -> list[float]:
    """Events/sec in each consecutive, non-overlapping window_seconds-wide bin
    spanning the timestamps - reveals short-lived bursts an average rate over
    the whole run would hide."""
    if not timestamps:
        return []
    start = timestamps[0]
    span = timestamps[-1] - start
    bin_count = int(span / window_seconds) + 1
    counts = [0] * bin_count
    for t in timestamps:
        index = min(int((t - start) / window_seconds), bin_count - 1)
        counts[index] += 1
    return [count / window_seconds for count in counts]


def arrival_variance_summary(timestamps: list[float]) -> dict[str, Any]:
    """Inter-arrival gap distribution plus sliding-window burst rates, from
    raw publish timestamps. Pure/testable: takes and returns plain data, adds
    no new Prometheus metric or production instrumentation."""
    gaps_ms = [gap * 1000 for gap in inter_arrival_gaps(timestamps)]
    summary: dict[str, Any] = {
        "inter_arrival_gap_ms": percentiles(gaps_ms)
        | {"max": max(gaps_ms) if gaps_ms else None},
    }
    for label, window_seconds in (("100ms", 0.1), ("500ms", 0.5), ("1s", 1.0)):
        rates = sliding_window_rates(timestamps, window_seconds)
        summary[f"window_rate_{label}"] = percentiles(rates) | {
            "max": max(rates) if rates else None,
            "min": min(rates) if rates else None,
            "window_count": len(rates),
        }
    return summary


def prepare_messages(
    *, bootstrap: str, seed: int, target_count: int, client_id: str
) -> list[Any]:
    """Prebuild valid, unique, mixed-persona journey messages off the hot path."""
    config = GeneratorConfig(
        kafka_bootstrap_servers=bootstrap,
        kafka_client_id=client_id,
        generator_seed=seed,
        generator_add_to_cart_probability=1,
        generator_checkout_probability=1,
        generator_refund_probability=0,
        generator_anomalies_enabled=False,
    )
    random_source = Random(seed)
    synthetic = SyntheticGenerator(random_source, SeededUuidFactory(seed))
    builder = JourneyBuilder(config, synthetic, SystemClock())
    messages: list[Any] = []
    while len(messages) < target_count:
        journey = builder.build()
        messages.extend(valid_message(event) for event in journey.events)
    return messages


def inject_messages(
    *,
    config_tag: str,
    bootstrap: str,
    rate: float,
    duration_seconds: float,
    seed: int,
    sample: Callable[[], None] | None = None,
    producer: DirectProducer | None = None,
    close_producer: bool = True,
) -> dict[str, Any]:
    """Publish prebuilt messages at a monotonic fixed rate."""
    target_count = max(1, int(rate * duration_seconds * 1.05))
    messages = prepare_messages(
        bootstrap=bootstrap,
        seed=seed,
        target_count=target_count,
        client_id=f"direct-injector-{config_tag}",
    )
    producer_config = GeneratorConfig(
        kafka_bootstrap_servers=bootstrap,
        kafka_client_id=f"direct-injector-{config_tag}",
        generator_seed=seed,
    )
    owned_producer = producer is None
    producer = producer or DirectProducer(producer_config)
    publish_latencies: list[float] = []
    missed_deadlines = 0
    drift: list[float] = []
    errors = 0
    interval = 1.0 / rate
    started = perf_counter()
    next_deadline = started
    next_sample = started
    published_ids: list[str] = []
    send_timestamps: list[float] = []
    try:
        for message in messages:
            now = perf_counter()
            if now - started >= duration_seconds:
                break
            if sample is not None and now >= next_sample:
                sample()
                next_sample += 1.0
            publish_started = perf_counter()
            try:
                producer.publish_message(message)
            except Exception:
                errors += 1
                raise
            publish_latencies.append(perf_counter() - publish_started)
            send_timestamps.append(publish_started)
            if message.event_id is not None:
                published_ids.append(str(message.event_id))
            next_deadline += interval
            now = perf_counter()
            lateness = now - next_deadline
            if lateness > 0:
                missed_deadlines += 1
                drift.append(lateness)
                if lateness > interval:
                    next_deadline = now
            elif next_deadline > now:
                time.sleep(next_deadline - now)
        producer.flush()
        if owned_producer and close_producer:
            producer.close()
    except Exception:
        errors += producer.delivery_failures
        raise
    ended = perf_counter()
    return {
        "requested_rate": rate,
        "target_count": target_count,
        "published_count": len(published_ids),
        "actual_published_rate": len(published_ids) / max(ended - started, 0.001),
        "publish_duration_seconds": ended - started,
        "publish_latency_ms": percentiles([item * 1000 for item in publish_latencies]),
        "missed_deadlines": missed_deadlines,
        "scheduler_drift_ms": percentiles([item * 1000 for item in drift]),
        "producer_errors": errors + producer.delivery_failures,
        "arrival_variance": arrival_variance_summary(send_timestamps),
        "event_ids": published_ids,
        "send_timestamps": send_timestamps,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--rate", type=float, required=True)
    parser.add_argument("--duration-seconds", type=float, default=10)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    configure_logging("WARNING")
    config = load_config(args.run_tag)
    result = inject_messages(
        config_tag=args.run_tag,
        bootstrap=config.kafka_bootstrap_servers,
        rate=args.rate,
        duration_seconds=args.duration_seconds,
        seed=args.seed or derive_seed(args.run_tag, "direct-injector", str(args.rate)),
    )
    result["run_tag"] = args.run_tag
    result["seed"] = args.seed or derive_seed(
        args.run_tag, "direct-injector", str(args.rate)
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2))
    else:
        print(
            json.dumps(
                {
                    key: value
                    for key, value in result.items()
                    if key not in {"event_ids", "send_timestamps"}
                }
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
