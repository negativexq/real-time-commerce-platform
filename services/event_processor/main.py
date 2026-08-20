"""Processor CLI, signal handling, finite mode, and graceful shutdown."""

import os
import signal
import socket
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from time import monotonic, perf_counter
from types import FrameType

from services.event_processor.config import ProcessorConfig, parse_config
from services.event_processor.consumer import KafkaEventConsumer
from services.event_processor.dlq import DlqPublisher
from services.event_processor.idempotency import RedisIdempotencyStore
from services.event_processor.logging import configure_logging, get_logger
from services.event_processor.models import RunSummary
from services.event_processor.persistence import (
    Database,
    UnitOfWorkFactory,
    default_persistence_registry,
)
from services.event_processor.persistence.database import safe_postgres_endpoint
from services.event_processor.processor import MessageProcessor
from shared.observability import ApplicationMetrics, MetricsConfig, MetricsServer
from shared.schemas import EVENT_PAYLOAD_REGISTRY

HEALTH_FILE = Path("/tmp/event-processor-healthy")


def queue_wait_seconds(
    produced_at: datetime | None, observed_at: datetime
) -> float | None:
    """Time a record spent sitting in the Kafka topic before this consumer's
    poll() returned it. None when the broker supplied no record timestamp;
    negative values (producer/consumer clock skew) are dropped rather than
    fed into the histogram, since they cannot reflect real queueing time."""
    if produced_at is None:
        return None
    wait = (observed_at - produced_at).total_seconds()
    return wait if wait >= 0 else None


class ShutdownController:
    def __init__(self) -> None:
        self._event = Event()

    @property
    def requested(self) -> bool:
        return self._event.is_set()

    def request(self) -> None:
        self._event.set()


def run_processor(
    config: ProcessorConfig,
    consumer: KafkaEventConsumer,
    store: RedisIdempotencyStore,
    dlq: DlqPublisher,
    shutdown: ShutdownController,
    database: Database,
    metrics: ApplicationMetrics | None = None,
) -> tuple[int, RunSummary]:
    """Run until signal, terminal-count bound, idle timeout, or unresolved work."""
    logger = get_logger()
    summary = RunSummary()
    instance_id = f"{config.processor_client_id}-{socket.gethostname()}"
    processor = MessageProcessor(
        config,
        store,
        dlq,
        consumer,
        default_persistence_registry(),
        summary,
        processor_instance_id=instance_id,
        persistence=UnitOfWorkFactory(database, config, metrics=metrics),
        metrics=metrics,
    )
    store.ping()
    database.open()
    consumer.subscribe()
    HEALTH_FILE.touch()
    last_record_at = monotonic()
    terminal_count = 0
    previous_process_end: float | None = None
    logger.info(
        "processor_started",
        kafka_bootstrap_servers=config.kafka_bootstrap_servers,
        input_topic=config.processor_input_topic,
        dlq_topic=config.processor_dlq_topic,
        consumer_group=config.processor_consumer_group,
        client_id=config.processor_client_id,
        redis_endpoint=_safe_redis_endpoint(config.redis_url),
        postgres_endpoint=safe_postgres_endpoint(config.postgres_dsn),
        required_schema_version=config.processor_required_schema_version,
        run_mode="finite" if config.processor_max_messages else "continuous",
        supported_event_count=len(EVENT_PAYLOAD_REGISTRY),
    )
    try:
        while not shutdown.requested:
            if (
                config.processor_max_messages is not None
                and terminal_count >= config.processor_max_messages
            ):
                break
            poll_started = perf_counter()
            if metrics is not None and previous_process_end is not None:
                metrics.processor_loop_gap_duration.observe(
                    poll_started - previous_process_end
                )
            message = consumer.poll()
            if metrics is not None:
                metrics.processor_last_poll.set_to_current_time()
            HEALTH_FILE.touch()
            if message is None:
                # The interval-based flush threshold must hold on wall-clock
                # time, not merely on new terminal records arriving - once
                # traffic goes idle, a partial batch below the batch-size
                # threshold still needs its time-based commit.
                consumer.maybe_flush_idle()
                if (
                    config.processor_max_messages is not None
                    and monotonic() - last_record_at
                    >= config.processor_idle_timeout_seconds
                ):
                    if not consumer.has_assignment:
                        summary.kafka_errors += 1
                        summary.unresolved_records += 1
                    break
                continue
            last_record_at = monotonic()
            if metrics is not None:
                metrics.processor_poll_to_handler_duration.observe(
                    perf_counter() - poll_started
                )
                wait = queue_wait_seconds(message.timestamp, datetime.now(UTC))
                if wait is not None:
                    metrics.processor_consumer_queue_wait_duration.observe(wait)
            outcome = processor.process(message)
            previous_process_end = perf_counter()
            if not outcome.terminal:
                break
            terminal_count += 1
    finally:
        HEALTH_FILE.unlink(missing_ok=True)
        try:
            consumer.flush_pending("shutdown")
        except Exception:
            logger.exception("offset_flush_on_shutdown_failed")
        consumer.close()
        dlq.close()
        store.close()
        database.close()
        logger.info("processor_stopped", **summary.as_log())
        if metrics is not None:
            metrics.processor_shutdowns.labels(
                "unresolved" if summary.unresolved_records else "graceful"
            ).inc()
    return (1 if summary.unresolved_records else 0), summary


def _run_pooled(
    config: ProcessorConfig, shutdown: ShutdownController, metrics: ApplicationMetrics
) -> tuple[int, RunSummary]:
    """Stage 28 diagnostic dispatch: only reached when
    processor_worker_pool_size > 1 (never the default). Imported locally to
    avoid a circular import - main_pooled.py imports HEALTH_FILE and
    ShutdownController from this module."""
    import random

    from services.event_processor.main_pooled import run_processor_pooled
    from services.event_processor.persistence import (
        UnitOfWorkFactory,
        default_persistence_registry,
    )
    from services.event_processor.processor import MessageProcessor, OffsetCommitter

    instance_id = f"{config.processor_client_id}-{socket.gethostname()}"
    store = RedisIdempotencyStore(config, metrics=metrics)
    dlq = DlqPublisher(config)
    consumer = KafkaEventConsumer(config, metrics=metrics)
    database = Database(config)
    handlers = default_persistence_registry()
    persistence = UnitOfWorkFactory(database, config, metrics=metrics)

    def build_processor(
        summary: RunSummary, rng: random.Random, committer: OffsetCommitter
    ) -> MessageProcessor:
        return MessageProcessor(
            config,
            store,
            dlq,
            committer,
            handlers,
            summary,
            processor_instance_id=instance_id,
            persistence=persistence,
            rng=rng,
            metrics=metrics,
        )

    return run_processor_pooled(
        config, consumer, build_processor, store, dlq, shutdown, database, metrics
    )


def _safe_redis_endpoint(url: str) -> str:
    """Return scheme/host/port/database without credentials."""
    from urllib.parse import urlsplit

    parsed = urlsplit(url)
    host = parsed.hostname or "unknown"
    port = parsed.port or 6379
    return f"{parsed.scheme}://{host}:{port}{parsed.path}"


def main(arguments: Sequence[str] | None = None) -> int:
    configure_logging("INFO")
    try:
        config = parse_config(arguments)
        configure_logging(config.processor_log_level)
        shutdown = ShutdownController()
        metrics_config = MetricsConfig.from_environment(
            "event-processor",
            9101,
            os.environ,
            port_environment_name="PROCESSOR_METRICS_PORT",
        )
        metrics = ApplicationMetrics(
            metrics_config.service_name, metrics_config.namespace
        )
        metrics_server = MetricsServer(
            metrics_config, metrics.registry, HEALTH_FILE.exists
        )
        try:
            metrics_server.start()
        except OSError:
            get_logger().exception("metrics_server_failed")
            metrics.service_healthy.labels(metrics.service).set(0)

        def handle_signal(signum: int, frame: FrameType | None) -> None:
            del frame
            get_logger().info("shutdown_requested", signal=signum)
            shutdown.request()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
        if config.processor_worker_pool_size > 1:
            code, _ = _run_pooled(config, shutdown, metrics)
        else:
            code, _ = run_processor(
                config,
                KafkaEventConsumer(config, metrics=metrics),
                RedisIdempotencyStore(config, metrics=metrics),
                DlqPublisher(config),
                shutdown,
                Database(config),
                metrics,
            )
        metrics_server.stop()
        return code
    except Exception:
        get_logger().exception("processor_failed")
        HEALTH_FILE.unlink(missing_ok=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
