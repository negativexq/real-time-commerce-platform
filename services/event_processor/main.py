"""Processor CLI, signal handling, finite mode, and graceful shutdown."""

import signal
import socket
import sys
from collections.abc import Sequence
from pathlib import Path
from threading import Event
from time import monotonic
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
from shared.schemas import EVENT_PAYLOAD_REGISTRY

HEALTH_FILE = Path("/tmp/event-processor-healthy")


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
        persistence=UnitOfWorkFactory(database, config),
    )
    store.ping()
    database.open()
    consumer.subscribe()
    HEALTH_FILE.touch()
    last_record_at = monotonic()
    terminal_count = 0
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
            message = consumer.poll()
            HEALTH_FILE.touch()
            if message is None:
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
            outcome = processor.process(message)
            if not outcome.terminal:
                break
            terminal_count += 1
    finally:
        HEALTH_FILE.unlink(missing_ok=True)
        consumer.close()
        dlq.close()
        store.close()
        database.close()
        logger.info("processor_stopped", **summary.as_log())
    return (1 if summary.unresolved_records else 0), summary


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

        def handle_signal(signum: int, frame: FrameType | None) -> None:
            del frame
            get_logger().info("shutdown_requested", signal=signum)
            shutdown.request()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
        code, _ = run_processor(
            config,
            KafkaEventConsumer(config),
            RedisIdempotencyStore(config),
            DlqPublisher(config),
            shutdown,
            Database(config),
        )
        return code
    except Exception:
        get_logger().exception("processor_failed")
        HEALTH_FILE.unlink(missing_ok=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
