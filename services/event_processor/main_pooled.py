"""Stage 28 diagnostic: an isolated, opt-in bounded worker-pool consumer
loop, alternate to ``main.run_processor()``.

Tests whether bounded concurrency raises the single-threaded synchronous
processing ceiling identified in Stages 25-27, without changing business
logic, idempotency guarantees, or offset safety. Only reached when
``ProcessorConfig.processor_worker_pool_size > 1`` (default 1, unchanged
default behavior); ``run_processor()`` itself is never modified or called
from here.

Design: one poll thread (this function) owns the Kafka client, the
``OffsetCommitTracker`` (via ``consumer.commit_terminal()``), and rebalance
callbacks - exactly as today, since ``offset_tracker.py`` documents that its
per-partition contiguous-offset bookkeeping assumes single-threaded access.
N worker threads only ever call ``MessageProcessor.process()`` concurrently.
``MessageProcessor.process()`` already calls its injected committer
synchronously for every terminal outcome (see ``processor.py:_commit()``) -
each worker is built with a ``_QueuedCommitter`` that redirects that call
onto a thread-safe queue instead of touching the tracker directly, so every
real ``consumer.commit_terminal()`` call still happens on this thread. The
tracker's heap-based contiguous-offset logic already tolerates terminal
completions arriving out of order, but its *bootstrap* step ("the first
offset this tracker ever sees for a partition minus one is already safe")
assumed observation order matches delivery order - true only when
completion order can't differ from delivery order, which concurrent workers
break. This function calls the new ``consumer.observe_dispatched()`` at
dispatch time, in delivery (poll) order, before handing a record to a
worker, so the tracker always bootstraps from the true lowest offset
regardless of which worker finishes first. No locking is needed anywhere:
every tracker-touching call - ``observe_dispatched()`` and
``commit_terminal()`` - happens only on this one thread.
"""

import queue
import random
import threading
from collections.abc import Callable, Sequence
from time import monotonic
from typing import Protocol

from services.event_processor.config import ProcessorConfig
from services.event_processor.consumer import KafkaEventConsumer
from services.event_processor.logging import get_logger
from services.event_processor.main import HEALTH_FILE, ShutdownController
from services.event_processor.models import ConsumedMessage, RunSummary
from services.event_processor.processor import MessageProcessor, OffsetCommitter
from shared.observability.metrics import ApplicationMetrics

_STOP = object()


class _Store(Protocol):
    def ping(self) -> object: ...
    def close(self) -> None: ...


class _Dlq(Protocol):
    def close(self) -> None: ...


class _Database(Protocol):
    def open(self) -> None: ...
    def close(self) -> None: ...


ProcessorFactory = Callable[
    [RunSummary, random.Random, OffsetCommitter], MessageProcessor
]


class _QueuedCommitter:
    """Routes a worker thread's ``commit_terminal()`` call onto a
    thread-safe queue instead of the shared ``OffsetCommitTracker`` -
    the real commit happens later, on the poll thread, when it drains
    this queue."""

    def __init__(self, sink: "queue.Queue[ConsumedMessage]") -> None:
        self._sink = sink

    def commit_terminal(self, message: ConsumedMessage) -> None:
        self._sink.put(message)


def _merge_summaries(summaries: Sequence[RunSummary]) -> RunSummary:
    """Combine one RunSummary per worker into one. Every field is an
    additive count except ``latency_max_ms``, which must take the max
    across workers, not the sum."""
    merged = RunSummary()
    for summary in summaries:
        merged.consumed_records += summary.consumed_records
        merged.valid_records += summary.valid_records
        merged.processed_records += summary.processed_records
        merged.duplicate_records += summary.duplicate_records
        merged.dlq_records += summary.dlq_records
        merged.retries += summary.retries
        merged.retry_exhausted += summary.retry_exhausted
        merged.kafka_errors += summary.kafka_errors
        merged.redis_errors += summary.redis_errors
        merged.commit_successes += summary.commit_successes
        merged.commit_failures += summary.commit_failures
        merged.processing_failures += summary.processing_failures
        merged.unresolved_records += summary.unresolved_records
        merged.latency_total_ms += summary.latency_total_ms
        merged.latency_max_ms = max(merged.latency_max_ms, summary.latency_max_ms)
        merged.validation_failures.update(summary.validation_failures)
        merged.processed_events.update(summary.processed_events)
        merged.database_transactions_started += summary.database_transactions_started
        merged.database_transactions_committed += (
            summary.database_transactions_committed
        )
        merged.database_transactions_rolled_back += (
            summary.database_transactions_rolled_back
        )
        merged.already_persisted_events += summary.already_persisted_events
        merged.database_retries += summary.database_retries
        merged.database_errors += summary.database_errors
        merged.missing_dependency_errors += summary.missing_dependency_errors
        merged.integrity_failures += summary.integrity_failures
        merged.rows_written_by_table.update(summary.rows_written_by_table)
        merged.slow_database_operations += summary.slow_database_operations
        merged.fraud_evaluations += summary.fraud_evaluations
        merged.approve_decisions += summary.approve_decisions
        merged.review_decisions += summary.review_decisions
        merged.block_decisions += summary.block_decisions
        merged.matched_rules.update(summary.matched_rules)
        merged.fraud_context_failures += summary.fraud_context_failures
        merged.fraud_integrity_failures += summary.fraud_integrity_failures
        merged.fraud_alerts_created += summary.fraud_alerts_created
        merged.fraud_outbox_rows_created += summary.fraud_outbox_rows_created
    return merged


def run_processor_pooled(
    config: ProcessorConfig,
    consumer: KafkaEventConsumer,
    build_processor: ProcessorFactory,
    store: _Store,
    dlq: _Dlq,
    shutdown: ShutdownController,
    database: _Database,
    metrics: ApplicationMetrics | None = None,
) -> tuple[int, RunSummary]:
    """Bounded worker-pool variant of ``run_processor()``. ``build_processor``
    is called once per worker with that worker's own ``RunSummary`` and
    ``random.Random`` (both must never be shared across threads: RunSummary
    counters use plain ``+=``, and ``random.Random`` mutates internal state
    on every call) plus the shared queued committer to use."""
    logger = get_logger()
    pool_size = config.processor_worker_pool_size
    commit_queue: queue.Queue[ConsumedMessage] = queue.Queue()
    errors: queue.Queue[BaseException] = queue.Queue()
    work_queue: queue.Queue[ConsumedMessage | object] = queue.Queue(
        maxsize=pool_size * 2
    )
    summaries = [RunSummary() for _ in range(pool_size)]
    committer = _QueuedCommitter(commit_queue)
    processors = [
        build_processor(summaries[i], random.Random(), committer)
        for i in range(pool_size)
    ]

    def worker(target: MessageProcessor) -> None:
        while True:
            item = work_queue.get()
            try:
                if item is _STOP:
                    return
                assert isinstance(item, ConsumedMessage)
                try:
                    target.process(item)
                except BaseException as exc:  # noqa: BLE001 - hand to main thread
                    errors.put(exc)
            finally:
                work_queue.task_done()

    threads = [
        threading.Thread(
            target=worker, args=(processors[i],), name=f"processor-worker-{i}"
        )
        for i in range(pool_size)
    ]
    for thread in threads:
        thread.start()

    terminal_count = 0

    def drain_commits() -> None:
        nonlocal terminal_count
        while True:
            try:
                message = commit_queue.get_nowait()
            except queue.Empty:
                return
            consumer.commit_terminal(message)
            terminal_count += 1
            commit_queue.task_done()

    def check_errors() -> None:
        try:
            error = errors.get_nowait()
        except queue.Empty:
            return
        raise error

    store.ping()
    database.open()
    consumer.subscribe()
    HEALTH_FILE.touch()
    last_record_at = monotonic()
    logger.info(
        "processor_pooled_started",
        worker_pool_size=pool_size,
        kafka_bootstrap_servers=config.kafka_bootstrap_servers,
        input_topic=config.processor_input_topic,
        consumer_group=config.processor_consumer_group,
        run_mode="finite" if config.processor_max_messages else "continuous",
    )
    try:
        while not shutdown.requested:
            if (
                config.processor_max_messages is not None
                and terminal_count >= config.processor_max_messages
            ):
                break
            check_errors()
            message = consumer.poll()
            HEALTH_FILE.touch()
            if message is None:
                consumer.maybe_flush_idle()
                drain_commits()
                if (
                    config.processor_max_messages is not None
                    and monotonic() - last_record_at
                    >= config.processor_idle_timeout_seconds
                    and not consumer.has_assignment
                ):
                    break
                continue
            last_record_at = monotonic()
            # Bootstrap the tracker's safe-offset in delivery order before
            # dispatch, since worker completion order can differ from
            # delivery order - see OffsetCommitTracker.observe().
            consumer.observe_dispatched(message)
            # Bounded: blocks (real backpressure, not a growing buffer) once
            # every worker is busy and this queue is already full.
            work_queue.put(message)
            drain_commits()
        for _ in threads:
            work_queue.put(_STOP)
        work_queue.join()
        for thread in threads:
            thread.join(timeout=config.processor_shutdown_timeout_seconds)
        drain_commits()
        check_errors()
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
        merged = _merge_summaries(summaries)
        logger.info("processor_pooled_stopped", **merged.as_log())
        if metrics is not None:
            metrics.processor_shutdowns.labels(
                "unresolved" if merged.unresolved_records else "graceful"
            ).inc()
    return (1 if merged.unresolved_records else 0), merged
