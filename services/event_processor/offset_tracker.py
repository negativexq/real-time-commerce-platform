"""Bounded batched Kafka offset commit accumulator.

Replaces one-commit-per-terminal-record with a tracker that accumulates
terminal offsets per partition and flushes a single batched commit call when
a record-count or time threshold is reached, or when explicitly forced
(rebalance revoke, graceful shutdown). At-least-once delivery semantics are
preserved: an offset is only ever considered "safe" once every offset before
it in that partition has reached a terminal state (processed, duplicate, or
DLQ), and a commit failure never discards the tracker's pending state.

This module assumes single-threaded, synchronous use: the processor's poll
loop and librdkafka's rebalance callbacks (delivered inside ``poll()``) both
run on the same thread, so no locking is required for the invariants below
to hold. If a future concurrent processing model is introduced, this
tracker's per-partition contiguous-offset bookkeeping is what must be
preserved or replaced with an equivalent guarantee.
"""

import heapq
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from time import monotonic

from services.event_processor.logging import get_logger
from shared.observability.metrics import ApplicationMetrics

PartitionKey = tuple[str, int]
CommitFn = Callable[[dict[PartitionKey, int]], None]

# Bounded label set for the "what caused this commit call" metric.
FlushTrigger = str  # one of: "batch_size", "interval", "rebalance", "shutdown"


@dataclass
class _PartitionState:
    """Tracks the contiguous-safe offset and any out-of-order terminal gaps
    for one partition. ``safe_offset`` is the highest offset such that it and
    every offset before it (since this tracker first observed the partition)
    has reached a terminal state - i.e. the offset it would be safe to commit
    ``safe_offset + 1`` for. ``committed_offset`` is the highest offset this
    tracker has actually confirmed committed to Kafka. ``records_since_commit``
    counts how many terminal records have advanced ``safe_offset`` since the
    last successful commit for this specific partition."""

    safe_offset: int | None = None
    committed_offset: int | None = None
    records_since_commit: int = 0
    pending_heap: list[int] = field(default_factory=list)
    pending_seen: set[int] = field(default_factory=set)

    def mark_terminal(self, offset: int) -> None:
        if self.safe_offset is None:
            # First record observed for this partition: Kafka only ever
            # delivers the offset immediately after the last committed one
            # (or the reset policy's start), so treat the offset before it
            # as already safe.
            self.safe_offset = offset - 1
        if offset <= self.safe_offset or offset in self.pending_seen:
            return  # duplicate delivery of an already-safe/pending offset
        heapq.heappush(self.pending_heap, offset)
        self.pending_seen.add(offset)
        while self.pending_heap and self.pending_heap[0] == self.safe_offset + 1:
            popped = heapq.heappop(self.pending_heap)
            self.pending_seen.discard(popped)
            self.safe_offset = popped
            self.records_since_commit += 1

    @property
    def has_uncommitted_safe_offset(self) -> bool:
        return (
            self.safe_offset is not None and self.safe_offset != self.committed_offset
        )


class OffsetCommitTracker:
    """Accumulates terminal offsets and issues bounded batched commits.

    ``batch_size``: flush once this many terminal records have been marked
    since the last flush (across all partitions combined).
    ``interval_seconds``: flush once this much time has elapsed since the
    last successful flush, even if the batch size has not been reached.
    ``commit_fn``: performs the actual (synchronous) Kafka commit call for a
    ``{(topic, partition): next_offset}`` mapping; raising propagates the
    failure without any tracker state being discarded.
    """

    def __init__(
        self,
        batch_size: int,
        interval_seconds: float,
        commit_fn: CommitFn,
        *,
        metrics: ApplicationMetrics | None = None,
        now: Callable[[], float] = monotonic,
    ) -> None:
        self._batch_size = batch_size
        self._interval_seconds = interval_seconds
        self._commit_fn = commit_fn
        self._metrics = metrics
        self._now = now
        self._partitions: dict[PartitionKey, _PartitionState] = {}
        self._records_since_flush = 0
        self._last_flush_at = now()
        self._logger = get_logger()

    def mark_terminal(self, topic: str, partition: int, offset: int) -> None:
        key = (topic, partition)
        state = self._partitions.setdefault(key, _PartitionState())
        state.mark_terminal(offset)
        self._records_since_flush += 1

    def maybe_flush(self) -> None:
        """Flush every tracked partition if the batch-size or time
        threshold has been reached."""
        if self._records_since_flush == 0:
            return
        if self._records_since_flush >= self._batch_size:
            self._flush("batch_size")
        elif self._now() - self._last_flush_at >= self._interval_seconds:
            self._flush("interval")

    def flush_all(self, trigger: FlushTrigger) -> None:
        """Force a flush of every tracked partition regardless of
        thresholds - used for graceful shutdown."""
        self._flush(trigger)

    def flush_partitions(
        self, keys: Iterable[PartitionKey], trigger: FlushTrigger
    ) -> None:
        """Force a flush scoped to specific partitions - used when those
        partitions are about to be revoked in a rebalance. Safe to call even
        if some keys have no pending state. Does not reset the global
        batch/interval counters, which track the partitions this tracker
        continues to own."""
        self._flush(trigger, only=set(keys))

    def drop_partition(self, key: PartitionKey) -> None:
        """Forget a partition's state without committing - used after a
        partition has been revoked (whether or not its flush succeeded), so
        this tracker never later attempts to commit for a partition it no
        longer owns."""
        self._partitions.pop(key, None)

    def _flush(
        self, trigger: FlushTrigger, only: set[PartitionKey] | None = None
    ) -> None:
        items = (
            self._partitions.items()
            if only is None
            else (
                (key, state) for key, state in self._partitions.items() if key in only
            )
        )
        commits: dict[PartitionKey, int] = {}
        for key, state in items:
            if state.has_uncommitted_safe_offset:
                assert state.safe_offset is not None
                commits[key] = state.safe_offset + 1

        if not commits:
            if only is None:
                self._reset_counters()
            return

        started = self._now()
        try:
            self._commit_fn(commits)
        except Exception:
            if self._metrics is not None:
                self._metrics.processor_offset_commits.labels("failed").inc()
                self._metrics.processor_offset_commit_calls.labels(trigger).inc()
            self._logger.warning(
                "offset_batch_commit_failed",
                trigger=trigger,
                partitions=[f"{topic}:{partition}" for topic, partition in commits],
            )
            raise
        else:
            duration = self._now() - started
            records_represented = 0
            for key, next_offset in commits.items():
                state = self._partitions[key]
                state.committed_offset = next_offset - 1
                records_represented += state.records_since_commit
                state.records_since_commit = 0
            if self._metrics is not None:
                self._metrics.processor_offset_commits.labels("success").inc()
                self._metrics.processor_offset_duration.observe(duration)
                self._metrics.processor_offset_commit_calls.labels(trigger).inc()
                self._metrics.processor_offset_commit_batch_records.observe(
                    records_represented
                )
            self._logger.debug(
                "offset_batch_committed",
                trigger=trigger,
                records_represented=records_represented,
                commits={
                    f"{topic}:{partition}": next_offset
                    for (topic, partition), next_offset in commits.items()
                },
            )
            if only is None:
                self._reset_counters()

    def _reset_counters(self) -> None:
        self._records_since_flush = 0
        self._last_flush_at = self._now()
