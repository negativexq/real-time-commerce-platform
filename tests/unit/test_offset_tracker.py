"""Bounded batched offset commit accumulator tests.

Covers the correctness invariants required for batching offset commits
without weakening at-least-once semantics: contiguous-only advancement,
threshold-triggered flush, multi-partition independence, failure-preserves-
state, and duplicate-mark idempotence.
"""

from services.event_processor.offset_tracker import OffsetCommitTracker

TOPIC = "commerce.events"


class FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class RecordingCommitter:
    def __init__(self, fail_next: bool = False) -> None:
        self.calls: list[dict[tuple[str, int], int]] = []
        self.fail_next = fail_next

    def __call__(self, offsets: dict[tuple[str, int], int]) -> None:
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("simulated Kafka commit failure")
        self.calls.append(dict(offsets))


def make_tracker(
    batch_size: int = 50,
    interval_seconds: float = 100.0,
    committer: RecordingCommitter | None = None,
    clock: FakeClock | None = None,
) -> tuple[OffsetCommitTracker, RecordingCommitter, FakeClock]:
    committer = committer or RecordingCommitter()
    clock = clock or FakeClock()
    tracker = OffsetCommitTracker(batch_size, interval_seconds, committer, now=clock)
    return tracker, committer, clock


def test_single_terminal_event_does_not_commit_below_thresholds() -> None:
    tracker, committer, _ = make_tracker(batch_size=50, interval_seconds=100.0)
    tracker.mark_terminal(TOPIC, 0, 100)
    tracker.maybe_flush()
    assert committer.calls == []


def test_batch_size_threshold_triggers_commit() -> None:
    tracker, committer, _ = make_tracker(batch_size=3, interval_seconds=100.0)
    for offset in (100, 101, 102):
        tracker.mark_terminal(TOPIC, 0, offset)
        tracker.maybe_flush()
    assert committer.calls == [{(TOPIC, 0): 103}]


def test_time_threshold_triggers_commit() -> None:
    tracker, committer, clock = make_tracker(batch_size=1000, interval_seconds=5.0)
    tracker.mark_terminal(TOPIC, 0, 100)
    tracker.maybe_flush()
    assert committer.calls == []  # neither threshold reached yet
    clock.advance(5.0)
    tracker.maybe_flush()
    assert committer.calls == [{(TOPIC, 0): 101}]


def test_committed_value_is_highest_safe_offset_plus_one() -> None:
    tracker, committer, _ = make_tracker(batch_size=5, interval_seconds=100.0)
    for offset in (10, 11, 12, 13, 14):
        tracker.mark_terminal(TOPIC, 0, offset)
    tracker.maybe_flush()
    assert committer.calls == [{(TOPIC, 0): 15}]


def test_multiple_partitions_tracked_independently() -> None:
    tracker, committer, _ = make_tracker(batch_size=4, interval_seconds=100.0)
    tracker.mark_terminal(TOPIC, 0, 50)
    tracker.mark_terminal(TOPIC, 1, 900)
    tracker.mark_terminal(TOPIC, 0, 51)
    tracker.mark_terminal(TOPIC, 1, 901)
    tracker.maybe_flush()
    assert committer.calls == [{(TOPIC, 0): 52, (TOPIC, 1): 902}]


def test_shutdown_performs_synchronous_final_flush() -> None:
    tracker, committer, _ = make_tracker(batch_size=1000, interval_seconds=1000.0)
    tracker.mark_terminal(TOPIC, 0, 1)
    tracker.maybe_flush()
    assert committer.calls == []
    tracker.flush_all("shutdown")
    assert committer.calls == [{(TOPIC, 0): 2}]


def test_revoke_flushes_only_the_revoked_partitions() -> None:
    tracker, committer, _ = make_tracker(batch_size=1000, interval_seconds=1000.0)
    tracker.mark_terminal(TOPIC, 0, 10)
    tracker.mark_terminal(TOPIC, 1, 20)
    tracker.flush_partitions([(TOPIC, 0)], "rebalance")
    assert committer.calls == [{(TOPIC, 0): 11}]
    # Partition 1's pending state is untouched and can still be flushed later.
    tracker.flush_all("shutdown")
    assert committer.calls[-1] == {(TOPIC, 1): 21}


def test_dropped_partition_state_is_never_committed_again() -> None:
    tracker, committer, _ = make_tracker(batch_size=1000, interval_seconds=1000.0)
    tracker.mark_terminal(TOPIC, 0, 10)
    tracker.flush_partitions([(TOPIC, 0)], "rebalance")
    tracker.drop_partition((TOPIC, 0))
    committer.calls.clear()
    tracker.flush_all("shutdown")
    assert committer.calls == []


def test_commit_failure_does_not_lose_pending_state() -> None:
    committer = RecordingCommitter(fail_next=True)
    tracker, committer, _ = make_tracker(
        batch_size=2, interval_seconds=100.0, committer=committer
    )
    tracker.mark_terminal(TOPIC, 0, 1)
    tracker.mark_terminal(TOPIC, 0, 2)
    try:
        tracker.maybe_flush()
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected the simulated commit failure to propagate")
    assert committer.calls == []
    # A subsequent successful flush must still commit the offset the failed
    # attempt tried to commit - nothing was silently discarded.
    tracker.flush_all("shutdown")
    assert committer.calls == [{(TOPIC, 0): 3}]


def test_unresolved_gap_blocks_commit_past_it() -> None:
    """Partition 0: offset 100 terminal, 101 terminal, 102 unresolved
    (never marked), 103 marked terminal out of order. The committed offset
    must never advance beyond 102 (i.e. next_offset must be 102, not 104)."""
    tracker, committer, _ = make_tracker(batch_size=1000, interval_seconds=1000.0)
    tracker.mark_terminal(TOPIC, 0, 100)
    tracker.mark_terminal(TOPIC, 0, 101)
    tracker.mark_terminal(TOPIC, 0, 103)  # 102 is skipped: still unresolved
    tracker.flush_all("shutdown")
    assert committer.calls == [{(TOPIC, 0): 102}]
    # Once the gap is filled, the deferred offset 103 becomes committable too.
    tracker.mark_terminal(TOPIC, 0, 102)
    tracker.flush_all("shutdown")
    assert committer.calls[-1] == {(TOPIC, 0): 104}


def test_duplicate_marks_are_idempotent() -> None:
    tracker, committer, _ = make_tracker(batch_size=1000, interval_seconds=1000.0)
    tracker.mark_terminal(TOPIC, 0, 5)
    tracker.mark_terminal(TOPIC, 0, 5)  # replayed/duplicate mark
    tracker.mark_terminal(TOPIC, 0, 6)
    tracker.flush_all("shutdown")
    assert committer.calls == [{(TOPIC, 0): 7}]


def test_metrics_record_trigger_and_batch_size_with_bounded_labels() -> None:
    from shared.observability.metrics import ApplicationMetrics

    metrics = ApplicationMetrics("test-processor")
    committer = RecordingCommitter()
    tracker = OffsetCommitTracker(
        2, 1000.0, committer, metrics=metrics, now=FakeClock()
    )
    tracker.mark_terminal(TOPIC, 0, 1)
    tracker.mark_terminal(TOPIC, 0, 2)
    tracker.maybe_flush()

    assert (
        metrics.registry.get_sample_value(
            "commerce_processor_offset_commits_total", {"result": "success"}
        )
        == 1
    )
    assert (
        metrics.registry.get_sample_value(
            "commerce_processor_offset_commit_calls_total", {"trigger": "batch_size"}
        )
        == 1
    )
    assert (
        metrics.registry.get_sample_value(
            "commerce_processor_offset_commit_batch_records_sum", {}
        )
        == 2
    )
