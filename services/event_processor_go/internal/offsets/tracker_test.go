package offsets

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/metrics"
)

// Ports tests/unit/test_offset_tracker.py.

const topic = "commerce.events"

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

type recorder struct {
	calls    []map[PartitionKey]int64
	failNext bool
}

func (r *recorder) commit(offsets map[PartitionKey]int64) error {
	if r.failNext {
		r.failNext = false
		return errors.New("simulated Kafka commit failure")
	}
	copied := map[PartitionKey]int64{}
	for k, v := range offsets {
		copied[k] = v
	}
	r.calls = append(r.calls, copied)
	return nil
}

func newTracker(batch int, interval time.Duration) (*Tracker, *recorder, *clock) {
	r, c := &recorder{}, &clock{t: time.Unix(0, 0)}
	return NewTracker(batch, interval, r.commit, nil, c.now), r, c
}

func p(n int32) PartitionKey { return PartitionKey{topic, n} }

func one(n int32, next int64) map[PartitionKey]int64 { return map[PartitionKey]int64{p(n): next} }

func assertCalls(t *testing.T, got []map[PartitionKey]int64, want ...map[PartitionKey]int64) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("commits = %v, want %v", got, want)
	}
}

func assertLast(t *testing.T, got []map[PartitionKey]int64, want map[PartitionKey]int64) {
	t.Helper()
	if len(got) == 0 || !reflect.DeepEqual(got[len(got)-1], want) {
		t.Fatalf("last commit = %v, want %v", got, want)
	}
}

const long = 1000 * time.Second

func TestBelowThresholdsDoesNotCommit(t *testing.T) {
	tr, r, _ := newTracker(50, 100*time.Second)
	tr.MarkTerminal(topic, 0, 100)
	_ = tr.MaybeFlush()
	assertCalls(t, r.calls)
}

func TestBatchSizeThreshold(t *testing.T) {
	tr, r, _ := newTracker(3, 100*time.Second)
	for _, o := range []int64{100, 101, 102} {
		tr.MarkTerminal(topic, 0, o)
		_ = tr.MaybeFlush()
	}
	assertCalls(t, r.calls, one(0, 103))
}

func TestTimeThreshold(t *testing.T) {
	tr, r, c := newTracker(1000, 5*time.Second)
	tr.MarkTerminal(topic, 0, 100)
	_ = tr.MaybeFlush()
	assertCalls(t, r.calls)
	c.advance(5 * time.Second)
	_ = tr.MaybeFlush()
	assertCalls(t, r.calls, one(0, 101))
}

func TestMultiplePartitionsIndependent(t *testing.T) {
	tr, r, _ := newTracker(4, 100*time.Second)
	tr.MarkTerminal(topic, 0, 50)
	tr.MarkTerminal(topic, 1, 900)
	tr.MarkTerminal(topic, 0, 51)
	tr.MarkTerminal(topic, 1, 901)
	_ = tr.MaybeFlush()
	assertCalls(t, r.calls, map[PartitionKey]int64{p(0): 52, p(1): 902})
}

func TestRevokeFlushesOnlyRevoked(t *testing.T) {
	tr, r, _ := newTracker(1000, long)
	tr.MarkTerminal(topic, 0, 10)
	tr.MarkTerminal(topic, 1, 20)
	_ = tr.FlushPartitions([]PartitionKey{p(0)}, "rebalance")
	assertCalls(t, r.calls, one(0, 11))
	_ = tr.FlushAll("shutdown")
	assertLast(t, r.calls, one(1, 21))
}

func TestDroppedPartitionNeverCommittedAgain(t *testing.T) {
	tr, r, _ := newTracker(1000, long)
	tr.MarkTerminal(topic, 0, 10)
	_ = tr.FlushPartitions([]PartitionKey{p(0)}, "rebalance")
	tr.DropPartition(p(0))
	r.calls = nil
	_ = tr.FlushAll("shutdown")
	assertCalls(t, r.calls)
}

func TestCommitFailureKeepsPendingState(t *testing.T) {
	tr, r, _ := newTracker(2, 100*time.Second)
	r.failNext = true
	tr.MarkTerminal(topic, 0, 1)
	tr.MarkTerminal(topic, 0, 2)
	if err := tr.MaybeFlush(); err == nil {
		t.Fatal("expected simulated commit failure to propagate")
	}
	assertCalls(t, r.calls)
	_ = tr.FlushAll("shutdown")
	assertCalls(t, r.calls, one(0, 3))
}

func TestGapBlocksCommit(t *testing.T) {
	tr, r, _ := newTracker(1000, long)
	tr.MarkTerminal(topic, 0, 100)
	tr.MarkTerminal(topic, 0, 101)
	tr.MarkTerminal(topic, 0, 103)
	_ = tr.FlushAll("shutdown")
	assertCalls(t, r.calls, one(0, 102))
	tr.MarkTerminal(topic, 0, 102)
	_ = tr.FlushAll("shutdown")
	assertLast(t, r.calls, one(0, 104))
}

func TestDuplicateMarksIdempotent(t *testing.T) {
	tr, r, _ := newTracker(1000, long)
	tr.MarkTerminal(topic, 0, 5)
	tr.MarkTerminal(topic, 0, 5)
	tr.MarkTerminal(topic, 0, 6)
	_ = tr.FlushAll("shutdown")
	assertCalls(t, r.calls, one(0, 7))
}

func TestObserveThenOutOfOrderCompletion(t *testing.T) {
	tr, r, _ := newTracker(1000, long)
	tr.Observe(topic, 0, 10)
	tr.Observe(topic, 0, 11)
	tr.MarkTerminal(topic, 0, 11)
	_ = tr.FlushAll("shutdown")
	assertCalls(t, r.calls, one(0, 10))
	tr.MarkTerminal(topic, 0, 10)
	_ = tr.FlushAll("shutdown")
	assertLast(t, r.calls, one(0, 12))
}

func TestObserveAloneNeverCommits(t *testing.T) {
	tr, r, _ := newTracker(1000, long)
	tr.Observe(topic, 0, 10)
	tr.Observe(topic, 0, 11)
	_ = tr.FlushAll("shutdown")
	assertCalls(t, r.calls)
}

func TestMetrics(t *testing.T) {
	m := metrics.New("test-processor", "commerce")
	r, c := &recorder{}, &clock{}
	tr := NewTracker(2, long, r.commit, m, c.now)
	tr.MarkTerminal(topic, 0, 1)
	tr.MarkTerminal(topic, 0, 2)
	_ = tr.MaybeFlush()
	if v := testutil.ToFloat64(m.OffsetCommits.WithLabelValues("success")); v != 1 {
		t.Fatalf("offset commits = %v", v)
	}
	if v := testutil.ToFloat64(m.OffsetCommitCalls.WithLabelValues("batch_size")); v != 1 {
		t.Fatalf("commit calls = %v", v)
	}
}
