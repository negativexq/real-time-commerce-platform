// Package offsets ports services.event_processor.offset_tracker: bounded,
// batched Kafka offset commits that never advance past an unresolved gap.
// Single-threaded use only, exactly like the Python tracker.
package offsets

import (
	"container/heap"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/metrics"
)

type PartitionKey struct {
	Topic     string
	Partition int32
}

func (k PartitionKey) String() string { return k.Topic + ":" + strconv.Itoa(int(k.Partition)) }

// CommitFn synchronously commits {partition: next_offset}.
type CommitFn func(map[PartitionKey]int64) error

type int64Heap []int64

func (h int64Heap) Len() int           { return len(h) }
func (h int64Heap) Less(i, j int) bool { return h[i] < h[j] }
func (h int64Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *int64Heap) Push(x any)        { *h = append(*h, x.(int64)) }
func (h *int64Heap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type partitionState struct {
	safe, committed, dispatchFloor *int64
	recordsSinceCommit             int
	pending                        int64Heap
	pendingSeen                    map[int64]struct{}
}

func ptr(v int64) *int64 { return &v }

func (s *partitionState) observe(offset int64) {
	if s.dispatchFloor == nil {
		s.dispatchFloor = ptr(offset - 1)
	}
}

func (s *partitionState) markTerminal(offset int64) {
	if s.safe == nil {
		if s.dispatchFloor != nil {
			s.safe = ptr(*s.dispatchFloor)
		} else {
			s.safe = ptr(offset - 1)
		}
	}
	if s.pendingSeen == nil {
		s.pendingSeen = map[int64]struct{}{}
	}
	if _, seen := s.pendingSeen[offset]; offset <= *s.safe || seen {
		return
	}
	heap.Push(&s.pending, offset)
	s.pendingSeen[offset] = struct{}{}
	for s.pending.Len() > 0 && s.pending[0] == *s.safe+1 {
		popped := heap.Pop(&s.pending).(int64)
		delete(s.pendingSeen, popped)
		*s.safe = popped
		s.recordsSinceCommit++
	}
}

func (s *partitionState) hasUncommittedSafeOffset() bool {
	return s.safe != nil && (s.committed == nil || *s.safe != *s.committed)
}

// Tracker accumulates terminal offsets and issues bounded batched commits.
type Tracker struct {
	batchSize         int
	interval          time.Duration
	commit            CommitFn
	metrics           *metrics.Metrics
	now               func() time.Time
	partitions        map[PartitionKey]*partitionState
	recordsSinceFlush int
	lastFlushAt       time.Time
}

func NewTracker(batchSize int, interval time.Duration, commit CommitFn, m *metrics.Metrics, now func() time.Time) *Tracker {
	if now == nil {
		now = time.Now
	}
	return &Tracker{batchSize: batchSize, interval: interval, commit: commit, metrics: m, now: now,
		partitions: map[PartitionKey]*partitionState{}, lastFlushAt: now()}
}

func (t *Tracker) state(key PartitionKey) *partitionState {
	s, ok := t.partitions[key]
	if !ok {
		s = &partitionState{}
		t.partitions[key] = s
	}
	return s
}

func (t *Tracker) MarkTerminal(topic string, partition int32, offset int64) {
	t.state(PartitionKey{topic, partition}).markTerminal(offset)
	t.recordsSinceFlush++
}

func (t *Tracker) Observe(topic string, partition int32, offset int64) {
	t.state(PartitionKey{topic, partition}).observe(offset)
}

func (t *Tracker) MaybeFlush() error {
	if t.recordsSinceFlush == 0 {
		return nil
	}
	if t.recordsSinceFlush >= t.batchSize {
		return t.flush("batch_size", nil)
	}
	if t.now().Sub(t.lastFlushAt) >= t.interval {
		return t.flush("interval", nil)
	}
	return nil
}

func (t *Tracker) FlushAll(trigger string) error { return t.flush(trigger, nil) }

func (t *Tracker) FlushPartitions(keys []PartitionKey, trigger string) error {
	only := make(map[PartitionKey]bool, len(keys))
	for _, k := range keys {
		only[k] = true
	}
	return t.flush(trigger, only)
}

func (t *Tracker) DropPartition(key PartitionKey) { delete(t.partitions, key) }

func (t *Tracker) flush(trigger string, only map[PartitionKey]bool) error {
	commits := map[PartitionKey]int64{}
	for key, s := range t.partitions {
		if only != nil && !only[key] {
			continue
		}
		if s.hasUncommittedSafeOffset() {
			commits[key] = *s.safe + 1
		}
	}
	if len(commits) == 0 {
		if only == nil {
			t.resetCounters()
		}
		return nil
	}
	started := t.now()
	if err := t.commit(commits); err != nil {
		if t.metrics != nil {
			t.metrics.OffsetCommits.WithLabelValues("failed").Inc()
			t.metrics.OffsetCommitCalls.WithLabelValues(trigger).Inc()
		}
		slog.Warn("offset_batch_commit_failed", "trigger", trigger, "partitions", keysOf(commits))
		return err
	}
	duration := t.now().Sub(started)
	represented := 0
	for key, next := range commits {
		s := t.partitions[key]
		s.committed = ptr(next - 1)
		represented += s.recordsSinceCommit
		s.recordsSinceCommit = 0
	}
	if t.metrics != nil {
		t.metrics.OffsetCommits.WithLabelValues("success").Inc()
		t.metrics.OffsetDuration.Observe(duration.Seconds())
		t.metrics.OffsetCommitCalls.WithLabelValues(trigger).Inc()
		t.metrics.OffsetCommitBatch.Observe(float64(represented))
	}
	slog.Debug("offset_batch_committed", "trigger", trigger, "records_represented", represented)
	if only == nil {
		t.resetCounters()
	}
	return nil
}

func keysOf(m map[PartitionKey]int64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k.String())
	}
	sort.Strings(out)
	return out
}

func (t *Tracker) resetCounters() {
	t.recordsSinceFlush = 0
	t.lastFlushAt = t.now()
}
