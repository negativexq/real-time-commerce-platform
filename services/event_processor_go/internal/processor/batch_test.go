package processor

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/idempotency"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/model"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
)

// messages returns n valid order_created records with distinct event IDs at
// consecutive offsets.
func messages(t *testing.T, n int) []*model.ConsumedMessage {
	t.Helper()
	out := make([]*model.ConsumedMessage, n)
	for i := range out {
		raw := strings.Replace(orderCreated, "00000000-0000-4000-8000-000000000001",
			fmt.Sprintf("00000000-0000-4000-8000-%012d", 100+i), 1)
		event, err := contracts.ParseEvent([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		m := &model.ConsumedMessage{Topic: "commerce.events", Partition: 1, Offset: int64(10 + i),
			Key: event.MessageKey(), Value: []byte(raw)}
		for _, h := range event.MessageHeaders() {
			m.Headers = append(m.Headers, model.Header{Key: h.Key, Value: h.Value})
		}
		out[i] = m
	}
	return out
}

func statuses(outcomes []model.ProcessingOutcome) []model.ProcessingStatus {
	out := make([]model.ProcessingStatus, len(outcomes))
	for i, o := range outcomes {
		out[i] = o.Status
	}
	return out
}

func TestBatchOfNewEventsSharesOneTransaction(t *testing.T) {
	h := &harness{reserve: idempotency.Reserved, completeOK: true}
	p, summary := newProcessor(t, h, true)
	outcomes, err := p.ProcessBatch(messages(t, 3))
	if err != nil || len(outcomes) != 3 {
		t.Fatalf("outcomes %v err %v", outcomes, err)
	}
	// Every lease is taken before the transaction; each is completed before
	// its offset is marked, in order.
	expectCalls(t, h, "reserve", "reserve", "reserve", "persist_group:3",
		"complete", "commit", "complete", "commit", "complete", "commit")
	if summary.ProcessedRecords != 3 || summary.DatabaseTransactionsCommitted != 3 {
		t.Fatalf("summary %+v", summary)
	}
}

func TestFailedGroupIsRetriedOneEventAtATime(t *testing.T) {
	missing := procerr.New(procerr.MissingBusinessDependency, "customer is missing")
	h := &harness{reserve: idempotency.Reserved, completeOK: true, groupErr: missing,
		// the second event fails alone on every attempt; the others commit
		persistErrs: []error{nil, missing, missing, missing, nil}}
	p, summary := newProcessor(t, h, true)
	outcomes, err := p.ProcessBatch(messages(t, 3))
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(statuses(outcomes)); got != fmt.Sprint([]model.ProcessingStatus{model.Processed, model.DLQ, model.Processed}) {
		t.Fatalf("statuses %s", got)
	}
	expectCalls(t, h, "reserve", "reserve", "reserve", "persist_group:3",
		"persist", "complete", "commit",
		"persist", "persist", "persist", "release", "dlq:missing_business_dependency", "commit",
		"persist", "complete", "commit")
	if summary.DatabaseTransactionsRolledBack != 3+3 || summary.DLQRecords != 1 {
		t.Fatalf("summary %+v", summary)
	}
}

func TestRecordOutsideTheGroupCommitsTheEventsBeforeIt(t *testing.T) {
	msgs := messages(t, 4)
	msgs[2].Value = []byte("{")
	h := &harness{reserve: idempotency.Reserved, completeOK: true}
	p, _ := newProcessor(t, h, true)
	outcomes, err := p.ProcessBatch(msgs)
	if err != nil || len(outcomes) != 4 {
		t.Fatalf("outcomes %v err %v", outcomes, err)
	}
	expectCalls(t, h, "reserve", "reserve", "persist_group:2", "complete", "commit", "complete", "commit",
		"dlq:malformed_json", "commit",
		"reserve", "persist", "complete", "commit")
}

func TestActiveLeaseStopsTheBatchAfterCommittingTheEventsBeforeIt(t *testing.T) {
	h := &harness{completeOK: true,
		reserveSeq: []idempotency.ReservationState{idempotency.Reserved, idempotency.Reserved,
			// held by another worker: still held when asked again after the group
			idempotency.Processing, idempotency.Processing}}
	p, summary := newProcessor(t, h, true)
	outcomes, err := p.ProcessBatch(messages(t, 5))
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(statuses(outcomes)); got != fmt.Sprint([]model.ProcessingStatus{model.Processed, model.Processed, model.Unresolved}) {
		t.Fatalf("statuses %s", got)
	}
	// The last two records are neither reserved nor committed: redelivered.
	expectCalls(t, h, "reserve", "reserve", "reserve", "persist_group:2",
		"complete", "commit", "complete", "commit", "reserve")
	if summary.UnresolvedRecords != 1 {
		t.Fatalf("summary %+v", summary)
	}
}

func TestCompletionFailureInAGroupStillCompletesTheOthers(t *testing.T) {
	h := &harness{reserve: idempotency.Reserved, completeSeq: []bool{true, false, true}}
	p, _ := newProcessor(t, h, true)
	outcomes, err := p.ProcessBatch(messages(t, 3))
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(statuses(outcomes)); got != fmt.Sprint([]model.ProcessingStatus{model.Processed, model.Unresolved, model.Processed}) {
		t.Fatalf("statuses %s", got)
	}
	// The unresolved record's offset is never marked, so the tracker cannot
	// commit past it; the record after it is completed and marked.
	expectCalls(t, h, "reserve", "reserve", "reserve", "persist_group:3",
		"complete", "commit", "complete", "complete", "commit")
}

func TestOffsetCommitFailureInABatchIsFatal(t *testing.T) {
	h := &harness{reserve: idempotency.Reserved, completeOK: true, commitErr: errors.New("broker down")}
	p, _ := newProcessor(t, h, true)
	_, err := p.ProcessBatch(messages(t, 2))
	var fatal *FatalError
	if !errors.As(err, &fatal) {
		t.Fatalf("error %v", err)
	}
}

// Two copies of one event in a batch: the second sees the first one's lease,
// so it is asked again once the group completes, and is a duplicate.
func TestDuplicateInTheSameBatchIsAskedAgainAfterTheGroup(t *testing.T) {
	msgs := messages(t, 1)
	again := *msgs[0]
	again.Offset++
	h := &harness{completeOK: true, alreadyDone: true,
		reserveSeq: []idempotency.ReservationState{idempotency.Reserved, idempotency.Processing, idempotency.Completed}}
	p, summary := newProcessor(t, h, true)
	outcomes, err := p.ProcessBatch([]*model.ConsumedMessage{msgs[0], &again})
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(statuses(outcomes)); got != fmt.Sprint([]model.ProcessingStatus{model.Processed, model.Duplicate}) {
		t.Fatalf("statuses %s", got)
	}
	expectCalls(t, h, "reserve", "reserve", "persist", "complete", "commit", "reserve", "persist", "commit")
	if summary.DuplicateRecords != 1 {
		t.Fatalf("summary %+v", summary)
	}
}
