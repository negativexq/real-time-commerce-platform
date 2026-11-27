package processor

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/config"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/dbtx"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/dlq"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/idempotency"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/model"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/persistence"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
)

// These tests port tests/unit/test_processor_orchestration.py and
// test_processor_retry.py: the order of Redis, persistence, DLQ and commit
// calls decides delivery guarantees, so each test asserts the call log.

const orderCreated = `{"correlation_id":"00000000-0000-4000-8000-000000000002","event_id":"00000000-0000-4000-8000-000000000001","event_time":"2026-01-01T00:00:00Z","event_type":"order_created","event_version":1,"payload":{"billing_country_code":"TR","cart_id":"00000000-0000-4000-8000-000000000006","created_at":"2026-01-01T00:00:00Z","currency":"TRY","customer_id":"00000000-0000-4000-8000-000000000003","discount_amount":"0","item_count":1,"order_id":"00000000-0000-4000-8000-000000000004","session_id":"00000000-0000-4000-8000-000000000005","shipping_country_code":"TR","subtotal":"10.00","total_amount":"10.00"},"produced_at":"2026-01-01T00:00:00Z","source":"tests"}`

func message(t *testing.T) *model.ConsumedMessage {
	t.Helper()
	event, err := contracts.ParseEvent([]byte(orderCreated))
	if err != nil {
		t.Fatal(err)
	}
	m := &model.ConsumedMessage{Topic: "commerce.events", Partition: 1, Offset: 10,
		Key: event.MessageKey(), Value: []byte(orderCreated)}
	for _, h := range event.MessageHeaders() {
		m.Headers = append(m.Headers, model.Header{Key: h.Key, Value: h.Value})
	}
	return m
}

type harness struct {
	calls       []string
	reserve     idempotency.ReservationState
	reserveErr  error
	completeOK  bool
	persistErrs []error
	alreadyDone bool
	dlqErr      error
	commitErr   error
	dlqRecords  []*dlq.Envelope
	// Per-call overrides for batch tests, consumed in order.
	reserveSeq  []idempotency.ReservationState
	completeSeq []bool
	groupErr    error
}

func (h *harness) Reserve(context.Context, uuid.UUID, string, *model.ConsumedMessage, time.Time) (idempotency.ReservationState, error) {
	h.calls = append(h.calls, "reserve")
	if len(h.reserveSeq) > 0 {
		state := h.reserveSeq[0]
		h.reserveSeq = h.reserveSeq[1:]
		return state, nil
	}
	return h.reserve, h.reserveErr
}

func (h *harness) Complete(context.Context, uuid.UUID, string, time.Time) (bool, error) {
	h.calls = append(h.calls, "complete")
	if len(h.completeSeq) > 0 {
		ok := h.completeSeq[0]
		h.completeSeq = h.completeSeq[1:]
		return ok, nil
	}
	return h.completeOK, nil
}

func (h *harness) Release(context.Context, uuid.UUID, string) (bool, error) {
	h.calls = append(h.calls, "release")
	return true, nil
}

func (h *harness) Publish(r *dlq.Envelope) error {
	h.calls = append(h.calls, "dlq:"+r.ErrorCategory)
	h.dlqRecords = append(h.dlqRecords, r)
	return h.dlqErr
}

func (h *harness) CommitTerminal(*model.ConsumedMessage) error {
	h.calls = append(h.calls, "commit")
	return h.commitErr
}

func (h *harness) Persist(*contracts.Envelope, *model.ConsumedMessage, model.ProcessingContext, persistence.Handler) (*persistence.Result, error) {
	h.calls = append(h.calls, "persist")
	if len(h.persistErrs) > 0 {
		err := h.persistErrs[0]
		h.persistErrs = h.persistErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return &persistence.Result{AlreadyPersisted: h.alreadyDone, RowsWritten: map[string]int{"orders": 1}}, nil
}

func (h *harness) PersistGroup(items []persistence.GroupItem) ([]*persistence.Result, error) {
	h.calls = append(h.calls, fmt.Sprintf("persist_group:%d", len(items)))
	if h.groupErr != nil {
		return nil, h.groupErr
	}
	results := make([]*persistence.Result, len(items))
	for i := range items {
		results[i] = &persistence.Result{RowsWritten: map[string]int{"orders": 1}}
	}
	return results, nil
}

func processorConfig(t *testing.T) *config.Processor {
	t.Helper()
	cfg, err := config.LoadProcessor(nil, func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func newProcessor(t *testing.T, h *harness, withHandler bool) (*Processor, *model.RunSummary) {
	t.Helper()
	handlers := map[contracts.EventType]persistence.Handler{}
	if withHandler {
		handlers[contracts.OrderCreatedType] = func(*dbtx.Tx, *contracts.Envelope) (map[string]int, error) { return nil, nil }
	}
	summary := model.NewRunSummary()
	p := New(processorConfig(t), h, h, h, handlers, h, summary, "instance-1", nil)
	p.wait = func(time.Duration) {}
	return p, summary
}

func run(t *testing.T, h *harness, withHandler bool, m *model.ConsumedMessage) (model.ProcessingOutcome, error, *model.RunSummary) {
	t.Helper()
	p, summary := newProcessor(t, h, withHandler)
	outcome, err := p.Process(m)
	return outcome, err, summary
}

func expectCalls(t *testing.T, h *harness, want ...string) {
	t.Helper()
	if !reflect.DeepEqual(h.calls, want) {
		t.Fatalf("calls %v, want %v", h.calls, want)
	}
}

func TestSuccessCompletesRedisBeforeCommit(t *testing.T) {
	h := &harness{reserve: idempotency.Reserved, completeOK: true}
	outcome, err, summary := run(t, h, true, message(t))
	if err != nil || outcome.Status != model.Processed || outcome.Attempts != 1 {
		t.Fatalf("outcome %+v err %v", outcome, err)
	}
	expectCalls(t, h, "reserve", "persist", "complete", "commit")
	if summary.ProcessedRecords != 1 || summary.CommitSuccesses != 1 || summary.RowsWrittenByTable["orders"] != 1 {
		t.Fatalf("summary %+v", summary)
	}
}

func TestCompletedDuplicateRecoversPersistenceAndCommits(t *testing.T) {
	h := &harness{reserve: idempotency.Completed, alreadyDone: true}
	outcome, err, summary := run(t, h, true, message(t))
	if err != nil || outcome.Status != model.Duplicate {
		t.Fatalf("outcome %+v err %v", outcome, err)
	}
	// Redelivery after Redis completion but before commit re-runs the
	// idempotent persistence check, never a second write.
	expectCalls(t, h, "reserve", "persist", "commit")
	if summary.DuplicateRecords != 1 || summary.DatabaseTransactionsCommitted != 0 {
		t.Fatalf("summary %+v", summary)
	}
}

func TestActiveLeaseIsUnresolvedAndNotCommitted(t *testing.T) {
	h := &harness{reserve: idempotency.Processing}
	outcome, err, summary := run(t, h, true, message(t))
	if err != nil || outcome.Status != model.Unresolved {
		t.Fatalf("outcome %+v err %v", outcome, err)
	}
	expectCalls(t, h, "reserve")
	if summary.UnresolvedRecords != 1 {
		t.Fatalf("summary %+v", summary)
	}
}

func TestRedisUnavailableIsUnresolved(t *testing.T) {
	h := &harness{reserveErr: errors.New("down")}
	outcome, _, summary := run(t, h, true, message(t))
	if outcome.Status != model.Unresolved || summary.RedisErrors != 1 {
		t.Fatalf("outcome %+v summary %+v", outcome, summary)
	}
	expectCalls(t, h, "reserve")
}

func TestRedisCompletionFailureIsUnresolvedAndNotCommitted(t *testing.T) {
	h := &harness{reserve: idempotency.Reserved, completeOK: false}
	outcome, err, _ := run(t, h, true, message(t))
	if err != nil || outcome.Status != model.Unresolved {
		t.Fatalf("outcome %+v err %v", outcome, err)
	}
	expectCalls(t, h, "reserve", "persist", "complete")
}

func TestValidationFailurePublishesDLQThenCommits(t *testing.T) {
	m := message(t)
	m.Value = []byte("{")
	h := &harness{}
	outcome, err, summary := run(t, h, true, m)
	if err != nil || outcome.Status != model.DLQ {
		t.Fatalf("outcome %+v err %v", outcome, err)
	}
	expectCalls(t, h, "dlq:malformed_json", "commit")
	if summary.ValidationFailures[model.MalformedJSON] != 1 || summary.DLQRecords != 1 {
		t.Fatalf("summary %+v", summary)
	}
}

func TestDLQFailureNeverCommitsSource(t *testing.T) {
	m := message(t)
	m.Value = []byte("{")
	h := &harness{dlqErr: errors.New("broker down")}
	outcome, err, summary := run(t, h, true, m)
	if err != nil || outcome.Status != model.Unresolved || summary.UnresolvedRecords != 1 {
		t.Fatalf("outcome %+v err %v", outcome, err)
	}
	expectCalls(t, h, "dlq:malformed_json")
}

func TestRetryThenSuccessCompletesAndCommits(t *testing.T) {
	h := &harness{reserve: idempotency.Reserved, completeOK: true,
		persistErrs: []error{procerr.New(procerr.RetryableDatabase, "deadlock")}}
	outcome, err, summary := run(t, h, true, message(t))
	if err != nil || outcome.Status != model.Processed || outcome.Attempts != 2 {
		t.Fatalf("outcome %+v err %v", outcome, err)
	}
	expectCalls(t, h, "reserve", "persist", "persist", "complete", "commit")
	if summary.Retries != 1 || summary.DatabaseRetries != 1 || summary.DatabaseTransactionsRolledBack != 1 {
		t.Fatalf("summary %+v", summary)
	}
}

func TestExhaustedFailureReleasesThenDeadLetters(t *testing.T) {
	missing := procerr.New(procerr.MissingBusinessDependency, "customer is missing")
	h := &harness{reserve: idempotency.Reserved, persistErrs: []error{missing, missing, missing}}
	outcome, err, summary := run(t, h, true, message(t))
	if err != nil || outcome.Status != model.DLQ || outcome.Attempts != 3 {
		t.Fatalf("outcome %+v err %v", outcome, err)
	}
	expectCalls(t, h, "reserve", "persist", "persist", "persist", "release", "dlq:missing_business_dependency", "commit")
	if summary.RetryExhausted != 1 || summary.MissingDependencyErrors != 1 {
		t.Fatalf("summary %+v", summary)
	}
	body := string(h.dlqRecords[0].Body())
	for _, want := range []string{`"error_type":"MissingBusinessDependencyError"`, `"processing_attempts":3`,
		`"original_event_id":"00000000-0000-4000-8000-000000000001"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("DLQ body lacks %s: %s", want, body)
		}
	}
}

func TestPermanentFailureDeadLettersWithoutRetry(t *testing.T) {
	h := &harness{reserve: idempotency.Reserved,
		persistErrs: []error{procerr.New(procerr.PermanentDatabaseIntegrity, "conflict")}}
	outcome, _, summary := run(t, h, true, message(t))
	if outcome.Status != model.DLQ || outcome.Attempts != 1 {
		t.Fatalf("outcome %+v", outcome)
	}
	expectCalls(t, h, "reserve", "persist", "release", "dlq:database_integrity_error", "commit")
	if summary.IntegrityFailures != 1 {
		t.Fatalf("summary %+v", summary)
	}
}

func TestMissingHandlerIsPermanentDLQ(t *testing.T) {
	h := &harness{reserve: idempotency.Reserved}
	outcome, _, _ := run(t, h, false, message(t))
	if outcome.Status != model.DLQ {
		t.Fatalf("outcome %+v", outcome)
	}
	expectCalls(t, h, "reserve", "release", "dlq:permanent_processing_error", "commit")
}

func TestCommitFailureIsFatal(t *testing.T) {
	h := &harness{reserve: idempotency.Reserved, completeOK: true, commitErr: errors.New("rebalance")}
	outcome, err, summary := run(t, h, true, message(t))
	var fatal *FatalError
	if !errors.As(err, &fatal) || outcome.Status != model.Unresolved || summary.CommitFailures != 1 {
		t.Fatalf("outcome %+v err %v", outcome, err)
	}
}

func TestBackoffIsExponentialCappedAndJittered(t *testing.T) {
	h := &harness{}
	p, _ := newProcessor(t, h, true)
	p.cfg.RetryInitialBackoffMs, p.cfg.RetryMaxBackoffMs, p.cfg.RetryMultiplier = 100, 1000, 2
	p.cfg.RetryJitterRatio = 0
	var got []time.Duration
	for attempt := 1; attempt <= 6; attempt++ {
		got = append(got, p.backoff(attempt))
	}
	want := []time.Duration{100, 200, 400, 800, 1000, 1000}
	for i := range want {
		want[i] *= time.Millisecond
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("backoff %v, want %v", got, want)
	}
	p.cfg.RetryJitterRatio = 0.1
	p.rng = rand.New(rand.NewSource(7))
	first := p.backoff(1)
	p.rng = rand.New(rand.NewSource(7))
	if second := p.backoff(1); first != second {
		t.Fatalf("seeded jitter differs: %v %v", first, second)
	}
	if first < 90*time.Millisecond || first > 110*time.Millisecond {
		t.Fatalf("jitter out of bounds: %v", first)
	}
}
