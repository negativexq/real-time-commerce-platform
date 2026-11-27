package processor

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/idempotency"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/metrics"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/model"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
)

func value(t *testing.T, c prometheus.Metric) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	if m.Counter != nil {
		return m.Counter.GetValue()
	}
	return m.Gauge.GetValue()
}

// Every outcome is counted once under its status and leaves nothing in flight, so
// dashboards built on the terminal counter agree with the DLQ counter.
func TestEveryOutcomeIsObservedOnce(t *testing.T) {
	invalid := message(t)
	invalid.Value = []byte("{not json")
	cases := []struct {
		name      string
		h         *harness
		m         *model.ConsumedMessage
		eventType string
		status    model.ProcessingStatus
	}{
		{"processed", &harness{reserve: idempotency.Reserved, completeOK: true}, message(t), "order_created", model.Processed},
		{"duplicate", &harness{reserve: idempotency.Completed, alreadyDone: true}, message(t), "order_created", model.Duplicate},
		{"active lease", &harness{reserve: idempotency.Processing}, message(t), "order_created", model.Unresolved},
		{"redis down", &harness{reserveErr: errors.New("down")}, message(t), "order_created", model.Unresolved},
		{"invalid", &harness{}, invalid, "unknown", model.DLQ},
		{"retry exhausted", &harness{reserve: idempotency.Reserved, persistErrs: repeat(procerr.New(procerr.MissingBusinessDependency, "customer is missing"), 10)}, message(t), "order_created", model.DLQ},
		{"permanent", &harness{reserve: idempotency.Reserved, persistErrs: []error{procerr.New(procerr.PermanentDatabaseIntegrity, "conflict")}}, message(t), "order_created", model.DLQ},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := metrics.New("test-processor", "commerce")
			p, _ := newProcessor(t, tc.h, true)
			p.metrics = m
			outcome, _ := p.Process(tc.m)
			if outcome.Status != tc.status {
				t.Fatalf("status %s, want %s", outcome.Status, tc.status)
			}
			if got := value(t, m.EventsTerminal.WithLabelValues(tc.eventType, string(tc.status))); got != 1 {
				t.Fatalf("terminal{%s,%s} = %v, want 1", tc.eventType, tc.status, got)
			}
			if got := value(t, m.Inflight); got != 0 {
				t.Fatalf("inflight = %v, want 0", got)
			}
		})
	}
}

func repeat(err error, n int) []error {
	out := make([]error, n)
	for i := range out {
		out[i] = err
	}
	return out
}
