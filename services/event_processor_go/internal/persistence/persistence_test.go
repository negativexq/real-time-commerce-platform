package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/config"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/model"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/testenv"
)

// Pure tests port tests/unit/test_persistence_handlers.py and
// test_persistence_config.py. The database tests replace the Python
// persistence smoke scripts: each runs against its own freshly migrated
// database (see testenv) so the real SQL, constraints and rollback are
// exercised.

func TestHandlersCoverEveryEventType(t *testing.T) {
	for _, name := range []string{"user_registered", "session_started", "product_viewed", "added_to_cart",
		"checkout_started", "order_created", "payment_completed", "payment_failed", "refund_requested",
		"fraud_alert_created"} {
		if !contracts.IsKnownEventType(name) {
			t.Fatalf("%s is not a contract event type", name)
		}
		if Handlers[contracts.EventType(name)] == nil {
			t.Errorf("no persistence handler for %s", name)
		}
	}
	for eventType := range Handlers {
		if !contracts.IsKnownEventType(string(eventType)) {
			t.Errorf("handler registered for unknown event type %s", eventType)
		}
	}
}

func TestSafeEndpointNeverContainsCredentials(t *testing.T) {
	got := SafeEndpoint("postgresql://commerce:secret@db.internal:6543/commerce?sslmode=disable")
	if got != "postgresql://db.internal:6543/commerce" || strings.Contains(got, "secret") {
		t.Fatalf("endpoint %q", got)
	}
	if got := SafeEndpoint("postgresql://u:p@db/x"); got != "postgresql://db:5432/x" {
		t.Fatalf("default port endpoint %q", got)
	}
}

func TestClassifyDatabaseErrors(t *testing.T) {
	cases := []struct {
		err  error
		kind procerr.Kind
	}{
		{&pgconn.PgError{Code: "40001"}, procerr.RetryableDatabase},
		{&pgconn.PgError{Code: "40P01"}, procerr.RetryableDatabase},
		{&pgconn.PgError{Code: "08006"}, procerr.RetryableDatabase},
		{&pgconn.PgError{Code: "23505"}, procerr.PermanentDatabaseIntegrity},
		{&pgconn.PgError{Code: "23503"}, procerr.PermanentDatabaseIntegrity},
		{context.DeadlineExceeded, procerr.RetryableDatabase},
		{procerr.New(procerr.MissingBusinessDependency, "x"), procerr.MissingBusinessDependency},
		{errors.New("unexpected"), procerr.PermanentDatabaseIntegrity},
	}
	for _, tc := range cases {
		if got := classify(tc.err); !procerr.Is(got, tc.kind) {
			t.Errorf("classify(%v) = %v", tc.err, got)
		}
	}
}

// --- database-backed tests -------------------------------------------------

type harness struct {
	t      *testing.T
	pool   *pgxpool.Pool
	uow    *UnitOfWorkFactory
	offset int64
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dsn := testenv.FreshDatabase(t)
	env := map[string]string{"POSTGRES_DSN": dsn, "PROCESSOR_DB_POOL_MAX_SIZE": "2"}
	cfg, err := config.LoadProcessor(nil, func(n string) (string, bool) { v, ok := env[n]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	fraudCfg, err := config.LoadFraud(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	db, err := NewDatabase(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	uow, err := NewUnitOfWorkFactory(db, cfg, fraudCfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &harness{t: t, pool: db.Pool, uow: uow}
}

var base = time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

type ids struct {
	customer, session, device, product, cart, order, payment, failedPayment, refund, alert uuid.UUID
}

func newIDs() ids {
	return ids{uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()}
}

func (h *harness) event(eventType string, at time.Duration, payload map[string]any) *contracts.Envelope {
	h.t.Helper()
	when := base.Add(at).Format(time.RFC3339)
	raw, _ := json.Marshal(map[string]any{
		"event_id": uuid.NewString(), "event_type": eventType, "event_version": 1,
		"event_time": when, "produced_at": when, "source": "go-persistence-test",
		"correlation_id": uuid.NewString(), "payload": payload,
	})
	e, err := contracts.ParseEvent(raw)
	if err != nil {
		h.t.Fatalf("%s fixture is invalid: %v", eventType, err)
	}
	return e
}

func (h *harness) persist(e *contracts.Envelope) (*Result, error) {
	h.offset++
	return h.persistAt(e, h.offset)
}

func (h *harness) persistAt(e *contracts.Envelope, offset int64) (*Result, error) {
	m := &model.ConsumedMessage{Topic: "commerce.events", Partition: 0, Offset: offset}
	ctx := model.ProcessingContext{Topic: m.Topic, Partition: 0, Offset: offset,
		ConsumerGroup: "go-test", ProcessorInstanceID: "go-test-1", Attempt: 1}
	return h.uow.Persist(e, m, ctx, Handlers[e.EventType])
}

func (h *harness) count(table, where string, args ...any) int {
	h.t.Helper()
	var n int
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	if where != "" {
		q += " WHERE " + where
	}
	if err := h.pool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		h.t.Fatalf("%s: %v", q, err)
	}
	return n
}

func journey(h *harness, id ids) []*contracts.Envelope {
	c := id.customer.String()
	ts := func(d time.Duration) string { return base.Add(d).Format(time.RFC3339) }
	events := []*contracts.Envelope{
		h.event("user_registered", 0, map[string]any{"customer_id": c, "email_hash": "sha256:go-test",
			"country_code": "TR", "persona": "normal", "registered_at": ts(0)}),
		h.event("session_started", time.Minute, map[string]any{"session_id": id.session.String(), "customer_id": c,
			"device_id": id.device.String(), "device_type": "mobile", "ip_address": "2001:db8::1",
			"country_code": "TR", "channel": "web", "started_at": ts(time.Minute)}),
		h.event("product_viewed", 2*time.Minute, map[string]any{"session_id": id.session.String(), "customer_id": c,
			"product_id": id.product.String(), "category": "books", "unit_price": "59.95", "currency": "TRY",
			"quantity_available": 10}),
		h.event("added_to_cart", 3*time.Minute, map[string]any{"session_id": id.session.String(), "customer_id": c,
			"cart_id": id.cart.String(), "product_id": id.product.String(), "quantity": 2,
			"unit_price": "59.95", "currency": "TRY"}),
		h.event("checkout_started", 4*time.Minute, map[string]any{"session_id": id.session.String(), "customer_id": c,
			"cart_id": id.cart.String(), "item_count": 2, "subtotal": "119.90", "discount_amount": "0",
			"total_amount": "119.90", "currency": "TRY"}),
		h.event("order_created", 5*time.Minute, map[string]any{"order_id": id.order.String(), "customer_id": c,
			"session_id": id.session.String(), "cart_id": id.cart.String(), "item_count": 2,
			"subtotal": "119.90", "discount_amount": "0", "total_amount": "119.90", "currency": "TRY",
			"shipping_country_code": "TR", "billing_country_code": "TR", "created_at": ts(5 * time.Minute)}),
		h.event("payment_failed", 6*time.Minute, map[string]any{"payment_id": id.failedPayment.String(),
			"order_id": id.order.String(), "customer_id": c, "amount": "119.90", "currency": "TRY",
			"payment_method": "credit_card", "failure_reason": "declined", "device_id": id.device.String(),
			"ip_address": "2001:db8::1", "country_code": "TR", "failed_at": ts(6 * time.Minute)}),
		h.event("payment_completed", 7*time.Minute, map[string]any{"payment_id": id.payment.String(),
			"order_id": id.order.String(), "customer_id": c, "amount": "119.90", "currency": "TRY",
			"payment_method": "credit_card", "device_id": id.device.String(), "ip_address": "2001:db8::1",
			"country_code": "TR", "completed_at": ts(7 * time.Minute)}),
		h.event("refund_requested", 8*time.Minute, map[string]any{"refund_id": id.refund.String(),
			"payment_id": id.payment.String(), "order_id": id.order.String(), "customer_id": c,
			"amount": "50.00", "currency": "TRY", "reason": "damaged", "requested_at": ts(8 * time.Minute)}),
	}
	// An externally raised alert must reference an already processed event.
	return append(events, h.event("fraud_alert_created", 9*time.Minute, map[string]any{
		"alert_id": id.alert.String(), "event_id": events[5].EventID.String(), "customer_id": c,
		"order_id": id.order.String(), "fraud_score": "75", "decision": "REVIEW",
		"reasons": []string{"EXTERNAL_SIGNAL"}, "created_at": ts(9 * time.Minute)}))
}

func TestJourneyPersistsEveryTableOnceAndReplaysIdempotently(t *testing.T) {
	h := newHarness(t)
	id := newIDs()
	events := journey(h, id)
	expect := map[string]string{"user_registered": "customers", "session_started": "sessions",
		"product_viewed": "product_views", "added_to_cart": "cart_items", "checkout_started": "carts",
		"order_created": "orders", "payment_failed": "payments", "payment_completed": "payments",
		"refund_requested": "refunds", "fraud_alert_created": "fraud_alerts"}
	for _, e := range events {
		res, err := h.persist(e)
		if err != nil {
			t.Fatalf("%s: %v", e.EventType, err)
		}
		if res.AlreadyPersisted || res.RowsWritten[expect[string(e.EventType)]] == 0 || res.RowsWritten["processed_events"] != 1 {
			t.Fatalf("%s result %+v", e.EventType, res)
		}
		if fraudEligible(e) && res.FraudDecision == "" {
			t.Fatalf("%s was not fraud-evaluated", e.EventType)
		}
	}
	tables := map[string]int{"processed_events": len(events), "customers": 1, "sessions": 1, "product_views": 1,
		"carts": 1, "cart_items": 1, "orders": 1, "payments": 2, "refunds": 1, "fraud_evaluations": 5}
	snapshot := func() map[string]int {
		got := map[string]int{}
		for table := range tables {
			got[table] = h.count(table, "")
		}
		return got
	}
	first := snapshot()
	for table, want := range tables {
		if first[table] != want {
			t.Errorf("%s has %d rows, want %d", table, first[table], want)
		}
	}
	var cartStatus string
	if err := h.pool.QueryRow(context.Background(), "SELECT status FROM carts WHERE cart_id = $1", id.cart).Scan(&cartStatus); err != nil || cartStatus != "ordered" {
		t.Fatalf("cart status %q (%v)", cartStatus, err)
	}

	// Redelivery of every event, at the same Kafka coordinates, writes nothing.
	for i, e := range events {
		res, err := h.persistAt(e, int64(i+1))
		if err != nil || !res.AlreadyPersisted {
			t.Fatalf("replay of %s: %+v %v", e.EventType, res, err)
		}
	}
	if again := snapshot(); fmt.Sprint(again) != fmt.Sprint(first) {
		t.Fatalf("replay changed row counts: %v -> %v", first, again)
	}
}

func fraudEligible(e *contracts.Envelope) bool {
	switch e.EventType {
	case contracts.CheckoutStartedType, contracts.OrderCreatedType, contracts.PaymentFailedType,
		contracts.PaymentCompletedType, contracts.RefundRequestedType:
		return true
	}
	return false
}

func TestConflictingContentForSameEventIDIsIntegrityFailure(t *testing.T) {
	h := newHarness(t)
	e := journey(h, newIDs())[0]
	if _, err := h.persist(e); err != nil {
		t.Fatal(err)
	}
	conflicting, err := contracts.ParseEvent(contracts.NewEnvelope(e.EventID, e.EventType, e.EventTime,
		"different", e.CorrelationID, e.Payload).CanonicalJSON())
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.persist(conflicting)
	if !procerr.Is(err, procerr.PermanentDatabaseIntegrity) {
		t.Fatalf("error %v", err)
	}
}

func TestKafkaCoordinateReuseIsIntegrityFailure(t *testing.T) {
	h := newHarness(t)
	events := journey(h, newIDs())
	if _, err := h.persistAt(events[0], 7); err != nil {
		t.Fatal(err)
	}
	if _, err := h.persistAt(events[1], 7); !procerr.Is(err, procerr.PermanentDatabaseIntegrity) {
		t.Fatalf("error %v", err)
	}
}

func TestMissingDependencyRollsBackTheWholeTransaction(t *testing.T) {
	h := newHarness(t)
	order := journey(h, newIDs())[5] // order_created with no customer, session or cart
	_, err := h.persist(order)
	if !procerr.IsMissingDependency(err) || !procerr.IsRetryable(err) {
		t.Fatalf("error %v", err)
	}
	if n := h.count("processed_events", "event_id = $1", order.EventID); n != 0 {
		t.Fatalf("ledger row survived rollback: %d", n)
	}
	if n := h.count("orders", ""); n != 0 {
		t.Fatalf("orders row survived rollback: %d", n)
	}
}

func TestRefundAbovePaidAmountIsRejected(t *testing.T) {
	h := newHarness(t)
	id := newIDs()
	events := journey(h, id)
	for _, e := range events[:8] { // up to and including payment_completed
		if _, err := h.persist(e); err != nil {
			t.Fatalf("%s: %v", e.EventType, err)
		}
	}
	tooMuch := h.event("refund_requested", 9*time.Minute, map[string]any{"refund_id": uuid.NewString(),
		"payment_id": id.payment.String(), "order_id": id.order.String(), "customer_id": id.customer.String(),
		"amount": "200.00", "currency": "TRY", "reason": "greedy", "requested_at": base.Add(9 * time.Minute).Format(time.RFC3339)})
	if _, err := h.persist(tooMuch); !procerr.Is(err, procerr.PermanentDatabaseIntegrity) {
		t.Fatalf("error %v", err)
	}
	if n := h.count("refunds", ""); n != 0 {
		t.Fatalf("refund row written: %d", n)
	}
}
