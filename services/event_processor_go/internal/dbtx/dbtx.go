// Package dbtx is the instrumented transaction handle shared by repositories
// and the fraud context builder. It ports persistence.instrumentation: every
// statement is timed and classified into the same bounded operation labels.
package dbtx

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/metrics"
)

// Tx wraps one pgx transaction for one source event.
type Tx struct {
	Ctx     context.Context
	tx      pgx.Tx
	metrics *metrics.Metrics
	phase   string
}

func New(ctx context.Context, tx pgx.Tx, m *metrics.Metrics) *Tx {
	return &Tx{Ctx: ctx, tx: tx, metrics: m, phase: "other"}
}

func (t *Tx) SetPhase(phase string) { t.phase = phase }

func (t *Tx) Exec(sql string, args ...any) (pgconn.CommandTag, error) {
	started := time.Now()
	tag, err := t.tx.Exec(t.Ctx, sql, args...)
	t.observe(sql, started)
	return tag, err
}

// QueryRow executes and scans one row; timing covers the round trip.
func (t *Tx) QueryRow(sql string, args []any, dest ...any) error {
	started := time.Now()
	err := t.tx.QueryRow(t.Ctx, sql, args...).Scan(dest...)
	t.observe(sql, started)
	return err
}

// QueryEach executes and calls fn for every row (psycopg fetchall()).
func (t *Tx) QueryEach(sql string, args []any, fn func(pgx.Rows) error) error {
	started := time.Now()
	defer t.observe(sql, started)
	rows, err := t.tx.Query(t.Ctx, sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := fn(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (t *Tx) observe(sql string, started time.Time) {
	if t.metrics == nil {
		return
	}
	operation, kind := classify(sql, t.phase)
	t.metrics.DBSQLDuration.WithLabelValues(operation, kind).Observe(time.Since(started).Seconds())
	t.metrics.DBSQLCount.WithLabelValues(operation, kind).Inc()
}

type cacheKey struct{ sql, phase string }
type classification struct{ operation, kind string }

var classCache sync.Map

// classify ports _classify(); results are memoized per (statement, phase)
// because the statement set is static.
func classify(sql, phase string) (string, string) {
	key := cacheKey{sql, phase}
	if v, ok := classCache.Load(key); ok {
		c := v.(classification)
		return c.operation, c.kind
	}
	op, kind := classifyUncached(sql, phase)
	classCache.Store(key, classification{op, kind})
	return op, kind
}

func classifyUncached(query, phase string) (string, string) {
	normalized := strings.Join(strings.Fields(strings.ToLower(query)), " ")
	kind := "other"
	for _, k := range []string{"select", "insert", "update", "delete"} {
		if strings.HasPrefix(normalized, k) {
			kind = k
			break
		}
	}
	has := func(s string) bool { return strings.Contains(normalized, s) }
	switch {
	case has("processed_events"):
		if kind == "insert" {
			return "processed_events_insert", kind
		}
		return "processed_events_select", kind
	case has("fraud_evaluations"):
		if kind == "insert" {
			return "fraud_evaluation_write", kind
		}
		return "fraud_evaluation_select", kind
	case has("fraud_alerts"):
		return "fraud_alert_write", kind
	case has("fraud_outbox"):
		return "outbox_insert", kind
	}
	if phase == "fraud_context" {
		if kind != "select" {
			return "fraud_context_other", kind
		}
		switch {
		case has("home_country") && has("billing_country"):
			return "fraud_context_customer_order", kind
		case has("home_country"):
			return "fraud_context_customer", kind
		case has("select session_id from orders"):
			return "fraud_context_order_session", kind
		case has("started_at from sessions"):
			return "fraud_context_session", kind
		case has("ordered_at, total"):
			return "fraud_context_order", kind
		case has("requested_at, amount from refunds"):
			return "fraud_context_refunds", kind
		case has("count(*)") && has("product_views"):
			return "fraud_context_product_views", kind
		case has("count(*)") && has("orders"):
			return "fraud_context_recent_orders", kind
		case has("p.amount - coalesce"):
			return "fraud_context_refund_facts", kind
		case has("attempted_at <="):
			return "fraud_context_recent_payments", kind
		case has("attempted_at <"):
			return "fraud_context_prior_payments", kind
		}
		return "fraud_context_other", kind
	}
	if phase == "business" {
		for _, table := range []string{"customers", "sessions", "product_views", "carts", "cart_items", "orders", "payments", "refunds"} {
			if has(table) {
				return "business_" + table, kind
			}
		}
		return "business_other", kind
	}
	return "other_sql", kind
}

// UUID encodes a UUID parameter in binary.
func UUID(u uuid.UUID) pgtype.UUID { return pgtype.UUID{Bytes: u, Valid: true} }

// NullableUUID encodes an optional UUID parameter.
func NullableUUID(u *uuid.UUID) pgtype.UUID {
	if u == nil {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: *u, Valid: true}
}

// Numeric converts a scanned NUMERIC.
func Numeric(n pgtype.Numeric) (contracts.Decimal, error) { return contracts.DecimalFromNumeric(n) }
