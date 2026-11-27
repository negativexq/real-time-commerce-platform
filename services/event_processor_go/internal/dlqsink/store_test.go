package dlqsink

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/testenv"
)

func TestInsertIsIdempotentAndKeepsTriageStatus(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testenv.FreshDatabase(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := NewStore(pool)

	var body string
	for _, c := range loadReference(t) {
		if c.DLQBody != "" {
			body = c.DLQBody
			break
		}
	}
	row := Parse([]byte(body), "commerce.events.dlq", 0, 42, time.Now())

	inserted, err := store.Insert(ctx, row)
	if err != nil || !inserted {
		t.Fatalf("first insert: %v %v", inserted, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE dead_letters SET status='TRIAGED' WHERE dlq_record_id=$1", row.RecordID); err != nil {
		t.Fatal(err)
	}
	// Kafka redelivers the same record after a crash before commit.
	inserted, err = store.Insert(ctx, row)
	if err != nil || inserted {
		t.Fatalf("redelivery must be a no-op: %v %v", inserted, err)
	}

	var count int
	var status, category string
	var envelopeID string
	if err := pool.QueryRow(ctx, `SELECT count(*) OVER (), status, error_category, envelope->>'dlq_record_id'
		FROM dead_letters WHERE dlq_record_id=$1`, row.RecordID).Scan(&count, &status, &category, &envelopeID); err != nil {
		t.Fatal(err)
	}
	if count != 1 || status != "TRIAGED" || category != row.ErrorCategory || envelopeID != row.RecordID.String() {
		t.Fatalf("row: count=%d status=%s category=%s envelope=%s", count, status, category, envelopeID)
	}

	unreadable := Parse([]byte("garbage"), "commerce.events.dlq", 1, 5, time.Now())
	if inserted, err := store.Insert(ctx, unreadable); err != nil || !inserted {
		t.Fatalf("unreadable insert: %v %v", inserted, err)
	}
}
