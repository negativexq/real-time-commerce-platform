package dlqsink

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store writes rows to dead_letters.
type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

const insertSQL = `INSERT INTO dead_letters (
    dlq_record_id, error_category, error_type, error_message,
    source_topic, source_partition, source_offset,
    original_event_id, original_event_type, anomaly_type,
    processing_attempts, consumer_group, failed_at,
    dlq_topic, dlq_partition, dlq_offset, envelope
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
ON CONFLICT (dlq_record_id) DO NOTHING`

// Insert stores a row and reports whether it was new. A row that already
// exists is left untouched, including its triage status.
func (s *Store) Insert(ctx context.Context, r Row) (bool, error) {
	tag, err := s.pool.Exec(ctx, insertSQL,
		r.RecordID, r.ErrorCategory, r.ErrorType, r.ErrorMessage,
		r.SourceTopic, r.SourcePartition, r.SourceOffset,
		r.OriginalEventID, r.OriginalEventType, r.AnomalyType,
		r.ProcessingAttempts, r.ConsumerGroup, r.FailedAt,
		r.DLQTopic, r.DLQPartition, r.DLQOffset, []byte(r.Envelope),
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
