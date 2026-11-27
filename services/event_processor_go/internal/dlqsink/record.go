// Package dlqsink copies DLQ envelopes from the commerce.events.dlq topic
// into the dead_letters table, so dead letters outlive topic retention and
// can be queried, triaged and later replayed.
//
// The processor stays the only writer of the topic and never touches
// PostgreSQL for dead letters. The sink is idempotent: rows are keyed by the
// envelope's deterministic dlq_record_id, so Kafka redelivery is harmless.
package dlqsink

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// UnreadableCategory marks a topic record that is not a DLQ envelope. It is
// stored, not skipped, so nothing on the topic is silently lost.
const UnreadableCategory = "unreadable_dlq_record"

var unreadableNamespace = uuid.MustParse("6f1d0a52-7f0e-4f53-9d0c-3c1c2b1f6a10")

// Row is one dead_letters row.
type Row struct {
	RecordID           uuid.UUID
	ErrorCategory      string
	ErrorType          *string
	ErrorMessage       string
	SourceTopic        string
	SourcePartition    *int32
	SourceOffset       *int64
	OriginalEventID    *uuid.UUID
	OriginalEventType  *string
	AnomalyType        *string
	ProcessingAttempts *int32
	ConsumerGroup      *string
	FailedAt           time.Time
	DLQTopic           string
	DLQPartition       int32
	DLQOffset          int64
	Envelope           json.RawMessage
}

// envelope is the processor's DLQ record schema (internal/dlq), read loosely:
// only dlq_record_id, error_category, source_topic and failed_at are required.
type envelope struct {
	RecordID           *uuid.UUID      `json:"dlq_record_id"`
	ErrorCategory      string          `json:"error_category"`
	ErrorType          *string         `json:"error_type"`
	ErrorMessage       *string         `json:"error_message"`
	SourceTopic        string          `json:"source_topic"`
	SourcePartition    *int32          `json:"source_partition"`
	SourceOffset       *int64          `json:"source_offset"`
	OriginalEventID    *string         `json:"original_event_id"`
	OriginalEventType  *string         `json:"original_event_type"`
	AnomalyType        *string         `json:"anomaly_type"`
	ProcessingAttempts *int32          `json:"processing_attempts"`
	ConsumerGroup      *string         `json:"consumer_group"`
	FailedAt           *time.Time      `json:"failed_at"`
	SourceHeaders      json.RawMessage `json:"source_headers"`
}

// headerIdentity reads event_id and event_type from the envelope's sanitized
// source headers (a JSON object; a JSON-encoded string of one is accepted
// too). Records that fail before parsing, such as malformed JSON, only carry
// their identity there.
func headerIdentity(raw json.RawMessage) (*uuid.UUID, *string) {
	if len(raw) == 0 {
		return nil, nil
	}
	var headers map[string]any
	if err := json.Unmarshal(raw, &headers); err != nil {
		var encoded string
		if json.Unmarshal(raw, &encoded) != nil || json.Unmarshal([]byte(encoded), &headers) != nil {
			return nil, nil
		}
	}
	var id *uuid.UUID
	if s, ok := headers["event_id"].(string); ok {
		if parsed, err := uuid.Parse(s); err == nil {
			id = &parsed
		}
	}
	var eventType *string
	if s, ok := headers["event_type"].(string); ok && s != "" {
		eventType = &s
	}
	return id, eventType
}

// Parse turns one DLQ topic record into a row. It never fails: a record that
// is not a valid envelope becomes an UnreadableCategory row whose ID is
// derived from its topic coordinates, so re-reading it is still idempotent.
func Parse(value []byte, topic string, partition int32, offset int64, observedAt time.Time) Row {
	row, err := parseEnvelope(value)
	if err != nil {
		return unreadable(value, topic, partition, offset, observedAt, err)
	}
	row.DLQTopic, row.DLQPartition, row.DLQOffset = topic, partition, offset
	return row
}

func parseEnvelope(value []byte) (Row, error) {
	var e envelope
	if err := json.Unmarshal(value, &e); err != nil {
		return Row{}, fmt.Errorf("not JSON: %w", err)
	}
	switch {
	case e.RecordID == nil:
		return Row{}, errors.New("missing dlq_record_id")
	case e.ErrorCategory == "":
		return Row{}, errors.New("missing error_category")
	case e.SourceTopic == "":
		return Row{}, errors.New("missing source_topic")
	case e.FailedAt == nil:
		return Row{}, errors.New("missing failed_at")
	}
	row := Row{
		RecordID:           *e.RecordID,
		ErrorCategory:      e.ErrorCategory,
		ErrorType:          e.ErrorType,
		SourceTopic:        e.SourceTopic,
		SourcePartition:    e.SourcePartition,
		SourceOffset:       e.SourceOffset,
		OriginalEventType:  e.OriginalEventType,
		AnomalyType:        e.AnomalyType,
		ProcessingAttempts: e.ProcessingAttempts,
		ConsumerGroup:      e.ConsumerGroup,
		FailedAt:           e.FailedAt.UTC(),
		Envelope:           json.RawMessage(value),
	}
	if e.ErrorMessage != nil {
		row.ErrorMessage = *e.ErrorMessage
	}
	// Validation failures can carry an event_id that is not a UUID; keep the
	// row and leave the typed column empty rather than reject the envelope.
	if e.OriginalEventID != nil {
		if id, err := uuid.Parse(*e.OriginalEventID); err == nil {
			row.OriginalEventID = &id
		}
	}
	if row.OriginalEventID == nil || row.OriginalEventType == nil {
		id, eventType := headerIdentity(e.SourceHeaders)
		if row.OriginalEventID == nil {
			row.OriginalEventID = id
		}
		if row.OriginalEventType == nil {
			row.OriginalEventType = eventType
		}
	}
	return row, nil
}

func unreadable(value []byte, topic string, partition int32, offset int64, observedAt time.Time, cause error) Row {
	identity := fmt.Sprintf("%s:%d:%d", topic, partition, offset)
	raw, _ := json.Marshal(map[string]string{"raw_value_base64": base64.StdEncoding.EncodeToString(value)})
	return Row{
		RecordID:      uuid.NewSHA1(unreadableNamespace, []byte(identity)),
		ErrorCategory: UnreadableCategory,
		ErrorMessage:  "DLQ topic record is not a valid envelope: " + cause.Error(),
		SourceTopic:   topic,
		FailedAt:      observedAt.UTC(),
		DLQTopic:      topic,
		DLQPartition:  partition,
		DLQOffset:     offset,
		Envelope:      raw,
	}
}
