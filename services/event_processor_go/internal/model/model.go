// Package model ports services.event_processor.models.
package model

import (
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
)

type ValidationCategory string

const (
	MissingValue              ValidationCategory = "missing_value"
	MissingKey                ValidationCategory = "missing_key"
	MalformedJSON             ValidationCategory = "malformed_json"
	UnknownEventType          ValidationCategory = "unknown_event_type"
	ContractValidationFailed  ValidationCategory = "contract_validation_failed"
	MissingHeader             ValidationCategory = "missing_header"
	DuplicateHeader           ValidationCategory = "duplicate_header"
	InvalidHeaderEncoding     ValidationCategory = "invalid_header_encoding"
	InvalidContentType        ValidationCategory = "invalid_content_type"
	HeaderBodyMismatch        ValidationCategory = "header_body_mismatch"
	KeyBodyMismatch           ValidationCategory = "key_body_mismatch"
	UnsupportedEventVersion   ValidationCategory = "unsupported_event_version"
	PermanentProcessingError  ValidationCategory = "permanent_processing_error"
	RetryExhausted            ValidationCategory = "retry_exhausted"
	MissingBusinessDependency ValidationCategory = "missing_business_dependency"
	DatabaseIntegrityError    ValidationCategory = "database_integrity_error"
)

type ProcessingStatus string

const (
	Processed  ProcessingStatus = "processed"
	Duplicate  ProcessingStatus = "duplicate"
	DLQ        ProcessingStatus = "dlq"
	Unresolved ProcessingStatus = "unresolved"
)

// Header is one Kafka header; a nil Value is a null header value.
type Header struct {
	Key   string
	Value []byte
}

// ConsumedMessage is one Kafka record handed to the processor.
type ConsumedMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp *time.Time
	Key       []byte
	Value     []byte
	Headers   []Header
}

type ValidationErrorInfo struct {
	Category      ValidationCategory
	Message       string
	ErrorType     string
	EventID       *uuid.UUID
	EventType     *string
	CorrelationID *uuid.UUID
	AnomalyType   *string
}

type ProcessingContext struct {
	Topic               string
	Partition           int32
	Offset              int64
	ConsumerGroup       string
	ProcessorInstanceID string
	Attempt             int
}

type ProcessingOutcome struct {
	Status    ProcessingStatus
	Attempts  int
	EventType contracts.EventType
}

func (o ProcessingOutcome) Terminal() bool { return o.Status != Unresolved }

// RunSummary ports RunSummary; it is logged once at shutdown.
type RunSummary struct {
	ConsumedRecords, ValidRecords, ProcessedRecords, DuplicateRecords, DLQRecords int
	Retries, RetryExhausted, KafkaErrors, RedisErrors                             int
	CommitSuccesses, CommitFailures, ProcessingFailures, UnresolvedRecords        int
	LatencyTotalMs, LatencyMaxMs                                                  float64
	ValidationFailures                                                            map[ValidationCategory]int
	ProcessedEvents                                                               map[contracts.EventType]int
	DatabaseTransactionsStarted, DatabaseTransactionsCommitted                    int
	DatabaseTransactionsRolledBack, AlreadyPersistedEvents                        int
	DatabaseRetries, DatabaseErrors, MissingDependencyErrors, IntegrityFailures   int
	RowsWrittenByTable                                                            map[string]int
	SlowDatabaseOperations, FraudEvaluations                                      int
	ApproveDecisions, ReviewDecisions, BlockDecisions                             int
	MatchedRules                                                                  map[string]int
	FraudContextFailures, FraudIntegrityFailures                                  int
	FraudAlertsCreated, FraudOutboxRowsCreated                                    int
}

func NewRunSummary() *RunSummary {
	return &RunSummary{
		ValidationFailures: map[ValidationCategory]int{},
		ProcessedEvents:    map[contracts.EventType]int{},
		RowsWrittenByTable: map[string]int{},
		MatchedRules:       map[string]int{},
	}
}

func (s *RunSummary) RecordLatency(ms float64) {
	s.LatencyTotalMs += ms
	if ms > s.LatencyMaxMs {
		s.LatencyMaxMs = ms
	}
}

func sortedCounts[K ~string](m map[K]int) map[string]int {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	out := make(map[string]int, len(m))
	for _, k := range keys {
		out[k] = m[K(k)]
	}
	return out
}

func round3(v float64) float64 { return float64(int64(v*1000+0.5)) / 1000 }

// AsLog returns the same fields as RunSummary.as_log().
func (s *RunSummary) AsLog() []any {
	avg := 0.0
	if s.ProcessedRecords > 0 {
		avg = s.LatencyTotalMs / float64(s.ProcessedRecords)
	}
	return []any{
		"consumed_records", s.ConsumedRecords,
		"valid_records", s.ValidRecords,
		"processed_records", s.ProcessedRecords,
		"duplicate_records", s.DuplicateRecords,
		"dlq_records", s.DLQRecords,
		"validation_failures", sortedCounts(s.ValidationFailures),
		"processed_events", sortedCounts(s.ProcessedEvents),
		"retry_attempts", s.Retries,
		"retry_exhausted", s.RetryExhausted,
		"kafka_errors", s.KafkaErrors,
		"redis_errors", s.RedisErrors,
		"commit_successes", s.CommitSuccesses,
		"commit_failures", s.CommitFailures,
		"processing_failures", s.ProcessingFailures,
		"processing_latency_average_ms", round3(avg),
		"processing_latency_max_ms", round3(s.LatencyMaxMs),
		"unresolved_records", s.UnresolvedRecords,
		"database_transactions_started", s.DatabaseTransactionsStarted,
		"database_transactions_committed", s.DatabaseTransactionsCommitted,
		"database_transactions_rolled_back", s.DatabaseTransactionsRolledBack,
		"already_persisted_events", s.AlreadyPersistedEvents,
		"database_retries", s.DatabaseRetries,
		"database_errors", s.DatabaseErrors,
		"missing_dependency_errors", s.MissingDependencyErrors,
		"integrity_failures", s.IntegrityFailures,
		"rows_written_by_table", sortedCounts(s.RowsWrittenByTable),
		"slow_database_operations", s.SlowDatabaseOperations,
		"fraud_evaluations", s.FraudEvaluations,
		"approve_decisions", s.ApproveDecisions,
		"review_decisions", s.ReviewDecisions,
		"block_decisions", s.BlockDecisions,
		"matched_rules", sortedCounts(s.MatchedRules),
		"fraud_context_failures", s.FraudContextFailures,
		"fraud_integrity_failures", s.FraudIntegrityFailures,
		"fraud_alerts_created", s.FraudAlertsCreated,
		"fraud_outbox_rows_created", s.FraudOutboxRowsCreated,
	}
}
