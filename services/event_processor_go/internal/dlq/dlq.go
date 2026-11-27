// Package dlq ports services.event_processor.dlq: deterministic dead-letter
// records and a confirmed, idempotent Kafka publication.
package dlq

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/config"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/model"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
)

var namespace = uuid.MustParse("bb8709f1-40b8-4a8f-93ca-96775f4c7d7a")

const (
	schemaVersion          = 1
	maxErrorMessageLength  = 512
	maxSanitizedHeaderRune = 256
)

var safeHeaderNames = map[string]bool{
	"event_id": true, "event_type": true, "event_version": true, "correlation_id": true,
	"source": true, "content_type": true, "synthetic_anomaly": true,
}

// Envelope is the processor-owned DLQ record schema.
type Envelope struct {
	RecordID      uuid.UUID
	ErrorCategory string
	SourceTopic   string
	SourcePart    int32
	SourceOffset  int64
	OriginalEvent *uuid.UUID
	body          []byte
}

// DeterministicID ports deterministic_dlq_id().
func DeterministicID(m *model.ConsumedMessage, category string) uuid.UUID {
	identity := fmt.Sprintf("%s:%d:%d:%s", m.Topic, m.Partition, m.Offset, category)
	return uuid.NewSHA1(namespace, []byte(identity))
}

func optionalUUID(u *uuid.UUID) any {
	if u == nil {
		return nil
	}
	return u.String()
}

func optionalString(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n])
}

// Build ports build_dlq_envelope().
func Build(m *model.ConsumedMessage, e *model.ValidationErrorInfo, attempts int,
	consumerGroup, instanceID string, maxPayloadBytes int, failedAt time.Time) *Envelope {
	var included []byte
	truncated := false
	if m.Value != nil {
		included = m.Value
		if len(included) > maxPayloadBytes {
			included = included[:maxPayloadBytes]
			truncated = true
		}
	}
	sanitized := contracts.Object{}
	for _, h := range m.Headers {
		if !safeHeaderNames[h.Key] || h.Value == nil {
			continue
		}
		if _, seen := sanitized[h.Key]; seen {
			continue
		}
		if utf8.Valid(h.Value) {
			sanitized[h.Key] = truncateRunes(string(h.Value), maxSanitizedHeaderRune)
		} else {
			sanitized[h.Key] = "<invalid-utf8>"
		}
	}
	message := truncateRunes(e.Message, maxErrorMessageLength)
	record := &Envelope{
		RecordID:      DeterministicID(m, string(e.Category)),
		ErrorCategory: string(e.Category),
		SourceTopic:   m.Topic,
		SourcePart:    m.Partition,
		SourceOffset:  m.Offset,
		OriginalEvent: e.EventID,
	}
	var sourceTimestamp, sourceKey, originalValue any
	if m.Timestamp != nil {
		sourceTimestamp = contracts.FormatUTCZ(*m.Timestamp)
	}
	if m.Key != nil {
		sourceKey = base64.StdEncoding.EncodeToString(m.Key)
	}
	if m.Value != nil {
		originalValue = base64.StdEncoding.EncodeToString(included)
	}
	record.body = contracts.CanonicalJSON(contracts.Object{
		"dlq_schema_version":       int64(schemaVersion),
		"dlq_record_id":            record.RecordID.String(),
		"failed_at":                contracts.FormatUTCZ(failedAt),
		"source_topic":             m.Topic,
		"source_partition":         int64(m.Partition),
		"source_offset":            m.Offset,
		"source_timestamp":         sourceTimestamp,
		"source_key_base64":        sourceKey,
		"source_headers":           sanitized,
		"original_value_base64":    originalValue,
		"original_value_truncated": truncated,
		"error_category":           string(e.Category),
		"error_message":            message,
		"error_type":               e.ErrorType,
		"processing_attempts":      int64(attempts),
		"consumer_group":           consumerGroup,
		"processor_instance_id":    instanceID,
		"original_event_id":        optionalUUID(e.EventID),
		"original_event_type":      optionalString(e.EventType),
		"original_correlation_id":  optionalUUID(e.CorrelationID),
		"anomaly_type":             optionalString(e.AnomalyType),
	})
	return record
}

func (r *Envelope) key() []byte {
	if r.OriginalEvent != nil {
		return []byte(r.OriginalEvent.String())
	}
	return []byte(fmt.Sprintf("%s:%d:%d", r.SourceTopic, r.SourcePart, r.SourceOffset))
}

func (r *Envelope) headers() []kafka.Header {
	hs := []kafka.Header{
		{Key: "dlq_record_id", Value: []byte(r.RecordID.String())},
		{Key: "error_category", Value: []byte(r.ErrorCategory)},
		{Key: "source_topic", Value: []byte(r.SourceTopic)},
		{Key: "source_partition", Value: []byte(strconv.Itoa(int(r.SourcePart)))},
		{Key: "source_offset", Value: []byte(strconv.FormatInt(r.SourceOffset, 10))},
		{Key: "content_type", Value: []byte("application/json")},
		{Key: "dlq_schema_version", Value: []byte(strconv.Itoa(schemaVersion))},
	}
	if r.OriginalEvent != nil {
		hs = append(hs, kafka.Header{Key: "original_event_id", Value: []byte(r.OriginalEvent.String())})
	}
	return hs
}

// Body exposes the canonical record bytes (tests).
func (r *Envelope) Body() []byte { return r.body }

// Publisher waits boundedly for each DLQ delivery report.
type Publisher struct {
	cfg      *config.Processor
	producer *kafka.Producer
}

func NewPublisher(cfg *config.Processor) (*Publisher, error) {
	requestTimeout := cfg.DLQDeliveryTimeoutMs
	if requestTimeout > 10_000 {
		requestTimeout = 10_000
	}
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":   cfg.KafkaBootstrapServers,
		"client.id":           cfg.ClientID + "-dlq",
		"enable.idempotence":  true,
		"acks":                "all",
		"delivery.timeout.ms": cfg.DLQDeliveryTimeoutMs,
		"request.timeout.ms":  requestTimeout,
	})
	if err != nil {
		return nil, err
	}
	go func() {
		for ev := range p.Events() {
			if kerr, ok := ev.(kafka.Error); ok {
				slog.Warn("dlq_producer_error", "error", kerr.Error(), "fatal", kerr.IsFatal())
			}
		}
	}()
	return &Publisher{cfg: cfg, producer: p}, nil
}

func (p *Publisher) Publish(r *Envelope) error {
	delivery := make(chan kafka.Event, 1)
	topic := p.cfg.DLQTopic
	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            r.key(),
		Value:          r.body,
		Headers:        r.headers(),
	}, delivery)
	if err != nil {
		return procerr.Wrap(procerr.RetryableProcessing, "DLQ publication failed", err)
	}
	timeout := time.Duration(p.cfg.DLQDeliveryTimeoutMs) * time.Millisecond
	select {
	case ev := <-delivery:
		msg, ok := ev.(*kafka.Message)
		if !ok || msg.TopicPartition.Error != nil {
			return procerr.New(procerr.RetryableProcessing, "DLQ delivery was not confirmed")
		}
		return nil
	case <-time.After(timeout):
		return procerr.New(procerr.RetryableProcessing, "DLQ delivery was not confirmed")
	}
}

func (p *Publisher) Close() {
	p.producer.Flush(int(p.cfg.ShutdownTimeout.Milliseconds()))
	p.producer.Close()
}
