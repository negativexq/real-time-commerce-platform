// Package contracts ports shared.schemas: strict, versioned commerce event
// envelopes, canonical serialization (which feeds the durable payload hash)
// and the Kafka key/header rules in shared.kafka_metadata.
package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

const CurrentEventVersion = 1

// MaxProducerClockSkew mirrors MAX_PRODUCER_CLOCK_SKEW.
const MaxProducerClockSkew = 5 * time.Minute

type EventType string

const (
	UserRegisteredType    EventType = "user_registered"
	SessionStartedType    EventType = "session_started"
	ProductViewedType     EventType = "product_viewed"
	AddedToCartType       EventType = "added_to_cart"
	CheckoutStartedType   EventType = "checkout_started"
	OrderCreatedType      EventType = "order_created"
	PaymentCompletedType  EventType = "payment_completed"
	PaymentFailedType     EventType = "payment_failed"
	RefundRequestedType   EventType = "refund_requested"
	FraudAlertCreatedType EventType = "fraud_alert_created"
)

// AllEventTypes lists every registered type in declaration order.
var AllEventTypes = []EventType{
	UserRegisteredType, SessionStartedType, ProductViewedType, AddedToCartType,
	CheckoutStartedType, OrderCreatedType, PaymentCompletedType, PaymentFailedType,
	RefundRequestedType, FraudAlertCreatedType,
}

var payloadParsers = map[EventType]func([]byte) (Payload, error){
	UserRegisteredType:    parseUserRegistered,
	SessionStartedType:    parseSessionStarted,
	ProductViewedType:     parseProductViewed,
	AddedToCartType:       parseAddedToCart,
	CheckoutStartedType:   parseCheckoutStarted,
	OrderCreatedType:      parseOrderCreated,
	PaymentCompletedType:  parsePaymentCompleted,
	PaymentFailedType:     parsePaymentFailed,
	RefundRequestedType:   parseRefundRequested,
	FraudAlertCreatedType: parseFraudAlertCreated,
}

// IsKnownEventType reports whether s names a registered event type.
func IsKnownEventType(s string) bool {
	_, ok := payloadParsers[EventType(s)]
	return ok
}

// Envelope is a validated EventEnvelope with its registered payload.
type Envelope struct {
	EventID       uuid.UUID
	EventType     EventType
	EventVersion  int64
	EventTime     time.Time
	ProducedAt    time.Time
	Source        string
	CorrelationID uuid.UUID
	Payload       Payload

	canonical []byte
}

var envelopeFields = []string{
	"event_id", "event_type", "event_version", "event_time", "produced_at",
	"source", "correlation_id", "payload",
}

// ParseEvent ports shared.schemas.envelope.parse_event.
func ParseEvent(data []byte) (*Envelope, error) {
	f := newFields(data, envelopeFields...)
	if f.err != nil {
		return nil, f.err
	}
	rawType := f.str("event_type")
	if f.err != nil {
		return nil, f.err
	}
	parser, ok := payloadParsers[EventType(rawType)]
	if !ok {
		return nil, fmt.Errorf("unknown event type: %q", rawType)
	}
	event := &Envelope{
		EventID:       f.uuid("event_id"),
		EventType:     EventType(rawType),
		EventVersion:  f.integer("event_version", 1),
		EventTime:     f.datetime("event_time"),
		ProducedAt:    f.datetime("produced_at"),
		Source:        f.nonEmpty("source"),
		CorrelationID: f.uuid("correlation_id"),
	}
	rawPayload := f.get("payload")
	if f.err != nil {
		return nil, f.err
	}
	if kind(rawPayload) != '{' {
		return nil, errNotObject
	}
	payload, err := parser(rawPayload)
	if err != nil {
		return nil, err
	}
	event.Payload = payload
	if event.ProducedAt.Before(event.EventTime.Add(-MaxProducerClockSkew)) {
		return nil, errors.New("produced_at cannot precede event_time by more than five minutes")
	}
	return event, nil
}

// NewEnvelope builds an envelope locally (fraud alerts).
func NewEnvelope(id uuid.UUID, eventType EventType, at time.Time, source string,
	correlation uuid.UUID, payload Payload) *Envelope {
	return &Envelope{
		EventID: id, EventType: eventType, EventVersion: CurrentEventVersion,
		EventTime: at, ProducedAt: at, Source: source, CorrelationID: correlation,
		Payload: payload,
	}
}

// CanonicalJSON ports canonical_json(): the deterministic serialization
// whose SHA-256 is stored in processed_events.payload_sha256.
func (e *Envelope) CanonicalJSON() []byte {
	if e.canonical == nil {
		e.canonical = CanonicalJSON(Object{
			"event_id":       e.EventID.String(),
			"event_type":     string(e.EventType),
			"event_version":  e.EventVersion,
			"event_time":     FormatUTCZ(e.EventTime),
			"produced_at":    FormatUTCZ(e.ProducedAt),
			"source":         e.Source,
			"correlation_id": e.CorrelationID.String(),
			"payload":        e.Payload.canonical(),
		})
	}
	return e.canonical
}

// PayloadSHA256 ports canonical_payload_hash().
func (e *Envelope) PayloadSHA256() string {
	sum := sha256.Sum256(e.CanonicalJSON())
	return hex.EncodeToString(sum[:])
}

// MessageKey ports event_message_key(): customer ID when present, otherwise
// the correlation ID.
func (e *Envelope) MessageKey() []byte {
	if customer, ok := e.Payload.OrderingCustomer(); ok {
		return []byte(customer.String())
	}
	return []byte(e.CorrelationID.String())
}

// Header is one Kafka header.
type Header struct {
	Key   string
	Value []byte
}

// MessageHeaders ports event_message_headers() in its declaration order.
func (e *Envelope) MessageHeaders() []Header {
	return []Header{
		{"event_id", []byte(e.EventID.String())},
		{"event_type", []byte(e.EventType)},
		{"event_version", []byte(strconv.FormatInt(e.EventVersion, 10))},
		{"correlation_id", []byte(e.CorrelationID.String())},
		{"source", []byte(e.Source)},
		{"content_type", []byte("application/json")},
	}
}

// RequiredEventHeaders mirrors REQUIRED_EVENT_HEADERS, sorted.
var RequiredEventHeaders = []string{
	"content_type", "correlation_id", "event_id", "event_type", "event_version", "source",
}
