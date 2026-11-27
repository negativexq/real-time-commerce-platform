// Package validation ports services.event_processor.validation: Kafka
// metadata checks plus shared-contract parsing, in the same order so each
// failure maps to the same DLQ category.
package validation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/model"
)

type Result struct {
	Event *contracts.Envelope
	Error *model.ValidationErrorInfo
}

func (r Result) Valid() bool { return r.Event != nil }

type hints struct {
	eventID       *uuid.UUID
	eventType     *string
	correlationID *uuid.UUID
}

func fail(category model.ValidationCategory, message string, h hints, anomaly *string) Result {
	if len(message) > 512 {
		message = message[:512]
	}
	return Result{Error: &model.ValidationErrorInfo{
		Category: category, Message: message, ErrorType: "PermanentMessageError",
		EventID: h.eventID, EventType: h.eventType, CorrelationID: h.correlationID,
		AnomalyType: anomaly,
	}}
}

func eventHints(e *contracts.Envelope) hints {
	id, corr, typ := e.EventID, e.CorrelationID, string(e.EventType)
	return hints{eventID: &id, eventType: &typ, correlationID: &corr}
}

// safeUUID mirrors UUID(str(value)) on a decoded JSON value.
func safeUUID(raw json.RawMessage) *uuid.UUID {
	if len(raw) == 0 {
		return nil
	}
	var text string
	if raw[0] == '"' {
		if json.Unmarshal(raw, &text) != nil {
			return nil
		}
	} else {
		text = string(raw)
	}
	u, err := uuid.Parse(text)
	if err != nil {
		return nil
	}
	return &u
}

func bodyHints(decoded map[string]json.RawMessage) hints {
	var h hints
	if decoded == nil {
		return h
	}
	h.eventID = safeUUID(decoded["event_id"])
	h.correlationID = safeUUID(decoded["correlation_id"])
	if raw := decoded["event_type"]; len(raw) > 0 && raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) == nil {
			h.eventType = &s
		}
	}
	return h
}

// ValidateMessage validates one record without side effects.
func ValidateMessage(m *model.ConsumedMessage) Result {
	if m.Value == nil {
		return fail(model.MissingValue, "Kafka value is required", hints{}, nil)
	}
	if m.Key == nil {
		return fail(model.MissingKey, "Kafka key is required", hints{}, nil)
	}
	grouped := make(map[string][][]byte, len(m.Headers))
	for _, h := range m.Headers {
		grouped[h.Key] = append(grouped[h.Key], h.Value)
	}
	var anomaly *string
	if values, ok := grouped["synthetic_anomaly"]; ok && values[0] != nil && utf8.Valid(values[0]) {
		s := string(values[0])
		anomaly = &s
	}
	for _, name := range contracts.RequiredEventHeaders {
		if _, ok := grouped[name]; !ok {
			return fail(model.MissingHeader, "missing required header: "+name, hints{}, anomaly)
		}
	}
	for _, name := range contracts.RequiredEventHeaders {
		if len(grouped[name]) != 1 {
			return fail(model.DuplicateHeader, "duplicate required header: "+name, hints{}, anomaly)
		}
	}
	headers := make(map[string]string, len(contracts.RequiredEventHeaders))
	for _, name := range contracts.RequiredEventHeaders {
		raw := grouped[name][0]
		if raw == nil {
			return fail(model.InvalidHeaderEncoding, "header "+name+" has no value", hints{}, anomaly)
		}
		if !utf8.Valid(raw) {
			return fail(model.InvalidHeaderEncoding, "header "+name+" is not UTF-8", hints{}, anomaly)
		}
		headers[name] = string(raw)
	}
	if headers["content_type"] != "application/json" {
		return fail(model.InvalidContentType, "content_type must be application/json", hints{}, anomaly)
	}

	var decoded map[string]json.RawMessage
	isObject := false
	if !utf8.Valid(m.Value) || !json.Valid(m.Value) {
		return fail(model.MalformedJSON, "malformed event JSON", hints{}, anomaly)
	}
	if trimmed := bytes.TrimLeft(m.Value, " \t\r\n"); len(trimmed) > 0 && trimmed[0] == '{' {
		isObject = json.Unmarshal(m.Value, &decoded) == nil
	}
	h := bodyHints(decoded)
	if isObject {
		var rawType string
		typeOK := len(decoded["event_type"]) > 0 && decoded["event_type"][0] == '"' &&
			json.Unmarshal(decoded["event_type"], &rawType) == nil
		if !typeOK || !contracts.IsKnownEventType(rawType) {
			return fail(model.UnknownEventType, "unknown event type", h, anomaly)
		}
		if raw := decoded["event_version"]; len(raw) > 0 && !bytes.ContainsAny(raw, ".eE") {
			if version, err := strconv.ParseInt(string(raw), 10, 64); err == nil && version > contracts.CurrentEventVersion {
				return fail(model.UnsupportedEventVersion, fmt.Sprintf("event version %d is unsupported", version), h, anomaly)
			}
		}
	}
	event, err := contracts.ParseEvent(m.Value)
	if err != nil {
		return fail(model.ContractValidationFailed, "event contract validation failed", h, anomaly)
	}
	if event.EventVersion != contracts.CurrentEventVersion {
		return fail(model.UnsupportedEventVersion, fmt.Sprintf("event version %d is unsupported", event.EventVersion), eventHints(event), anomaly)
	}
	expected := []struct{ name, value string }{
		{"event_id", event.EventID.String()},
		{"event_type", string(event.EventType)},
		{"event_version", strconv.FormatInt(event.EventVersion, 10)},
		{"correlation_id", event.CorrelationID.String()},
		{"source", event.Source},
	}
	for _, item := range expected {
		if headers[item.name] != item.value {
			return fail(model.HeaderBodyMismatch, item.name+" header does not match body", eventHints(event), anomaly)
		}
	}
	if !bytes.Equal(m.Key, event.MessageKey()) {
		return fail(model.KeyBodyMismatch, "Kafka key does not match body ordering key", eventHints(event), anomaly)
	}
	return Result{Event: event}
}
