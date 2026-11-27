package dlqsink

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The frozen DLQ reference holds the exact envelope bytes the processor
// publishes, so the sink is checked against the real producer contract.
type referenceCase struct {
	Name     string `json:"name"`
	Category string `json:"category"`
	DLQBody  string `json:"dlq_body"`
}

func loadReference(t *testing.T) []referenceCase {
	t.Helper()
	raw, err := os.ReadFile("../dlq/testdata/validation_reference.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []referenceCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc.Cases
}

func TestParseReadsEveryProcessorEnvelope(t *testing.T) {
	seen := 0
	for _, c := range loadReference(t) {
		if c.DLQBody == "" {
			continue
		}
		seen++
		var want struct {
			RecordID string `json:"dlq_record_id"`
			Message  string `json:"error_message"`
		}
		if err := json.Unmarshal([]byte(c.DLQBody), &want); err != nil {
			t.Fatal(err)
		}
		row := Parse([]byte(c.DLQBody), "commerce.events.dlq", 0, 7, time.Now())
		if row.ErrorCategory == UnreadableCategory {
			t.Fatalf("%s: envelope parsed as unreadable: %s", c.Name, row.ErrorMessage)
		}
		if row.RecordID.String() != want.RecordID || row.ErrorCategory != c.Category || row.ErrorMessage != want.Message {
			t.Fatalf("%s: got %s/%s/%q", c.Name, row.RecordID, row.ErrorCategory, row.ErrorMessage)
		}
		if row.DLQTopic != "commerce.events.dlq" || row.DLQOffset != 7 || string(row.Envelope) != c.DLQBody {
			t.Fatalf("%s: DLQ coordinates or envelope not kept", c.Name)
		}
	}
	if seen < 30 {
		t.Fatalf("only %d reference envelopes found", seen)
	}
}

func TestParseKeepsEnvelopeWithNonUUIDEventID(t *testing.T) {
	body := `{"dlq_record_id":"ef6de4be-3cb6-5852-a7e6-05a609824f0b","error_category":"invalid_event_id",` +
		`"error_message":"bad id","source_topic":"commerce.events","failed_at":"2026-01-02T03:04:05Z","original_event_id":"not-a-uuid"}`
	row := Parse([]byte(body), "commerce.events.dlq", 0, 1, time.Now())
	if row.ErrorCategory != "invalid_event_id" || row.OriginalEventID != nil {
		t.Fatalf("unexpected row: %+v", row)
	}
}

func TestParseFallsBackToSourceHeadersForIdentity(t *testing.T) {
	// Malformed JSON never yields an event, so its id only exists in headers.
	body := `{"dlq_record_id":"ef6de4be-3cb6-5852-a7e6-05a609824f0b","error_category":"malformed_json",` +
		`"error_message":"bad json","source_topic":"commerce.events","failed_at":"2026-01-02T03:04:05Z",` +
		`"original_event_id":null,"original_event_type":null,` +
		`"source_headers":"{\"event_id\": \"5c65ecf2-73ca-4ff1-b9d9-031bf0ee4222\", \"event_type\": \"product_viewed\"}"}`
	row := Parse([]byte(body), "commerce.events.dlq", 0, 1, time.Now())
	if row.OriginalEventID == nil || row.OriginalEventID.String() != "5c65ecf2-73ca-4ff1-b9d9-031bf0ee4222" ||
		row.OriginalEventType == nil || *row.OriginalEventType != "product_viewed" {
		t.Fatalf("identity not taken from headers: %+v", row)
	}
	object := `{"dlq_record_id":"ef6de4be-3cb6-5852-a7e6-05a609824f0b","error_category":"malformed_json",` +
		`"error_message":"","source_topic":"commerce.events","failed_at":"2026-01-02T03:04:05Z",` +
		`"source_headers":{"event_id":"5c65ecf2-73ca-4ff1-b9d9-031bf0ee4222","event_type":"session_started"}}`
	if row := Parse([]byte(object), "t", 0, 1, time.Now()); row.OriginalEventID == nil || *row.OriginalEventType != "session_started" {
		t.Fatalf("object headers not read: %+v", row)
	}
	broken := `{"dlq_record_id":"ef6de4be-3cb6-5852-a7e6-05a609824f0b","error_category":"x","error_message":"",` +
		`"source_topic":"t","failed_at":"2026-01-02T03:04:05Z","source_headers":"{'event_id': 'not json'}"}`
	if row := Parse([]byte(broken), "t", 0, 1, time.Now()); row.ErrorCategory != "x" || row.OriginalEventID != nil {
		t.Fatalf("unparseable headers must be ignored: %+v", row)
	}
}

func TestParseStoresUnreadableRecordsDeterministically(t *testing.T) {
	observed := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	for _, value := range [][]byte{[]byte("not json"), []byte(`{"error_category":"x"}`), nil} {
		a := Parse(value, "commerce.events.dlq", 2, 99, observed)
		b := Parse(value, "commerce.events.dlq", 2, 99, observed.Add(time.Hour))
		if a.ErrorCategory != UnreadableCategory || a.RecordID != b.RecordID || a.RecordID == uuid.Nil {
			t.Fatalf("value %q: %+v", value, a)
		}
		if !json.Valid(a.Envelope) {
			t.Fatalf("value %q: envelope is not JSON", value)
		}
	}
	if Parse([]byte("x"), "t", 0, 1, observed).RecordID == Parse([]byte("x"), "t", 0, 2, observed).RecordID {
		t.Fatal("different offsets must give different IDs")
	}
}

func TestLoadConfigDefaultsAndValidation(t *testing.T) {
	cfg, err := LoadConfig(func(string) (string, bool) { return "", false })
	if err != nil || cfg.Topic != "commerce.events.dlq" || cfg.ConsumerGroup != "commerce-dlq-sink-v1" || cfg.MetricsPort != 9104 {
		t.Fatalf("defaults: %+v %v", cfg, err)
	}
	bad := map[string]string{"DLQ_SINK_METRICS_PORT": "x"}
	if _, err := LoadConfig(func(k string) (string, bool) { v, ok := bad[k]; return v, ok }); err == nil {
		t.Fatal("invalid port accepted")
	}
}
