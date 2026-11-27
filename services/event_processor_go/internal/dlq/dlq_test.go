package dlq

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/model"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/validation"
)

// referenceCase is one Kafka record run through the retired Python
// validate_message() and build_dlq_envelope() (testdata/
// validation_reference.json, recorded 2026-11-27). It pins the validation
// category, error hints and the exact DLQ record bytes, key and headers.
type referenceCase struct {
	Name            string       `json:"name"`
	Topic           string       `json:"topic"`
	Partition       int32        `json:"partition"`
	Offset          int64        `json:"offset"`
	Timestamp       *string      `json:"timestamp"`
	Key             *string      `json:"key"`
	Value           *string      `json:"value"`
	Headers         [][2]*string `json:"headers"`
	MaxPayloadBytes int          `json:"max_payload_bytes"`
	Valid           bool         `json:"valid"`
	Category        string       `json:"category"`
	Message         string       `json:"message"`
	EventID         *string      `json:"event_id"`
	EventType       *string      `json:"event_type"`
	CorrelationID   *string      `json:"correlation_id"`
	AnomalyType     *string      `json:"anomaly_type"`
	DLQBody         string       `json:"dlq_body"`
	DLQKey          string       `json:"dlq_key"`
	DLQHeaders      [][2]string  `json:"dlq_headers"`
}

func decode(t *testing.T, s *string) []byte {
	t.Helper()
	if s == nil {
		return nil
	}
	b, err := base64.StdEncoding.DecodeString(*s)
	if err != nil {
		t.Fatal(err)
	}
	if b == nil {
		b = []byte{}
	}
	return b
}

func optString[T interface{ String() string }](v *T) *string {
	if v == nil {
		return nil
	}
	s := (*v).String()
	return &s
}

func TestValidationAndDLQMatchPythonReference(t *testing.T) {
	raw, err := os.ReadFile("testdata/validation_reference.json")
	if err != nil {
		t.Fatal(err)
	}
	var ref struct {
		FailedAt string          `json:"failed_at"`
		Cases    []referenceCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &ref); err != nil {
		t.Fatal(err)
	}
	failedAt, err := time.Parse(time.RFC3339Nano, ref.FailedAt)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range ref.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			m := &model.ConsumedMessage{
				Topic: tc.Topic, Partition: tc.Partition, Offset: tc.Offset,
				Key: decode(t, tc.Key), Value: decode(t, tc.Value),
			}
			if tc.Timestamp != nil {
				ts, err := time.Parse(time.RFC3339Nano, *tc.Timestamp)
				if err != nil {
					t.Fatal(err)
				}
				m.Timestamp = &ts
			}
			for _, h := range tc.Headers {
				m.Headers = append(m.Headers, model.Header{Key: *h[0], Value: decode(t, h[1])})
			}
			result := validation.ValidateMessage(m)
			if result.Valid() != tc.Valid {
				t.Fatalf("valid=%v, Python valid=%v (%+v)", result.Valid(), tc.Valid, result.Error)
			}
			if tc.Valid {
				return
			}
			e := result.Error
			if string(e.Category) != tc.Category || e.Message != tc.Message {
				t.Fatalf("go %s %q, py %s %q", e.Category, e.Message, tc.Category, tc.Message)
			}
			hints := []*string{optString(e.EventID), e.EventType, optString(e.CorrelationID), e.AnomalyType}
			want := []*string{tc.EventID, tc.EventType, tc.CorrelationID, tc.AnomalyType}
			if !reflect.DeepEqual(hints, want) {
				t.Fatalf("hints differ from Python")
			}
			record := Build(m, e, 2, "group-a", "instance-1", tc.MaxPayloadBytes, failedAt)
			if string(record.Body()) != tc.DLQBody {
				t.Fatalf("DLQ body\n go: %s\n py: %s", record.Body(), tc.DLQBody)
			}
			if string(record.key()) != tc.DLQKey {
				t.Fatalf("DLQ key %s != %s", record.key(), tc.DLQKey)
			}
			var headers [][2]string
			for _, h := range record.headers() {
				headers = append(headers, [2]string{h.Key, string(h.Value)})
			}
			if !reflect.DeepEqual(headers, tc.DLQHeaders) {
				t.Fatalf("DLQ headers\n go: %v\n py: %v", headers, tc.DLQHeaders)
			}
		})
	}
}
