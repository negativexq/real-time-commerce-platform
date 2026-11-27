package fraud

import (
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/config"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/dbtx"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/metrics"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
)

var (
	alertNamespace      = uuid.MustParse("8738db40-333e-491a-a20d-4658782dfc44")
	alertEventNamespace = uuid.MustParse("8b3f028f-d1cb-465c-a662-ebfdcfb74d38")
	outboxNamespace     = uuid.MustParse("dc27cc58-f4b8-4849-9417-490923fc7ae2")
)

// AlertMessage ports publisher.FraudAlertMessage.
type AlertMessage struct {
	AlertID      uuid.UUID
	AlertEventID uuid.UUID
	OutboxID     uuid.UUID
	Event        *contracts.Envelope
	Key          []byte
	Headers      []contracts.Header
	PayloadBytes []byte
}

// BuildAlertMessage ports build_alert_message().
func BuildAlertMessage(ev *Evaluation, source *contracts.Envelope) *AlertMessage {
	alertID := uuid.NewSHA1(alertNamespace, []byte(ev.EvaluationID.String()))
	eventID := uuid.NewSHA1(alertEventNamespace, []byte(alertID.String()))
	reasons := make([]string, 0, len(ev.RuleResults))
	for _, r := range ev.MatchedRules() {
		reasons = append(reasons, r.ReasonCode)
	}
	if len(reasons) == 0 {
		reasons = []string{"DECISION_THRESHOLD"}
	}
	customer := ev.CustomerID
	payload := &contracts.FraudAlertCreated{
		AlertID:    alertID,
		EventID:    ev.SourceEventID,
		CustomerID: &customer,
		OrderID:    ev.OrderID,
		FraudScore: contracts.DecimalFromInt(int64(ev.TotalScore)),
		Decision:   string(ev.Decision),
		Reasons:    reasons,
		CreatedAt:  ev.EvaluatedAt,
	}
	event := contracts.NewEnvelope(eventID, contracts.FraudAlertCreatedType, ev.EvaluatedAt,
		"fraud-engine", source.CorrelationID, payload)
	headers := append(event.MessageHeaders(), contracts.Header{Key: "causation_id", Value: []byte(source.EventID.String())})
	return &AlertMessage{
		AlertID:      alertID,
		AlertEventID: eventID,
		OutboxID:     uuid.NewSHA1(outboxNamespace, []byte(eventID.String())),
		Event:        event,
		Key:          event.MessageKey(),
		Headers:      headers,
		PayloadBytes: event.CanonicalJSON(),
	}
}

func ruleResultsJSON(ev *Evaluation) ([]byte, []any) {
	items := make([]any, 0, len(ev.RuleResults))
	for _, r := range ev.RuleResults {
		items = append(items, contracts.Object{
			"rule_id":      r.RuleID,
			"rule_version": r.RuleVersion,
			"matched":      r.Matched,
			"score":        int64(r.Score),
			"severity":     string(r.Severity),
			"reason_code":  r.ReasonCode,
			"explanation":  r.Explanation,
			"evidence":     r.Evidence,
			"evaluated_at": contracts.FormatISOOffset(r.EvaluatedAt),
		})
	}
	return contracts.CanonicalJSON(items), items
}

// Repository ports FraudRepository.persist().
type Repository struct {
	cfg     *config.Fraud
	metrics *metrics.Metrics
}

func NewRepository(cfg *config.Fraud, m *metrics.Metrics) *Repository {
	return &Repository{cfg: cfg, metrics: m}
}

func (r *Repository) Persist(tx *dbtx.Tx, ev *Evaluation, source *contracts.Envelope) (map[string]int, error) {
	encoded, _ := ruleResultsJSON(ev)
	var score int64
	var decision, severity, engineVersion, rulesetVersion string
	var existingResults []byte
	err := tx.QueryRow(`
                SELECT total_score, decision, severity, engine_version,
                       ruleset_version, rule_results
                FROM fraud_evaluations WHERE source_event_id = $1
                `, []any{dbtx.UUID(ev.SourceEventID)}, &score, &decision, &severity, &engineVersion, &rulesetVersion, &existingResults)
	switch {
	case err == nil:
		var actual, expected any
		sameResults := json.Unmarshal(existingResults, &actual) == nil &&
			json.Unmarshal(encoded, &expected) == nil && reflect.DeepEqual(actual, expected)
		if int(score) != ev.TotalScore || decision != string(ev.Decision) || severity != string(ev.Severity) ||
			engineVersion != ev.EngineVersion || rulesetVersion != ev.RulesetVersion || !sameResults {
			return nil, procerr.New(procerr.FraudEvaluationIntegrity, "source event has conflicting deterministic fraud evaluation")
		}
		return map[string]int{"fraud_evaluations": 0}, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return nil, err
	}
	matched := ev.MatchedRules()
	if _, err := tx.Exec(`
                INSERT INTO fraud_evaluations (
                    evaluation_id, source_event_id, customer_id, order_id,
                    payment_id, total_score, decision, severity, engine_version,
                    ruleset_version, matched_rule_count, rule_results, evaluated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13
                )
                `, dbtx.UUID(ev.EvaluationID), dbtx.UUID(ev.SourceEventID), dbtx.UUID(ev.CustomerID),
		dbtx.NullableUUID(ev.OrderID), dbtx.NullableUUID(ev.PaymentID), ev.TotalScore, string(ev.Decision),
		string(ev.Severity), ev.EngineVersion, ev.RulesetVersion, len(matched), string(encoded), ev.EvaluatedAt); err != nil {
		return nil, err
	}
	rows := map[string]int{"fraud_evaluations": 1}
	if ev.Decision == Approve {
		return rows, nil
	}
	message := BuildAlertMessage(ev, source)
	reasonCodes := make([]string, 0, len(matched))
	for _, m := range matched {
		reasonCodes = append(reasonCodes, m.ReasonCode)
	}
	reasonJSON, _ := json.Marshal(reasonCodes)
	if _, err := tx.Exec(`
                INSERT INTO fraud_alerts (
                    alert_id, evaluation_id, source_event_id, customer_id,
                    order_id, payment_id, score, decision, severity, status,
                    reason_codes, alert_event_id, created_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, 'OPEN',
                    $10::jsonb, $11, $12, $13
                )
                `, dbtx.UUID(message.AlertID), dbtx.UUID(ev.EvaluationID), dbtx.UUID(ev.SourceEventID),
		dbtx.UUID(ev.CustomerID), dbtx.NullableUUID(ev.OrderID), dbtx.NullableUUID(ev.PaymentID), ev.TotalScore,
		string(ev.Decision), string(ev.Severity), string(reasonJSON), dbtx.UUID(message.AlertEventID),
		ev.EvaluatedAt, ev.EvaluatedAt); err != nil {
		return nil, err
	}
	rows["fraud_alerts"] = 1
	if r.cfg.OutboxEnabled {
		started := time.Now()
		headerPairs := make([][]string, 0, len(message.Headers))
		for _, h := range message.Headers {
			headerPairs = append(headerPairs, []string{h.Key, string(h.Value)})
		}
		headersJSON, _ := json.Marshal(headerPairs)
		if _, err := tx.Exec(`
                    INSERT INTO fraud_outbox (
                        outbox_id, aggregate_type, aggregate_id, event_id,
                        event_type, event_version, topic, message_key,
                        headers_json, payload_json, payload_bytes, status,
                        available_at
                    ) VALUES (
                        $1, 'fraud_alert', $2, $3, $4, $5, $6, $7,
                        $8::jsonb, $9::jsonb, $10, 'PENDING', CURRENT_TIMESTAMP
                    )
                    `, dbtx.UUID(message.OutboxID), dbtx.UUID(message.AlertID), dbtx.UUID(message.AlertEventID),
			string(message.Event.EventType), message.Event.EventVersion, r.cfg.AlertTopic, message.Key,
			string(headersJSON), string(message.PayloadBytes), message.PayloadBytes); err != nil {
			return nil, err
		}
		if r.metrics != nil {
			r.metrics.OutboxWriteDuration.Observe(time.Since(started).Seconds())
		}
		rows["fraud_outbox"] = 1
	}
	return rows, nil
}
