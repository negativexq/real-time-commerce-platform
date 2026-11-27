package fraud

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/config"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
)

// referenceCase is one frozen evaluation of the retired Python fraud engine
// (testdata/python_reference.json, recorded 2026-11-27 from 600 seeded
// contexts around every rule boundary under four configurations).
type referenceCase struct {
	Config       map[string]string `json:"config"`
	Context      referenceContext  `json:"context"`
	EvaluationID string            `json:"evaluation_id"`
	TotalScore   int               `json:"total_score"`
	Decision     string            `json:"decision"`
	Severity     string            `json:"severity"`
	RuleResults  string            `json:"rule_results"`
	Alert        *referenceAlert   `json:"alert"`
}

type referenceContext struct {
	SourceEventID              string   `json:"source_event_id"`
	EventType                  string   `json:"event_type"`
	EventTime                  string   `json:"event_time"`
	CustomerID                 string   `json:"customer_id"`
	OrderID                    *string  `json:"order_id"`
	PaymentID                  *string  `json:"payment_id"`
	SessionID                  *string  `json:"session_id"`
	Amount                     *string  `json:"amount"`
	Currency                   *string  `json:"currency"`
	CurrentDevice              *string  `json:"current_device"`
	CurrentCountry             *string  `json:"current_country"`
	HomeCountry                string   `json:"home_country"`
	SessionStartedAt           *string  `json:"session_started_at"`
	HistoricalAverageAmount    *string  `json:"historical_average_amount"`
	RecentPaymentAttempts      int      `json:"recent_payment_attempts"`
	RecentFailedPayments       int      `json:"recent_failed_payments"`
	RecentFailureReasons       int      `json:"recent_failure_reasons"`
	RecentDistinctDevices      int      `json:"recent_distinct_devices"`
	RecentDistinctCountries    int      `json:"recent_distinct_countries"`
	RecentOrders               int      `json:"recent_orders"`
	RecentRefunds              int      `json:"recent_refunds"`
	LifetimeSuccessfulPayments int      `json:"lifetime_successful_payments"`
	LifetimeFailedPayments     int      `json:"lifetime_failed_payments"`
	RefundableAmount           *string  `json:"refundable_amount"`
	PreviousSuccessfulDevices  []string `json:"previous_successful_devices"`
	ProductViewCount           int      `json:"product_view_count"`
	RefundAmount               *string  `json:"refund_amount"`
	SecondsSincePayment        *int     `json:"seconds_since_payment"`
	PaymentAmountMatchesOrder  bool     `json:"payment_amount_matches_order"`
}

type referenceAlert struct {
	AlertID      string      `json:"alert_id"`
	AlertEventID string      `json:"alert_event_id"`
	OutboxID     string      `json:"outbox_id"`
	Key          string      `json:"key"`
	Headers      [][2]string `json:"headers"`
	Payload      string      `json:"payload"`
}

type referenceFile struct {
	SourceEventID string          `json:"source_event_id"`
	CorrelationID string          `json:"correlation_id"`
	Cases         []referenceCase `json:"cases"`
}

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func optUUID(s *string) *uuid.UUID {
	if s == nil {
		return nil
	}
	v := uuid.MustParse(*s)
	return &v
}

func optDecimal(s *string) *contracts.Decimal {
	if s == nil {
		return nil
	}
	v := contracts.MustDecimal(*s)
	return &v
}

func (r referenceContext) build(t *testing.T) *Context {
	t.Helper()
	devices := map[string]bool{}
	for _, d := range r.PreviousSuccessfulDevices {
		devices[d] = true
	}
	var started *time.Time
	if r.SessionStartedAt != nil {
		v := mustTime(t, *r.SessionStartedAt)
		started = &v
	}
	return &Context{
		SourceEventID:              uuid.MustParse(r.SourceEventID),
		EventType:                  contracts.EventType(r.EventType),
		EventTime:                  mustTime(t, r.EventTime),
		CustomerID:                 uuid.MustParse(r.CustomerID),
		OrderID:                    optUUID(r.OrderID),
		PaymentID:                  optUUID(r.PaymentID),
		SessionID:                  optUUID(r.SessionID),
		Amount:                     optDecimal(r.Amount),
		Currency:                   r.Currency,
		CurrentDevice:              r.CurrentDevice,
		CurrentCountry:             r.CurrentCountry,
		HomeCountry:                r.HomeCountry,
		SessionStartedAt:           started,
		HistoricalAverageAmount:    optDecimal(r.HistoricalAverageAmount),
		RecentPaymentAttempts:      r.RecentPaymentAttempts,
		RecentFailedPayments:       r.RecentFailedPayments,
		RecentFailureReasons:       r.RecentFailureReasons,
		RecentDistinctDevices:      r.RecentDistinctDevices,
		RecentDistinctCountries:    r.RecentDistinctCountries,
		RecentOrders:               r.RecentOrders,
		RecentRefunds:              r.RecentRefunds,
		LifetimeSuccessfulPayments: r.LifetimeSuccessfulPayments,
		LifetimeFailedPayments:     r.LifetimeFailedPayments,
		RefundableAmount:           optDecimal(r.RefundableAmount),
		PreviousSuccessfulDevices:  devices,
		ProductViewCount:           r.ProductViewCount,
		RefundAmount:               optDecimal(r.RefundAmount),
		SecondsSincePayment:        r.SecondsSincePayment,
		PaymentAmountMatchesOrder:  r.PaymentAmountMatchesOrder,
	}
}

func fraudConfig(t *testing.T, env map[string]string) *config.Fraud {
	t.Helper()
	cfg, err := config.LoadFraud(func(name string) (string, bool) {
		v, ok := env[name]
		return v, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestEvaluationsMatchPythonReference(t *testing.T) {
	raw, err := os.ReadFile("testdata/python_reference.json")
	if err != nil {
		t.Fatal(err)
	}
	var ref referenceFile
	if err := json.Unmarshal(raw, &ref); err != nil {
		t.Fatal(err)
	}
	if len(ref.Cases) == 0 {
		t.Fatal("reference fixture has no cases")
	}
	source := &contracts.Envelope{
		EventID:       uuid.MustParse(ref.SourceEventID),
		CorrelationID: uuid.MustParse(ref.CorrelationID),
	}
	engines := map[string]*Engine{}
	for i, tc := range ref.Cases {
		configKey, _ := json.Marshal(tc.Config)
		engine, ok := engines[string(configKey)]
		if !ok {
			engine, err = NewEngine(fraudConfig(t, tc.Config))
			if err != nil {
				t.Fatal(err)
			}
			engines[string(configKey)] = engine
		}
		ev, err := engine.Evaluate(tc.Context.build(t))
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if got, _ := ruleResultsJSON(ev); string(got) != tc.RuleResults {
			t.Fatalf("case %d rule results\n go: %s\n py: %s", i, got, tc.RuleResults)
		}
		if ev.EvaluationID.String() != tc.EvaluationID || ev.TotalScore != tc.TotalScore ||
			string(ev.Decision) != tc.Decision || string(ev.Severity) != tc.Severity {
			t.Fatalf("case %d: go (%s %d %s %s) != py (%s %d %s %s)", i,
				ev.EvaluationID, ev.TotalScore, ev.Decision, ev.Severity,
				tc.EvaluationID, tc.TotalScore, tc.Decision, tc.Severity)
		}
		if tc.Alert == nil {
			continue
		}
		msg := BuildAlertMessage(ev, source)
		if msg.AlertID.String() != tc.Alert.AlertID || msg.AlertEventID.String() != tc.Alert.AlertEventID ||
			msg.OutboxID.String() != tc.Alert.OutboxID || string(msg.Key) != tc.Alert.Key {
			t.Fatalf("case %d: alert identity differs from Python", i)
		}
		if string(msg.PayloadBytes) != tc.Alert.Payload {
			t.Fatalf("case %d alert payload\n go: %s\n py: %s", i, msg.PayloadBytes, tc.Alert.Payload)
		}
		headers := make([][2]string, 0, len(msg.Headers))
		for _, h := range msg.Headers {
			headers = append(headers, [2]string{h.Key, string(h.Value)})
		}
		if !reflect.DeepEqual(headers, tc.Alert.Headers) {
			t.Fatalf("case %d headers\n go: %v\n py: %v", i, headers, tc.Alert.Headers)
		}
	}
}

// The scenario tests below port tests/unit/test_fraud_engine.py.

var (
	now        = time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	sourceID   = uuid.MustParse("10000000-0000-4000-8000-000000000001")
	customerID = uuid.MustParse("10000000-0000-4000-8000-000000000002")
	orderID    = uuid.MustParse("10000000-0000-4000-8000-000000000003")
	paymentID  = uuid.MustParse("10000000-0000-4000-8000-000000000004")
	sessionID  = uuid.MustParse("10000000-0000-4000-8000-000000000005")
)

func dec(s string) *contracts.Decimal { v := contracts.MustDecimal(s); return &v }
func str(s string) *string            { return &s }
func at(d time.Duration) *time.Time   { v := now.Add(-d); return &v }

func baseContext() *Context {
	return &Context{
		SourceEventID:              sourceID,
		EventType:                  contracts.PaymentCompletedType,
		EventTime:                  now,
		CustomerID:                 customerID,
		OrderID:                    &orderID,
		PaymentID:                  &paymentID,
		SessionID:                  &sessionID,
		Amount:                     dec("100"),
		Currency:                   str("TRY"),
		CurrentDevice:              str("device-current"),
		CurrentCountry:             str("TR"),
		HomeCountry:                "TR",
		SessionStartedAt:           at(5 * time.Minute),
		HistoricalAverageAmount:    dec("100"),
		RecentPaymentAttempts:      1,
		RecentDistinctDevices:      1,
		RecentDistinctCountries:    1,
		RecentOrders:               1,
		LifetimeSuccessfulPayments: 3,
		PreviousSuccessfulDevices:  map[string]bool{"device-current": true},
		ProductViewCount:           2,
		PaymentAmountMatchesOrder:  true,
	}
}

func contextWith(change func(*Context)) *Context {
	c := baseContext()
	change(c)
	return c
}

// evaluateRule runs one rule from the registry built for env.
func evaluateRule(t *testing.T, env map[string]string, id string, c *Context) RuleResult {
	t.Helper()
	for _, r := range buildRules(fraudConfig(t, env)) {
		if r.id == id {
			return r.evaluate(c)
		}
	}
	t.Fatalf("rule %s is not registered", id)
	return RuleResult{}
}

func TestInvalidConfigurationIsRejected(t *testing.T) {
	cases := []map[string]string{
		{"FRAUD_REVIEW_THRESHOLD": "60", "FRAUD_BLOCK_THRESHOLD": "60"},
		{"FRAUD_HIGH_AMOUNT_THRESHOLD": "-1"},
		{"FRAUD_REFUND_RATE_THRESHOLD": "1.1"},
		{"FRAUD_AMOUNT_MULTIPLIER_THRESHOLD": "1"},
		{"FRAUD_ENABLED_RULES": "high_amount,high_amount"},
		{"FRAUD_ACCOUNT_TAKEOVER_MIN_SIGNALS": "6"},
	}
	for _, env := range cases {
		if _, err := config.LoadFraud(func(n string) (string, bool) { v, ok := env[n]; return v, ok }); err == nil {
			t.Errorf("configuration %v was accepted", env)
		}
	}
}

func TestRegistryIsCompleteOrderedAndRejectsUnknownRules(t *testing.T) {
	cfg := fraudConfig(t, nil)
	infos, err := Registry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, info := range infos {
		ids = append(ids, info.ID)
		if !info.Enabled || info.Version != RuleVersion || info.MaxScore < 0 || info.MaxScore > 100 {
			t.Errorf("unexpected registry entry %+v", info)
		}
	}
	if !reflect.DeepEqual(ids, cfg.EnabledRules) {
		t.Fatalf("registry order %v != configured %v", ids, cfg.EnabledRules)
	}
	_, err = NewEngine(fraudConfig(t, map[string]string{"FRAUD_ENABLED_RULES": "missing"}))
	if err == nil || !strings.Contains(err.Error(), "do not exist") {
		t.Fatalf("unknown rule error = %v", err)
	}
}

func TestDisabledRuleDoesNotExecute(t *testing.T) {
	cfg := fraudConfig(t, map[string]string{"FRAUD_ENABLED_RULES": "high_amount"})
	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := engine.Evaluate(baseContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(ev.RuleResults) != 1 || ev.RuleResults[0].RuleID != "high_amount" {
		t.Fatalf("rules executed: %+v", ev.RuleResults)
	}
	infos, _ := Registry(cfg)
	for _, info := range infos {
		if info.Enabled != (info.ID == "high_amount") {
			t.Errorf("%s enabled=%v", info.ID, info.Enabled)
		}
	}
}

func TestHighAmountBoundaries(t *testing.T) {
	cases := []struct {
		amount, average string
		score           int
	}{
		{"4999.99", "2000", 0},
		{"5000", "2000", 25},
		{"300", "100", 20},
		{"5000", "100", 45},
	}
	for _, tc := range cases {
		r := evaluateRule(t, nil, "high_amount", contextWith(func(c *Context) {
			c.Amount, c.HistoricalAverageAmount = dec(tc.amount), dec(tc.average)
		}))
		if r.Score != tc.score {
			t.Errorf("amount %s average %s: score %d, want %d", tc.amount, tc.average, r.Score, tc.score)
		}
	}
}

func TestHighAmountMissingHistoryAndDecimalPrecision(t *testing.T) {
	env := map[string]string{"FRAUD_HIGH_AMOUNT_THRESHOLD": "0.3"}
	below := evaluateRule(t, env, "high_amount", contextWith(func(c *Context) {
		c.Amount, c.HistoricalAverageAmount = dec("0.299999999999"), nil
	}))
	if below.Score != 0 {
		t.Fatalf("below threshold scored %d", below.Score)
	}
	exact := evaluateRule(t, env, "high_amount", contextWith(func(c *Context) {
		c.Amount, c.HistoricalAverageAmount = dec("0.300000000000"), nil
	}))
	if !exact.Matched {
		t.Fatal("exact threshold did not match")
	}
}

func TestVelocityThreshold(t *testing.T) {
	for attempts, want := range map[int]bool{2: false, 3: true, 4: true} {
		r := evaluateRule(t, nil, "payment_velocity", contextWith(func(c *Context) { c.RecentPaymentAttempts = attempts }))
		if r.Matched != want {
			t.Errorf("attempts %d matched=%v", attempts, r.Matched)
		}
	}
}

func TestFailedPaymentBurstThresholdAndReasons(t *testing.T) {
	if evaluateRule(t, nil, "failed_payment_burst", contextWith(func(c *Context) { c.RecentFailedPayments = 2 })).Matched {
		t.Fatal("two failures matched")
	}
	r := evaluateRule(t, nil, "failed_payment_burst", contextWith(func(c *Context) {
		c.RecentFailedPayments, c.RecentFailureReasons = 3, 2
	}))
	if !r.Matched || fmt.Sprint(r.Evidence["distinct_reason_count"]) != "2" {
		t.Fatalf("burst result %+v", r)
	}
}

func TestNewDeviceRequiresEstablishedHistory(t *testing.T) {
	newDevice := func(history int) *Context {
		return contextWith(func(c *Context) {
			c.CurrentDevice = str("new")
			c.PreviousSuccessfulDevices = map[string]bool{"old": true}
			c.LifetimeSuccessfulPayments = history
		})
	}
	if evaluateRule(t, nil, "new_device", newDevice(0)).Matched {
		t.Fatal("matched without history")
	}
	if !evaluateRule(t, nil, "new_device", newDevice(3)).Matched {
		t.Fatal("did not match with history")
	}
}

func TestCountryMatchMismatchAndMultiCountry(t *testing.T) {
	if evaluateRule(t, nil, "country_mismatch", baseContext()).Matched {
		t.Fatal("home country matched")
	}
	if s := evaluateRule(t, nil, "country_mismatch", contextWith(func(c *Context) { c.CurrentCountry = str("DE") })).Score; s != 10 {
		t.Fatalf("mismatch score %d", s)
	}
	multi := contextWith(func(c *Context) { c.CurrentCountry, c.RecentDistinctCountries = str("DE"), 2 })
	if s := evaluateRule(t, nil, "country_mismatch", multi).Score; s != 20 {
		t.Fatalf("multi-country score %d", s)
	}
}

func TestRapidCheckoutExactBoundary(t *testing.T) {
	env := map[string]string{"FRAUD_RAPID_CHECKOUT_SECONDS": "15"}
	if !evaluateRule(t, env, "rapid_checkout", contextWith(func(c *Context) { c.SessionStartedAt = at(15 * time.Second) })).Matched {
		t.Fatal("15s did not match")
	}
	if evaluateRule(t, env, "rapid_checkout", contextWith(func(c *Context) { c.SessionStartedAt = at(16 * time.Second) })).Matched {
		t.Fatal("16s matched")
	}
}

func TestAccountTakeoverRequiresSignals(t *testing.T) {
	if evaluateRule(t, nil, "account_takeover_composite", contextWith(func(c *Context) { c.LifetimeSuccessfulPayments = 0 })).Matched {
		t.Fatal("weak context matched")
	}
	strong := evaluateRule(t, nil, "account_takeover_composite", contextWith(func(c *Context) {
		c.Amount, c.HistoricalAverageAmount = dec("1000"), dec("100")
		c.CurrentDevice, c.PreviousSuccessfulDevices = str("new"), map[string]bool{"old": true}
		c.CurrentCountry = str("DE")
		c.SessionStartedAt = at(2 * time.Second)
	}))
	if !strong.Matched || strong.Severity != Critical {
		t.Fatalf("strong result %+v", strong)
	}
}

func TestRefundAbuseIgnoresLegitimateSingleRefund(t *testing.T) {
	refund := func(refunds int, amount string, seconds int) *Context {
		return contextWith(func(c *Context) {
			c.EventType = contracts.RefundRequestedType
			c.RecentRefunds = refunds
			c.RefundAmount, c.RefundableAmount = dec(amount), dec("100")
			c.SecondsSincePayment = &seconds
		})
	}
	if evaluateRule(t, nil, "refund_abuse", refund(1, "10", 10_000)).Matched {
		t.Fatal("legitimate refund matched")
	}
	if !evaluateRule(t, nil, "refund_abuse", refund(3, "100", 30)).Matched {
		t.Fatal("abusive refund did not match")
	}
}

func TestBotViewsAloneAreUnsupportedButTransactionMatches(t *testing.T) {
	for _, r := range buildRules(fraudConfig(t, nil)) {
		if r.id == "bot_checkout" && r.supported[contracts.ProductViewedType] {
			t.Fatal("bot rule supports product views")
		}
	}
	r := evaluateRule(t, nil, "bot_checkout", contextWith(func(c *Context) {
		c.ProductViewCount, c.SessionStartedAt = 20, at(2*time.Second)
	}))
	if !r.Matched {
		t.Fatal("bot checkout did not match")
	}
}

func TestPaymentAmountMismatchIsCriticalMaximum(t *testing.T) {
	r := evaluateRule(t, nil, "payment_amount_mismatch", contextWith(func(c *Context) { c.PaymentAmountMatchesOrder = false }))
	if r.Score != 100 || r.Severity != Critical {
		t.Fatalf("mismatch result %+v", r)
	}
}

func riskyContext() *Context {
	return contextWith(func(c *Context) {
		c.Amount, c.HistoricalAverageAmount = dec("10000"), dec("100")
		c.RecentPaymentAttempts, c.RecentFailedPayments = 4, 3
		c.CurrentDevice, c.PreviousSuccessfulDevices = str("new"), map[string]bool{"old": true}
		c.CurrentCountry, c.RecentDistinctCountries = str("DE"), 2
		c.SessionStartedAt = at(2 * time.Second)
	})
}

func TestEngineThresholdsClampSeverityAndDeterminism(t *testing.T) {
	engine, err := NewEngine(fraudConfig(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	ordinary, _ := engine.Evaluate(contextWith(func(c *Context) { c.LifetimeSuccessfulPayments = 0 }))
	if ordinary.Decision != Approve {
		t.Fatalf("ordinary decision %s", ordinary.Decision)
	}
	first, _ := engine.Evaluate(riskyContext())
	second, _ := engine.Evaluate(riskyContext())
	if !reflect.DeepEqual(first, second) {
		t.Fatal("evaluation is not deterministic")
	}
	if first.TotalScore != 100 || first.Decision != Block || first.Severity != Critical {
		t.Fatalf("risky evaluation %d %s %s", first.TotalScore, first.Decision, first.Severity)
	}
	changed, _ := NewEngine(fraudConfig(t, map[string]string{"FRAUD_RULESET_VERSION": "next"}))
	other, _ := changed.Evaluate(riskyContext())
	if other.EvaluationID == first.EvaluationID {
		t.Fatal("ruleset version does not change the evaluation ID")
	}
}

func TestAlertEventIsDeterministicCanonicalAndCausal(t *testing.T) {
	engine, _ := NewEngine(fraudConfig(t, nil))
	ev, _ := engine.Evaluate(contextWith(func(c *Context) {
		c.Amount, c.HistoricalAverageAmount, c.RecentFailedPayments = dec("10000"), dec("100"), 3
	}))
	source := &contracts.Envelope{EventID: sourceID, CorrelationID: uuid.MustParse("10000000-0000-4000-8000-000000000007")}
	first, second := BuildAlertMessage(ev, source), BuildAlertMessage(ev, source)
	if !reflect.DeepEqual(first.PayloadBytes, second.PayloadBytes) || !reflect.DeepEqual(first.Headers, second.Headers) {
		t.Fatal("alert message is not deterministic")
	}
	parsed, err := contracts.ParseEvent(first.PayloadBytes)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.EventID != first.AlertEventID || parsed.CorrelationID != source.CorrelationID {
		t.Fatalf("parsed alert %s/%s", parsed.EventID, parsed.CorrelationID)
	}
	causation := ""
	for _, h := range first.Headers {
		if h.Key == "causation_id" {
			causation = string(h.Value)
		}
	}
	if causation != sourceID.String() {
		t.Fatalf("causation_id = %q", causation)
	}
}
