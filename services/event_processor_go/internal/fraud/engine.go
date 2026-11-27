package fraud

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/config"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
)

type Severity string

const (
	Low      Severity = "LOW"
	Medium   Severity = "MEDIUM"
	High     Severity = "HIGH"
	Critical Severity = "CRITICAL"
)

var severityOrder = map[Severity]int{Low: 0, Medium: 1, High: 2, Critical: 3}

type Decision string

const (
	Approve Decision = "APPROVE"
	Review  Decision = "REVIEW"
	Block   Decision = "BLOCK"
)

// RuleResult ports FraudRuleResult. Evidence values are bool, int or nil.
type RuleResult struct {
	RuleID      string
	RuleVersion string
	Matched     bool
	Score       int
	Severity    Severity
	ReasonCode  string
	Explanation string
	Evidence    contracts.Object
	EvaluatedAt time.Time
}

type Evaluation struct {
	EvaluationID   uuid.UUID
	SourceEventID  uuid.UUID
	CustomerID     uuid.UUID
	OrderID        *uuid.UUID
	PaymentID      *uuid.UUID
	TotalScore     int
	Decision       Decision
	Severity       Severity
	RuleResults    []RuleResult
	EvaluatedAt    time.Time
	EngineVersion  string
	RulesetVersion string
}

func (e *Evaluation) MatchedRules() []RuleResult {
	var out []RuleResult
	for _, r := range e.RuleResults {
		if r.Matched {
			out = append(out, r)
		}
	}
	return out
}

// RuleVersion is the version every registered rule reports.
const RuleVersion = "1.0"

// RuleInfo describes one registered rule for read-only diagnostics.
type RuleInfo struct {
	ID       string
	Version  string
	Enabled  bool
	MaxScore int
}

// Registry lists every rule in registration order, marking the ones
// FRAUD_ENABLED_RULES turns on. It validates the configuration the same way
// NewEngine does.
func Registry(cfg *config.Fraud) ([]RuleInfo, error) {
	if _, err := NewEngine(cfg); err != nil {
		return nil, err
	}
	enabled := map[string]bool{}
	for _, id := range cfg.EnabledRules {
		enabled[id] = true
	}
	all := buildRules(cfg)
	out := make([]RuleInfo, 0, len(all))
	for _, r := range all {
		out = append(out, RuleInfo{ID: r.id, Version: RuleVersion, Enabled: enabled[r.id], MaxScore: r.maxScore})
	}
	return out, nil
}

type rule struct {
	id        string
	supported map[contracts.EventType]bool
	maxScore  int
	evaluate  func(*Context) RuleResult
}

var evaluationNamespace = uuid.MustParse("af5fe7db-8df2-4e57-b506-39fc223b6e9c")

// DeterministicEvaluationID ports deterministic_evaluation_id().
func DeterministicEvaluationID(source uuid.UUID, ruleset string) uuid.UUID {
	return uuid.NewSHA1(evaluationNamespace, []byte(source.String()+":"+ruleset))
}

type Engine struct {
	cfg   *config.Fraud
	rules []rule
}

func NewEngine(cfg *config.Fraud) (*Engine, error) {
	all := buildRules(cfg)
	ids := map[string]bool{}
	for _, r := range all {
		if ids[r.id] {
			return nil, procerr.New(procerr.FraudRuleConfiguration, "duplicate fraud rule ID")
		}
		ids[r.id] = true
		if r.maxScore < 0 || r.maxScore > 100 {
			return nil, procerr.New(procerr.FraudRuleConfiguration, "fraud rule score bound is invalid")
		}
	}
	enabled := map[string]bool{}
	for _, id := range cfg.EnabledRules {
		if !ids[id] {
			return nil, procerr.New(procerr.FraudRuleConfiguration, fmt.Sprintf("configured fraud rules do not exist: [%q]", id))
		}
		enabled[id] = true
	}
	var rules []rule
	for _, r := range all {
		if enabled[r.id] {
			rules = append(rules, r)
		}
	}
	return &Engine{cfg: cfg, rules: rules}, nil
}

func (g *Engine) Evaluate(c *Context) (*Evaluation, error) {
	results := make([]RuleResult, 0, len(g.rules))
	for _, r := range g.rules {
		if !r.supported[c.EventType] {
			continue
		}
		res := r.evaluate(c)
		if res.Score > r.maxScore {
			return nil, procerr.New(procerr.FraudRuleConfiguration, "fraud rule exceeded maximum: "+r.id)
		}
		results = append(results, res)
	}
	score, decision, severity, err := aggregate(results, g.cfg)
	if err != nil {
		return nil, err
	}
	return &Evaluation{
		EvaluationID:   DeterministicEvaluationID(c.SourceEventID, g.cfg.RulesetVersion),
		SourceEventID:  c.SourceEventID,
		CustomerID:     c.CustomerID,
		OrderID:        c.OrderID,
		PaymentID:      c.PaymentID,
		TotalScore:     score,
		Decision:       decision,
		Severity:       severity,
		RuleResults:    results,
		EvaluatedAt:    c.EventTime,
		EngineVersion:  g.cfg.EngineVersion,
		RulesetVersion: g.cfg.RulesetVersion,
	}, nil
}

// aggregate ports scoring.aggregate().
func aggregate(results []RuleResult, cfg *config.Fraud) (int, Decision, Severity, error) {
	score := 0
	var matched []RuleResult
	for _, r := range results {
		if r.Score < 0 {
			return 0, "", "", procerr.New(procerr.PermanentProcessing, "fraud rules cannot return negative scores")
		}
		if r.Matched {
			matched = append(matched, r)
			score += r.Score
		}
	}
	if score > 100 {
		score = 100
	}
	decision, minimum := Approve, Low
	switch {
	case score >= cfg.BlockThreshold:
		decision, minimum = Block, High
	case score >= cfg.ReviewThreshold:
		decision, minimum = Review, Medium
	}
	severity := minimum
	if len(matched) > 0 {
		severity = matched[0].Severity
		for _, r := range matched[1:] {
			if severityOrder[r.Severity] > severityOrder[severity] {
				severity = r.Severity
			}
		}
	}
	if severityOrder[severity] < severityOrder[minimum] {
		severity = minimum
	}
	return score, decision, severity, nil
}

func types(ts ...contracts.EventType) map[contracts.EventType]bool {
	m := make(map[contracts.EventType]bool, len(ts))
	for _, t := range ts {
		m[t] = true
	}
	return m
}

func result(c *Context, id string, matched bool, score int, severity Severity, reason, explanation string, evidence contracts.Object) RuleResult {
	if !matched {
		score, severity = 0, Low
	}
	if len(explanation) > 256 {
		explanation = explanation[:256]
	}
	return RuleResult{RuleID: id, RuleVersion: RuleVersion, Matched: matched, Score: score, Severity: severity,
		ReasonCode: reason, Explanation: explanation, Evidence: evidence, EvaluatedAt: c.EventTime}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func intOrNil(p *int) any {
	if p == nil {
		return nil
	}
	return int64(*p)
}

// buildRules ports registry.build_rule_registry() in registration order.
func buildRules(cfg *config.Fraud) []rule {
	eligible := types(contracts.CheckoutStartedType, contracts.OrderCreatedType, contracts.PaymentFailedType,
		contracts.PaymentCompletedType, contracts.RefundRequestedType)
	checkoutAndPayments := types(contracts.CheckoutStartedType, contracts.OrderCreatedType,
		contracts.PaymentFailedType, contracts.PaymentCompletedType)
	paymentsOnly := types(contracts.PaymentFailedType, contracts.PaymentCompletedType)
	zero := contracts.DecimalFromInt(0)

	deviation := func(c *Context) bool {
		return c.Amount != nil && c.HistoricalAverageAmount != nil && c.HistoricalAverageAmount.Cmp(zero) > 0 &&
			c.Amount.Cmp(c.HistoricalAverageAmount.Mul(cfg.AmountMultiplierThreshold)) >= 0
	}
	rapid := func(c *Context) bool {
		s := c.CheckoutSeconds()
		return s != nil && *s <= cfg.RapidCheckoutSeconds
	}

	highAmountMax := minInt(100, cfg.HighAmountScore+cfg.AmountDeviationScore)
	countryMax := minInt(100, cfg.CountryMismatchScore+cfg.MultiCountryScore)
	return []rule{
		{"high_amount", eligible, highAmountMax, func(c *Context) RuleResult {
			absolute := c.Amount != nil && c.Amount.Cmp(cfg.HighAmountThreshold) >= 0
			dev := deviation(c)
			score := cfg.HighAmountScore*boolInt(absolute) + cfg.AmountDeviationScore*boolInt(dev)
			severity := Medium
			if dev {
				severity = High
			}
			return result(c, "high_amount", absolute || dev, minInt(score, highAmountMax), severity, "HIGH_AMOUNT",
				"Transaction amount exceeds a configured behavioral threshold.",
				contracts.Object{"absolute_threshold": absolute, "historical_deviation": dev})
		}},
		{"payment_velocity", paymentsOnly, cfg.VelocityScore, func(c *Context) RuleResult {
			return result(c, "payment_velocity", c.RecentPaymentAttempts >= cfg.VelocityMaxAttempts, cfg.VelocityScore,
				Medium, "PAYMENT_VELOCITY", "Several payment attempts occurred inside the configured window.",
				contracts.Object{"attempt_count": int64(minInt(c.RecentPaymentAttempts, 999))})
		}},
		{"failed_payment_burst", paymentsOnly, cfg.FailedPaymentScore, func(c *Context) RuleResult {
			return result(c, "failed_payment_burst", c.RecentFailedPayments >= cfg.FailedPaymentThreshold,
				cfg.FailedPaymentScore, High, "FAILED_PAYMENT_BURST", "Repeated normalized payment failures occurred recently.",
				contracts.Object{
					"failure_count":         int64(minInt(c.RecentFailedPayments, 999)),
					"distinct_reason_count": int64(minInt(c.RecentFailureReasons, 20)),
				})
		}},
		{"new_device", paymentsOnly, cfg.NewDeviceScore, func(c *Context) RuleResult {
			matched := c.LifetimeSuccessfulPayments >= cfg.MinHistoryForDeviceRule && c.IsNewDevice()
			return result(c, "new_device", matched, cfg.NewDeviceScore, Medium, "NEW_DEVICE",
				"An established account used an unrecognized device indicator.",
				contracts.Object{"established_history": c.EstablishedHistory(), "known_device": !c.IsNewDevice()})
		}},
		{"country_mismatch", types(contracts.OrderCreatedType, contracts.PaymentFailedType, contracts.PaymentCompletedType), countryMax, func(c *Context) RuleResult {
			mismatch := c.CountryMismatch()
			multiple := c.RecentDistinctCountries > 1
			score := cfg.CountryMismatchScore*boolInt(mismatch) + cfg.MultiCountryScore*boolInt(multiple)
			severity := Medium
			if multiple {
				severity = High
			}
			return result(c, "country_mismatch", mismatch || multiple, minInt(score, countryMax), severity, "COUNTRY_MISMATCH",
				"Synthetic country indicators differ from established activity.",
				contracts.Object{"home_match": !mismatch, "recent_country_count": int64(c.RecentDistinctCountries)})
		}},
		{"rapid_checkout", checkoutAndPayments, cfg.RapidCheckoutScore, func(c *Context) RuleResult {
			return result(c, "rapid_checkout", rapid(c), cfg.RapidCheckoutScore, Medium, "RAPID_CHECKOUT",
				"The transaction followed session start unusually quickly.",
				contracts.Object{"elapsed_seconds": intOrNil(c.CheckoutSeconds())})
		}},
		{"account_takeover_composite", checkoutAndPayments, cfg.AccountTakeoverScore, func(c *Context) RuleResult {
			count := boolInt(c.EstablishedHistory()) + boolInt(c.IsNewDevice()) + boolInt(c.CountryMismatch()) +
				boolInt(rapid(c)) + boolInt(deviation(c))
			return result(c, "account_takeover_composite", count >= cfg.AccountTakeoverMinSignals, cfg.AccountTakeoverScore,
				Critical, "ACCOUNT_TAKEOVER_PATTERN", "Multiple established-account change indicators occurred together.",
				contracts.Object{"signal_count": int64(count), "required_signal_count": int64(cfg.AccountTakeoverMinSignals)})
		}},
		{"refund_abuse", types(contracts.RefundRequestedType), cfg.RefundAbuseScore, func(c *Context) RuleResult {
			rapidRefund := c.SecondsSincePayment != nil && *c.SecondsSincePayment <= cfg.RapidRefundSeconds
			nearFull := false
			if c.RefundAmount != nil && c.RefundableAmount != nil && c.RefundableAmount.Sign() > 0 {
				threshold := cfg.RefundRateThreshold
				if floor := contracts.MustDecimal("0.9"); threshold.Cmp(floor) < 0 {
					threshold = floor
				}
				if ratio, err := c.RefundAmount.Div(*c.RefundableAmount); err == nil {
					nearFull = ratio.Cmp(threshold) >= 0
				}
			}
			repeated := c.RecentRefunds >= 3
			matched := repeated || (rapidRefund && nearFull && c.RecentRefunds >= 2)
			return result(c, "refund_abuse", matched, cfg.RefundAbuseScore, High, "REFUND_ABUSE_PATTERN",
				"Refund timing, frequency, and normalized amount form a suspicious pattern.",
				contracts.Object{"rapid": rapidRefund, "near_full": nearFull, "recent_refund_count": int64(c.RecentRefunds)})
		}},
		{"bot_checkout", checkoutAndPayments, cfg.BotCheckoutScore, func(c *Context) RuleResult {
			isRapid := rapid(c)
			return result(c, "bot_checkout", c.ProductViewCount >= cfg.BotViewThreshold && isRapid, cfg.BotCheckoutScore,
				Medium, "BOT_CHECKOUT_PATTERN", "A high browsing count was followed by a very rapid transaction.",
				contracts.Object{"view_count": int64(minInt(c.ProductViewCount, 999)), "rapid": isRapid})
		}},
		{"payment_amount_mismatch", eligible, cfg.AmountMismatchScore, func(c *Context) RuleResult {
			matched := !c.PaymentAmountMatchesOrder
			return result(c, "payment_amount_mismatch", matched, cfg.AmountMismatchScore, Critical, "PAYMENT_AMOUNT_MISMATCH",
				"Persisted monetary integrity indicators do not reconcile.",
				contracts.Object{"integrity_match": !matched})
		}},
	}
}
