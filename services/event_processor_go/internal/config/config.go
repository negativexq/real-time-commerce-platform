// Package config ports services.event_processor.config.ProcessorConfig and
// fraud.config.FraudConfig: the same environment variables, defaults,
// bounds and cross-field rules.
package config

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
)

type Processor struct {
	KafkaBootstrapServers    string
	InputTopic               string
	DLQTopic                 string
	ConsumerGroup            string
	ClientID                 string
	AutoOffsetReset          string
	SessionTimeoutMs         int
	HeartbeatIntervalMs      int
	MaxPollIntervalMs        int
	PollTimeout              time.Duration
	IdleTimeout              time.Duration
	MaxProcessingAttempts    int
	RetryInitialBackoffMs    int
	RetryMaxBackoffMs        int
	RetryMultiplier          float64
	RetryJitterRatio         float64
	IdempotencyProcessingTTL int
	IdempotencyCompletedTTL  int
	IdempotencyKeyPrefix     string
	RedisSocketTimeout       time.Duration
	RedisConnectTimeout      time.Duration
	DLQMaxPayloadBytes       int
	DLQDeliveryTimeoutMs     int
	ShutdownTimeout          time.Duration
	LogLevel                 string
	RedisURL                 string
	PostgresDSN              string
	DBPoolMinSize            int
	DBPoolMaxSize            int
	DBConnectTimeoutSeconds  int
	DBAcquireTimeout         time.Duration
	DBStatementTimeoutMs     int
	DBMaxIdle                time.Duration
	DBHealthcheckInterval    time.Duration
	DBStartupAttempts        int
	DBStartupBackoff         time.Duration
	RequiredSchemaVersion    int
	PersistRawEventJSON      bool
	DBLogSlowQueryMs         int
	MaxMessages              int // 0 = continuous
	FromBeginning            bool
	OffsetCommitBatchSize    int
	OffsetCommitIntervalMs   int
	MetricsEnabled           bool
	MetricsHost              string
	MetricsPort              int
	MetricsPath              string
	MetricsNamespace         string
	MetricsServiceName       string
}

// env is a validating environment reader that accumulates the first error.
type env struct {
	lookup func(string) (string, bool)
	err    error
}

func (e *env) fail(name string, reason string) {
	if e.err == nil {
		e.err = fmt.Errorf("invalid %s: %s", name, reason)
	}
}

func (e *env) str(name, def string) string {
	v, ok := e.lookup(name)
	if !ok {
		return def
	}
	v = strings.TrimSpace(v)
	if v == "" {
		e.fail(name, "must not be blank")
	}
	return v
}

func (e *env) oneOf(name, def string, allowed ...string) string {
	v := e.str(name, def)
	for _, a := range allowed {
		if v == a {
			return v
		}
	}
	e.fail(name, "must be one of "+strings.Join(allowed, ", "))
	return def
}

func (e *env) integer(name string, def, lo, hi int) int {
	v, ok := e.lookup(name)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		e.fail(name, "must be an integer")
		return def
	}
	if n < lo || n > hi {
		e.fail(name, fmt.Sprintf("must be between %d and %d", lo, hi))
	}
	return n
}

func (e *env) float(name string, def, lo, hi float64, loExclusive bool) float64 {
	v, ok := e.lookup(name)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil || math.IsNaN(f) {
		e.fail(name, "must be a number")
		return def
	}
	if f < lo || f > hi || (loExclusive && f == lo) {
		e.fail(name, "is out of range")
	}
	return f
}

func (e *env) boolean(name string, def bool) bool {
	v, ok := e.lookup(name)
	if !ok {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	}
	e.fail(name, "must be a boolean")
	return def
}

func (e *env) decimal(name string, def string) contracts.Decimal {
	v, ok := e.lookup(name)
	if !ok {
		return contracts.MustDecimal(def)
	}
	d, err := contracts.ParseDecimal(v)
	if err != nil {
		e.fail(name, "must be a decimal")
		return contracts.MustDecimal(def)
	}
	return d
}

func seconds(f float64) time.Duration { return time.Duration(f * float64(time.Second)) }

const maxInt = math.MaxInt32

// LoadProcessor reads ProcessorConfig.from_environment() and applies CLI
// overrides the same way parse_config() does.
func LoadProcessor(args []string, lookup func(string) (string, bool)) (*Processor, error) {
	e := &env{lookup: lookup}
	c := &Processor{
		KafkaBootstrapServers:    e.str("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
		InputTopic:               e.str("PROCESSOR_INPUT_TOPIC", "commerce.events"),
		DLQTopic:                 e.str("PROCESSOR_DLQ_TOPIC", "commerce.events.dlq"),
		ConsumerGroup:            e.str("PROCESSOR_CONSUMER_GROUP", "commerce-event-processor-v1"),
		ClientID:                 e.str("PROCESSOR_CLIENT_ID", "commerce-event-processor"),
		AutoOffsetReset:          e.oneOf("PROCESSOR_AUTO_OFFSET_RESET", "earliest", "earliest", "latest", "error"),
		SessionTimeoutMs:         e.integer("PROCESSOR_SESSION_TIMEOUT_MS", 10_000, 6_000, 300_000),
		HeartbeatIntervalMs:      e.integer("PROCESSOR_HEARTBEAT_INTERVAL_MS", 3_000, 1, maxInt),
		MaxPollIntervalMs:        e.integer("PROCESSOR_MAX_POLL_INTERVAL_MS", 300_000, 10_000, maxInt),
		PollTimeout:              seconds(e.float("PROCESSOR_POLL_TIMEOUT_SECONDS", 1.0, 0, 30, true)),
		IdleTimeout:              seconds(e.float("PROCESSOR_IDLE_TIMEOUT_SECONDS", 10.0, 0, 3_600, true)),
		MaxProcessingAttempts:    e.integer("PROCESSOR_MAX_PROCESSING_ATTEMPTS", 3, 1, 20),
		RetryInitialBackoffMs:    e.integer("PROCESSOR_RETRY_INITIAL_BACKOFF_MS", 100, 0, 60_000),
		RetryMaxBackoffMs:        e.integer("PROCESSOR_RETRY_MAX_BACKOFF_MS", 2_000, 0, 300_000),
		RetryMultiplier:          e.float("PROCESSOR_RETRY_MULTIPLIER", 2.0, 1, 10, false),
		RetryJitterRatio:         e.float("PROCESSOR_RETRY_JITTER_RATIO", 0.1, 0, 1, false),
		IdempotencyProcessingTTL: e.integer("PROCESSOR_IDEMPOTENCY_PROCESSING_TTL_SECONDS", 60, 1, maxInt),
		IdempotencyCompletedTTL:  e.integer("PROCESSOR_IDEMPOTENCY_COMPLETED_TTL_SECONDS", 604_800, 1, maxInt),
		IdempotencyKeyPrefix:     e.str("PROCESSOR_IDEMPOTENCY_KEY_PREFIX", "commerce:processor:v1:event"),
		RedisSocketTimeout:       seconds(e.float("PROCESSOR_REDIS_SOCKET_TIMEOUT_SECONDS", 2.0, 0, 60, true)),
		RedisConnectTimeout:      seconds(e.float("PROCESSOR_REDIS_CONNECT_TIMEOUT_SECONDS", 2.0, 0, 60, true)),
		DLQMaxPayloadBytes:       e.integer("PROCESSOR_DLQ_MAX_PAYLOAD_BYTES", 65_536, 1, maxInt),
		DLQDeliveryTimeoutMs:     e.integer("PROCESSOR_DLQ_DELIVERY_TIMEOUT_MS", 10_000, 1, maxInt),
		ShutdownTimeout:          seconds(e.float("PROCESSOR_SHUTDOWN_TIMEOUT_SECONDS", 10.0, 0, 120, true)),
		LogLevel:                 e.oneOf("PROCESSOR_LOG_LEVEL", "INFO", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
		RedisURL:                 e.str("REDIS_URL", "redis://redis:6379/0"),
		PostgresDSN:              e.str("POSTGRES_DSN", "postgresql://commerce:commerce_local_dev@postgres:5432/commerce"),
		DBPoolMinSize:            e.integer("PROCESSOR_DB_POOL_MIN_SIZE", 1, 1, 20),
		DBPoolMaxSize:            e.integer("PROCESSOR_DB_POOL_MAX_SIZE", 4, 1, 50),
		DBConnectTimeoutSeconds:  e.integer("PROCESSOR_DB_CONNECT_TIMEOUT_SECONDS", 5, 1, 120),
		DBAcquireTimeout:         seconds(e.float("PROCESSOR_DB_ACQUIRE_TIMEOUT_SECONDS", 5.0, 0, 120, true)),
		DBStatementTimeoutMs:     e.integer("PROCESSOR_DB_STATEMENT_TIMEOUT_MS", 5_000, 1, maxInt),
		DBMaxIdle:                seconds(e.float("PROCESSOR_DB_MAX_IDLE_SECONDS", 300.0, 0, math.MaxFloat64, true)),
		DBHealthcheckInterval:    seconds(e.float("PROCESSOR_DB_HEALTHCHECK_INTERVAL_SECONDS", 30.0, 0, math.MaxFloat64, true)),
		DBStartupAttempts:        e.integer("PROCESSOR_DB_STARTUP_ATTEMPTS", 5, 1, 100),
		DBStartupBackoff:         seconds(e.float("PROCESSOR_DB_STARTUP_BACKOFF_SECONDS", 1.0, 0, 60, false)),
		RequiredSchemaVersion:    e.integer("PROCESSOR_REQUIRED_SCHEMA_VERSION", 4, 1, maxInt),
		PersistRawEventJSON:      e.boolean("PROCESSOR_PERSIST_RAW_EVENT_JSON", true),
		DBLogSlowQueryMs:         e.integer("PROCESSOR_DB_LOG_SLOW_QUERY_MS", 250, 1, maxInt),
		OffsetCommitBatchSize:    e.integer("PROCESSOR_OFFSET_COMMIT_BATCH_SIZE", 50, 1, 10_000),
		OffsetCommitIntervalMs:   e.integer("PROCESSOR_OFFSET_COMMIT_INTERVAL_MS", 100, 1, 60_000),
		MetricsEnabled:           e.boolean("METRICS_ENABLED", true),
		MetricsHost:              e.str("METRICS_HOST", "0.0.0.0"),
		MetricsPath:              e.str("METRICS_PATH", "/metrics"),
		MetricsNamespace:         e.str("METRICS_NAMESPACE", "commerce"),
		MetricsServiceName:       e.str("METRICS_SERVICE_NAME", "event-processor"),
	}
	c.MetricsPort = e.integer("PROCESSOR_METRICS_PORT", e.integer("METRICS_PORT", 9101, 1, 65535), 1, 65535)
	if e.err != nil {
		return nil, e.err
	}
	if v, ok := lookup("PROCESSOR_WORKER_POOL_SIZE"); ok && strings.TrimSpace(v) != "1" {
		return nil, errors.New("the Go processor implements only the default synchronous model (PROCESSOR_WORKER_POOL_SIZE=1)")
	}

	flags := flag.NewFlagSet("event-processor", flag.ContinueOnError)
	maxMessages := flags.Int("max-messages", 0, "stop after this many terminal records")
	idleTimeout := flags.Float64("idle-timeout", 0, "finite-mode idle timeout in seconds")
	logLevel := flags.String("log-level", "", "DEBUG, INFO, WARNING, ERROR or CRITICAL")
	fromBeginning := flags.Bool("from-beginning", false, "reset to earliest for a new group")
	groupID := flags.String("group-id", "", "consumer group override")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	if *maxMessages < 0 || (*idleTimeout < 0) {
		return nil, errors.New("CLI bounds must be positive")
	}
	if *maxMessages > 0 {
		c.MaxMessages = *maxMessages
	}
	if *idleTimeout > 0 {
		c.IdleTimeout = seconds(*idleTimeout)
	}
	if *logLevel != "" {
		c.LogLevel = strings.ToUpper(*logLevel)
	}
	if *groupID != "" {
		c.ConsumerGroup = *groupID
	}
	if *fromBeginning {
		c.FromBeginning = true
		c.AutoOffsetReset = "earliest"
	}
	return c, c.validate()
}

func (c *Processor) validate() error {
	switch {
	case c.HeartbeatIntervalMs >= c.SessionTimeoutMs:
		return errors.New("heartbeat interval must be less than session timeout")
	case c.RetryInitialBackoffMs > c.RetryMaxBackoffMs:
		return errors.New("initial retry backoff must not exceed maximum")
	case c.IdempotencyCompletedTTL <= c.IdempotencyProcessingTTL:
		return errors.New("completed TTL must be greater than processing TTL")
	case c.DBPoolMaxSize < c.DBPoolMinSize:
		return errors.New("database pool maximum must be at least its minimum")
	case !strings.HasPrefix(c.MetricsPath, "/") || c.MetricsPath == "/":
		return errors.New("metrics path must be an absolute non-root path")
	}
	return nil
}

// Fraud ports FraudConfig (the fields the processor reads).
type Fraud struct {
	EngineEnabled              bool
	EngineVersion              string
	RulesetVersion             string
	EnabledRules               []string
	ReviewThreshold            int
	BlockThreshold             int
	HistoryLookbackDays        int
	MaxHistoryRecords          int
	HighAmountThreshold        contracts.Decimal
	AmountMultiplierThreshold  contracts.Decimal
	HighAmountScore            int
	AmountDeviationScore       int
	VelocityMaxAttempts        int
	VelocityWindowSeconds      int
	VelocityScore              int
	FailedPaymentThreshold     int
	FailedPaymentWindowSeconds int
	FailedPaymentScore         int
	NewDeviceScore             int
	MinHistoryForDeviceRule    int
	CountryMismatchScore       int
	MultiCountryScore          int
	CountryWindowSeconds       int
	RapidCheckoutSeconds       int
	RapidCheckoutScore         int
	AccountTakeoverScore       int
	AccountTakeoverMinSignals  int
	RefundRateThreshold        contracts.Decimal
	RapidRefundSeconds         int
	RefundAbuseScore           int
	BotViewThreshold           int
	BotCheckoutScore           int
	AmountMismatchScore        int
	AlertTopic                 string
	OutboxEnabled              bool
}

const defaultRules = "high_amount,payment_velocity,failed_payment_burst,new_device," +
	"country_mismatch,rapid_checkout,account_takeover_composite," +
	"refund_abuse,bot_checkout,payment_amount_mismatch"

func LoadFraud(lookup func(string) (string, bool)) (*Fraud, error) {
	e := &env{lookup: lookup}
	score := func(name string, def int) int { return e.integer(name, def, 0, 100) }
	positive := func(name string, def int) int { return e.integer(name, def, 1, maxInt) }
	c := &Fraud{
		EngineEnabled:              e.boolean("FRAUD_ENGINE_ENABLED", true),
		EngineVersion:              e.str("FRAUD_ENGINE_VERSION", "1.0.0"),
		RulesetVersion:             e.str("FRAUD_RULESET_VERSION", "2026-08"),
		ReviewThreshold:            score("FRAUD_REVIEW_THRESHOLD", 30),
		BlockThreshold:             score("FRAUD_BLOCK_THRESHOLD", 60),
		HistoryLookbackDays:        e.integer("FRAUD_HISTORY_LOOKBACK_DAYS", 30, 1, 365),
		MaxHistoryRecords:          e.integer("FRAUD_MAX_HISTORY_RECORDS", 500, 1, 10_000),
		HighAmountThreshold:        e.decimal("FRAUD_HIGH_AMOUNT_THRESHOLD", "5000"),
		AmountMultiplierThreshold:  e.decimal("FRAUD_AMOUNT_MULTIPLIER_THRESHOLD", "3"),
		HighAmountScore:            score("FRAUD_HIGH_AMOUNT_SCORE", 25),
		AmountDeviationScore:       score("FRAUD_AMOUNT_DEVIATION_SCORE", 20),
		VelocityMaxAttempts:        positive("FRAUD_VELOCITY_MAX_ATTEMPTS", 3),
		VelocityWindowSeconds:      positive("FRAUD_VELOCITY_WINDOW_SECONDS", 120),
		VelocityScore:              score("FRAUD_VELOCITY_SCORE", 20),
		FailedPaymentThreshold:     positive("FRAUD_FAILED_PAYMENT_THRESHOLD", 3),
		FailedPaymentWindowSeconds: positive("FRAUD_FAILED_PAYMENT_WINDOW_SECONDS", 300),
		FailedPaymentScore:         score("FRAUD_FAILED_PAYMENT_SCORE", 25),
		NewDeviceScore:             score("FRAUD_NEW_DEVICE_SCORE", 15),
		MinHistoryForDeviceRule:    positive("FRAUD_MIN_HISTORY_FOR_DEVICE_RULE", 2),
		CountryMismatchScore:       score("FRAUD_COUNTRY_MISMATCH_SCORE", 10),
		MultiCountryScore:          score("FRAUD_MULTI_COUNTRY_SCORE", 10),
		CountryWindowSeconds:       positive("FRAUD_COUNTRY_WINDOW_SECONDS", 600),
		RapidCheckoutSeconds:       positive("FRAUD_RAPID_CHECKOUT_SECONDS", 15),
		RapidCheckoutScore:         score("FRAUD_RAPID_CHECKOUT_SCORE", 15),
		AccountTakeoverScore:       score("FRAUD_ACCOUNT_TAKEOVER_SCORE", 60),
		AccountTakeoverMinSignals:  e.integer("FRAUD_ACCOUNT_TAKEOVER_MIN_SIGNALS", 4, 2, 5),
		RefundRateThreshold:        e.decimal("FRAUD_REFUND_RATE_THRESHOLD", "0.5"),
		RapidRefundSeconds:         positive("FRAUD_RAPID_REFUND_SECONDS", 600),
		RefundAbuseScore:           score("FRAUD_REFUND_ABUSE_SCORE", 35),
		BotViewThreshold:           positive("FRAUD_BOT_VIEW_THRESHOLD", 12),
		BotCheckoutScore:           score("FRAUD_BOT_CHECKOUT_SCORE", 30),
		AmountMismatchScore:        score("FRAUD_AMOUNT_MISMATCH_SCORE", 100),
		AlertTopic:                 e.str("FRAUD_ALERT_TOPIC", "commerce.fraud-alerts"),
		OutboxEnabled:              e.boolean("FRAUD_OUTBOX_ENABLED", true),
	}
	rules := e.str("FRAUD_ENABLED_RULES", defaultRules)
	if e.err != nil {
		return nil, e.err
	}
	seen := map[string]bool{}
	for _, item := range strings.Split(rules, ",") {
		if item = strings.TrimSpace(item); item != "" {
			if seen[item] {
				return nil, errors.New("enabled fraud rules must be unique")
			}
			seen[item] = true
			c.EnabledRules = append(c.EnabledRules, item)
		}
	}
	zero, one := contracts.DecimalFromInt(0), contracts.DecimalFromInt(1)
	switch {
	case !(0 <= c.ReviewThreshold && c.ReviewThreshold < c.BlockThreshold && c.BlockThreshold <= 100):
		return nil, errors.New("require 0 <= review threshold < block threshold <= 100")
	case c.HighAmountThreshold.Sign() < 0:
		return nil, errors.New("FRAUD_HIGH_AMOUNT_THRESHOLD must be >= 0")
	case c.AmountMultiplierThreshold.Cmp(one) <= 0:
		return nil, errors.New("FRAUD_AMOUNT_MULTIPLIER_THRESHOLD must be > 1")
	case c.RefundRateThreshold.Cmp(zero) < 0 || c.RefundRateThreshold.Cmp(one) > 0:
		return nil, errors.New("FRAUD_REFUND_RATE_THRESHOLD must be within [0, 1]")
	}
	return c, nil
}

// OSLookup adapts os.LookupEnv.
func OSLookup(name string) (string, bool) { return os.LookupEnv(name) }
