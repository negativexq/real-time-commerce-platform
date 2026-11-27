package config

import (
	"testing"
	"time"
)

// These tests port tests/unit/test_processor_config.py and
// test_persistence_config.py.

func lookup(env map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := env[name]
		return v, ok
	}
}

func TestProcessorDefaultsAreSafe(t *testing.T) {
	c, err := LoadProcessor(nil, lookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.InputTopic != "commerce.events" || c.DLQTopic != "commerce.events.dlq" ||
		c.ConsumerGroup != "commerce-event-processor-v1" || c.AutoOffsetReset != "earliest" ||
		c.MaxProcessingAttempts != 3 || c.MaxMessages != 0 || c.RequiredSchemaVersion != 4 ||
		c.DBPoolMinSize != 1 || c.DBPoolMaxSize != 4 || c.MetricsPort != 9101 ||
		c.IdleTimeout != 10*time.Second || !c.PersistRawEventJSON {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestProcessorRejectsInvalidEnvironment(t *testing.T) {
	cases := []map[string]string{
		{"PROCESSOR_AUTO_OFFSET_RESET": "middle"},
		{"PROCESSOR_SESSION_TIMEOUT_MS": "1000"},
		{"PROCESSOR_MAX_PROCESSING_ATTEMPTS": "0"},
		{"PROCESSOR_MAX_PROCESSING_ATTEMPTS": "x"},
		{"PROCESSOR_POLL_TIMEOUT_SECONDS": "0"},
		{"PROCESSOR_RETRY_JITTER_RATIO": "1.5"},
		{"PROCESSOR_LOG_LEVEL": "TRACE"},
		{"PROCESSOR_DB_POOL_MAX_SIZE": "51"},
		{"PROCESSOR_DB_POOL_MIN_SIZE": "5", "PROCESSOR_DB_POOL_MAX_SIZE": "4"},
		{"PROCESSOR_REQUIRED_SCHEMA_VERSION": "0"},
		{"PROCESSOR_HEARTBEAT_INTERVAL_MS": "10000"},
		{"PROCESSOR_RETRY_INITIAL_BACKOFF_MS": "3000", "PROCESSOR_RETRY_MAX_BACKOFF_MS": "2000"},
		{"PROCESSOR_IDEMPOTENCY_PROCESSING_TTL_SECONDS": "604800"},
		{"METRICS_PATH": "/"},
		{"PROCESSOR_WORKER_POOL_SIZE": "4"},
		{"KAFKA_BOOTSTRAP_SERVERS": "  "},
	}
	for _, env := range cases {
		if _, err := LoadProcessor(nil, lookup(env)); err == nil {
			t.Errorf("environment %v was accepted", env)
		}
	}
}

func TestProcessorCLIOverridesEnvironment(t *testing.T) {
	env := map[string]string{"PROCESSOR_AUTO_OFFSET_RESET": "latest", "PROCESSOR_CONSUMER_GROUP": "env-group"}
	args := []string{"--max-messages", "5", "--idle-timeout", "2.5", "--log-level", "debug",
		"--from-beginning", "--group-id", "cli-group"}
	c, err := LoadProcessor(args, lookup(env))
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxMessages != 5 || c.IdleTimeout != 2500*time.Millisecond || c.LogLevel != "DEBUG" ||
		!c.FromBeginning || c.AutoOffsetReset != "earliest" || c.ConsumerGroup != "cli-group" {
		t.Fatalf("CLI overrides not applied: %+v", c)
	}
	if _, err := LoadProcessor([]string{"--max-messages", "-1"}, lookup(nil)); err == nil {
		t.Fatal("negative --max-messages was accepted")
	}
}

func TestProcessorMetricsPortPrecedence(t *testing.T) {
	c, err := LoadProcessor(nil, lookup(map[string]string{"METRICS_PORT": "9200", "PROCESSOR_METRICS_PORT": "9300"}))
	if err != nil || c.MetricsPort != 9300 {
		t.Fatalf("port %d err %v", c.MetricsPort, err)
	}
	c, err = LoadProcessor(nil, lookup(map[string]string{"METRICS_PORT": "9200"}))
	if err != nil || c.MetricsPort != 9200 {
		t.Fatalf("port %d err %v", c.MetricsPort, err)
	}
}

func TestFraudDefaults(t *testing.T) {
	c, err := LoadFraud(lookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.ReviewThreshold != 30 || c.BlockThreshold != 60 || c.RulesetVersion != "2026-08" ||
		len(c.EnabledRules) != 10 || c.HighAmountThreshold.String() != "5000" || !c.OutboxEnabled {
		t.Fatalf("unexpected fraud defaults: %+v", c)
	}
}
