// Command event-processor is the Go port of services.event_processor.main:
// CLI, signal handling, finite mode and graceful shutdown.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/config"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/dlq"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/idempotency"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/kafkaconsumer"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/logging"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/metrics"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/model"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/persistence"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/processor"
)

const healthFile = "/tmp/event-processor-healthy"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		if _, err := os.Stat(healthFile); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(run(os.Args[1:]))
}

func touchHealth() {
	now := time.Now()
	if err := os.Chtimes(healthFile, now, now); err != nil {
		if f, err := os.Create(healthFile); err == nil {
			_ = f.Close()
		}
	}
}

func safeRedisEndpoint(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "unknown"
	}
	host := u.Hostname()
	if host == "" {
		host = "unknown"
	}
	port := u.Port()
	if port == "" {
		port = "6379"
	}
	return fmt.Sprintf("%s://%s:%s%s", u.Scheme, host, port, u.Path)
}

// queueWaitSeconds ports queue_wait_seconds: negative skew is dropped.
func queueWaitSeconds(produced *time.Time, observed time.Time) (float64, bool) {
	if produced == nil {
		return 0, false
	}
	wait := observed.Sub(*produced).Seconds()
	return wait, wait >= 0
}

func run(args []string) int {
	logging.Configure("INFO")
	cfg, err := config.LoadProcessor(args, config.OSLookup)
	if err != nil {
		slog.Error("processor_failed", "error", err.Error())
		return 1
	}
	logging.Configure(cfg.LogLevel)
	fraudCfg, err := config.LoadFraud(config.OSLookup)
	if err != nil {
		slog.Error("processor_failed", "error", err.Error())
		return 1
	}
	m := metrics.New(cfg.MetricsServiceName, cfg.MetricsNamespace)
	var server *metrics.Server
	if cfg.MetricsEnabled {
		server, err = metrics.StartServer(m, cfg.MetricsHost, cfg.MetricsPort, cfg.MetricsPath, healthFile)
		if err != nil {
			slog.Error("metrics_server_failed", "error", err.Error())
			m.MarkUnhealthy()
		}
	}
	defer server.Stop()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			slog.Info("shutdown_requested")
		case <-done:
		}
	}()

	code, err := runProcessor(ctx, cfg, fraudCfg, m)
	close(done)
	if err != nil {
		slog.Error("processor_failed", "error", err.Error())
		_ = os.Remove(healthFile)
		return 1
	}
	return code
}

func runProcessor(ctx context.Context, cfg *config.Processor, fraudCfg *config.Fraud, m *metrics.Metrics) (code int, err error) {
	hostname, _ := os.Hostname()
	instanceID := cfg.ClientID + "-" + hostname
	summary := model.NewRunSummary()

	consumer, err := kafkaconsumer.New(cfg, m)
	if err != nil {
		return 1, err
	}
	store, err := idempotency.New(cfg, m)
	if err != nil {
		_ = consumer.Close()
		return 1, err
	}
	publisher, err := dlq.NewPublisher(cfg)
	if err != nil {
		_ = consumer.Close()
		_ = store.Close()
		return 1, err
	}
	db, err := persistence.NewDatabase(cfg)
	if err != nil {
		_ = consumer.Close()
		_ = store.Close()
		publisher.Close()
		return 1, err
	}
	defer func() {
		_ = os.Remove(healthFile)
		if ferr := consumer.FlushPending("shutdown"); ferr != nil {
			slog.Error("offset_flush_on_shutdown_failed", "error", ferr.Error())
		}
		_ = consumer.Close()
		publisher.Close()
		_ = store.Close()
		db.Close()
		slog.Info("processor_stopped", summary.AsLog()...)
		result := "graceful"
		if summary.UnresolvedRecords > 0 {
			result = "unresolved"
			if code == 0 {
				code = 1
			}
		}
		m.Shutdowns.WithLabelValues(result).Inc()
	}()

	uow, err := persistence.NewUnitOfWorkFactory(db, cfg, fraudCfg, m)
	if err != nil {
		return 1, err
	}
	proc := processor.New(cfg, store, publisher, consumer, persistence.Handlers, uow, summary, instanceID, m)

	if err := store.Ping(ctx); err != nil {
		return 1, err
	}
	if err := db.Open(ctx); err != nil {
		return 1, err
	}
	if err := consumer.Subscribe(); err != nil {
		return 1, err
	}
	touchHealth()
	runMode := "continuous"
	if cfg.MaxMessages > 0 {
		runMode = "finite"
	}
	slog.Info("processor_started",
		"kafka_bootstrap_servers", cfg.KafkaBootstrapServers,
		"input_topic", cfg.InputTopic,
		"dlq_topic", cfg.DLQTopic,
		"consumer_group", cfg.ConsumerGroup,
		"client_id", cfg.ClientID,
		"redis_endpoint", safeRedisEndpoint(cfg.RedisURL),
		"postgres_endpoint", persistence.SafeEndpoint(cfg.PostgresDSN),
		"required_schema_version", cfg.RequiredSchemaVersion,
		"run_mode", runMode,
		"supported_event_count", len(contracts.AllEventTypes),
		"runtime", "go",
	)

	lastRecordAt := time.Now()
	terminalCount := 0
	var previousProcessEnd time.Time
	for ctx.Err() == nil {
		if cfg.MaxMessages > 0 && terminalCount >= cfg.MaxMessages {
			break
		}
		pollStarted := time.Now()
		if !previousProcessEnd.IsZero() {
			m.LoopGap.Observe(pollStarted.Sub(previousProcessEnd).Seconds())
		}
		message, perr := consumer.Poll()
		if perr != nil {
			return 1, perr
		}
		m.LastPoll.SetToCurrentTime()
		touchHealth()
		if message == nil {
			if ferr := consumer.MaybeFlushIdle(); ferr != nil {
				return 1, ferr
			}
			if cfg.MaxMessages > 0 && time.Since(lastRecordAt) >= cfg.IdleTimeout {
				if !consumer.HasAssignment() {
					summary.KafkaErrors++
					summary.UnresolvedRecords++
				}
				break
			}
			continue
		}
		lastRecordAt = time.Now()
		m.PollToHandler.Observe(time.Since(pollStarted).Seconds())
		if wait, ok := queueWaitSeconds(message.Timestamp, time.Now()); ok {
			m.QueueWait.Observe(wait)
		}
		outcome, perr := proc.Process(message)
		previousProcessEnd = time.Now()
		if perr != nil {
			// processor.FatalError: the offset commit path failed.
			return 1, perr
		}
		if !outcome.Terminal() {
			break
		}
		terminalCount++
	}
	return 0, nil
}
