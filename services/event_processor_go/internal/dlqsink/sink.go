package dlqsink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HealthFile is touched on every loop iteration; `event-processor
// healthcheck /tmp/dlq-sink-healthy` checks it.
const HealthFile = "/tmp/dlq-sink-healthy"

// Config is read from the environment.
type Config struct {
	KafkaBootstrapServers string
	Topic                 string
	ConsumerGroup         string
	PostgresDSN           string
	MetricsHost           string
	MetricsPort           int
}

// LoadConfig reads the sink's settings. The DLQ topic name is shared with the
// processor (PROCESSOR_DLQ_TOPIC) so the two cannot drift apart.
func LoadConfig(lookup func(string) (string, bool)) (*Config, error) {
	get := func(name, def string) string {
		if v, ok := lookup(name); ok && v != "" {
			return v
		}
		return def
	}
	port, err := strconv.Atoi(get("DLQ_SINK_METRICS_PORT", "9104"))
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("DLQ_SINK_METRICS_PORT must be a port number")
	}
	c := &Config{
		KafkaBootstrapServers: get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
		Topic:                 get("PROCESSOR_DLQ_TOPIC", "commerce.events.dlq"),
		ConsumerGroup:         get("DLQ_SINK_CONSUMER_GROUP", "commerce-dlq-sink-v1"),
		PostgresDSN:           get("POSTGRES_DSN", "postgresql://commerce:commerce_local_dev@postgres:5432/commerce"),
		MetricsHost:           get("METRICS_HOST", "0.0.0.0"),
		MetricsPort:           port,
	}
	return c, nil
}

type sinkMetrics struct {
	registry    *prometheus.Registry
	records     *prometheus.CounterVec
	storeErrors prometheus.Counter
	lastIngest  prometheus.Gauge
}

func newMetrics() *sinkMetrics {
	m := &sinkMetrics{
		registry: prometheus.NewRegistry(),
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "commerce_dlq_sink_records_total",
			Help: "DLQ topic records handled by the sink, by result (inserted, duplicate, unreadable).",
		}, []string{"result"}),
		storeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "commerce_dlq_sink_store_errors_total",
			Help: "Failed dead_letters writes; each is retried before the offset is committed.",
		}),
		lastIngest: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "commerce_dlq_sink_last_record_timestamp_seconds",
			Help: "Unix time of the last DLQ record the sink stored.",
		}),
	}
	m.registry.MustRegister(m.records, m.storeErrors, m.lastIngest)
	return m
}

func touch() {
	now := time.Now()
	if err := os.Chtimes(HealthFile, now, now); err != nil {
		if f, err := os.Create(HealthFile); err == nil {
			_ = f.Close()
		}
	}
}

func serveMetrics(cfg *Config, m *sinkMetrics) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		status, code := "healthy", http.StatusOK
		if _, err := os.Stat(HealthFile); err != nil {
			status, code = "unhealthy", http.StatusServiceUnavailable
		}
		body, _ := json.Marshal(map[string]string{"status": status})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write(body)
	})
	srv := &http.Server{Addr: net.JoinHostPort(cfg.MetricsHost, strconv.Itoa(cfg.MetricsPort)), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return nil, err
	}
	go func() { _ = srv.Serve(ln) }()
	return srv, nil
}

// storeWithRetry keeps trying until the row is written or ctx ends. The
// offset is committed only after a successful write, so a database outage
// pauses the sink instead of losing records.
func storeWithRetry(ctx context.Context, store *Store, row Row, m *sinkMetrics) (bool, error) {
	backoff := 200 * time.Millisecond
	for {
		inserted, err := store.Insert(ctx, row)
		if err == nil {
			return inserted, nil
		}
		m.storeErrors.Inc()
		slog.Warn("dlq_sink_store_failed", "dlq_record_id", row.RecordID.String(), "error", err.Error(), "retry_in_ms", backoff.Milliseconds())
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// Run consumes the DLQ topic until ctx is cancelled.
func Run(ctx context.Context, cfg *Config) error {
	m := newMetrics()
	srv, err := serveMetrics(cfg, m)
	if err != nil {
		return err
	}
	defer func() { _ = srv.Close() }()
	defer func() { _ = os.Remove(HealthFile) }()

	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres unreachable: %w", err)
	}
	store := NewStore(pool)

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.KafkaBootstrapServers,
		"group.id":           cfg.ConsumerGroup,
		"client.id":          "commerce-dlq-sink",
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
		"isolation.level":    "read_committed",
	})
	if err != nil {
		return err
	}
	defer func() { _ = consumer.Close() }()
	if err := consumer.Subscribe(cfg.Topic, nil); err != nil {
		return err
	}
	slog.Info("dlq_sink_started", "topic", cfg.Topic, "consumer_group", cfg.ConsumerGroup)

	for ctx.Err() == nil {
		touch()
		msg, err := consumer.ReadMessage(time.Second)
		if err != nil {
			var kerr kafka.Error
			if errors.As(err, &kerr) && kerr.Code() == kafka.ErrTimedOut {
				continue
			}
			if errors.As(err, &kerr) && kerr.IsFatal() {
				return err
			}
			slog.Warn("dlq_sink_kafka_error", "error", err.Error())
			continue
		}
		row := Parse(msg.Value, *msg.TopicPartition.Topic, msg.TopicPartition.Partition, int64(msg.TopicPartition.Offset), time.Now())
		inserted, err := storeWithRetry(ctx, store, row, m)
		if err != nil {
			return nil // shutting down; the offset stays uncommitted and is redelivered
		}
		result := "duplicate"
		switch {
		case row.ErrorCategory == UnreadableCategory:
			result = "unreadable"
		case inserted:
			result = "inserted"
		}
		m.records.WithLabelValues(result).Inc()
		m.lastIngest.SetToCurrentTime()
		if _, err := consumer.CommitMessage(msg); err != nil {
			slog.Warn("dlq_sink_commit_failed", "error", err.Error())
		}
	}
	slog.Info("dlq_sink_stopped")
	return nil
}
