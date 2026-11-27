// Package metrics registers the processor's Prometheus families under the
// exact names, labels and buckets the Python prometheus_client registry used,
// so benchmark scripts, recording rules and Grafana dashboards work unchanged.
package metrics

import (
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// pythonDefaultBuckets is prometheus_client.Histogram.DEFAULT_BUCKETS.
var pythonDefaultBuckets = []float64{.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0}

type Metrics struct {
	Registry *prometheus.Registry
	service  string

	ServiceUp, ServiceHealthy, ServiceLastSuccess, ServiceStart *prometheus.GaugeVec

	EventsReceived       *prometheus.CounterVec
	EventsTerminal       *prometheus.CounterVec
	ValidationFailures   *prometheus.CounterVec
	Retries              *prometheus.CounterVec
	DLQPublished         *prometheus.CounterVec
	OffsetCommits        *prometheus.CounterVec
	OffsetCommitCalls    *prometheus.CounterVec
	OffsetCommitBatch    prometheus.Histogram
	Rebalances           *prometheus.CounterVec
	Shutdowns            *prometheus.CounterVec
	ProcessingDuration   *prometheus.HistogramVec
	PollToHandler        prometheus.Histogram
	LoopGap              prometheus.Histogram
	QueueWait            prometheus.Histogram
	PollDuration         prometheus.Histogram
	EmptyPolls           prometheus.Counter
	FetchqRecords        prometheus.Gauge
	BrokerRTTMs          prometheus.Gauge
	ValidationDuration   prometheus.Histogram
	RedisDuration        *prometheus.HistogramVec
	DatabaseDuration     *prometheus.HistogramVec
	DLQDuration          prometheus.Histogram
	OffsetDuration       prometheus.Histogram
	Inflight             prometheus.Gauge
	Assigned             prometheus.Gauge
	LastSuccess          prometheus.Gauge
	LastPoll             prometheus.Gauge
	IdempotencyOps       *prometheus.CounterVec
	IdempotencyDuration  *prometheus.HistogramVec
	DBTransactions       *prometheus.CounterVec
	DBRowsWritten        *prometheus.CounterVec
	DBSlowOperations     *prometheus.CounterVec
	DBTransactionTime    *prometheus.HistogramVec
	DBPoolAcquire        prometheus.Histogram
	DBSQLDuration        *prometheus.HistogramVec
	DBSQLCount           *prometheus.CounterVec
	DBStageDuration      *prometheus.HistogramVec
	DBConnectionRelease  prometheus.Histogram
	FraudEvaluations     *prometheus.CounterVec
	FraudRuleMatches     *prometheus.CounterVec
	FraudAlertsCreated   *prometheus.CounterVec
	FraudEvalDuration    *prometheus.HistogramVec
	FraudContextDuration prometheus.Histogram
	FraudScore           prometheus.Histogram
	FraudLastEvaluation  prometheus.Gauge
	OutboxWriteDuration  prometheus.Histogram
}

func New(service, namespace string) *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Name: name + "_total", Help: help}, labels)
		r.MustRegister(c)
		return c
	}
	gauge := func(name, help string, labels ...string) *prometheus.GaugeVec {
		g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: namespace, Name: name, Help: help}, labels)
		r.MustRegister(g)
		return g
	}
	plainGauge := func(name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: name, Help: help})
		r.MustRegister(g)
		return g
	}
	histogram := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		if buckets == nil {
			buckets = pythonDefaultBuckets
		}
		h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: namespace, Name: name, Help: help, Buckets: buckets}, labels)
		r.MustRegister(h)
		return h
	}
	plainHistogram := func(name, help string, buckets []float64) prometheus.Histogram {
		if buckets == nil {
			buckets = pythonDefaultBuckets
		}
		h := prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: namespace, Name: name, Help: help, Buckets: buckets})
		r.MustRegister(h)
		return h
	}
	emptyPolls := prometheus.NewCounter(prometheus.CounterOpts{Namespace: namespace, Name: "processor_empty_polls_total",
		Help: "poll() calls that returned no record within the poll timeout."})
	r.MustRegister(emptyPolls)

	m := &Metrics{
		Registry:           r,
		service:            service,
		ServiceUp:          gauge("service_up", "Application process is running.", "service"),
		ServiceHealthy:     gauge("service_healthy", "Application health evaluation.", "service"),
		ServiceLastSuccess: gauge("service_last_success_timestamp_seconds", "Unix timestamp of last successful work.", "service"),
		ServiceStart:       gauge("service_start_time_seconds", "Application start time.", "service"),

		EventsReceived:     counter("processor_events_received", "Source records received.", "event_type", "topic"),
		EventsTerminal:     counter("processor_events_terminal", "Terminal source outcomes.", "event_type", "result"),
		ValidationFailures: counter("processor_validation_failures", "Validation failures.", "category"),
		Retries:            counter("processor_retries", "Bounded processing retries.", "category", "outcome"),
		DLQPublished:       counter("processor_dlq_published", "Confirmed DLQ publications.", "error_category"),
		OffsetCommits:      counter("processor_offset_commits", "Batched manual offset commit call outcomes.", "result"),
		OffsetCommitCalls:  counter("processor_offset_commit_calls", "Batched offset commit calls by trigger.", "trigger"),
		OffsetCommitBatch: plainHistogram("processor_offset_commit_batch_records",
			"Terminal records represented by each batched offset commit call.", []float64{1, 5, 10, 25, 50, 100, 250, 500}),
		Rebalances:         counter("processor_rebalances", "Consumer rebalance actions.", "action"),
		Shutdowns:          counter("processor_shutdowns", "Processor shutdown outcomes.", "result"),
		ProcessingDuration: histogram("processor_event_processing_duration_seconds", "Record receipt through terminal handling.", nil, "event_type", "result"),
		PollToHandler:      plainHistogram("processor_poll_to_handler_duration_seconds", "Kafka poll through dispatch into the processor handler.", nil),
		LoopGap:            plainHistogram("processor_loop_gap_duration_seconds", "Time from one terminal process return to the next Kafka poll.", nil),
		QueueWait:          plainHistogram("processor_consumer_queue_wait_duration_seconds", "Kafka record timestamp through poll() return.", nil),
		PollDuration:       plainHistogram("processor_poll_duration_seconds", "Time actually blocked inside the Kafka client's poll() call.", nil),
		EmptyPolls:         emptyPolls,
		FetchqRecords:      plainGauge("processor_consumer_fetchq_records", "librdkafka's internal fetch-queue depth."),
		BrokerRTTMs:        plainGauge("processor_consumer_broker_rtt_ms", "Average broker round-trip time (ms)."),
		ValidationDuration: plainHistogram("processor_validation_duration_seconds", "Contract validation latency.", nil),
		RedisDuration:      histogram("processor_redis_duration_seconds", "Redis path latency.", nil, "operation", "result"),
		DatabaseDuration:   histogram("processor_database_duration_seconds", "Processor database latency.", nil, "operation", "result"),
		DLQDuration:        plainHistogram("processor_dlq_publish_duration_seconds", "DLQ confirmation latency.", nil),
		OffsetDuration:     plainHistogram("processor_offset_commit_duration_seconds", "Synchronous batched offset commit call latency.", nil),
		Inflight:           plainGauge("processor_inflight_events", "Current in-flight records."),
		Assigned:           plainGauge("processor_assigned_partitions", "Currently assigned source partitions."),
		LastSuccess:        plainGauge("processor_last_success_timestamp_seconds", "Last successful terminal source record."),
		LastPoll:           plainGauge("processor_last_poll_timestamp_seconds", "Last Kafka poll timestamp."),

		IdempotencyOps:      counter("idempotency_operations", "Redis idempotency outcomes.", "operation", "result"),
		IdempotencyDuration: histogram("idempotency_operation_duration_seconds", "Redis idempotency latency.", nil, "operation", "result"),

		DBTransactions:      counter("database_transactions", "Application database transactions.", "result"),
		DBRowsWritten:       counter("database_rows_written", "Rows written by bounded table name.", "table", "operation"),
		DBSlowOperations:    counter("database_slow_operations", "Slow operations over configured threshold.", "operation"),
		DBTransactionTime:   histogram("database_transaction_duration_seconds", "Transaction latency.", nil, "result"),
		DBPoolAcquire:       plainHistogram("database_pool_acquire_duration_seconds", "Connection acquisition latency.", nil),
		DBSQLDuration:       histogram("database_sql_duration_seconds", "Individual SQL round-trip latency by bounded operation.", nil, "operation", "statement_kind"),
		DBSQLCount:          counter("database_sql_statement_count", "Individual SQL statements by bounded operation.", "operation", "statement_kind"),
		DBStageDuration:     histogram("database_stage_duration_seconds", "Measured transaction stage latency.", nil, "stage"),
		DBConnectionRelease: plainHistogram("database_connection_release_duration_seconds", "Connection context release latency.", nil),

		FraudEvaluations:     counter("fraud_evaluations", "Fraud evaluations.", "event_type", "decision", "severity"),
		FraudRuleMatches:     counter("fraud_rule_matches", "Matched fraud rules.", "rule_id", "severity"),
		FraudAlertsCreated:   counter("fraud_alerts_created", "Durable fraud alerts.", "decision", "severity"),
		FraudEvalDuration:    histogram("fraud_evaluation_duration_seconds", "Fraud evaluation latency.", nil, "event_type", "decision"),
		FraudContextDuration: plainHistogram("fraud_context_build_duration_seconds", "Fraud context build latency.", nil),
		FraudScore:           plainHistogram("fraud_score", "Synthetic rule-based score distribution.", []float64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100}),
		FraudLastEvaluation:  plainGauge("fraud_last_evaluation_timestamp_seconds", "Last fraud evaluation timestamp."),
		OutboxWriteDuration:  plainHistogram("outbox_write_duration_seconds", "Transactional fraud outbox row write latency.", nil),
	}
	now := float64(time.Now().UnixNano()) / 1e9
	m.ServiceUp.WithLabelValues(service).Set(1)
	m.ServiceHealthy.WithLabelValues(service).Set(1)
	m.ServiceStart.WithLabelValues(service).Set(now)
	return m
}

// Success ports ApplicationMetrics.success().
func (m *Metrics) Success() {
	m.ServiceLastSuccess.WithLabelValues(m.service).SetToCurrentTime()
	m.ServiceHealthy.WithLabelValues(m.service).Set(1)
}

// MarkUnhealthy flags the service as unhealthy (metrics server failure).
func (m *Metrics) MarkUnhealthy() { m.ServiceHealthy.WithLabelValues(m.service).Set(0) }

// Server serves /metrics and /health like shared.observability.MetricsServer.
type Server struct{ srv *http.Server }

func StartServer(m *Metrics, host string, port int, path, healthFile string) (*Server, error) {
	mux := http.NewServeMux()
	mux.Handle(path, promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		_, err := os.Stat(healthFile)
		healthy := err == nil
		status := "healthy"
		code := http.StatusOK
		if !healthy {
			status, code = "unhealthy", http.StatusServiceUnavailable
		}
		body, _ := json.Marshal(map[string]string{"status": status})
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(code)
		_, _ = w.Write(body)
	})
	srv := &http.Server{Addr: host + ":" + strconv.Itoa(port), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ln, err := listen(srv.Addr)
	if err != nil {
		return nil, err
	}
	go func() { _ = srv.Serve(ln) }()
	return &Server{srv: srv}, nil
}

func (s *Server) Stop() {
	if s != nil && s.srv != nil {
		_ = s.srv.Close()
	}
}
