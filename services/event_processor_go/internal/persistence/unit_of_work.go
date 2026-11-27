package persistence

import (
	"context"
	"errors"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/config"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/dbtx"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/fraud"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/metrics"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/model"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
)

// Result ports PersistenceResult.
type Result struct {
	AlreadyPersisted bool
	AffectedTables   []string
	RowsWritten      map[string]int
	DurationMs       float64
	FraudEventType   string
	FraudSeverity    string
	FraudScore       int
	FraudDecision    string
	MatchedRuleIDs   []string
}

// Handler applies one event type's business effects inside the transaction.
type Handler func(tx *dbtx.Tx, e *contracts.Envelope) (map[string]int, error)

func payloadMismatch() error {
	return procerr.New(procerr.PermanentDatabaseIntegrity, "handler payload type mismatch")
}

func single(table string, n int, err error) (map[string]int, error) {
	if err != nil {
		return nil, err
	}
	return map[string]int{table: n}, nil
}

// Handlers ports default_persistence_registry().
var Handlers = map[contracts.EventType]Handler{
	contracts.UserRegisteredType: func(tx *dbtx.Tx, e *contracts.Envelope) (map[string]int, error) {
		p, ok := e.Payload.(*contracts.UserRegistered)
		if !ok {
			return nil, payloadMismatch()
		}
		n, err := registerCustomer(tx, p, e.EventID)
		return single("customers", n, err)
	},
	contracts.SessionStartedType: func(tx *dbtx.Tx, e *contracts.Envelope) (map[string]int, error) {
		p, ok := e.Payload.(*contracts.SessionStarted)
		if !ok {
			return nil, payloadMismatch()
		}
		n, err := insertSession(tx, p, e.EventID)
		return single("sessions", n, err)
	},
	contracts.ProductViewedType: func(tx *dbtx.Tx, e *contracts.Envelope) (map[string]int, error) {
		p, ok := e.Payload.(*contracts.ProductViewed)
		if !ok {
			return nil, payloadMismatch()
		}
		n, err := insertProductView(tx, p, e.EventID, e.EventTime)
		return single("product_views", n, err)
	},
	contracts.AddedToCartType: func(tx *dbtx.Tx, e *contracts.Envelope) (map[string]int, error) {
		p, ok := e.Payload.(*contracts.AddedToCart)
		if !ok {
			return nil, payloadMismatch()
		}
		return addCartItem(tx, p, e.EventID)
	},
	contracts.CheckoutStartedType: func(tx *dbtx.Tx, e *contracts.Envelope) (map[string]int, error) {
		p, ok := e.Payload.(*contracts.CheckoutStarted)
		if !ok {
			return nil, payloadMismatch()
		}
		n, err := checkout(tx, p, e.EventID)
		return single("carts", n, err)
	},
	contracts.OrderCreatedType: func(tx *dbtx.Tx, e *contracts.Envelope) (map[string]int, error) {
		p, ok := e.Payload.(*contracts.OrderCreated)
		if !ok {
			return nil, payloadMismatch()
		}
		return insertOrder(tx, p, e.EventID)
	},
	contracts.PaymentCompletedType: paymentHandler(false),
	contracts.PaymentFailedType:    paymentHandler(true),
	contracts.RefundRequestedType: func(tx *dbtx.Tx, e *contracts.Envelope) (map[string]int, error) {
		p, ok := e.Payload.(*contracts.RefundRequested)
		if !ok {
			return nil, payloadMismatch()
		}
		return insertRefund(tx, p, e.EventID)
	},
	contracts.FraudAlertCreatedType: func(tx *dbtx.Tx, e *contracts.Envelope) (map[string]int, error) {
		p, ok := e.Payload.(*contracts.FraudAlertCreated)
		if !ok {
			return nil, payloadMismatch()
		}
		n, err := insertExternalFraudAlert(tx, p)
		return single("fraud_alerts", n, err)
	},
}

func paymentHandler(failed bool) Handler {
	return func(tx *dbtx.Tx, e *contracts.Envelope) (map[string]int, error) {
		p, ok := e.Payload.(*contracts.Payment)
		if !ok || p.Failed != failed {
			return nil, payloadMismatch()
		}
		return insertPayment(tx, p, e.EventID)
	}
}

// UnitOfWorkFactory ports UnitOfWorkFactory.persist().
type UnitOfWorkFactory struct {
	db         *Database
	cfg        *config.Processor
	fraudCfg   *config.Fraud
	metrics    *metrics.Metrics
	builder    *fraud.Builder
	engine     *fraud.Engine
	fraudStore *fraud.Repository
}

func NewUnitOfWorkFactory(db *Database, cfg *config.Processor, fraudCfg *config.Fraud, m *metrics.Metrics) (*UnitOfWorkFactory, error) {
	engine, err := fraud.NewEngine(fraudCfg)
	if err != nil {
		return nil, err
	}
	return &UnitOfWorkFactory{db: db, cfg: cfg, fraudCfg: fraudCfg, metrics: m,
		builder: fraud.NewBuilder(fraudCfg), engine: engine, fraudStore: fraud.NewRepository(fraudCfg, m)}, nil
}

func (f *UnitOfWorkFactory) stage(name string, started time.Time) {
	if f.metrics != nil {
		f.metrics.DBStageDuration.WithLabelValues(name).Observe(time.Since(started).Seconds())
	}
}

func (f *UnitOfWorkFactory) Persist(e *contracts.Envelope, m *model.ConsumedMessage, pctx model.ProcessingContext, handler Handler) (*Result, error) {
	started := time.Now()
	result, err := f.persist(e, m, pctx, handler, started)
	if err == nil {
		return result, nil
	}
	if procerr.Is(err, procerr.AlreadyPersisted) {
		return &Result{AlreadyPersisted: true, RowsWritten: map[string]int{}, DurationMs: msSince(started)}, nil
	}
	return nil, classify(err)
}

func msSince(t time.Time) float64 { return float64(time.Since(t).Nanoseconds()) / 1e6 }

func (f *UnitOfWorkFactory) persist(e *contracts.Envelope, m *model.ConsumedMessage, pctx model.ProcessingContext, handler Handler, started time.Time) (*Result, error) {
	ctx := context.Background()
	acquireStarted := time.Now()
	actx, cancel := context.WithTimeout(ctx, f.cfg.DBAcquireTimeout)
	conn, err := f.db.Pool.Acquire(actx)
	cancel()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, procerr.Wrap(procerr.RetryableDatabase, "database pool acquisition timed out", err)
		}
		return nil, err
	}
	defer func() {
		releaseStarted := time.Now()
		conn.Release()
		if f.metrics != nil {
			f.metrics.DBConnectionRelease.Observe(time.Since(releaseStarted).Seconds())
		}
	}()
	if f.metrics != nil {
		f.metrics.DBPoolAcquire.Observe(time.Since(acquireStarted).Seconds())
	}
	pgxTx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = pgxTx.Rollback(ctx)
		}
	}()
	tx := dbtx.New(ctx, pgxTx, f.metrics)
	res := &Result{}

	tx.SetPhase("processed_events")
	stageStarted := time.Now()
	if err := insertProcessedEvent(tx, e, m, pctx, f.cfg.PersistRawEventJSON); err != nil {
		return nil, err
	}
	f.stage("processed_events", stageStarted)

	tx.SetPhase("business")
	stageStarted = time.Now()
	rows, err := handler(tx, e)
	if err != nil {
		return nil, err
	}
	f.stage("business_persistence", stageStarted)

	if f.fraudCfg.EngineEnabled && fraud.EligibleEventTypes[e.EventType] {
		tx.SetPhase("fraud_context")
		contextStarted := time.Now()
		fctx, err := f.builder.Build(tx, e)
		if err != nil {
			return nil, err
		}
		if f.metrics != nil {
			d := time.Since(contextStarted).Seconds()
			f.metrics.FraudContextDuration.Observe(d)
			f.metrics.DBStageDuration.WithLabelValues("fraud_context").Observe(d)
		}
		evaluationStarted := time.Now()
		evaluation, err := f.engine.Evaluate(fctx)
		if err != nil {
			return nil, err
		}
		if f.metrics != nil {
			f.metrics.FraudEvalDuration.WithLabelValues(string(e.EventType), string(evaluation.Decision)).
				Observe(time.Since(evaluationStarted).Seconds())
		}
		res.FraudDecision = string(evaluation.Decision)
		res.FraudEventType = string(e.EventType)
		res.FraudSeverity = string(evaluation.Severity)
		res.FraudScore = evaluation.TotalScore
		for _, r := range evaluation.MatchedRules() {
			res.MatchedRuleIDs = append(res.MatchedRuleIDs, r.RuleID)
		}
		tx.SetPhase("fraud_persistence")
		stageStarted = time.Now()
		fraudRows, err := f.fraudStore.Persist(tx, evaluation, e)
		if err != nil {
			return nil, err
		}
		f.stage("fraud_persistence", stageStarted)
		for table, n := range fraudRows {
			rows[table] += n
		}
	}
	commitStarted := time.Now()
	if err := pgxTx.Commit(ctx); err != nil {
		return nil, err
	}
	committed = true
	f.stage("commit", commitStarted)

	rows["processed_events"] = 1
	for name, n := range rows {
		if n != 0 {
			res.AffectedTables = append(res.AffectedTables, name)
		}
	}
	sort.Strings(res.AffectedTables)
	res.RowsWritten = rows
	res.DurationMs = msSince(started)
	return res, nil
}

// classify maps database failures the way UnitOfWorkFactory.persist does:
// classified processor errors pass through; SQLSTATE class 08 and
// serialization/deadlock failures are retryable; other SQLSTATEs are
// deterministic integrity failures. One deliberate difference: transport
// failures without a SQLSTATE (connection reset, broken pipe) are treated as
// retryable here instead of being dead-lettered.
func classify(err error) error {
	if _, ok := procerr.As(err); ok {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if strings.HasPrefix(pgErr.Code, "08") || pgErr.Code == "40001" || pgErr.Code == "40P01" {
			return procerr.Wrap(procerr.RetryableDatabase, "transient PostgreSQL failure", err)
		}
		return procerr.Wrap(procerr.PermanentDatabaseIntegrity, "deterministic PostgreSQL constraint failure", err)
	}
	var netErr net.Error
	var connectErr *pgconn.ConnectError
	if errors.As(err, &netErr) || errors.As(err, &connectErr) || pgconn.SafeToRetry(err) ||
		errors.Is(err, context.DeadlineExceeded) || pgconn.Timeout(err) {
		return procerr.Wrap(procerr.RetryableDatabase, "transient PostgreSQL failure", err)
	}
	return procerr.Wrap(procerr.PermanentDatabaseIntegrity, "deterministic PostgreSQL constraint failure", err)
}
