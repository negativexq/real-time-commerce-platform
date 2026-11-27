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

// GroupItem is one source event of a group transaction.
type GroupItem struct {
	Event   *contracts.Envelope
	Message *model.ConsumedMessage
	Context model.ProcessingContext
	Handler Handler
}

// Persist writes one source event in its own transaction.
func (f *UnitOfWorkFactory) Persist(e *contracts.Envelope, m *model.ConsumedMessage, pctx model.ProcessingContext, handler Handler) (*Result, error) {
	results, err := f.PersistGroup([]GroupItem{{Event: e, Message: m, Context: pctx, Handler: handler}})
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

// PersistGroup writes several source events in one transaction with one
// COMMIT, so the commit and its WAL flush are paid once per group instead of
// once per event. Events are applied in order, so each one's reads (fraud
// context included) see the writes of the events before it, exactly as when
// they are committed one by one. An event that is already in the ledger is
// reported as AlreadyPersisted and skipped. Any other failure rolls back the
// whole group and is returned classified: the caller then retries the events
// one at a time to find the one that failed.
func (f *UnitOfWorkFactory) PersistGroup(items []GroupItem) ([]*Result, error) {
	started := time.Now()
	results, err := f.persistGroup(items)
	if err != nil {
		return nil, classify(err)
	}
	// One transaction's time, shared out so per-event averages stay comparable.
	share := msSince(started) / float64(len(items))
	for _, r := range results {
		r.DurationMs = share
	}
	return results, nil
}

func msSince(t time.Time) float64 { return float64(time.Since(t).Nanoseconds()) / 1e6 }

func (f *UnitOfWorkFactory) persistGroup(items []GroupItem) ([]*Result, error) {
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
	// BEGIN and every ledger insert of the group share one round trip.
	stageStarted := time.Now()
	ledger := &dbtx.Batch{}
	inserted := make([]bool, len(items))
	for i, it := range items {
		queueProcessedEventInsert(ledger, it.Event, it.Message, it.Context, f.cfg.PersistRawEventJSON, &inserted[i])
	}
	tx, err := dbtx.Begin(ctx, conn, f.metrics, "processed_events", ledger)
	committed := false
	if tx != nil {
		defer func() {
			if !committed {
				_ = tx.Tx().Rollback(ctx)
			}
		}()
	}
	if err != nil {
		return nil, err
	}
	results := make([]*Result, len(items))
	pending := 0
	for i, it := range items {
		if inserted[i] {
			pending++
			continue
		}
		err := resolveProcessedEventConflict(tx, it.Event, it.Message)
		if !procerr.Is(err, procerr.AlreadyPersisted) {
			return nil, err
		}
		results[i] = &Result{AlreadyPersisted: true, RowsWritten: map[string]int{}}
	}
	f.stage("processed_events", stageStarted)
	if pending == 0 {
		return results, nil // nothing new: roll back the empty transaction
	}
	for i, it := range items {
		if results[i] != nil {
			continue
		}
		res, err := f.apply(tx, it.Event, it.Handler)
		if err != nil {
			return nil, err
		}
		results[i] = res
	}
	commitStarted := time.Now()
	if err := tx.Tx().Commit(ctx); err != nil {
		return nil, err
	}
	committed = true
	f.stage("commit", commitStarted)
	return results, nil
}

// apply writes one event's business rows and, when eligible, its fraud
// evaluation, inside the group transaction.
func (f *UnitOfWorkFactory) apply(tx *dbtx.Tx, e *contracts.Envelope, handler Handler) (*Result, error) {
	res := &Result{}
	tx.SetPhase("business")
	stageStarted := time.Now()
	rows, err := handler(tx, e)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		rows = map[string]int{}
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
	rows["processed_events"] = 1
	for name, n := range rows {
		if n != 0 {
			res.AffectedTables = append(res.AffectedTables, name)
		}
	}
	sort.Strings(res.AffectedTables)
	res.RowsWritten = rows
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
