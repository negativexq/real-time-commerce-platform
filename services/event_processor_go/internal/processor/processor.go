// Package processor ports services.event_processor.processor.MessageProcessor:
// validation, Redis reservation, bounded retry, transactional persistence,
// DLQ, and offset commit, in the same order with the same outcomes.
package processor

import (
	"context"
	"log/slog"
	"math"
	"math/rand"
	"time"

	"github.com/google/uuid"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/config"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/dlq"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/idempotency"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/metrics"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/model"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/persistence"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/validation"
)

// Committer records terminal offsets (the Kafka consumer).
type Committer interface {
	CommitTerminal(*model.ConsumedMessage) error
}

// Persister is the transactional persistence boundary.
type Persister interface {
	Persist(*contracts.Envelope, *model.ConsumedMessage, model.ProcessingContext, persistence.Handler) (*persistence.Result, error)
	PersistGroup([]persistence.GroupItem) ([]*persistence.Result, error)
}

// Idempotency is the Redis lease store.
type Idempotency interface {
	Reserve(ctx context.Context, id uuid.UUID, token string, m *model.ConsumedMessage, at time.Time) (idempotency.ReservationState, error)
	Complete(ctx context.Context, id uuid.UUID, token string, at time.Time) (bool, error)
	Release(ctx context.Context, id uuid.UUID, token string) (bool, error)
}

// DLQPublisher confirms dead-letter publication.
type DLQPublisher interface {
	Publish(*dlq.Envelope) error
}

type Processor struct {
	cfg        *config.Processor
	store      Idempotency
	dlq        DLQPublisher
	committer  Committer
	handlers   map[contracts.EventType]persistence.Handler
	persist    Persister
	summary    *model.RunSummary
	instanceID string
	metrics    *metrics.Metrics
	wait       func(time.Duration)
	rng        *rand.Rand
	now        func() time.Time
}

func New(cfg *config.Processor, store Idempotency, publisher DLQPublisher, committer Committer,
	handlers map[contracts.EventType]persistence.Handler, persist Persister, summary *model.RunSummary,
	instanceID string, m *metrics.Metrics) *Processor {
	return &Processor{
		cfg: cfg, store: store, dlq: publisher, committer: committer, handlers: handlers,
		persist: persist, summary: summary, instanceID: instanceID, metrics: m,
		wait: time.Sleep, rng: rand.New(rand.NewSource(time.Now().UnixNano())), now: time.Now,
	}
}

// FatalError is returned when the loop must stop (offset commit failure).
type FatalError struct{ Err error }

func (e *FatalError) Error() string { return e.Err.Error() }
func (e *FatalError) Unwrap() error { return e.Err }

// Process handles one record to a terminal or unresolved outcome. A
// FatalError means the commit path failed and the caller must shut down.
func (p *Processor) Process(m *model.ConsumedMessage) (model.ProcessingOutcome, error) {
	started := time.Now()
	if p.metrics != nil {
		p.metrics.Inflight.Inc()
	}
	// Every exit, dead letters and unresolved records included, is observed exactly once,
	// so the terminal counter matches the DLQ counter and the in-flight gauge returns to zero.
	eventType := "unknown"
	outcome, err := p.process(m, &eventType)
	p.observeTerminal(eventType, outcome, started)
	return outcome, err
}

func (p *Processor) process(m *model.ConsumedMessage, observedType *string) (model.ProcessingOutcome, error) {
	pr := p.prepare(m)
	*observedType = pr.eventType
	return p.finish(pr)
}

// prepared is a record after validation and its Redis reservation.
type prepared struct {
	m         *model.ConsumedMessage
	started   time.Time
	eventType string
	invalid   *model.ValidationErrorInfo
	event     *contracts.Envelope
	token     string
	state     idempotency.ReservationState
	redisErr  error
}

// fresh reports whether the record is a new event this processor now holds
// the lease for and can persist: the only kind a group transaction takes.
func (pr *prepared) fresh(handlers map[contracts.EventType]persistence.Handler) bool {
	if pr.invalid != nil || pr.redisErr != nil || pr.state != idempotency.Reserved {
		return false
	}
	_, ok := handlers[pr.event.EventType]
	return ok
}

func (p *Processor) prepare(m *model.ConsumedMessage) *prepared {
	pr := &prepared{m: m, started: time.Now(), eventType: "unknown"}
	p.summary.ConsumedRecords++
	validationStarted := time.Now()
	result := validation.ValidateMessage(m)
	if p.metrics != nil {
		p.metrics.ValidationDuration.Observe(time.Since(validationStarted).Seconds())
	}
	if !result.Valid() {
		e := result.Error
		p.summary.ValidationFailures[e.Category]++
		if p.metrics != nil {
			p.metrics.EventsReceived.WithLabelValues("unknown", m.Topic).Inc()
			p.metrics.ValidationFailures.WithLabelValues(string(e.Category)).Inc()
		}
		pr.invalid = e
		return pr
	}
	pr.event = result.Event
	pr.eventType = string(pr.event.EventType)
	if p.metrics != nil {
		p.metrics.EventsReceived.WithLabelValues(pr.eventType, m.Topic).Inc()
	}
	p.summary.ValidRecords++
	pr.token = uuid.NewString()
	pr.state, pr.redisErr = p.store.Reserve(context.Background(), pr.event.EventID, pr.token, m, p.now().UTC())
	return pr
}

// finish takes a prepared record to its outcome on its own, with its own
// transaction and retries.
func (p *Processor) finish(pr *prepared) (model.ProcessingOutcome, error) {
	m, event := pr.m, pr.event
	if pr.invalid != nil {
		return p.deadLetter(m, pr.invalid, 1)
	}
	if pr.redisErr != nil {
		p.summary.RedisErrors++
		p.summary.UnresolvedRecords++
		return model.ProcessingOutcome{Status: model.Unresolved, Attempts: 1}, nil
	}
	ctx := context.Background()
	switch pr.state {
	case idempotency.Completed:
		recoveredPersisted := false
		handler, ok := p.handlers[event.EventType]
		if ok {
			res, attempts, err := p.runWithRetry(handler, event, m)
			if err != nil {
				outcome, ferr := p.persistRecoveryFailure(m, event, err, attempts)
				return outcome, ferr
			}
			recoveredPersisted = res.AlreadyPersisted
			if !res.AlreadyPersisted {
				p.recordPersistence(res)
			}
		}
		slog.Info("duplicate_event_skipped", "event_id", event.EventID.String(), "topic", m.Topic,
			"partition", m.Partition, "offset", m.Offset, "existing_state", "completed", "action", "skip_duplicate")
		if err := p.commit(m); err != nil {
			return model.ProcessingOutcome{Status: model.Unresolved}, err
		}
		p.summary.DuplicateRecords++
		if recoveredPersisted {
			p.summary.AlreadyPersistedEvents++
		}
		return model.ProcessingOutcome{Status: model.Duplicate, Attempts: 1, EventType: event.EventType}, nil
	case idempotency.Processing:
		slog.Warn("active_processing_lease", "event_id", event.EventID.String(), "topic", m.Topic,
			"partition", m.Partition, "offset", m.Offset)
		p.summary.UnresolvedRecords++
		return model.ProcessingOutcome{Status: model.Unresolved, Attempts: 1}, nil
	}

	handler, ok := p.handlers[event.EventType]
	var res *persistence.Result
	var err error
	attempts := 1
	if !ok {
		err = procerr.New(procerr.PermanentProcessing, "no handler registered for "+pr.eventType)
	} else {
		res, attempts, err = p.runWithRetry(handler, event, m)
	}
	if err != nil {
		p.summary.ProcessingFailures++
		p.safeRelease(ctx, event.EventID, pr.token)
		if procerr.IsPermanent(err) {
			if procerr.Is(err, procerr.FraudEvaluationIntegrity) {
				p.summary.FraudIntegrityFailures++
			}
			category := model.PermanentProcessingError
			if procerr.Is(err, procerr.PermanentDatabaseIntegrity) {
				p.summary.IntegrityFailures++
				p.summary.DatabaseErrors++
				category = model.DatabaseIntegrityError
			}
			return p.deadLetter(m, processingError(event, err, category), attempts)
		}
		p.summary.RetryExhausted++
		if procerr.IsMissingDependency(err) {
			p.summary.MissingDependencyErrors++
		}
		if procerr.Is(err, procerr.FraudContextDependency) {
			p.summary.FraudContextFailures++
		}
		if procerr.Is(err, procerr.RetryableDatabase) {
			p.summary.DatabaseErrors++
		}
		category := model.RetryExhausted
		if procerr.IsMissingDependency(err) {
			category = model.MissingBusinessDependency
		}
		return p.deadLetter(m, processingError(event, err, category), p.cfg.MaxProcessingAttempts)
	}
	return p.persisted(pr, res, attempts)
}

// persisted completes a record whose transaction committed: the Redis lease
// becomes completed, then the offset is marked terminal.
func (p *Processor) persisted(pr *prepared, res *persistence.Result, attempts int) (model.ProcessingOutcome, error) {
	m, event := pr.m, pr.event
	ctx := context.Background()
	p.recordPersistence(res)
	completed, err := p.store.Complete(ctx, event.EventID, pr.token, p.now().UTC())
	if err != nil || !completed {
		p.summary.RedisErrors++
		p.summary.UnresolvedRecords++
		return model.ProcessingOutcome{Status: model.Unresolved, Attempts: attempts}, nil
	}
	if err := p.commit(m); err != nil {
		return model.ProcessingOutcome{Status: model.Unresolved, Attempts: attempts}, err
	}
	durationMs := float64(time.Since(pr.started).Nanoseconds()) / 1e6
	p.summary.ProcessedRecords++
	p.summary.ProcessedEvents[event.EventType]++
	p.summary.RecordLatency(durationMs)
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("event_processed", "event_id", event.EventID.String(), "event_type", pr.eventType,
			"correlation_id", event.CorrelationID.String(), "topic", m.Topic, "partition", m.Partition,
			"offset", m.Offset, "processing_attempt", attempts, "processing_duration_ms", durationMs,
			"already_persisted", res.AlreadyPersisted, "affected_tables", res.AffectedTables,
			"database_duration_ms", res.DurationMs)
	}
	return model.ProcessingOutcome{Status: model.Processed, Attempts: attempts, EventType: event.EventType}, nil
}

// ProcessBatch handles records polled together, in order. Consecutive new
// events share one database transaction and one COMMIT; any other record
// (invalid, duplicate, lease held elsewhere, Redis down) first commits the
// events before it, then goes through the one-record path. If a group
// transaction fails, its events are retried one at a time, so a bad event is
// retried and dead-lettered alone and the others still commit.
//
// The returned outcomes cover a prefix of records: processing stops after
// the first record that is not terminal, as the single-record loop does; the
// records after it are left uncommitted and are redelivered. A FatalError
// means the offset commit path failed.
func (p *Processor) ProcessBatch(msgs []*model.ConsumedMessage) ([]model.ProcessingOutcome, error) {
	outcomes := make([]model.ProcessingOutcome, 0, len(msgs))
	var group []*prepared
	stop := false
	finishOne := func(pr *prepared, outcome model.ProcessingOutcome, err error) error {
		p.observeTerminal(pr.eventType, outcome, pr.started)
		outcomes = append(outcomes, outcome)
		if !outcome.Terminal() {
			stop = true
		}
		return err
	}
	flush := func() error {
		defer func() { group = group[:0] }()
		if len(group) == 0 {
			return nil
		}
		if len(group) == 1 {
			outcome, err := p.finish(group[0])
			return finishOne(group[0], outcome, err)
		}
		results, err := p.persistGroup(group)
		if err != nil {
			slog.Warn("group_transaction_failed", "events", len(group), "error", err.Error(),
				"action", "retry_one_by_one")
			if p.metrics != nil {
				p.metrics.Retries.WithLabelValues("group_transaction", "scheduled").Inc()
			}
			for _, pr := range group {
				if stop {
					// An earlier event is unresolved: give up this one's lease so its
					// redelivery is not blocked until the lease expires.
					p.safeRelease(context.Background(), pr.event.EventID, pr.token)
					p.abandon()
					continue
				}
				outcome, err := p.finish(pr)
				if ferr := finishOne(pr, outcome, err); ferr != nil {
					return ferr
				}
			}
			return nil
		}
		// Committed: every event completes, even after an unresolved one, so no
		// lease is left held on a committed event.
		for i, pr := range group {
			outcome, err := p.persisted(pr, results[i], 1)
			if ferr := finishOne(pr, outcome, err); ferr != nil {
				return ferr
			}
		}
		return nil
	}
	for _, m := range msgs {
		if stop {
			break
		}
		if p.metrics != nil {
			p.metrics.Inflight.Inc()
		}
		pr := p.prepare(m)
		if pr.fresh(p.handlers) {
			group = append(group, pr)
			continue
		}
		pending := len(group) > 0
		if err := flush(); err != nil {
			p.abandon()
			return outcomes, err
		}
		if pending && !stop && pr.redisErr == nil && pr.state == idempotency.Processing {
			// The lease may be one this batch just held, for an earlier copy of the
			// same event: now that the group has completed, ask again, as the
			// one-record loop would have.
			pr.token = uuid.NewString()
			pr.state, pr.redisErr = p.store.Reserve(context.Background(), pr.event.EventID, pr.token, m, p.now().UTC())
			if pr.fresh(p.handlers) {
				group = append(group, pr)
				continue
			}
		}
		if stop {
			// Not processed: release a lease taken in prepare and leave it for redelivery.
			if pr.redisErr == nil && pr.state == idempotency.Reserved {
				p.safeRelease(context.Background(), pr.event.EventID, pr.token)
			}
			p.abandon()
			break
		}
		outcome, err := p.finish(pr)
		if ferr := finishOne(pr, outcome, err); ferr != nil {
			return outcomes, ferr
		}
	}
	return outcomes, flush()
}

// abandon accounts for a record the batch stopped before processing: it
// leaves the in-flight gauge and is redelivered, so it has no outcome.
func (p *Processor) abandon() {
	if p.metrics != nil {
		p.metrics.Inflight.Dec()
	}
}

// persistGroup writes the group's events in one transaction.
func (p *Processor) persistGroup(group []*prepared) ([]*persistence.Result, error) {
	items := make([]persistence.GroupItem, len(group))
	for i, pr := range group {
		m := pr.m
		items[i] = persistence.GroupItem{Event: pr.event, Message: m, Handler: p.handlers[pr.event.EventType],
			Context: model.ProcessingContext{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset,
				ConsumerGroup: p.cfg.ConsumerGroup, ProcessorInstanceID: p.instanceID, Attempt: 1}}
	}
	p.summary.DatabaseTransactionsStarted += len(group)
	results, err := p.persist.PersistGroup(items)
	if err != nil {
		p.summary.DatabaseTransactionsRolledBack += len(group)
	}
	return results, err
}

// persistRecoveryFailure ports the except branches of _persist_with_retry.
func (p *Processor) persistRecoveryFailure(m *model.ConsumedMessage, event *contracts.Envelope, err error, attempts int) (model.ProcessingOutcome, error) {
	if procerr.IsPermanent(err) {
		if procerr.Is(err, procerr.PermanentDatabaseIntegrity) {
			p.summary.IntegrityFailures++
		}
		return p.deadLetter(m, processingError(event, err, model.DatabaseIntegrityError), attempts)
	}
	p.summary.RetryExhausted++
	category := model.RetryExhausted
	if procerr.IsMissingDependency(err) {
		category = model.MissingBusinessDependency
	}
	return p.deadLetter(m, processingError(event, err, category), p.cfg.MaxProcessingAttempts)
}

// runWithRetry ports retry.run_with_retry with capped jittered backoff.
func (p *Processor) runWithRetry(handler persistence.Handler, event *contracts.Envelope, m *model.ConsumedMessage) (*persistence.Result, int, error) {
	for attempt := 1; ; attempt++ {
		res, err := p.handle(handler, event, m, attempt)
		if err == nil {
			return res, attempt, nil
		}
		if !procerr.IsRetryable(err) || attempt >= p.cfg.MaxProcessingAttempts {
			return nil, attempt, err
		}
		p.onRetry(attempt, err)
		p.wait(p.backoff(attempt))
	}
}

func (p *Processor) backoff(failedAttempt int) time.Duration {
	base := math.Min(float64(p.cfg.RetryMaxBackoffMs),
		float64(p.cfg.RetryInitialBackoffMs)*math.Pow(p.cfg.RetryMultiplier, float64(failedAttempt-1)))
	jitter := base * p.cfg.RetryJitterRatio
	ms := math.Max(0, base+(p.rng.Float64()*2-1)*jitter)
	return time.Duration(ms * float64(time.Millisecond))
}

func (p *Processor) handle(handler persistence.Handler, event *contracts.Envelope, m *model.ConsumedMessage, attempt int) (*persistence.Result, error) {
	ctx := model.ProcessingContext{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset,
		ConsumerGroup: p.cfg.ConsumerGroup, ProcessorInstanceID: p.instanceID, Attempt: attempt}
	p.summary.DatabaseTransactionsStarted++
	res, err := p.persist.Persist(event, m, ctx, handler)
	if err != nil {
		p.summary.DatabaseTransactionsRolledBack++
	}
	return res, err
}

func (p *Processor) onRetry(attempt int, err error) {
	p.summary.Retries++
	if procerr.Is(err, procerr.RetryableDatabase) || procerr.IsMissingDependency(err) {
		p.summary.DatabaseRetries++
	}
	typeName := "error"
	if e, ok := procerr.As(err); ok {
		typeName = e.TypeName()
	}
	slog.Warn("processing_retry", "processing_attempt", attempt, "error_type", typeName)
	if p.metrics != nil {
		category := "processing"
		switch {
		case procerr.IsMissingDependency(err):
			category = "missing_dependency"
		case procerr.Is(err, procerr.RetryableDatabase):
			category = "database"
		}
		p.metrics.Retries.WithLabelValues(category, "scheduled").Inc()
	}
}

func (p *Processor) recordPersistence(r *persistence.Result) {
	if r.AlreadyPersisted {
		p.summary.AlreadyPersistedEvents++
		if p.metrics != nil {
			p.metrics.DBTransactions.WithLabelValues("already_persisted").Inc()
		}
	} else {
		p.summary.DatabaseTransactionsCommitted++
		for table, n := range r.RowsWritten {
			p.summary.RowsWrittenByTable[table] += n
		}
		p.summary.FraudEvaluations += r.RowsWritten["fraud_evaluations"]
		p.summary.FraudAlertsCreated += r.RowsWritten["fraud_alerts"]
		p.summary.FraudOutboxRowsCreated += r.RowsWritten["fraud_outbox"]
		switch r.FraudDecision {
		case "APPROVE":
			p.summary.ApproveDecisions++
		case "REVIEW":
			p.summary.ReviewDecisions++
		case "BLOCK":
			p.summary.BlockDecisions++
		}
		for _, id := range r.MatchedRuleIDs {
			p.summary.MatchedRules[id]++
		}
		if p.metrics != nil {
			p.metrics.DBTransactions.WithLabelValues("committed").Inc()
			for table, n := range r.RowsWritten {
				p.metrics.DBRowsWritten.WithLabelValues(table, "insert").Add(float64(n))
			}
			if r.FraudDecision != "" {
				severity := r.FraudSeverity
				if severity == "" {
					severity = "UNKNOWN"
				}
				eventType := r.FraudEventType
				if eventType == "" {
					eventType = "unknown"
				}
				p.metrics.FraudEvaluations.WithLabelValues(eventType, r.FraudDecision, severity).Inc()
				p.metrics.FraudScore.Observe(float64(r.FraudScore))
				p.metrics.FraudLastEvaluation.SetToCurrentTime()
				for _, id := range r.MatchedRuleIDs {
					p.metrics.FraudRuleMatches.WithLabelValues(id, severity).Inc()
				}
				if r.RowsWritten["fraud_alerts"] != 0 {
					p.metrics.FraudAlertsCreated.WithLabelValues(r.FraudDecision, severity).Inc()
				}
			}
		}
	}
	if r.DurationMs >= float64(p.cfg.DBLogSlowQueryMs) {
		p.summary.SlowDatabaseOperations++
		if p.metrics != nil {
			p.metrics.DBSlowOperations.WithLabelValues("source_event_transaction").Inc()
		}
	}
	if p.metrics != nil {
		label := "committed"
		if r.AlreadyPersisted {
			label = "already_persisted"
		}
		seconds := r.DurationMs / 1000
		p.metrics.DBTransactionTime.WithLabelValues(label).Observe(seconds)
		p.metrics.DatabaseDuration.WithLabelValues("source_event_transaction", label).Observe(seconds)
	}
}

func (p *Processor) safeRelease(ctx context.Context, id uuid.UUID, token string) {
	if _, err := p.store.Release(ctx, id, token); err != nil {
		p.summary.RedisErrors++
	}
}

func (p *Processor) deadLetter(m *model.ConsumedMessage, e *model.ValidationErrorInfo, attempts int) (model.ProcessingOutcome, error) {
	record := dlq.Build(m, e, attempts, p.cfg.ConsumerGroup, p.instanceID, p.cfg.DLQMaxPayloadBytes, p.now().UTC())
	started := time.Now()
	if err := p.dlq.Publish(record); err != nil {
		p.summary.UnresolvedRecords++
		return model.ProcessingOutcome{Status: model.Unresolved, Attempts: attempts}, nil
	}
	if err := p.commit(m); err != nil {
		return model.ProcessingOutcome{Status: model.Unresolved, Attempts: attempts}, err
	}
	p.summary.DLQRecords++
	if p.metrics != nil {
		p.metrics.DLQDuration.Observe(time.Since(started).Seconds())
		p.metrics.DLQPublished.WithLabelValues(string(e.Category)).Inc()
	}
	var eventID, eventType, anomaly any
	if e.EventID != nil {
		eventID = e.EventID.String()
	}
	if e.EventType != nil {
		eventType = *e.EventType
	}
	if e.AnomalyType != nil {
		anomaly = *e.AnomalyType
	}
	slog.Warn("event_dead_lettered", "dlq_record_id", record.RecordID.String(), "error_category", string(e.Category),
		"event_id", eventID, "event_type", eventType, "anomaly_type", anomaly, "topic", m.Topic,
		"partition", m.Partition, "offset", m.Offset, "processing_attempts", attempts)
	return model.ProcessingOutcome{Status: model.DLQ, Attempts: attempts}, nil
}

func (p *Processor) commit(m *model.ConsumedMessage) error {
	if err := p.committer.CommitTerminal(m); err != nil {
		p.summary.CommitFailures++
		p.summary.UnresolvedRecords++
		return &FatalError{Err: err}
	}
	p.summary.CommitSuccesses++
	return nil
}

func (p *Processor) observeTerminal(eventType string, outcome model.ProcessingOutcome, started time.Time) {
	if p.metrics == nil {
		return
	}
	p.metrics.EventsTerminal.WithLabelValues(eventType, string(outcome.Status)).Inc()
	p.metrics.ProcessingDuration.WithLabelValues(eventType, string(outcome.Status)).Observe(time.Since(started).Seconds())
	p.metrics.Inflight.Dec()
	if outcome.Terminal() {
		p.metrics.LastSuccess.SetToCurrentTime()
		p.metrics.Success()
	}
}

func processingError(event *contracts.Envelope, err error, category model.ValidationCategory) *model.ValidationErrorInfo {
	message := err.Error()
	if len([]rune(message)) > 512 {
		message = string([]rune(message)[:512])
	}
	if message == "" {
		message = string(category)
	}
	typeName := "Exception"
	if e, ok := procerr.As(err); ok {
		typeName = e.TypeName()
	}
	id, corr, typ := event.EventID, event.CorrelationID, string(event.EventType)
	return &model.ValidationErrorInfo{Category: category, Message: message, ErrorType: typeName,
		EventID: &id, EventType: &typ, CorrelationID: &corr}
}
