// Package fraud ports services.event_processor.fraud: the bounded history
// context, deterministic rules, aggregation and atomic persistence of
// evaluation, alert and transactional-outbox rows.
package fraud

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/config"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/dbtx"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
)

// EligibleEventTypes mirrors FRAUD_ELIGIBLE_EVENT_TYPES.
var EligibleEventTypes = map[contracts.EventType]bool{
	contracts.CheckoutStartedType:  true,
	contracts.OrderCreatedType:     true,
	contracts.PaymentFailedType:    true,
	contracts.PaymentCompletedType: true,
	contracts.RefundRequestedType:  true,
}

type Context struct {
	SourceEventID              uuid.UUID
	EventType                  contracts.EventType
	EventTime                  time.Time
	CustomerID                 uuid.UUID
	OrderID                    *uuid.UUID
	PaymentID                  *uuid.UUID
	SessionID                  *uuid.UUID
	Amount                     *contracts.Decimal
	Currency                   *string
	CurrentDevice              *string
	CurrentCountry             *string
	HomeCountry                string
	SessionStartedAt           *time.Time
	HistoricalAverageAmount    *contracts.Decimal
	RecentPaymentAttempts      int
	RecentFailedPayments       int
	RecentFailureReasons       int
	RecentDistinctDevices      int
	RecentDistinctCountries    int
	RecentOrders               int
	RecentRefunds              int
	LifetimeSuccessfulPayments int
	LifetimeFailedPayments     int
	RefundableAmount           *contracts.Decimal
	PreviousSuccessfulDevices  map[string]bool
	ProductViewCount           int
	RefundAmount               *contracts.Decimal
	SecondsSincePayment        *int
	PaymentAmountMatchesOrder  bool
}

func (c *Context) EstablishedHistory() bool { return c.LifetimeSuccessfulPayments > 0 }

func (c *Context) IsNewDevice() bool {
	return c.CurrentDevice != nil && len(c.PreviousSuccessfulDevices) > 0 &&
		!c.PreviousSuccessfulDevices[*c.CurrentDevice]
}

func (c *Context) CountryMismatch() bool {
	return c.CurrentCountry != nil && *c.CurrentCountry != c.HomeCountry
}

// CheckoutSeconds is max(0, int(total_seconds)), truncating toward zero.
func (c *Context) CheckoutSeconds() *int {
	if c.SessionStartedAt == nil {
		return nil
	}
	s := int(c.EventTime.Sub(*c.SessionStartedAt).Seconds())
	if s < 0 {
		s = 0
	}
	return &s
}

type paymentRow struct {
	status        string
	amount        contracts.Decimal
	currency      string
	device        string
	country       string
	attemptedAt   time.Time
	failureReason *string
}

type Builder struct{ cfg *config.Fraud }

func NewBuilder(cfg *config.Fraud) *Builder { return &Builder{cfg: cfg} }

// payloadFacts extracts the getattr()-based fields the Python builder reads.
type payloadFacts struct {
	customer                  *uuid.UUID
	order, payment, session   *uuid.UUID
	amount, totalAmount       *contracts.Decimal
	currency, method, country *string
	device                    *string
}

func ref[T any](v T) *T { return &v }

func facts(p contracts.Payload) payloadFacts {
	var f payloadFacts
	switch x := p.(type) {
	case *contracts.CheckoutStarted:
		f.customer, f.session, f.totalAmount, f.currency = ref(x.CustomerID), ref(x.SessionID), ref(x.TotalAmount), ref(x.Currency)
	case *contracts.OrderCreated:
		f.customer, f.order, f.session, f.totalAmount, f.currency = ref(x.CustomerID), ref(x.OrderID), ref(x.SessionID), ref(x.TotalAmount), ref(x.Currency)
	case *contracts.Payment:
		f.customer, f.order, f.payment, f.amount, f.currency = ref(x.CustomerID), ref(x.OrderID), ref(x.PaymentID), ref(x.Amount), ref(x.Currency)
		f.method, f.country, f.device = ref(x.PaymentMethod), ref(x.CountryCode), ref(x.DeviceID.String())
	case *contracts.RefundRequested:
		f.customer, f.order, f.payment, f.amount, f.currency = ref(x.CustomerID), ref(x.OrderID), ref(x.PaymentID), ref(x.Amount), ref(x.Currency)
	}
	return f
}

// Build ports FraudContextBuilder.build with identical queries and limits.
func (b *Builder) Build(tx *dbtx.Tx, e *contracts.Envelope) (*Context, error) {
	pf := facts(e.Payload)
	if pf.customer == nil {
		return nil, procerr.New(procerr.FraudContextDependency, "eligible event has no customer identity")
	}
	customerID := *pf.customer
	// `amount or total_amount`: a zero amount is falsy in Python.
	amount := pf.amount
	if amount == nil || amount.IsZero() {
		amount = pf.totalAmount
	}
	lookback := e.EventTime.AddDate(0, 0, -b.cfg.HistoryLookbackDays)
	limit := b.cfg.MaxHistoryRecords
	cust := dbtx.UUID(customerID)

	var homeCountry string
	var orderedAt pgtype.Timestamptz
	var orderTotal pgtype.Numeric
	var orderCurrency, billingCountry pgtype.Text
	err := tx.QueryRow(`
            SELECT c.home_country, o.ordered_at, o.total, o.currency,
                   o.billing_country
            FROM customers c
            LEFT JOIN orders o ON o.order_id = $1
            WHERE c.customer_id = $2
            `, []any{dbtx.NullableUUID(pf.order), cust}, &homeCountry, &orderedAt, &orderTotal, &orderCurrency, &billingCountry)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, procerr.New(procerr.FraudContextDependency, "customer fraud context is unavailable")
	}
	if err != nil {
		return nil, err
	}
	hasOrder := orderedAt.Valid

	session := pf.session
	if session == nil && pf.order != nil {
		var sid pgtype.UUID
		err := tx.QueryRow("SELECT session_id FROM orders WHERE order_id = $1", []any{dbtx.UUID(*pf.order)}, &sid)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		if err == nil && sid.Valid {
			session = ref(uuid.UUID(sid.Bytes))
		}
	}
	var sessionStarted *time.Time
	var started pgtype.Timestamptz
	err = tx.QueryRow("SELECT started_at FROM sessions WHERE session_id = $1", []any{dbtx.NullableUUID(session)}, &started)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if err == nil && started.Valid {
		sessionStarted = ref(started.Time)
	}
	orderMatches := true
	if hasOrder && amount != nil {
		total, err := dbtx.Numeric(orderTotal)
		if err != nil {
			return nil, err
		}
		orderMatches = total.Equal(*amount) && pf.currency != nil && orderCurrency.String == *pf.currency
	}

	var payments []paymentRow
	err = tx.QueryEach(`
                SELECT status, amount, currency, device_id, country,
                       attempted_at, failure_reason
                FROM payments
                WHERE customer_id = $1 AND attempted_at <= $2
                  AND attempted_at >= $3
                ORDER BY attempted_at DESC, payment_id
                LIMIT $4
                `, []any{cust, e.EventTime, lookback, limit}, func(rows pgx.Rows) error {
		var r paymentRow
		var amt pgtype.Numeric
		var reason pgtype.Text
		if err := rows.Scan(&r.status, &amt, &r.currency, &r.device, &r.country, &r.attemptedAt, &reason); err != nil {
			return err
		}
		d, err := dbtx.Numeric(amt)
		if err != nil {
			return err
		}
		r.amount = d
		if reason.Valid {
			r.failureReason = ref(reason.String)
		}
		payments = append(payments, r)
		return nil
	})
	if err != nil {
		return nil, err
	}
	var prior []paymentRow
	err = tx.QueryEach(`
                SELECT status, amount, currency, device_id, country, attempted_at
                FROM payments
                WHERE customer_id = $1 AND attempted_at < $2
                ORDER BY attempted_at DESC, payment_id
                LIMIT $3
                `, []any{cust, e.EventTime, limit}, func(rows pgx.Rows) error {
		var r paymentRow
		var amt pgtype.Numeric
		if err := rows.Scan(&r.status, &amt, &r.currency, &r.device, &r.country, &r.attemptedAt); err != nil {
			return err
		}
		d, err := dbtx.Numeric(amt)
		if err != nil {
			return err
		}
		r.amount = d
		prior = append(prior, r)
		return nil
	})
	if err != nil {
		return nil, err
	}
	refunds := 0
	err = tx.QueryEach(`
                SELECT requested_at, amount FROM refunds
                WHERE customer_id = $1 AND requested_at <= $2 AND requested_at >= $3
                ORDER BY requested_at DESC, refund_id LIMIT $4
                `, []any{cust, e.EventTime, lookback, limit}, func(pgx.Rows) error {
		refunds++
		return nil
	})
	if err != nil {
		return nil, err
	}
	var recentOrders int64
	if err := tx.QueryRow(`
                SELECT COUNT(*) FROM (
                    SELECT 1 FROM orders
                    WHERE customer_id = $1 AND ordered_at <= $2 AND ordered_at >= $3
                    ORDER BY ordered_at DESC LIMIT $4
                ) bounded
                `, []any{cust, e.EventTime, lookback, limit}, &recentOrders); err != nil {
		return nil, err
	}
	viewsFrom := lookback
	if sessionStarted != nil {
		viewsFrom = *sessionStarted
	}
	var viewCount int64
	if err := tx.QueryRow(`
                SELECT COUNT(*) FROM (
                    SELECT 1 FROM product_views
                    WHERE customer_id = $1 AND viewed_at <= $2 AND viewed_at >= $3
                    ORDER BY viewed_at DESC LIMIT $4
                ) bounded
                `, []any{cust, e.EventTime, viewsFrom, limit}, &viewCount); err != nil {
		return nil, err
	}
	refundable, secondsSince, err := refundFacts(tx, pf.payment, e.EventTime)
	if err != nil {
		return nil, err
	}

	var successful []paymentRow
	failedLifetime := 0
	for _, r := range prior {
		switch r.status {
		case "completed":
			successful = append(successful, r)
		case "failed":
			failedLifetime++
		}
	}
	var average *contracts.Decimal
	if pf.currency != nil {
		sum, n := contracts.DecimalFromInt(0), 0
		for _, r := range successful {
			if r.currency == *pf.currency {
				sum = sum.Add(r.amount)
				n++
			}
		}
		if n > 0 {
			avg, err := sum.Div(contracts.DecimalFromInt(int64(n)))
			if err != nil {
				return nil, err
			}
			average = &avg
		}
	}
	velocityStart := e.EventTime.Add(-time.Duration(b.cfg.VelocityWindowSeconds) * time.Second)
	failureStart := e.EventTime.Add(-time.Duration(b.cfg.FailedPaymentWindowSeconds) * time.Second)
	countryStart := e.EventTime.Add(-time.Duration(b.cfg.CountryWindowSeconds) * time.Second)
	velocity, failures := 0, 0
	devices, countries, reasons := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, r := range payments {
		if !r.attemptedAt.Before(velocityStart) {
			velocity++
			devices[r.device] = true
		}
		if r.status == "failed" && !r.attemptedAt.Before(failureStart) {
			failures++
			if r.failureReason != nil {
				reasons[*r.failureReason] = true
			}
		}
		if !r.attemptedAt.Before(countryStart) {
			countries[r.country] = true
		}
	}
	previousDevices := map[string]bool{}
	for _, r := range successful {
		previousDevices[r.device] = true
	}
	currentCountry := pf.country
	if currentCountry == nil && hasOrder && e.EventType == contracts.OrderCreatedType {
		currentCountry = ref(billingCountry.String)
	}
	var refundAmount *contracts.Decimal
	if e.EventType == contracts.RefundRequestedType {
		refundAmount = pf.amount
	}
	return &Context{
		SourceEventID:              e.EventID,
		EventType:                  e.EventType,
		EventTime:                  e.EventTime,
		CustomerID:                 customerID,
		OrderID:                    pf.order,
		PaymentID:                  pf.payment,
		SessionID:                  session,
		Amount:                     amount,
		Currency:                   pf.currency,
		CurrentDevice:              pf.device,
		CurrentCountry:             currentCountry,
		HomeCountry:                homeCountry,
		SessionStartedAt:           sessionStarted,
		HistoricalAverageAmount:    average,
		RecentPaymentAttempts:      velocity,
		RecentFailedPayments:       failures,
		RecentFailureReasons:       len(reasons),
		RecentDistinctDevices:      len(devices),
		RecentDistinctCountries:    len(countries),
		RecentOrders:               int(recentOrders),
		RecentRefunds:              refunds,
		LifetimeSuccessfulPayments: len(successful),
		LifetimeFailedPayments:     failedLifetime,
		RefundableAmount:           refundable,
		PreviousSuccessfulDevices:  previousDevices,
		ProductViewCount:           int(viewCount),
		RefundAmount:               refundAmount,
		SecondsSincePayment:        secondsSince,
		PaymentAmountMatchesOrder:  orderMatches,
	}, nil
}

func refundFacts(tx *dbtx.Tx, payment *uuid.UUID, eventTime time.Time) (*contracts.Decimal, *int, error) {
	if payment == nil {
		return nil, nil, nil
	}
	var remaining pgtype.Numeric
	var attemptedAt time.Time
	err := tx.QueryRow(`
            SELECT p.amount - COALESCE(SUM(r.amount), 0), p.attempted_at
            FROM payments p LEFT JOIN refunds r ON r.payment_id = p.payment_id
            WHERE p.payment_id = $1 GROUP BY p.amount, p.attempted_at
            `, []any{dbtx.UUID(*payment)}, &remaining, &attemptedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	d, err := dbtx.Numeric(remaining)
	if err != nil {
		return nil, nil, err
	}
	s := int(eventTime.Sub(attemptedAt).Seconds())
	if s < 0 {
		s = 0
	}
	return &d, &s, nil
}
