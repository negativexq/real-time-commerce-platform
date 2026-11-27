package persistence

// The fraud context builder as it was before its reads were batched: one
// round trip per query. Kept only as the reference that the batched builder
// must match field for field (TestBatchedFraudContextMatchesSequentialReference).

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/config"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/dbtx"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/fraud"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
)

type refPaymentRow struct {
	status        string
	amount        contracts.Decimal
	currency      string
	device        string
	country       string
	attemptedAt   time.Time
	failureReason *string
}

// refPayloadFacts extracts the getattr()-based fields the Python builder reads.
type refPayloadFacts struct {
	customer                  *uuid.UUID
	order, payment, session   *uuid.UUID
	amount, totalAmount       *contracts.Decimal
	currency, method, country *string
	device                    *string
}

func refOf[T any](v T) *T { return &v }

func refFacts(p contracts.Payload) refPayloadFacts {
	var f refPayloadFacts
	switch x := p.(type) {
	case *contracts.CheckoutStarted:
		f.customer, f.session, f.totalAmount, f.currency = refOf(x.CustomerID), refOf(x.SessionID), refOf(x.TotalAmount), refOf(x.Currency)
	case *contracts.OrderCreated:
		f.customer, f.order, f.session, f.totalAmount, f.currency = refOf(x.CustomerID), refOf(x.OrderID), refOf(x.SessionID), refOf(x.TotalAmount), refOf(x.Currency)
	case *contracts.Payment:
		f.customer, f.order, f.payment, f.amount, f.currency = refOf(x.CustomerID), refOf(x.OrderID), refOf(x.PaymentID), refOf(x.Amount), refOf(x.Currency)
		f.method, f.country, f.device = refOf(x.PaymentMethod), refOf(x.CountryCode), refOf(x.DeviceID.String())
	case *contracts.RefundRequested:
		f.customer, f.order, f.payment, f.amount, f.currency = refOf(x.CustomerID), refOf(x.OrderID), refOf(x.PaymentID), refOf(x.Amount), refOf(x.Currency)
	}
	return f
}

func referenceBuild(b *fraud.Builder, cfg *config.Fraud, tx *dbtx.Tx, e *contracts.Envelope) (*fraud.Context, error) {
	_ = b
	pf := refFacts(e.Payload)
	if pf.customer == nil {
		return nil, procerr.New(procerr.FraudContextDependency, "eligible event has no customer identity")
	}
	customerID := *pf.customer
	// `amount or total_amount`: a zero amount is falsy in Python.
	amount := pf.amount
	if amount == nil || amount.IsZero() {
		amount = pf.totalAmount
	}
	lookback := e.EventTime.AddDate(0, 0, -cfg.HistoryLookbackDays)
	limit := cfg.MaxHistoryRecords
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
			session = refOf(uuid.UUID(sid.Bytes))
		}
	}
	var sessionStarted *time.Time
	var started pgtype.Timestamptz
	err = tx.QueryRow("SELECT started_at FROM sessions WHERE session_id = $1", []any{dbtx.NullableUUID(session)}, &started)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if err == nil && started.Valid {
		sessionStarted = refOf(started.Time)
	}
	orderMatches := true
	if hasOrder && amount != nil {
		total, err := dbtx.Numeric(orderTotal)
		if err != nil {
			return nil, err
		}
		orderMatches = total.Equal(*amount) && pf.currency != nil && orderCurrency.String == *pf.currency
	}

	var payments []refPaymentRow
	err = tx.QueryEach(`
                SELECT status, amount, currency, device_id, country,
                       attempted_at, failure_reason
                FROM payments
                WHERE customer_id = $1 AND attempted_at <= $2
                  AND attempted_at >= $3
                ORDER BY attempted_at DESC, payment_id
                LIMIT $4
                `, []any{cust, e.EventTime, lookback, limit}, func(rows pgx.Rows) error {
		var r refPaymentRow
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
			r.failureReason = refOf(reason.String)
		}
		payments = append(payments, r)
		return nil
	})
	if err != nil {
		return nil, err
	}
	var prior []refPaymentRow
	err = tx.QueryEach(`
                SELECT status, amount, currency, device_id, country, attempted_at
                FROM payments
                WHERE customer_id = $1 AND attempted_at < $2
                ORDER BY attempted_at DESC, payment_id
                LIMIT $3
                `, []any{cust, e.EventTime, limit}, func(rows pgx.Rows) error {
		var r refPaymentRow
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
	refundable, secondsSince, err := refRefundFacts(tx, pf.payment, e.EventTime)
	if err != nil {
		return nil, err
	}

	var successful []refPaymentRow
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
	velocityStart := e.EventTime.Add(-time.Duration(cfg.VelocityWindowSeconds) * time.Second)
	failureStart := e.EventTime.Add(-time.Duration(cfg.FailedPaymentWindowSeconds) * time.Second)
	countryStart := e.EventTime.Add(-time.Duration(cfg.CountryWindowSeconds) * time.Second)
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
		currentCountry = refOf(billingCountry.String)
	}
	var refundAmount *contracts.Decimal
	if e.EventType == contracts.RefundRequestedType {
		refundAmount = pf.amount
	}
	return &fraud.Context{
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

func refRefundFacts(tx *dbtx.Tx, payment *uuid.UUID, eventTime time.Time) (*contracts.Decimal, *int, error) {
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
