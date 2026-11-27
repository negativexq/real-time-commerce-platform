package persistence

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/contracts"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/dbtx"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/model"
	"github.com/negativexq/real-time-commerce-platform/services/event_processor_go/internal/procerr"
)

// Statements are kept textually identical to the Python repositories so the
// SQL-class instrumentation and query plans are directly comparable.

var existsSQL = map[string]string{
	"customers": `SELECT 1 FROM "customers" WHERE "customer_id" = $1`,
	"sessions":  `SELECT 1 FROM "sessions" WHERE "session_id" = $1`,
}

func exists(tx *dbtx.Tx, table string, id uuid.UUID) (bool, error) {
	var one int32
	err := tx.QueryRow(existsSQL[table], []any{dbtx.UUID(id)}, &one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func integrity(msg string) error { return procerr.New(procerr.PermanentDatabaseIntegrity, msg) }
func missing(msg string) error   { return procerr.New(procerr.MissingBusinessDependency, msg) }

// queueProcessedEventInsert queues the ledger insert for e. ON CONFLICT DO
// NOTHING keeps the common case (a new event) to this one statement; *inserted
// reports whether the row was written. When it was not, the caller resolves
// why with resolveProcessedEventConflict.
func queueProcessedEventInsert(b *dbtx.Batch, e *contracts.Envelope, m *model.ConsumedMessage,
	ctx model.ProcessingContext, persistRawJSON bool, inserted *bool) {
	payloadJSON := "{}"
	if persistRawJSON {
		payloadJSON = string(e.CanonicalJSON())
	}
	b.QueryRow(`
                INSERT INTO processed_events (
                    event_id, event_type, event_version, correlation_id, source,
                    event_time, produced_at, kafka_topic, kafka_partition,
                    kafka_offset, kafka_key, payload_json, payload_sha256,
                    processed_at, processor_instance_id
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                    $12::jsonb, $13, CURRENT_TIMESTAMP, $14
                )
                ON CONFLICT DO NOTHING
                RETURNING event_id
                `, []any{
		dbtx.UUID(e.EventID), string(e.EventType), e.EventVersion, dbtx.UUID(e.CorrelationID),
		e.Source, e.EventTime, e.ProducedAt, m.Topic, m.Partition, m.Offset, m.Key,
		payloadJSON, e.PayloadSHA256(), ctx.ProcessorInstanceID,
	}, func(row pgx.Row) error {
		var id pgtype.UUID
		err := row.Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		*inserted = err == nil
		return err
	})
}

// resolveProcessedEventConflict explains a ledger insert that hit a unique
// constraint: the same event again (AlreadyPersisted), different content under
// the same event_id, or a Kafka coordinate already taken by another event.
func resolveProcessedEventConflict(tx *dbtx.Tx, e *contracts.Envelope, m *model.ConsumedMessage) error {
	var existingDigest pgtype.Text
	err := tx.QueryRow(`
                SELECT payload_sha256 FROM processed_events WHERE event_id = $1
                `, []any{dbtx.UUID(e.EventID)}, &existingDigest)
	switch {
	case err == nil:
		if existingDigest.Valid && existingDigest.String == e.PayloadSHA256() {
			return procerr.New(procerr.AlreadyPersisted, e.EventID.String())
		}
		return integrity("event_id conflicts with different canonical event content")
	case !errors.Is(err, pgx.ErrNoRows):
		return err
	}
	var other pgtype.UUID
	err = tx.QueryRow(`
                SELECT event_id FROM processed_events
                WHERE kafka_topic = $1 AND kafka_partition = $2 AND kafka_offset = $3
                `, []any{m.Topic, m.Partition, m.Offset}, &other)
	if err == nil {
		return integrity("Kafka source coordinate is already assigned to another event")
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	return integrity("processed event insert conflicted on an unknown constraint")
}

func registerCustomer(tx *dbtx.Tx, p *contracts.UserRegistered, eventID uuid.UUID) (int, error) {
	var email, persona, country string
	var registered time.Time
	err := tx.QueryRow(`
                SELECT email_hash, persona, home_country, registered_at
                FROM customers WHERE customer_id = $1
                `, []any{dbtx.UUID(p.CustomerID)}, &email, &persona, &country, &registered)
	switch {
	case err == nil:
		if email != p.EmailHash || persona != p.Persona || country != p.CountryCode || !registered.Equal(p.RegisteredAt) {
			return 0, integrity("customer immutable identity conflict")
		}
		_, err = tx.Exec(`
                    UPDATE customers
                    SET last_event_id = $1, updated_at = CURRENT_TIMESTAMP
                    WHERE customer_id = $2
                    `, dbtx.UUID(eventID), dbtx.UUID(p.CustomerID))
		return 0, err
	case !errors.Is(err, pgx.ErrNoRows):
		return 0, err
	}
	_, err = tx.Exec(`
                INSERT INTO customers (
                    customer_id, email_hash, persona, home_country,
                    registered_at, first_event_id, last_event_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                `, dbtx.UUID(p.CustomerID), p.EmailHash, p.Persona, p.CountryCode,
		p.RegisteredAt, dbtx.UUID(eventID), dbtx.UUID(eventID))
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func insertSession(tx *dbtx.Tx, p *contracts.SessionStarted, eventID uuid.UUID) (int, error) {
	ok, err := exists(tx, "customers", p.CustomerID)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, missing("customer is not persisted")
	}
	var customer pgtype.UUID
	var device, ip, country string
	var started time.Time
	err = tx.QueryRow(`
                SELECT customer_id, device_id, ip_address::text, country, started_at
                FROM sessions WHERE session_id = $1
                `, []any{dbtx.UUID(p.SessionID)}, &customer, &device, &ip, &country, &started)
	switch {
	case err == nil:
		// Same comparison as Python, including inet::text (which carries a
		// prefix length), so identical-session replays behave identically.
		if uuid.UUID(customer.Bytes) != p.CustomerID || device != p.DeviceID.String() ||
			ip != p.IPAddress || country != p.CountryCode || !started.Equal(p.StartedAt) {
			return 0, integrity("session identity conflict")
		}
		return 0, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return 0, err
	}
	_, err = tx.Exec(`
                INSERT INTO sessions (
                    session_id, customer_id, device_id, device_type, ip_address,
                    country, channel, started_at, first_event_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                `, dbtx.UUID(p.SessionID), dbtx.UUID(p.CustomerID), p.DeviceID.String(), p.DeviceType,
		p.IPAddress, p.CountryCode, p.Channel, p.StartedAt, dbtx.UUID(eventID))
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func requireCustomerAndSession(tx *dbtx.Tx, customer, session uuid.UUID) error {
	ok, err := exists(tx, "customers", customer)
	if err != nil {
		return err
	}
	if !ok {
		return missing("customer is not persisted")
	}
	ok, err = exists(tx, "sessions", session)
	if err != nil {
		return err
	}
	if !ok {
		return missing("session is not persisted")
	}
	return nil
}

func insertProductView(tx *dbtx.Tx, p *contracts.ProductViewed, eventID uuid.UUID, viewedAt time.Time) (int, error) {
	if err := requireCustomerAndSession(tx, p.CustomerID, p.SessionID); err != nil {
		return 0, err
	}
	tag, err := tx.Exec(`
                INSERT INTO product_views (
                    event_id, session_id, customer_id, product_id, category,
                    price, currency, quantity_available, viewed_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (event_id) DO NOTHING
                `, dbtx.UUID(eventID), dbtx.UUID(p.SessionID), dbtx.UUID(p.CustomerID), dbtx.UUID(p.ProductID),
		p.Category, p.UnitPrice, p.Currency, p.QuantityAvailable, viewedAt)
	return int(tag.RowsAffected()), err
}

func addCartItem(tx *dbtx.Tx, p *contracts.AddedToCart, eventID uuid.UUID) (map[string]int, error) {
	if err := requireCustomerAndSession(tx, p.CustomerID, p.SessionID); err != nil {
		return nil, err
	}
	lineTotal := p.UnitPrice.MulInt(p.Quantity)
	tag, err := tx.Exec(`
                INSERT INTO carts (
                    cart_id, customer_id, session_id, status, subtotal, discount,
                    total, currency, item_count, created_event_id, latest_event_id
                ) VALUES ($1, $2, $3, 'active', $4, 0, $5, $6, $7, $8, $9)
                ON CONFLICT (cart_id) DO UPDATE SET
                    latest_event_id = EXCLUDED.latest_event_id,
                    updated_at = CURRENT_TIMESTAMP
                WHERE carts.customer_id = EXCLUDED.customer_id
                  AND carts.session_id = EXCLUDED.session_id
                  AND carts.currency = EXCLUDED.currency
                `, dbtx.UUID(p.CartID), dbtx.UUID(p.CustomerID), dbtx.UUID(p.SessionID), lineTotal, lineTotal,
		p.Currency, p.Quantity, dbtx.UUID(eventID), dbtx.UUID(eventID))
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, integrity("cart identity conflict")
	}
	if _, err = tx.Exec(`
                INSERT INTO cart_items (
                    cart_id, product_id, quantity, unit_price, line_total
                ) VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (cart_id, product_id) DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    unit_price = EXCLUDED.unit_price,
                    line_total = EXCLUDED.line_total
                `, dbtx.UUID(p.CartID), dbtx.UUID(p.ProductID), p.Quantity, p.UnitPrice, lineTotal); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(`
                UPDATE carts SET
                    subtotal = totals.subtotal,
                    total = totals.subtotal - discount,
                    item_count = totals.item_count,
                    latest_event_id = $1,
                    updated_at = CURRENT_TIMESTAMP
                FROM (
                    SELECT cart_id, SUM(line_total) AS subtotal,
                           SUM(quantity)::integer AS item_count
                    FROM cart_items WHERE cart_id = $2 GROUP BY cart_id
                ) AS totals
                WHERE carts.cart_id = totals.cart_id
                `, dbtx.UUID(eventID), dbtx.UUID(p.CartID)); err != nil {
		return nil, err
	}
	return map[string]int{"carts": 1, "cart_items": 1}, nil
}

func checkout(tx *dbtx.Tx, p *contracts.CheckoutStarted, eventID uuid.UUID) (int, error) {
	var customer, session pgtype.UUID
	var subtotal pgtype.Numeric
	var currency string
	var itemCount int64
	err := tx.QueryRow(`
                SELECT customer_id, session_id, subtotal, currency, item_count
                FROM carts WHERE cart_id = $1 FOR UPDATE
                `, []any{dbtx.UUID(p.CartID)}, &customer, &session, &subtotal, &currency, &itemCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, missing("cart is not persisted")
	}
	if err != nil {
		return 0, err
	}
	sub, err := dbtx.Numeric(subtotal)
	if err != nil {
		return 0, err
	}
	if uuid.UUID(customer.Bytes) != p.CustomerID || uuid.UUID(session.Bytes) != p.SessionID ||
		!sub.Equal(p.Subtotal) || currency != p.Currency || itemCount != p.ItemCount {
		return 0, integrity("checkout totals or cart identity do not match persisted cart")
	}
	tag, err := tx.Exec(`
                UPDATE carts SET status = 'checkout_started', discount = $1,
                    total = $2, latest_event_id = $3,
                    updated_at = CURRENT_TIMESTAMP
                WHERE cart_id = $4
                `, p.DiscountAmount, p.TotalAmount, dbtx.UUID(eventID), dbtx.UUID(p.CartID))
	return int(tag.RowsAffected()), err
}

func insertOrder(tx *dbtx.Tx, p *contracts.OrderCreated, eventID uuid.UUID) (map[string]int, error) {
	var customer, session pgtype.UUID
	var subtotal, discount, total pgtype.Numeric
	var currency string
	var itemCount int64
	err := tx.QueryRow(`
                SELECT customer_id, session_id, subtotal, discount, total,
                       currency, item_count
                FROM carts WHERE cart_id = $1 FOR UPDATE
                `, []any{dbtx.UUID(p.CartID)}, &customer, &session, &subtotal, &discount, &total, &currency, &itemCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, missing("cart is not persisted")
	}
	if err != nil {
		return nil, err
	}
	sub, err1 := dbtx.Numeric(subtotal)
	disc, err2 := dbtx.Numeric(discount)
	tot, err3 := dbtx.Numeric(total)
	if err := errors.Join(err1, err2, err3); err != nil {
		return nil, err
	}
	if uuid.UUID(customer.Bytes) != p.CustomerID || uuid.UUID(session.Bytes) != p.SessionID ||
		!sub.Equal(p.Subtotal) || !disc.Equal(p.DiscountAmount) || !tot.Equal(p.TotalAmount) ||
		currency != p.Currency || itemCount != p.ItemCount {
		return nil, integrity("order totals or identity do not match persisted cart")
	}
	tag, err := tx.Exec(`
                INSERT INTO orders (
                    order_id, customer_id, session_id, cart_id, status,
                    subtotal, discount, total, currency, item_count,
                    shipping_country, billing_country, created_event_id,
                    latest_event_id, ordered_at
                ) VALUES (
                    $1, $2, $3, $4, 'created', $5, $6, $7, $8, $9,
                    $10, $11, $12, $13, $14
                )
                ON CONFLICT (order_id) DO NOTHING
                `, dbtx.UUID(p.OrderID), dbtx.UUID(p.CustomerID), dbtx.UUID(p.SessionID), dbtx.UUID(p.CartID),
		p.Subtotal, p.DiscountAmount, p.TotalAmount, p.Currency, p.ItemCount,
		p.ShippingCountryCode, p.BillingCountryCode, dbtx.UUID(eventID), dbtx.UUID(eventID), p.CreatedAt)
	if err != nil {
		return nil, err
	}
	orderRows := int(tag.RowsAffected())
	if orderRows == 0 {
		return nil, integrity("order identity conflict")
	}
	if _, err = tx.Exec(`
                UPDATE carts SET status = 'ordered', latest_event_id = $1,
                    updated_at = CURRENT_TIMESTAMP WHERE cart_id = $2
                `, dbtx.UUID(eventID), dbtx.UUID(p.CartID)); err != nil {
		return nil, err
	}
	return map[string]int{"orders": orderRows, "carts": 1}, nil
}

func insertPayment(tx *dbtx.Tx, p *contracts.Payment, eventID uuid.UUID) (map[string]int, error) {
	status := "completed"
	var failureReason *string
	if p.Failed {
		status = "failed"
		reason := p.FailureReason
		failureReason = &reason
	}
	var customer pgtype.UUID
	var total pgtype.Numeric
	var currency string
	err := tx.QueryRow(`
                SELECT customer_id, total, currency
                FROM orders WHERE order_id = $1 FOR UPDATE
                `, []any{dbtx.UUID(p.OrderID)}, &customer, &total, &currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, missing("order is not persisted")
	}
	if err != nil {
		return nil, err
	}
	tot, err := dbtx.Numeric(total)
	if err != nil {
		return nil, err
	}
	if uuid.UUID(customer.Bytes) != p.CustomerID || !tot.Equal(p.Amount) || currency != p.Currency {
		return nil, integrity("payment amount, currency, or customer does not match order")
	}
	tag, err := tx.Exec(`
                INSERT INTO payments (
                    payment_id, order_id, customer_id, status, amount, currency,
                    payment_method, failure_reason, device_id, ip_address,
                    country, event_id, attempted_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ON CONFLICT (payment_id) DO NOTHING
                `, dbtx.UUID(p.PaymentID), dbtx.UUID(p.OrderID), dbtx.UUID(p.CustomerID), status, p.Amount,
		p.Currency, p.PaymentMethod, failureReason, p.DeviceID.String(), p.IPAddress, p.CountryCode,
		dbtx.UUID(eventID), p.At)
	if err != nil {
		return nil, err
	}
	paymentRows := int(tag.RowsAffected())
	if paymentRows == 0 {
		return nil, integrity("payment identity conflict")
	}
	orderRows := 0
	if !p.Failed {
		tag, err = tx.Exec(`
                    UPDATE orders SET status = 'paid', latest_event_id = $1,
                        updated_at = CURRENT_TIMESTAMP WHERE order_id = $2
                    `, dbtx.UUID(eventID), dbtx.UUID(p.OrderID))
		if err != nil {
			return nil, err
		}
		orderRows = int(tag.RowsAffected())
	}
	return map[string]int{"payments": paymentRows, "orders": orderRows}, nil
}

func insertRefund(tx *dbtx.Tx, p *contracts.RefundRequested, eventID uuid.UUID) (map[string]int, error) {
	var order, customer pgtype.UUID
	var amount pgtype.Numeric
	var currency, status string
	err := tx.QueryRow(`
                SELECT order_id, customer_id, amount, currency, status
                FROM payments WHERE payment_id = $1 FOR UPDATE
                `, []any{dbtx.UUID(p.PaymentID)}, &order, &customer, &amount, &currency, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, missing("payment is not persisted")
	}
	if err != nil {
		return nil, err
	}
	if status != "completed" && status != "partially_refunded" && status != "refunded" {
		return nil, integrity("refund requires a successful payment")
	}
	if uuid.UUID(order.Bytes) != p.OrderID || uuid.UUID(customer.Bytes) != p.CustomerID || currency != p.Currency {
		return nil, integrity("refund identity or currency does not match payment")
	}
	var refundedN pgtype.Numeric
	if err := tx.QueryRow("SELECT COALESCE(SUM(amount), 0) FROM refunds WHERE payment_id = $1",
		[]any{dbtx.UUID(p.PaymentID)}, &refundedN); err != nil {
		return nil, err
	}
	refunded, err1 := dbtx.Numeric(refundedN)
	paid, err2 := dbtx.Numeric(amount)
	if err := errors.Join(err1, err2); err != nil {
		return nil, err
	}
	newTotal := refunded.Add(p.Amount)
	if newTotal.Cmp(paid) > 0 {
		return nil, integrity("refund exceeds remaining refundable amount")
	}
	tag, err := tx.Exec(`
                INSERT INTO refunds (
                    refund_id, payment_id, order_id, customer_id, amount,
                    currency, reason, event_id, requested_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (refund_id) DO NOTHING
                `, dbtx.UUID(p.RefundID), dbtx.UUID(p.PaymentID), dbtx.UUID(p.OrderID), dbtx.UUID(p.CustomerID),
		p.Amount, p.Currency, p.Reason, dbtx.UUID(eventID), p.RequestedAt)
	if err != nil {
		return nil, err
	}
	refundRows := int(tag.RowsAffected())
	if refundRows == 0 {
		return nil, integrity("refund identity conflict")
	}
	paymentStatus := "partially_refunded"
	if newTotal.Equal(paid) {
		paymentStatus = "refunded"
	}
	if _, err = tx.Exec(`
                UPDATE payments SET status = $1, updated_at = CURRENT_TIMESTAMP
                WHERE payment_id = $2
                `, paymentStatus, dbtx.UUID(p.PaymentID)); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(`
                UPDATE orders SET status = $1, latest_event_id = $2,
                    updated_at = CURRENT_TIMESTAMP WHERE order_id = $3
                `, paymentStatus, dbtx.UUID(eventID), dbtx.UUID(p.OrderID)); err != nil {
		return nil, err
	}
	return map[string]int{"refunds": refundRows, "payments": 1, "orders": 1}, nil
}

var externalAlertEventNamespace = uuid.MustParse("8b3f028f-d1cb-465c-a662-ebfdcfb74d38")

func insertExternalFraudAlert(tx *dbtx.Tx, p *contracts.FraudAlertCreated) (int, error) {
	reasons, _ := json.Marshal(p.Reasons)
	tag, err := tx.Exec(`
                INSERT INTO fraud_alerts (
                    alert_id, source_event_id, customer_id, order_id, score,
                    decision, severity, status, reason_codes, alert_event_id,
                    created_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5::numeric, $6,
                    CASE WHEN $7::text = 'BLOCK' THEN 'HIGH' ELSE 'MEDIUM' END,
                    'OPEN', $8::jsonb, $9, $10, $11
                )
                ON CONFLICT (source_event_id) DO NOTHING
                `, dbtx.UUID(p.AlertID), dbtx.UUID(p.EventID), dbtx.NullableUUID(p.CustomerID), dbtx.NullableUUID(p.OrderID),
		p.FraudScore, p.Decision, p.Decision, string(reasons),
		dbtx.UUID(uuid.NewSHA1(externalAlertEventNamespace, []byte(p.AlertID.String()))), p.CreatedAt, p.CreatedAt)
	return int(tag.RowsAffected()), err
}
