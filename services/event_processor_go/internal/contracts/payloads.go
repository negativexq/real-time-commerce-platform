package contracts

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// Enumerations mirror shared.commerce_common.enums.
var (
	currencies      = []string{"TRY", "EUR", "USD", "GBP"}
	personas        = []string{"normal", "indecisive", "discount_hunter", "suspicious", "bot", "account_takeover"}
	paymentMethods  = []string{"credit_card", "debit_card", "bank_transfer", "digital_wallet"}
	failureReasons  = []string{"insufficient_funds", "declined", "expired_card", "invalid_details", "processor_error", "fraud_suspected"}
	fraudDecisions  = []string{"APPROVE", "REVIEW", "BLOCK"}
	deviceTypes     = []string{"desktop", "mobile", "tablet", "bot"}
	sessionChannels = []string{"web", "mobile_app", "partner", "api"}
)

// Payload is one registered, validated event payload.
type Payload interface {
	canonical() Object
	// OrderingCustomer returns the payload customer_id used as the Kafka key.
	OrderingCustomer() (uuid.UUID, bool)
}

func uuidOrNil(u *uuid.UUID) any {
	if u == nil {
		return nil
	}
	return u.String()
}

type UserRegistered struct {
	CustomerID   uuid.UUID
	EmailHash    string
	CountryCode  string
	Persona      string
	RegisteredAt time.Time
}

func parseUserRegistered(data []byte) (Payload, error) {
	f := newFields(data, "customer_id", "email_hash", "country_code", "persona", "registered_at")
	p := &UserRegistered{
		CustomerID:   f.uuid("customer_id"),
		EmailHash:    f.nonEmpty("email_hash"),
		CountryCode:  f.country("country_code"),
		Persona:      f.enum("persona", personas...),
		RegisteredAt: f.datetime("registered_at"),
	}
	return p, f.err
}

func (p *UserRegistered) OrderingCustomer() (uuid.UUID, bool) { return p.CustomerID, true }
func (p *UserRegistered) canonical() Object {
	return Object{
		"customer_id":   p.CustomerID.String(),
		"email_hash":    p.EmailHash,
		"country_code":  p.CountryCode,
		"persona":       p.Persona,
		"registered_at": FormatUTCZ(p.RegisteredAt),
	}
}

type SessionStarted struct {
	SessionID   uuid.UUID
	CustomerID  uuid.UUID
	DeviceID    uuid.UUID
	DeviceType  string
	IPAddress   string
	CountryCode string
	Channel     string
	StartedAt   time.Time
}

func parseSessionStarted(data []byte) (Payload, error) {
	f := newFields(data, "session_id", "customer_id", "device_id", "device_type", "ip_address", "country_code", "channel", "started_at")
	p := &SessionStarted{
		SessionID:   f.uuid("session_id"),
		CustomerID:  f.uuid("customer_id"),
		DeviceID:    f.uuid("device_id"),
		DeviceType:  f.enum("device_type", deviceTypes...),
		IPAddress:   f.ip("ip_address"),
		CountryCode: f.country("country_code"),
		Channel:     f.enum("channel", sessionChannels...),
		StartedAt:   f.datetime("started_at"),
	}
	return p, f.err
}

func (p *SessionStarted) OrderingCustomer() (uuid.UUID, bool) { return p.CustomerID, true }
func (p *SessionStarted) canonical() Object {
	return Object{
		"session_id":   p.SessionID.String(),
		"customer_id":  p.CustomerID.String(),
		"device_id":    p.DeviceID.String(),
		"device_type":  p.DeviceType,
		"ip_address":   p.IPAddress,
		"country_code": p.CountryCode,
		"channel":      p.Channel,
		"started_at":   FormatUTCZ(p.StartedAt),
	}
}

type ProductViewed struct {
	SessionID         uuid.UUID
	CustomerID        uuid.UUID
	ProductID         uuid.UUID
	Category          string
	UnitPrice         Decimal
	Currency          string
	QuantityAvailable int64
}

func parseProductViewed(data []byte) (Payload, error) {
	f := newFields(data, "session_id", "customer_id", "product_id", "category", "unit_price", "currency", "quantity_available")
	p := &ProductViewed{
		SessionID:         f.uuid("session_id"),
		CustomerID:        f.uuid("customer_id"),
		ProductID:         f.uuid("product_id"),
		Category:          f.nonEmpty("category"),
		UnitPrice:         f.money("unit_price", false),
		Currency:          f.enum("currency", currencies...),
		QuantityAvailable: f.integer("quantity_available", 0),
	}
	return p, f.err
}

func (p *ProductViewed) OrderingCustomer() (uuid.UUID, bool) { return p.CustomerID, true }
func (p *ProductViewed) canonical() Object {
	return Object{
		"session_id":         p.SessionID.String(),
		"customer_id":        p.CustomerID.String(),
		"product_id":         p.ProductID.String(),
		"category":           p.Category,
		"unit_price":         p.UnitPrice.String(),
		"currency":           p.Currency,
		"quantity_available": p.QuantityAvailable,
	}
}

type AddedToCart struct {
	SessionID  uuid.UUID
	CustomerID uuid.UUID
	CartID     uuid.UUID
	ProductID  uuid.UUID
	Quantity   int64
	UnitPrice  Decimal
	Currency   string
}

func parseAddedToCart(data []byte) (Payload, error) {
	f := newFields(data, "session_id", "customer_id", "cart_id", "product_id", "quantity", "unit_price", "currency")
	p := &AddedToCart{
		SessionID:  f.uuid("session_id"),
		CustomerID: f.uuid("customer_id"),
		CartID:     f.uuid("cart_id"),
		ProductID:  f.uuid("product_id"),
		Quantity:   f.integer("quantity", 1),
		UnitPrice:  f.money("unit_price", false),
		Currency:   f.enum("currency", currencies...),
	}
	return p, f.err
}

func (p *AddedToCart) OrderingCustomer() (uuid.UUID, bool) { return p.CustomerID, true }
func (p *AddedToCart) canonical() Object {
	return Object{
		"session_id":  p.SessionID.String(),
		"customer_id": p.CustomerID.String(),
		"cart_id":     p.CartID.String(),
		"product_id":  p.ProductID.String(),
		"quantity":    p.Quantity,
		"unit_price":  p.UnitPrice.String(),
		"currency":    p.Currency,
	}
}

var (
	errDiscount = errors.New("discount_amount cannot exceed subtotal")
	errTotal    = errors.New("total_amount must equal subtotal minus discount_amount")
)

func validateTotals(subtotal, discount, total Decimal) error {
	if discount.Cmp(subtotal) > 0 {
		return errDiscount
	}
	if !total.Equal(subtotal.Sub(discount)) {
		return errTotal
	}
	return nil
}

type CheckoutStarted struct {
	SessionID      uuid.UUID
	CustomerID     uuid.UUID
	CartID         uuid.UUID
	ItemCount      int64
	Subtotal       Decimal
	DiscountAmount Decimal
	TotalAmount    Decimal
	Currency       string
}

func parseCheckoutStarted(data []byte) (Payload, error) {
	f := newFields(data, "session_id", "customer_id", "cart_id", "item_count", "subtotal", "discount_amount", "total_amount", "currency")
	p := &CheckoutStarted{
		SessionID:      f.uuid("session_id"),
		CustomerID:     f.uuid("customer_id"),
		CartID:         f.uuid("cart_id"),
		ItemCount:      f.integer("item_count", 1),
		Subtotal:       f.money("subtotal", false),
		DiscountAmount: f.money("discount_amount", false),
		TotalAmount:    f.money("total_amount", false),
		Currency:       f.enum("currency", currencies...),
	}
	if f.err != nil {
		return p, f.err
	}
	return p, validateTotals(p.Subtotal, p.DiscountAmount, p.TotalAmount)
}

func (p *CheckoutStarted) OrderingCustomer() (uuid.UUID, bool) { return p.CustomerID, true }
func (p *CheckoutStarted) canonical() Object {
	return Object{
		"session_id":      p.SessionID.String(),
		"customer_id":     p.CustomerID.String(),
		"cart_id":         p.CartID.String(),
		"item_count":      p.ItemCount,
		"subtotal":        p.Subtotal.String(),
		"discount_amount": p.DiscountAmount.String(),
		"total_amount":    p.TotalAmount.String(),
		"currency":        p.Currency,
	}
}

type OrderCreated struct {
	OrderID             uuid.UUID
	CustomerID          uuid.UUID
	SessionID           uuid.UUID
	CartID              uuid.UUID
	ItemCount           int64
	Subtotal            Decimal
	DiscountAmount      Decimal
	TotalAmount         Decimal
	Currency            string
	ShippingCountryCode string
	BillingCountryCode  string
	CreatedAt           time.Time
}

func parseOrderCreated(data []byte) (Payload, error) {
	f := newFields(data, "order_id", "customer_id", "session_id", "cart_id", "item_count", "subtotal",
		"discount_amount", "total_amount", "currency", "shipping_country_code", "billing_country_code", "created_at")
	p := &OrderCreated{
		OrderID:             f.uuid("order_id"),
		CustomerID:          f.uuid("customer_id"),
		SessionID:           f.uuid("session_id"),
		CartID:              f.uuid("cart_id"),
		ItemCount:           f.integer("item_count", 1),
		Subtotal:            f.money("subtotal", false),
		DiscountAmount:      f.money("discount_amount", false),
		TotalAmount:         f.money("total_amount", false),
		Currency:            f.enum("currency", currencies...),
		ShippingCountryCode: f.country("shipping_country_code"),
		BillingCountryCode:  f.country("billing_country_code"),
		CreatedAt:           f.datetime("created_at"),
	}
	if f.err != nil {
		return p, f.err
	}
	return p, validateTotals(p.Subtotal, p.DiscountAmount, p.TotalAmount)
}

func (p *OrderCreated) OrderingCustomer() (uuid.UUID, bool) { return p.CustomerID, true }
func (p *OrderCreated) canonical() Object {
	return Object{
		"order_id":              p.OrderID.String(),
		"customer_id":           p.CustomerID.String(),
		"session_id":            p.SessionID.String(),
		"cart_id":               p.CartID.String(),
		"item_count":            p.ItemCount,
		"subtotal":              p.Subtotal.String(),
		"discount_amount":       p.DiscountAmount.String(),
		"total_amount":          p.TotalAmount.String(),
		"currency":              p.Currency,
		"shipping_country_code": p.ShippingCountryCode,
		"billing_country_code":  p.BillingCountryCode,
		"created_at":            FormatUTCZ(p.CreatedAt),
	}
}

// Payment covers payment_completed and payment_failed.
type Payment struct {
	Failed        bool
	PaymentID     uuid.UUID
	OrderID       uuid.UUID
	CustomerID    uuid.UUID
	Amount        Decimal
	Currency      string
	PaymentMethod string
	FailureReason string
	DeviceID      uuid.UUID
	IPAddress     string
	CountryCode   string
	At            time.Time
}

func parsePaymentCompleted(data []byte) (Payload, error) {
	f := newFields(data, "payment_id", "order_id", "customer_id", "amount", "currency", "payment_method",
		"device_id", "ip_address", "country_code", "completed_at")
	p := &Payment{
		PaymentID:     f.uuid("payment_id"),
		OrderID:       f.uuid("order_id"),
		CustomerID:    f.uuid("customer_id"),
		Amount:        f.money("amount", true),
		Currency:      f.enum("currency", currencies...),
		PaymentMethod: f.enum("payment_method", paymentMethods...),
		DeviceID:      f.uuid("device_id"),
		IPAddress:     f.ip("ip_address"),
		CountryCode:   f.country("country_code"),
		At:            f.datetime("completed_at"),
	}
	return p, f.err
}

func parsePaymentFailed(data []byte) (Payload, error) {
	f := newFields(data, "payment_id", "order_id", "customer_id", "amount", "currency", "payment_method",
		"failure_reason", "device_id", "ip_address", "country_code", "failed_at")
	p := &Payment{
		Failed:        true,
		PaymentID:     f.uuid("payment_id"),
		OrderID:       f.uuid("order_id"),
		CustomerID:    f.uuid("customer_id"),
		Amount:        f.money("amount", true),
		Currency:      f.enum("currency", currencies...),
		PaymentMethod: f.enum("payment_method", paymentMethods...),
		FailureReason: f.enum("failure_reason", failureReasons...),
		DeviceID:      f.uuid("device_id"),
		IPAddress:     f.ip("ip_address"),
		CountryCode:   f.country("country_code"),
		At:            f.datetime("failed_at"),
	}
	return p, f.err
}

func (p *Payment) OrderingCustomer() (uuid.UUID, bool) { return p.CustomerID, true }
func (p *Payment) canonical() Object {
	o := Object{
		"payment_id":     p.PaymentID.String(),
		"order_id":       p.OrderID.String(),
		"customer_id":    p.CustomerID.String(),
		"amount":         p.Amount.String(),
		"currency":       p.Currency,
		"payment_method": p.PaymentMethod,
		"device_id":      p.DeviceID.String(),
		"ip_address":     p.IPAddress,
		"country_code":   p.CountryCode,
	}
	if p.Failed {
		o["failure_reason"] = p.FailureReason
		o["failed_at"] = FormatUTCZ(p.At)
	} else {
		o["completed_at"] = FormatUTCZ(p.At)
	}
	return o
}

type RefundRequested struct {
	RefundID    uuid.UUID
	PaymentID   uuid.UUID
	OrderID     uuid.UUID
	CustomerID  uuid.UUID
	Amount      Decimal
	Currency    string
	Reason      string
	RequestedAt time.Time
}

func parseRefundRequested(data []byte) (Payload, error) {
	f := newFields(data, "refund_id", "payment_id", "order_id", "customer_id", "amount", "currency", "reason", "requested_at")
	p := &RefundRequested{
		RefundID:    f.uuid("refund_id"),
		PaymentID:   f.uuid("payment_id"),
		OrderID:     f.uuid("order_id"),
		CustomerID:  f.uuid("customer_id"),
		Amount:      f.money("amount", true),
		Currency:    f.enum("currency", currencies...),
		Reason:      f.nonEmpty("reason"),
		RequestedAt: f.datetime("requested_at"),
	}
	return p, f.err
}

func (p *RefundRequested) OrderingCustomer() (uuid.UUID, bool) { return p.CustomerID, true }
func (p *RefundRequested) canonical() Object {
	return Object{
		"refund_id":    p.RefundID.String(),
		"payment_id":   p.PaymentID.String(),
		"order_id":     p.OrderID.String(),
		"customer_id":  p.CustomerID.String(),
		"amount":       p.Amount.String(),
		"currency":     p.Currency,
		"reason":       p.Reason,
		"requested_at": FormatUTCZ(p.RequestedAt),
	}
}

type FraudAlertCreated struct {
	AlertID    uuid.UUID
	EventID    uuid.UUID
	CustomerID *uuid.UUID
	OrderID    *uuid.UUID
	FraudScore Decimal
	Decision   string
	Reasons    []string
	CreatedAt  time.Time
}

func parseFraudAlertCreated(data []byte) (Payload, error) {
	f := newFields(data, "alert_id", "event_id", "customer_id", "order_id", "fraud_score", "decision", "reasons", "created_at")
	p := &FraudAlertCreated{
		AlertID:    f.uuid("alert_id"),
		EventID:    f.uuid("event_id"),
		CustomerID: f.nullableUUID("customer_id"),
		OrderID:    f.nullableUUID("order_id"),
		FraudScore: f.decimal("fraud_score"),
		Decision:   f.enum("decision", fraudDecisions...),
		Reasons:    f.nonEmptyList("reasons"),
		CreatedAt:  f.datetime("created_at"),
	}
	if f.err == nil && (p.FraudScore.Sign() < 0 || p.FraudScore.Cmp(DecimalFromInt(100)) > 0) {
		return p, errors.New("fraud_score is out of range")
	}
	return p, f.err
}

func (p *FraudAlertCreated) OrderingCustomer() (uuid.UUID, bool) {
	if p.CustomerID == nil {
		return uuid.Nil, false
	}
	return *p.CustomerID, true
}

func (p *FraudAlertCreated) canonical() Object {
	return Object{
		"alert_id":    p.AlertID.String(),
		"event_id":    p.EventID.String(),
		"customer_id": uuidOrNil(p.CustomerID),
		"order_id":    uuidOrNil(p.OrderID),
		"fraud_score": p.FraudScore.String(),
		"decision":    p.Decision,
		"reasons":     p.Reasons,
		"created_at":  FormatUTCZ(p.CreatedAt),
	}
}
