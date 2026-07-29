# Event contracts

Sprint 3 defines the shared, versioned contracts that future Kafka producers
and consumers must use. It does not publish or consume events.

## Envelope

Every event has the same strict envelope:

| Field | Type | Rule |
| --- | --- | --- |
| `event_id` | UUID | Unique, required event identity |
| `event_type` | `EventType` | Must be registered; arbitrary strings fail |
| `event_version` | integer | Positive contract version |
| `event_time` | datetime | Aware and normalized to UTC |
| `produced_at` | datetime | Aware, UTC, and no more than five minutes before `event_time` |
| `source` | string | Non-empty after trimming whitespace |
| `correlation_id` | UUID | Required journey/workflow identifier |
| `payload` | model | Exact model registered for `event_type` |

Unknown and malformed events must be rejected and, in a future consumer,
routed to the dead-letter topic. Extra envelope and payload fields are rejected
so accidental contract drift is visible.

## Event types and payloads

`shared.schemas.registry.EVENT_PAYLOAD_REGISTRY` is the single source of truth
for parsing:

| Event type | Payload |
| --- | --- |
| `user_registered` | Customer identity, country, persona, registration time |
| `session_started` | Customer/session/device, valid IP, channel, start time |
| `product_viewed` | Product/category, price/currency, available quantity |
| `added_to_cart` | Customer/cart/product, quantity, unit price |
| `checkout_started` | Cart counts and exactly reconciled monetary totals |
| `order_created` | Order identifiers, countries, and reconciled totals |
| `payment_completed` | Successful payment, instrument, device, IP, country |
| `payment_failed` | Failed payment plus normalized failure reason |
| `refund_requested` | Refund/payment/order identifiers, amount, reason |
| `fraud_alert_created` | Score, decision, affected identifiers, reasons |

Adding an event requires an enum value, a strict payload model, one registry
entry, representative fixtures where appropriate, and contract tests.

## Correlation IDs

Reuse one `correlation_id` across related steps in a customer journey. It is
for tracing and workflow association, not uniqueness; `event_id` remains the
unique identity of each event.

## Versioning and compatibility

- Versions begin at 1 and increase only for a real contract revision.
- Producers must populate the version they serialize.
- Backward-compatible additions require deliberate schema policy; current
  strict models reject extra fields.
- Breaking changes require a new version and consumer support before producer
  rollout.
- Never change the meaning or type of an existing field in place.
- Unknown event types or unsupported versions must fail validation rather than
  be guessed.

## Time and money

Naive datetimes are invalid. Aware inputs are normalized to UTC and serialized
as ISO 8601 with `Z`, for example `2026-01-15T10:05:00Z`.

Money and fraud scores use `Decimal`. Canonical JSON writes decimals as quoted
base-10 strings such as `"119.90"`, never binary JSON floats. This preserves
precision and avoids platform-dependent rounding.

## Canonical JSON

`canonical_json()` serializes UUIDs as strings, UTC datetimes as ISO 8601
strings, decimals as strings, and object keys in sorted order with compact
separators. `parse_event()` accepts JSON text or bytes, consults the registry,
and returns an envelope containing the concrete payload model.

Example:

```json
{
  "correlation_id": "00000000-0000-4000-8000-000000000100",
  "event_id": "00000000-0000-4000-8000-000000000006",
  "event_time": "2026-01-15T10:05:00Z",
  "event_type": "order_created",
  "event_version": 1,
  "payload": {
    "billing_country_code": "TR",
    "cart_id": "00000000-0000-4000-8000-000000000401",
    "created_at": "2026-01-15T10:05:00Z",
    "currency": "TRY",
    "customer_id": "00000000-0000-4000-8000-000000000101",
    "discount_amount": "10.00",
    "item_count": 2,
    "order_id": "00000000-0000-4000-8000-000000000301",
    "session_id": "00000000-0000-4000-8000-000000000201",
    "shipping_country_code": "TR",
    "subtotal": "129.90",
    "total_amount": "119.90"
  },
  "produced_at": "2026-01-15T10:05:01Z",
  "source": "order-service"
}
```

## Partition-key recommendations

- Customer journey events: `customer_id`.
- Session events: `customer_id` or `session_id`, based on the ordering scope.
- Order and payment events: `customer_id` when cross-event customer ordering
  matters.
- Fraud alerts: `customer_id` when present; otherwise `event_id`.

Kafka ordering exists only within a partition. These keys do not provide
global ordering.
