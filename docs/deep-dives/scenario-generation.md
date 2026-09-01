# Scenario & Journey Generation

The event generator models a stateful customer journey rather than producing
independent random Kafka messages. `JourneyBuilder` is the single place that
constructs the sequence, creates identifiers, and updates the generator's
customer state after a successful journey.

```text
new customer?
    ↓
user_registered
    ↓
session_started
    ↓
product_viewed × N
    ↓
added_to_cart?
    ↓
checkout_started?
    ↓
order_created
    ↓
payment_failed / payment_completed
    ↓
refund_requested?
```

The optional branches are genuine journey decisions. A customer can browse
and stop, add an item and abandon the cart, or continue through a complete
commerce transaction. Payment retries are emitted as additional failed or
completed payment events for the same order, and a refund is emitted only
after a successful payment.

## Causal validity

Events are generated with shared references, including:

- `customer_id`
- `session_id`
- `cart_id`
- `order_id`
- `payment_id`
- `correlation_id`

For example, a `payment_completed` event references the order created earlier
in that journey. It is not manufactured as a standalone payment. Checkout
references the cart and session, the order references the session/cart, and a
refund references the successful payment and its order.

This matters directly to benchmarking. A load generator that emits malformed
causal chains measures missing-dependency errors and DLQ behavior instead of
processor capacity. During benchmark development, manually spliced partial
workloads produced `MissingBusinessDependencyError`; that approach was
rejected in favor of complete `JourneyBuilder` journeys. This was an
experimental generator-design finding, not a production incident.

## Stateful customers

The builder keeps a bounded in-memory customer pool (100 customers by default)
and reuses customers once the pool has been populated. A journey can therefore
represent either:

```text
new customer
```

or:

```text
returning customer with historical behavior
```

Returning history includes prior sessions, viewed products, carts, orders,
payments, failed-payment counts, refunds, accumulated spend, known devices,
and known IP addresses. The builder can also seed an account-takeover scenario
from a customer with established normal history.

That history is what makes later fraud evaluation meaningful. The processor
can compare a new session with previous devices, countries, payments, failed
attempts, orders, refunds, and browsing behavior. The generator's plain state
store is only a scenario-building aid; the processor still evaluates the
events after they are delivered and persisted through the normal path.

## Personas describe behavior

The current registry contains `normal`, `indecisive`, `discount_hunter`,
`suspicious`, `bot`, and `account_takeover` strategies. Each supplies a
`PersonaProfile` to `JourneyBuilder`.

| Persona | Generated behavior |
| --- | --- |
| `normal` | Moderate browsing, stable identity, and high payment success. |
| `indecisive` | More repeated browsing and cart abandonment across sessions. |
| `discount_hunter` | Price-sensitive product selection, larger discounts, and more reuse of abandoned carts. |
| `suspicious` | Fast, high-value activity, device/country changes, and payment retries. |
| `bot` | Bounded catalogue-scanning bursts with very little purchasing. |
| `account_takeover` | Rapid high-value activity on a previously normal customer, with device and country changes. |

Profiles control behavior parameters such as product-view count, add-to-cart
probability, checkout probability, payment success, refund probability,
discounts, action timing, device changes, country changes, retry probability,
and high-value product preference. They do not directly determine a fraud
decision and persona names are not inputs to `FraudContext`.

For example:

```text
normal:
moderate browsing
stable identity
high payment success

suspicious:
fast activity
higher-value behavior
device changes
payment retries

account_takeover:
previously normal customer
+ new suspicious session behavior
```

The result is domain-valid, stateful traffic with realistic causal history,
not a bag of unrelated messages.

### Interview takeaway

`JourneyBuilder` produces causally valid, stateful domain traffic rather than
independent random Kafka messages.

## Related

- [Fraud Decision Strategy](fraud-decision-strategy.md)
- [Fraud-Eligible Workload Profiles](fraud-workload-profiles.md)
- [Benchmark Methodology](benchmark-methodology.md)
- [Architecture and event schemas](../architecture/README.md)
