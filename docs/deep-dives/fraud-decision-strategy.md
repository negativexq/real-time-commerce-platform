# Fraud Decision Strategy

The generator does not assign fraud decisions. It generates behavior. The
fraud engine evaluates that behavior after the source event and its history
have been persisted.

Persona is not a direct fraud signal. It is a generator-side strategy used to
produce different observable behavior. The processor does not ask whether a
customer is `suspicious` or `account_takeover`; it builds a bounded
`FraudContext` from persisted facts and evaluates rules against that context.

```text
Persona
   ↓
behavior generation
   ↓
Kafka events
   ↓
persisted state/history
   ↓
FraudContext
   ↓
rules
   ↓
score
   ↓
APPROVE / REVIEW / BLOCK
```

## Which events enter the fraud path?

The processor evaluates these event types:

- `checkout_started`
- `order_created`
- `payment_failed`
- `payment_completed`
- `refund_requested`

Registration, session, product-view, and cart events are not themselves fraud
evaluations. They establish the history used by later evaluations: customer
identity and home country, session timing, viewed products, orders, payments,
devices, countries, and refunds.

## Behavioral signals

The default ruleset is deterministic and explainable. Its rules use the
following observable signals:

| Rule | Signal |
| --- | --- |
| `high_amount` | Amount is at least 5,000, or is at least three times the customer's matching-currency historical average. |
| `payment_velocity` | At least three recent payment attempts in the 120-second window. |
| `failed_payment_burst` | At least three recent failed payments in five minutes. |
| `new_device` | An unrecognized device on an account with at least two successful payments. |
| `country_mismatch` | Current country differs from home country, or recent payment activity spans multiple countries. |
| `rapid_checkout` | Transaction follows session start within 15 seconds. |
| `account_takeover_composite` | At least four of established history, new device, country mismatch, rapid checkout, and high amount are present. |
| `refund_abuse` | Repeated refunds, or a rapid near-full refund after repeated refund activity. |
| `bot_checkout` | At least 12 product views followed by a rapid transaction. |
| `payment_amount_mismatch` | Persisted payment amount does not match the order amount. |

Each matched rule contributes its configured integer score. The engine sums
matched scores and caps the result at 100. With the default thresholds:

```text
0–29   APPROVE
30–59  REVIEW
60–100 BLOCK
```

The default rule scores include 25/20 for the two amount signals, 20 for
payment velocity, 25 for a failed-payment burst, 15 for a new device, up to
20 for country signals, 15 for rapid checkout, 60 for the account-takeover
composite, 35 for refund abuse, 30 for bot checkout, and 100 for an amount
mismatch. Configuration validates the threshold relationship and rule bounds.

For example:

```text
Established customer:
same country
known device
normal payment history

New session:
new device
different country
rapid checkout
unusually high payment

→ several behavioral rules match
→ accumulated score crosses a threshold
→ REVIEW or BLOCK
```

The outcome is caused by the persisted behavioral evidence, not by the persona
name that helped generate it.

## Persistence and downstream alerts

Every eligible event gets a deterministic row in `fraud_evaluations`.

```text
APPROVE
  → fraud_evaluations

REVIEW / BLOCK
  → fraud_evaluations
  → fraud_alerts
  → fraud_outbox
```

These writes happen in the same PostgreSQL transaction as source-event
processing. The outbox then separates durable state from Kafka publication:

```text
PostgreSQL transaction
        ↓
fraud_outbox
        ↓
separate publisher
        ↓
commerce.fraud-alerts
```

This avoids an unsafe direct PostgreSQL-plus-Kafka dual write. The publisher
claims committed rows, publishes the canonical alert event, and marks the row
published. Its delivery is at least once; deterministic derived IDs let
downstream consumers handle the crash window safely.

### Interview takeaway

Fraud is based on observable behavior and bounded persisted history, not
synthetic labels. See the [detailed fraud-engine documentation](../fraud-engine.md)
for implementation-level rules, context queries, and outbox recovery details.

## Related

- [Scenario & Journey Generation](scenario-generation.md)
- [Fraud-Eligible Workload Profiles](fraud-workload-profiles.md)
- [Benchmark Methodology](benchmark-methodology.md)
- [Detailed Fraud Engine](../fraud-engine.md)
