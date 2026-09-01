# Fraud-Eligible Workload Profiles

The benchmark varies incoming workload composition. It does not vary fraud
rules, thresholds, context construction, or processor behavior.

## The terminology matters

Approximately 42.8% is **not a fraud rate**. It is the fraud-eligible event
share:

```text
fraud-eligible event share =
fraud-eligible Kafka events / all generated Kafka events
```

The eligible event types are `checkout_started`, `order_created`,
`payment_failed`, `payment_completed`, and `refund_requested`.

An intuitive 100-event example is:

```text
~57  registration/session/view/cart
     → no fraud evaluation

~43  checkout/order/payment/refund
     → fraud evaluation
```

This does not mean that 43% of customers are fraudulent, that 43% of
payments are blocked, or that 43% of evaluations detect fraud. Most eligible
events can still produce `APPROVE`.

## What changes between profiles?

The historical baseline remains the original direct-injector path and its
historical behavior, approximately 42.8327% fraud-eligible events. Controlled
profiles use the same complete JourneyBuilder mechanism with calibrated
checkout progression:

| Profile | Representative retained eligible share |
| --- | ---: |
| `baseline` / historical behavior | ≈42.8327% |
| `fraud_eligible_20` | ≈19.98% |
| `fraud_eligible_10` | ≈9.87% |
| `fraud_eligible_5` | ≈4.81% |
| `fraud_eligible_0` | 0% |

The controlled builder keeps the non-checkout part of the journey valid and
lets a journey either stop after browsing/cart activity or continue through
checkout, order, and payment. No events are removed from an already-generated
journey, and no order or payment is manufactured independently.

The fraud engine remains fully enabled. The only intentional change is what
traffic arrives at Kafka.

## Checkout probability is not event share

`checkout_probability = 20%` does not imply a 20% fraud-eligible event share.
A journey that progresses can emit several eligible events:

```text
checkout_started
order_created
payment_completed
```

Profiles are therefore calibrated by event count:

```text
choose checkout progression probability
        ↓
generate a large deterministic sample
        ↓
count every emitted event type
        ↓
calculate actual eligible event share
        ↓
adjust until the target is reached
```

The `0%` profile is the important control case. It means no generated journey
reaches a fraud-eligible event. It does not mean `fraud_engine = disabled`;
the production fraud path is unchanged, but there is nothing eligible to
evaluate.

## Determinism and idempotency

Controlled profiles separate two concerns:

```text
common workload RNG: Random(seed)
+
profile-specific UUID namespace
```

The common random stream keeps customer, product, payment, and partition-key
realizations comparable between profiles while the checkout branch changes.
Profile-specific UUID namespaces prevent sequential profiles from reusing the
same event IDs in PostgreSQL's durable `processed_events` ledger.

An earlier implementation namespaced the entire random seed by profile. That
was rejected because it changed the whole random world—customer selection,
product choices, payments, and partition distribution—in addition to the
intended checkout composition. The remaining profile-specific difference is
the scenario branch and the event-ID namespace needed for safe replay.

### Interview takeaway

The experiment changes what traffic arrives, not how the processor handles
that traffic.

## Related

- [Scenario & Journey Generation](scenario-generation.md)
- [Fraud Decision Strategy](fraud-decision-strategy.md)
- [Benchmark Methodology](benchmark-methodology.md)
- [Detailed Fraud Engine](../fraud-engine.md)
