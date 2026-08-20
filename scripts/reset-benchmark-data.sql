-- Full reset of disposable synthetic benchmark data for a controlled A/B
-- performance experiment. Truncates only the tables populated by the
-- synthetic event generator / direct Kafka injector. Never touches
-- schema_migrations, demo_runs/demo_run_event_manifest, or any
-- non-benchmark-scoped table.
\set ON_ERROR_STOP on

BEGIN;
TRUNCATE TABLE
    fraud_outbox,
    fraud_alerts,
    fraud_evaluations,
    refunds,
    payments,
    orders,
    cart_items,
    carts,
    product_views,
    sessions,
    customers,
    processed_events
    RESTART IDENTITY CASCADE;
COMMIT;
