-- Indexes nothing reads. Lookups go through primary keys, the Kafka source
-- unique constraint and the (customer_id, time DESC) indexes, yet each of these
-- still costs a B-tree insert on the processor's hot write path.
-- Foreign-key-side indexes (orders.cart_id, payments.order_id) stay: they keep
-- cascading deletes from the parents from scanning the child table.
-- (DROP INDEX CONCURRENTLY cannot run inside the migration's transaction.)
DROP INDEX IF EXISTS idx_processed_events_event_type;
DROP INDEX IF EXISTS idx_processed_events_event_time;
DROP INDEX IF EXISTS idx_processed_events_correlation_id;
DROP INDEX IF EXISTS idx_processed_events_processed_at;
DROP INDEX IF EXISTS idx_product_views_session_id;
-- fraud_evaluations is only read by source event, customer and decision.
DROP INDEX IF EXISTS idx_fraud_evaluations_order_id;
DROP INDEX IF EXISTS idx_fraud_evaluations_payment_id;
-- A prefix of idx_product_views_customer_viewed_at.
DROP INDEX IF EXISTS idx_product_views_customer_id;
