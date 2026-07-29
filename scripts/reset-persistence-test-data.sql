\set ON_ERROR_STOP on

BEGIN;
CREATE TEMP TABLE test_event_ids ON COMMIT DROP AS
SELECT event_id
FROM processed_events
WHERE source = 'persistence-smoke:' || :'run_id';

DELETE FROM refunds WHERE event_id IN (SELECT event_id FROM test_event_ids);
DELETE FROM payments WHERE event_id IN (SELECT event_id FROM test_event_ids);
DELETE FROM fraud_alerts WHERE event_id IN (SELECT event_id FROM test_event_ids);
DELETE FROM orders WHERE created_event_id IN (SELECT event_id FROM test_event_ids);
DELETE FROM carts WHERE created_event_id IN (SELECT event_id FROM test_event_ids);
DELETE FROM product_views WHERE event_id IN (SELECT event_id FROM test_event_ids);
DELETE FROM sessions WHERE first_event_id IN (SELECT event_id FROM test_event_ids);
DELETE FROM customers WHERE first_event_id IN (SELECT event_id FROM test_event_ids);
DELETE FROM processed_events WHERE event_id IN (SELECT event_id FROM test_event_ids);
COMMIT;
