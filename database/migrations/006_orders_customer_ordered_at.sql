CREATE INDEX idx_orders_customer_ordered_at
    ON orders (customer_id, ordered_at DESC);
