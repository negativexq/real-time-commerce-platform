CREATE INDEX idx_payments_customer_attempted_at
    ON payments (customer_id, attempted_at DESC);
