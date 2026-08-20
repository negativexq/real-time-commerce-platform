CREATE INDEX idx_product_views_customer_viewed_at
    ON product_views (customer_id, viewed_at DESC);
