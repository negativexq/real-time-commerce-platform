ALTER TABLE processed_events
    RENAME COLUMN payload TO payload_json;

ALTER TABLE processed_events
    ADD COLUMN kafka_topic TEXT,
    ADD COLUMN kafka_partition INTEGER,
    ADD COLUMN kafka_offset BIGINT,
    ADD COLUMN kafka_key BYTEA,
    ADD COLUMN payload_sha256 CHAR(64),
    ADD COLUMN processor_instance_id TEXT;

ALTER TABLE processed_events
    ADD CONSTRAINT processed_events_kafka_partition_nonnegative
        CHECK (kafka_partition IS NULL OR kafka_partition >= 0),
    ADD CONSTRAINT processed_events_kafka_offset_nonnegative
        CHECK (kafka_offset IS NULL OR kafka_offset >= 0),
    ADD CONSTRAINT processed_events_payload_sha256_format
        CHECK (payload_sha256 IS NULL OR payload_sha256 ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT processed_events_kafka_source_unique
        UNIQUE (kafka_topic, kafka_partition, kafka_offset);

CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at
    ON processed_events (processed_at);

CREATE TABLE customers (
    customer_id UUID PRIMARY KEY,
    email_hash TEXT NOT NULL,
    persona TEXT NOT NULL,
    home_country CHAR(2) NOT NULL,
    preferred_currency CHAR(3),
    registered_at TIMESTAMPTZ NOT NULL,
    first_event_id UUID NOT NULL REFERENCES processed_events(event_id)
        DEFERRABLE INITIALLY DEFERRED,
    last_event_id UUID NOT NULL REFERENCES processed_events(event_id)
        DEFERRABLE INITIALLY DEFERRED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sessions (
    session_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL REFERENCES customers(customer_id),
    device_id TEXT NOT NULL,
    device_type TEXT NOT NULL,
    ip_address INET NOT NULL,
    country CHAR(2) NOT NULL,
    channel TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    first_event_id UUID NOT NULL REFERENCES processed_events(event_id)
        DEFERRABLE INITIALLY DEFERRED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_sessions_customer_id ON sessions (customer_id);

CREATE TABLE product_views (
    event_id UUID PRIMARY KEY REFERENCES processed_events(event_id)
        DEFERRABLE INITIALLY DEFERRED,
    session_id UUID NOT NULL REFERENCES sessions(session_id),
    customer_id UUID NOT NULL REFERENCES customers(customer_id),
    product_id UUID NOT NULL,
    category TEXT NOT NULL,
    price NUMERIC(38, 18) NOT NULL CHECK (price >= 0),
    currency CHAR(3) NOT NULL,
    quantity_available INTEGER NOT NULL CHECK (quantity_available >= 0),
    viewed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_product_views_session_id ON product_views (session_id);
CREATE INDEX idx_product_views_customer_id ON product_views (customer_id);

CREATE TABLE carts (
    cart_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL REFERENCES customers(customer_id),
    session_id UUID NOT NULL REFERENCES sessions(session_id),
    status TEXT NOT NULL,
    subtotal NUMERIC(38, 18) NOT NULL DEFAULT 0 CHECK (subtotal >= 0),
    discount NUMERIC(38, 18) NOT NULL DEFAULT 0 CHECK (discount >= 0),
    total NUMERIC(38, 18) NOT NULL DEFAULT 0 CHECK (total >= 0),
    currency CHAR(3) NOT NULL,
    item_count INTEGER NOT NULL DEFAULT 0 CHECK (item_count >= 0),
    created_event_id UUID NOT NULL REFERENCES processed_events(event_id)
        DEFERRABLE INITIALLY DEFERRED,
    latest_event_id UUID NOT NULL REFERENCES processed_events(event_id)
        DEFERRABLE INITIALLY DEFERRED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_carts_customer_id ON carts (customer_id);

CREATE TABLE cart_items (
    cart_id UUID NOT NULL REFERENCES carts(cart_id) ON DELETE CASCADE,
    product_id UUID NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(38, 18) NOT NULL CHECK (unit_price >= 0),
    line_total NUMERIC(38, 18) NOT NULL CHECK (line_total >= 0),
    PRIMARY KEY (cart_id, product_id)
);

CREATE TABLE orders (
    order_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL REFERENCES customers(customer_id),
    session_id UUID NOT NULL REFERENCES sessions(session_id),
    cart_id UUID NOT NULL REFERENCES carts(cart_id),
    status TEXT NOT NULL,
    subtotal NUMERIC(38, 18) NOT NULL CHECK (subtotal >= 0),
    discount NUMERIC(38, 18) NOT NULL CHECK (discount >= 0),
    total NUMERIC(38, 18) NOT NULL CHECK (total >= 0),
    currency CHAR(3) NOT NULL,
    item_count INTEGER NOT NULL CHECK (item_count > 0),
    shipping_country CHAR(2) NOT NULL,
    billing_country CHAR(2) NOT NULL,
    created_event_id UUID NOT NULL REFERENCES processed_events(event_id)
        DEFERRABLE INITIALLY DEFERRED,
    latest_event_id UUID NOT NULL REFERENCES processed_events(event_id)
        DEFERRABLE INITIALLY DEFERRED,
    ordered_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_orders_customer_id ON orders (customer_id);
CREATE INDEX idx_orders_cart_id ON orders (cart_id);

CREATE TABLE payments (
    payment_id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(order_id),
    customer_id UUID NOT NULL REFERENCES customers(customer_id),
    status TEXT NOT NULL CHECK (status IN ('completed', 'failed', 'partially_refunded', 'refunded')),
    amount NUMERIC(38, 18) NOT NULL CHECK (amount > 0),
    currency CHAR(3) NOT NULL,
    payment_method TEXT NOT NULL,
    failure_reason TEXT,
    device_id TEXT NOT NULL,
    ip_address INET NOT NULL,
    country CHAR(2) NOT NULL,
    event_id UUID NOT NULL UNIQUE REFERENCES processed_events(event_id)
        DEFERRABLE INITIALLY DEFERRED,
    attempted_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_payments_order_id ON payments (order_id);

CREATE TABLE refunds (
    refund_id UUID PRIMARY KEY,
    payment_id UUID NOT NULL REFERENCES payments(payment_id),
    order_id UUID NOT NULL REFERENCES orders(order_id),
    customer_id UUID NOT NULL REFERENCES customers(customer_id),
    amount NUMERIC(38, 18) NOT NULL CHECK (amount > 0),
    currency CHAR(3) NOT NULL,
    reason TEXT NOT NULL,
    event_id UUID NOT NULL UNIQUE REFERENCES processed_events(event_id)
        DEFERRABLE INITIALLY DEFERRED,
    requested_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_refunds_payment_id ON refunds (payment_id);

ALTER TABLE fraud_alerts
    ADD CONSTRAINT fraud_alerts_event_id_unique UNIQUE (event_id);
