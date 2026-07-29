-- Baseline for databases initialized by database/init/001_create_storage_tables.sql.
-- The migration runner records this version without reapplying it when the three
-- original Sprint 2 tables already exist.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR NOT NULL,
    event_version INTEGER NOT NULL
        CONSTRAINT processed_events_event_version_positive CHECK (event_version > 0),
    event_time TIMESTAMPTZ NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL,
    source VARCHAR NOT NULL,
    correlation_id UUID NOT NULL,
    payload JSONB NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    customer_id UUID,
    order_id UUID,
    fraud_score NUMERIC(5, 2) NOT NULL,
    decision VARCHAR NOT NULL,
    reasons JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dead_letter_events (
    id BIGSERIAL PRIMARY KEY,
    original_topic VARCHAR NOT NULL,
    original_partition INTEGER,
    original_offset BIGINT,
    event_key VARCHAR,
    payload JSONB,
    error_type VARCHAR NOT NULL,
    error_message TEXT NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
