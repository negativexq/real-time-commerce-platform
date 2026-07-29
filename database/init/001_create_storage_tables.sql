CREATE TABLE processed_events (
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

CREATE INDEX idx_processed_events_event_type
    ON processed_events (event_type);
CREATE INDEX idx_processed_events_event_time
    ON processed_events (event_time);
CREATE INDEX idx_processed_events_correlation_id
    ON processed_events (correlation_id);

CREATE TABLE fraud_alerts (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    customer_id UUID,
    order_id UUID,
    fraud_score NUMERIC(5, 2) NOT NULL
        CONSTRAINT fraud_alerts_score_range CHECK (
            fraud_score >= 0 AND fraud_score <= 100
        ),
    decision VARCHAR NOT NULL
        CONSTRAINT fraud_alerts_decision_valid CHECK (
            decision IN ('APPROVE', 'REVIEW', 'BLOCK')
        ),
    reasons JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fraud_alerts_event_id
    ON fraud_alerts (event_id);
CREATE INDEX idx_fraud_alerts_decision
    ON fraud_alerts (decision);
CREATE INDEX idx_fraud_alerts_created_at
    ON fraud_alerts (created_at);

CREATE TABLE dead_letter_events (
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

CREATE INDEX idx_dead_letter_events_failed_at
    ON dead_letter_events (failed_at);
