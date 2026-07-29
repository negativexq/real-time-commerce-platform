CREATE TABLE fraud_evaluations (
    evaluation_id UUID PRIMARY KEY,
    source_event_id UUID NOT NULL UNIQUE
        REFERENCES processed_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    customer_id UUID NOT NULL REFERENCES customers(customer_id),
    order_id UUID,
    payment_id UUID,
    total_score INTEGER NOT NULL CHECK (total_score BETWEEN 0 AND 100),
    decision TEXT NOT NULL CHECK (decision IN ('APPROVE', 'REVIEW', 'BLOCK')),
    severity TEXT NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    engine_version TEXT NOT NULL,
    ruleset_version TEXT NOT NULL,
    matched_rule_count INTEGER NOT NULL CHECK (matched_rule_count >= 0),
    rule_results JSONB NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_fraud_evaluations_customer_time
    ON fraud_evaluations (customer_id, evaluated_at);
CREATE INDEX idx_fraud_evaluations_decision_time
    ON fraud_evaluations (decision, evaluated_at);
CREATE INDEX idx_fraud_evaluations_payment_id ON fraud_evaluations (payment_id);
CREATE INDEX idx_fraud_evaluations_order_id ON fraud_evaluations (order_id);
CREATE INDEX idx_fraud_evaluations_total_score ON fraud_evaluations (total_score);

ALTER TABLE fraud_alerts RENAME COLUMN id TO legacy_id;
ALTER TABLE fraud_alerts RENAME COLUMN event_id TO source_event_id;
ALTER TABLE fraud_alerts RENAME COLUMN fraud_score TO score;
ALTER TABLE fraud_alerts RENAME COLUMN reasons TO reason_codes;
ALTER TABLE fraud_alerts
    ADD COLUMN alert_id UUID,
    ADD COLUMN evaluation_id UUID,
    ADD COLUMN payment_id UUID,
    ADD COLUMN severity TEXT,
    ADD COLUMN status TEXT NOT NULL DEFAULT 'OPEN',
    ADD COLUMN alert_event_id UUID,
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP;

UPDATE fraud_alerts
SET alert_id = gen_random_uuid(),
    severity = CASE
        WHEN decision = 'BLOCK' THEN 'HIGH'
        WHEN decision = 'REVIEW' THEN 'MEDIUM'
        ELSE 'LOW'
    END,
    alert_event_id = gen_random_uuid();

ALTER TABLE fraud_alerts
    ALTER COLUMN alert_id SET NOT NULL,
    ALTER COLUMN severity SET NOT NULL,
    ALTER COLUMN alert_event_id SET NOT NULL,
    ALTER COLUMN score TYPE INTEGER USING round(score)::integer,
    ADD CONSTRAINT fraud_alerts_alert_id_unique UNIQUE (alert_id),
    ADD CONSTRAINT fraud_alerts_evaluation_id_unique UNIQUE (evaluation_id),
    ADD CONSTRAINT fraud_alerts_alert_event_id_unique UNIQUE (alert_event_id),
    ADD CONSTRAINT fraud_alerts_evaluation_fk FOREIGN KEY (evaluation_id)
        REFERENCES fraud_evaluations(evaluation_id) DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT fraud_alerts_source_event_fk FOREIGN KEY (source_event_id)
        REFERENCES processed_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT fraud_alerts_severity_valid CHECK (
        severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')
    ),
    ADD CONSTRAINT fraud_alerts_status_valid CHECK (
        status IN ('OPEN', 'REVIEWING', 'RESOLVED', 'FALSE_POSITIVE', 'CONFIRMED_FRAUD')
    );
CREATE UNIQUE INDEX idx_fraud_alerts_evaluation_id_not_null
    ON fraud_alerts (evaluation_id) WHERE evaluation_id IS NOT NULL;
CREATE INDEX idx_fraud_alerts_status_created ON fraud_alerts (status, created_at);

CREATE TABLE fraud_outbox (
    outbox_id UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    event_id UUID NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL CHECK (event_version > 0),
    topic TEXT NOT NULL,
    message_key BYTEA NOT NULL,
    headers_json JSONB NOT NULL,
    payload_json JSONB NOT NULL,
    payload_bytes BYTEA NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'PUBLISHING', 'PUBLISHED', 'FAILED')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TIMESTAMPTZ NOT NULL,
    claimed_at TIMESTAMPTZ,
    claim_token UUID,
    published_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_fraud_outbox_status_available
    ON fraud_outbox (status, available_at);
CREATE INDEX idx_fraud_outbox_claimed_at
    ON fraud_outbox (claimed_at) WHERE status = 'PUBLISHING';
