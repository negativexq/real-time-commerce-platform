CREATE TABLE demo_runs (
    run_id UUID PRIMARY KEY,
    scenario_type TEXT NOT NULL CHECK (scenario_type IN (
        'normal_customer', 'suspicious_payment', 'account_takeover',
        'bot_checkout', 'refund_abuse', 'duplicate_delivery',
        'malformed_event', 'mixed_traffic'
    )),
    status TEXT NOT NULL CHECK (status IN (
        'PENDING', 'STARTING', 'RUNNING', 'STOP_REQUESTED',
        'STOPPED', 'COMPLETED', 'FAILED'
    )),
    requested_event_count INTEGER NOT NULL CHECK (requested_event_count > 0),
    requested_duration_seconds INTEGER CHECK (requested_duration_seconds > 0),
    requested_events_per_second NUMERIC CHECK (requested_events_per_second > 0),
    seed BIGINT NOT NULL,
    parameters_json JSONB NOT NULL,
    test_scope TEXT NOT NULL CHECK (char_length(test_scope) BETWEEN 1 AND 100),
    requested_by TEXT NOT NULL DEFAULT 'local-demo',
    generated_event_count INTEGER NOT NULL DEFAULT 0 CHECK (generated_event_count >= 0),
    processed_event_count INTEGER NOT NULL DEFAULT 0 CHECK (processed_event_count >= 0),
    duplicate_count INTEGER NOT NULL DEFAULT 0 CHECK (duplicate_count >= 0),
    dlq_count INTEGER NOT NULL DEFAULT 0 CHECK (dlq_count >= 0),
    approve_count INTEGER NOT NULL DEFAULT 0 CHECK (approve_count >= 0),
    review_count INTEGER NOT NULL DEFAULT 0 CHECK (review_count >= 0),
    block_count INTEGER NOT NULL DEFAULT 0 CHECK (block_count >= 0),
    fraud_alert_count INTEGER NOT NULL DEFAULT 0 CHECK (fraud_alert_count >= 0),
    outbox_published_count INTEGER NOT NULL DEFAULT 0 CHECK (outbox_published_count >= 0),
    status_message TEXT,
    error_category TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    stopped_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_demo_runs_created_at ON demo_runs (created_at DESC);
CREATE INDEX idx_demo_runs_status ON demo_runs (status);
CREATE INDEX idx_demo_runs_scenario_type ON demo_runs (scenario_type);
CREATE INDEX idx_demo_runs_test_scope ON demo_runs (test_scope);

CREATE TABLE demo_run_event_manifest (
    run_id UUID NOT NULL REFERENCES demo_runs(run_id) ON DELETE CASCADE,
    event_id UUID NOT NULL,
    expected_event_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, event_id)
);
CREATE INDEX idx_demo_run_manifest_event_id
    ON demo_run_event_manifest (event_id);
