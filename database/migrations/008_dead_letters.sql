-- Queryable record of every DLQ envelope the processor published.
--
-- The processor publishes dead letters only to the commerce.events.dlq
-- Kafka topic: it must not depend on PostgreSQL for them, since database
-- failures are one reason events dead-letter. The dlq-sink consumer copies
-- each envelope here, keyed by the envelope's deterministic dlq_record_id,
-- so redelivery from Kafka never creates a second row. Topic retention can
-- expire records; this table keeps them.
--
-- The legacy dead_letter_events table (001) was never written by any
-- processor and is left in place for older tooling.
CREATE TABLE dead_letters (
    dlq_record_id UUID PRIMARY KEY,
    error_category TEXT NOT NULL,
    error_type TEXT,
    error_message TEXT NOT NULL,
    source_topic TEXT NOT NULL,
    source_partition INTEGER,
    source_offset BIGINT,
    original_event_id UUID,
    original_event_type TEXT,
    anomaly_type TEXT,
    processing_attempts INTEGER,
    consumer_group TEXT,
    failed_at TIMESTAMPTZ NOT NULL,
    dlq_topic TEXT NOT NULL,
    dlq_partition INTEGER NOT NULL,
    dlq_offset BIGINT NOT NULL,
    -- The full envelope, including the base64 original payload, for replay.
    envelope JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'NEW'
        CHECK (status IN ('NEW', 'TRIAGED', 'REPLAYED', 'DISCARDED')),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dead_letters_failed_at ON dead_letters (failed_at DESC);
CREATE INDEX idx_dead_letters_category_failed_at ON dead_letters (error_category, failed_at DESC);
CREATE INDEX idx_dead_letters_original_event_id ON dead_letters (original_event_id)
    WHERE original_event_id IS NOT NULL;
