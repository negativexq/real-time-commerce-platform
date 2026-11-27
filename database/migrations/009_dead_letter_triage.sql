-- Operator workflow for stored dead letters, and removal of the legacy table.
--
-- Status moves NEW -> TRIAGED / DISCARDED / REPLAYED, and back to NEW to
-- reopen. A replay sends the original record back to its source topic; the
-- event then either applies or dead-letters again as a new record.
ALTER TABLE dead_letters
    ADD COLUMN status_changed_at TIMESTAMPTZ,
    ADD COLUMN triage_note TEXT
        CONSTRAINT dead_letters_triage_note_bounded CHECK (char_length(triage_note) <= 500),
    ADD COLUMN replay_count INTEGER NOT NULL DEFAULT 0
        CONSTRAINT dead_letters_replay_count_non_negative CHECK (replay_count >= 0),
    ADD COLUMN last_replayed_at TIMESTAMPTZ;

CREATE INDEX idx_dead_letters_status_failed_at
    ON dead_letters (status, failed_at DESC);

-- dead_letter_events (001) was never written by any processor. Drop it only
-- while it is empty, so this migration can never discard data someone added.
DO $$
BEGIN
    IF to_regclass('public.dead_letter_events') IS NOT NULL THEN
        IF EXISTS (SELECT 1 FROM dead_letter_events) THEN
            RAISE EXCEPTION 'dead_letter_events has rows; move them before dropping the table';
        END IF;
        DROP TABLE dead_letter_events;
    END IF;
END
$$;
