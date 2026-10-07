ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;
ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;
CREATE TABLE IF NOT EXISTS source_observations (
 source TEXT NOT NULL, entity TEXT NOT NULL, record_id TEXT NOT NULL,
 payload JSONB NOT NULL, observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 history_sequence BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY(source,entity,record_id)
);
ALTER TABLE source_observations ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS warning_related ON source_observations ((payload->>'related_event_id')) WHERE entity='tsunami-warnings';
CREATE TABLE IF NOT EXISTS source_history_cursors (
 source TEXT NOT NULL, entity TEXT NOT NULL, cursor BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY(source,entity)
);
CREATE TABLE IF NOT EXISTS source_poll_state (
 endpoint TEXT PRIMARY KEY, state TEXT NOT NULL DEFAULT 'unavailable',
 last_success TIMESTAMPTZ, last_attempt TIMESTAMPTZ, stale_since TIMESTAMPTZ,
 next_attempt TIMESTAMPTZ, failures INTEGER NOT NULL DEFAULT 0, revision BIGINT NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS report_notices (
 notice_id UUID PRIMARY KEY, source TEXT NOT NULL, source_ref_id TEXT NOT NULL,
 payload JSONB NOT NULL, state TEXT NOT NULL, revision BIGINT NOT NULL,
 UNIQUE(source,source_ref_id)
);
CREATE TABLE IF NOT EXISTS report_lifecycle_events (
 event_id UUID PRIMARY KEY, record_id TEXT NOT NULL, revision BIGINT NOT NULL,
 event_payload JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS lifecycle_record ON report_lifecycle_events(record_id,revision);
CREATE TABLE IF NOT EXISTS report_lifecycle_outbox (
 event_id UUID PRIMARY KEY REFERENCES report_lifecycle_events(event_id), queued_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
