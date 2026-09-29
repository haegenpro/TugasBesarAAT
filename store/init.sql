-- Canonical Store schema for the Aggregator (BNPB slice), IF4031 M1.
-- Initial schema, run once via postgres:16-alpine3.24's
-- /docker-entrypoint-initdb.d/ convention.

CREATE TABLE IF NOT EXISTS hazard_events (
    hazard_id      UUID PRIMARY KEY,
    source         TEXT NOT NULL,
    source_ref_id  TEXT NOT NULL,
    hazard_type    TEXT NOT NULL,
    severity       TEXT NOT NULL,
    area_name      TEXT NOT NULL,
    latitude       DOUBLE PRECISION NOT NULL,
    longitude      DOUBLE PRECISION NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL,
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    attributes     JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_payload    JSONB
);

-- Canonical deduplication key: a source record is uniquely identified by
-- (source, source_ref_id), never by hazard_id.
CREATE UNIQUE INDEX IF NOT EXISTS ux_hazard_events_source_ref
    ON hazard_events (source, source_ref_id);

CREATE INDEX IF NOT EXISTS ix_hazard_events_hazard_type ON hazard_events (hazard_type);
CREATE INDEX IF NOT EXISTS ix_hazard_events_severity ON hazard_events (severity);
CREATE INDEX IF NOT EXISTS ix_hazard_events_area_name ON hazard_events (area_name);
CREATE INDEX IF NOT EXISTS ix_hazard_events_occurred_at ON hazard_events (occurred_at);
CREATE INDEX IF NOT EXISTS ix_hazard_events_ingested_at ON hazard_events (ingested_at);

-- One pending publication per new hazard. The Aggregator writes this row in
-- the same transaction as hazard_events and removes it after NATS acknowledges.
CREATE TABLE IF NOT EXISTS hazard_outbox (
    hazard_id UUID PRIMARY KEY REFERENCES hazard_events(hazard_id) ON DELETE CASCADE,
    event_payload JSONB NOT NULL,
    correlation_id TEXT NOT NULL,
    queued_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_hazard_outbox_queued_at
    ON hazard_outbox (queued_at, hazard_id);

-- Static volcano reference data: volcano_id -> area_name, latitude, longitude.
-- Owned by the Aggregator; PVMBG's mock volcano_id values must match these.
CREATE TABLE IF NOT EXISTS volcano_reference (
    volcano_id  TEXT PRIMARY KEY,
    area_name   TEXT NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL
);

INSERT INTO volcano_reference (volcano_id, area_name, latitude, longitude) VALUES
    ('merapi',   'Gunung Merapi',    -7.5407, 110.4457),
    ('semeru',   'Gunung Semeru',    -8.1077, 112.9224),
    ('kelud',    'Gunung Kelud',     -7.9306, 112.3084),
    ('agung',    'Gunung Agung',     -8.3425, 115.5083),
    ('sinabung', 'Gunung Sinabung',   3.1700,  98.3928),
    ('krakatau', 'Anak Krakatau',    -6.1022, 105.4230),
    ('bromo',    'Gunung Bromo',     -7.9425, 112.9530),
    ('raung',    'Gunung Raung',     -8.1247, 114.0420),
    ('dukono',   'Gunung Dukono',     1.6997, 127.8944),
    ('ibu',      'Gunung Ibu',        1.4883, 127.6300)
ON CONFLICT (volcano_id) DO NOTHING;
