INSERT INTO canonical_ownership(scope,source) VALUES
 ('acquisition:SATELIT','SATELIT'), ('acquisition:SENSOR_PANTAI','SENSOR_PANTAI')
ON CONFLICT (scope) DO NOTHING;

CREATE TABLE source_progress (
 source text NOT NULL CHECK(source IN ('SATELIT','SENSOR_PANTAI')), namespace text COLLATE "C" NOT NULL,
 position bigint NOT NULL CHECK(position>=0), cursor text, updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(source,namespace)
);

CREATE TABLE ingestion_failure_payloads (
 namespace text COLLATE "C" NOT NULL, message_id text COLLATE "C" NOT NULL,
 content_type text NOT NULL, payload bytea NOT NULL, sha256 text NOT NULL,
 PRIMARY KEY(namespace,message_id), FOREIGN KEY(namespace,message_id) REFERENCES ingestion_failures(namespace,message_id)
);

CREATE SEQUENCE field_report_positions MINVALUE 0 START 0;
CREATE TABLE field_report_ledger (
 submitter text COLLATE "C" NOT NULL, report_id text COLLATE "C" NOT NULL,
 receipt_id uuid NOT NULL UNIQUE, fingerprint text NOT NULL, content_type text NOT NULL, payload bytea NOT NULL,
 position bigint NOT NULL UNIQUE, message_id text COLLATE "C" NOT NULL UNIQUE, received_at timestamptz NOT NULL,
 state text NOT NULL CHECK(state IN ('pending','accepted','processed','parked')), reason text,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(submitter,report_id)
);
CREATE INDEX field_report_pending ON field_report_ledger(received_at) WHERE state='pending';

CREATE TABLE field_reports (
 receipt_id uuid PRIMARY KEY REFERENCES field_report_ledger(receipt_id), submitter text NOT NULL, report_id text NOT NULL,
 report_type text NOT NULL, occurred_at timestamptz NOT NULL, latitude double precision NOT NULL, longitude double precision NOT NULL,
 description text NOT NULL, attributes jsonb NOT NULL, processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(submitter,report_id)
);
