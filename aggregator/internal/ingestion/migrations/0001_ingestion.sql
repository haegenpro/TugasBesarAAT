-- This schema belongs exclusively to the ingestion database, not canonical DB.
CREATE TABLE ingestion_state (
 namespace text COLLATE "C" PRIMARY KEY, initial_position bigint NOT NULL CHECK(initial_position>=0),
 next_position bigint NOT NULL CHECK(next_position>=0), published_through bigint NOT NULL,
 quota_bytes bigint NOT NULL CHECK(quota_bytes>0), used_bytes bigint NOT NULL DEFAULT 0 CHECK(used_bytes>=0),
 next_task integer NOT NULL DEFAULT 0 CHECK(next_task BETWEEN 0 AND 3)
);
CREATE TABLE ingestion_captures (
 namespace text COLLATE "C" NOT NULL REFERENCES ingestion_state(namespace), position bigint NOT NULL,
 payload_id text COLLATE "C" NOT NULL UNIQUE, size bigint NOT NULL, sha256 text NOT NULL,
 context jsonb NOT NULL, envelope jsonb, fingerprint text,
 state text NOT NULL CHECK(state IN ('captured','pending','published')),
 PRIMARY KEY(namespace,position)
);
CREATE UNIQUE INDEX ingestion_one_pending ON ingestion_captures(namespace) WHERE state<>'published';
CREATE TABLE ingestion_payload_chunks (
 payload_id text COLLATE "C" NOT NULL REFERENCES ingestion_captures(payload_id), ordinal integer NOT NULL,
 data bytea NOT NULL CHECK(octet_length(data)<=65536), PRIMARY KEY(payload_id,ordinal)
);
CREATE TABLE ingestion_progress(namespace text COLLATE "C" NOT NULL,entity text NOT NULL,archive_cursor bigint NOT NULL DEFAULT 0,PRIMARY KEY(namespace,entity));
CREATE TABLE ingestion_failures (
 namespace text COLLATE "C" NOT NULL, message_id text COLLATE "C" NOT NULL, source text NOT NULL,
 position bigint, received_at timestamptz NOT NULL, reason text NOT NULL, classification text NOT NULL CHECK(classification IN ('transient','incompatible')),
 status text NOT NULL CHECK(status IN ('retrying','parked','recovered')), attempts bigint NOT NULL DEFAULT 0,
 next_attempt_at timestamptz, recovered_at timestamptz, PRIMARY KEY(namespace,message_id)
);
CREATE TABLE ingestion_failure_attempts (
 namespace text COLLATE "C" NOT NULL,message_id text COLLATE "C" NOT NULL,attempt bigint NOT NULL,
 attempted_at timestamptz NOT NULL DEFAULT clock_timestamp(),reason text NOT NULL,
 PRIMARY KEY(namespace,message_id,attempt),FOREIGN KEY(namespace,message_id) REFERENCES ingestion_failures(namespace,message_id)
);
CREATE TABLE ingestion_replays (
 job_id text PRIMARY KEY,ops_identity text NOT NULL,request_id text NOT NULL,fingerprint text NOT NULL,
 namespace text NOT NULL,message_ids jsonb NOT NULL,adapter_version text NOT NULL,
 state text NOT NULL DEFAULT 'pending',UNIQUE(ops_identity,request_id)
);
CREATE TABLE input_schema_cache(schema_id integer PRIMARY KEY CHECK(schema_id>0),schema_text text NOT NULL,sha256 text NOT NULL);
