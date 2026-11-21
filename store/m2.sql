-- Run after init.sql, while application controllers are stopped. Never reset data.
CREATE TABLE IF NOT EXISTS m2_migrations (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS source_leases (scope text PRIMARY KEY, holder text NOT NULL, epoch bigint NOT NULL, expires_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS source_commit_guards (scope text PRIMARY KEY,holder text NOT NULL,epoch bigint NOT NULL);
CREATE OR REPLACE FUNCTION validate_source_commit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM source_leases WHERE scope=NEW.scope AND holder=NEW.holder AND epoch=NEW.epoch AND expires_at>clock_timestamp()) THEN
  RAISE EXCEPTION 'expired source ownership' USING ERRCODE='40001';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS source_commit_fence ON source_commit_guards;
CREATE CONSTRAINT TRIGGER source_commit_fence AFTER INSERT OR UPDATE ON source_commit_guards
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_source_commit();
ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS active boolean NOT NULL DEFAULT true;
CREATE TABLE IF NOT EXISTS canonical_batches (operation_id text PRIMARY KEY,digest text NOT NULL,source text NOT NULL,namespace text NOT NULL,position bigint NOT NULL,applied_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS canonical_progress (source text NOT NULL,namespace text NOT NULL,position bigint NOT NULL DEFAULT -1,PRIMARY KEY(source,namespace));
-- Keep outboxes coordinator-local and atomically commit them alongside Citus work.
ALTER TABLE hazard_outbox DROP CONSTRAINT IF EXISTS hazard_outbox_hazard_id_fkey;
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_dist_partition WHERE logicalrelid='hazard_events'::regclass) THEN
  ALTER TABLE hazard_events DROP CONSTRAINT hazard_events_pkey;
  ALTER TABLE hazard_events ADD PRIMARY KEY(source_ref_id,hazard_id);
  PERFORM create_distributed_table('hazard_events','source_ref_id',shard_count:=8);
 END IF;
END $$;
-- Isolate distributed index DDL from the surrounding migration statements.
DO $$ BEGIN
 EXECUTE 'CREATE INDEX IF NOT EXISTS hazard_events_active_order ON hazard_events(occurred_at DESC,hazard_id DESC) WHERE active';
END $$;
INSERT INTO m2_migrations(version) VALUES(1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS schema_cache(id integer PRIMARY KEY,schema text NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS failed_ingestion_payloads(payload_id uuid PRIMARY KEY,size bigint NOT NULL DEFAULT 0,sha256 text NOT NULL DEFAULT '',content_type text NOT NULL);
CREATE TABLE IF NOT EXISTS failed_ingestion_payload_chunks(payload_id uuid NOT NULL REFERENCES failed_ingestion_payloads(payload_id) ON DELETE CASCADE,ordinal integer NOT NULL,data bytea NOT NULL,PRIMARY KEY(payload_id,ordinal));
ALTER TABLE failed_ingestions ADD COLUMN IF NOT EXISTS payload_id uuid REFERENCES failed_ingestion_payloads(payload_id);
ALTER TABLE failed_ingestions ADD COLUMN IF NOT EXISTS replay_requested boolean NOT NULL DEFAULT false;
ALTER TABLE failed_ingestions ADD COLUMN IF NOT EXISTS replay_generation bigint NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS source_policy(source text PRIMARY KEY,version integer NOT NULL CHECK(version IN(1,2)));
INSERT INTO source_policy(source,version) VALUES('PVMBG',1) ON CONFLICT DO NOTHING;
ALTER TABLE failed_ingestions ADD COLUMN IF NOT EXISTS original_identity text NOT NULL DEFAULT '';
ALTER TABLE failed_ingestions DROP CONSTRAINT IF EXISTS failed_ingestions_source_entity_digest_key;
CREATE UNIQUE INDEX IF NOT EXISTS failed_original_identity ON failed_ingestions(source,entity,original_identity,digest);
CREATE TABLE IF NOT EXISTS canonical_visibility(txid text PRIMARY KEY,published boolean NOT NULL DEFAULT false,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),published_at timestamptz);
CREATE INDEX IF NOT EXISTS canonical_visibility_pending ON canonical_visibility(txid) WHERE NOT published;
