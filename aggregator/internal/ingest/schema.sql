CREATE TABLE IF NOT EXISTS source_leases(scope text PRIMARY KEY,holder text NOT NULL,epoch bigint NOT NULL,expires_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS source_commit_guards(scope text PRIMARY KEY,holder text NOT NULL,epoch bigint NOT NULL);
CREATE OR REPLACE FUNCTION validate_source_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM source_leases WHERE scope=NEW.scope AND holder=NEW.holder AND epoch=NEW.epoch AND expires_at>clock_timestamp()) THEN RAISE EXCEPTION 'expired source ownership' USING ERRCODE='40001';END IF;RETURN NEW;END $$;
DROP TRIGGER IF EXISTS source_commit_fence ON source_commit_guards;
CREATE CONSTRAINT TRIGGER source_commit_fence AFTER INSERT OR UPDATE ON source_commit_guards DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_source_commit();
CREATE TABLE IF NOT EXISTS source_progress(source text NOT NULL,namespace text NOT NULL,received bigint NOT NULL,prepared bigint NOT NULL,applied bigint NOT NULL,virtual_now timestamptz,watermark timestamptz,PRIMARY KEY(source,namespace));
ALTER TABLE source_progress ADD COLUMN IF NOT EXISTS output_position bigint NOT NULL DEFAULT -1;
CREATE TABLE IF NOT EXISTS raw_inputs(message_id uuid PRIMARY KEY,source text NOT NULL,namespace text NOT NULL,position bigint NOT NULL,entity_key text NOT NULL,payload bytea NOT NULL,metadata jsonb NOT NULL,digest text NOT NULL,correlation_id text NOT NULL,received_at timestamptz NOT NULL DEFAULT clock_timestamp(),state text NOT NULL DEFAULT 'pending',reason text NOT NULL DEFAULT '',UNIQUE(source,namespace,position));
CREATE INDEX IF NOT EXISTS raw_pending ON raw_inputs(source,namespace,position) WHERE state='pending';
CREATE TABLE IF NOT EXISTS raw_outbox(message_id uuid PRIMARY KEY REFERENCES raw_inputs(message_id),subject text NOT NULL);
CREATE TABLE IF NOT EXISTS schema_cache(id integer PRIMARY KEY,schema text NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS field_reports(receipt_id uuid PRIMARY KEY,submitter text NOT NULL,idempotency_key text NOT NULL,digest text NOT NULL,payload bytea NOT NULL,received_at timestamptz NOT NULL DEFAULT clock_timestamp(),status text NOT NULL DEFAULT 'accepted',UNIQUE(submitter,idempotency_key));
CREATE TABLE IF NOT EXISTS field_results(receipt_id uuid PRIMARY KEY REFERENCES field_reports(receipt_id),result jsonb NOT NULL,processed_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE IF NOT EXISTS warning_expectations(sequence bigserial PRIMARY KEY,notification_id text NOT NULL UNIQUE,message_id uuid NOT NULL REFERENCES raw_inputs(message_id),first_received_at timestamptz NOT NULL,rejected boolean NOT NULL DEFAULT false);
CREATE TABLE IF NOT EXISTS canonical_outbox(operation_id text PRIMARY KEY,message_id uuid NOT NULL REFERENCES raw_inputs(message_id),source text NOT NULL,namespace text NOT NULL,position bigint NOT NULL,payload jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS canonical_outbox_order ON canonical_outbox(source,namespace,position);
CREATE TABLE IF NOT EXISTS coastal_stations(station_id text PRIMARY KEY,payload jsonb NOT NULL,refreshed_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE IF NOT EXISTS coastal_state(entity_key text PRIMARY KEY,position bigint NOT NULL,payload jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS wildfire_observations(namespace text NOT NULL,key text NOT NULL,latitude double precision NOT NULL,longitude double precision NOT NULL,at timestamptz NOT NULL,confidence text NOT NULL,active boolean NOT NULL,payload jsonb NOT NULL,fire_id text,previous_fire_id text,last_position bigint NOT NULL,PRIMARY KEY(namespace,key));
CREATE INDEX IF NOT EXISTS wildfire_neighbors ON wildfire_observations(namespace,latitude,at) WHERE active AND confidence<>'l';
CREATE INDEX IF NOT EXISTS wildfire_members ON wildfire_observations(namespace,fire_id) WHERE fire_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS wildfire_fires(namespace text NOT NULL,fire_id text NOT NULL,birth_position bigint NOT NULL,earliest_key text NOT NULL,active boolean NOT NULL,payload jsonb NOT NULL,PRIMARY KEY(namespace,fire_id));
CREATE TABLE IF NOT EXISTS provinces(namespace text NOT NULL,id text NOT NULL,payload jsonb NOT NULL,PRIMARY KEY(namespace,id));
ALTER TABLE provinces ADD COLUMN IF NOT EXISTS namespace text NOT NULL DEFAULT '';
ALTER TABLE provinces DROP CONSTRAINT IF EXISTS provinces_pkey;
ALTER TABLE provinces ADD PRIMARY KEY(namespace,id);
CREATE TABLE IF NOT EXISTS source_cursors(source text NOT NULL,namespace text NOT NULL,value text,PRIMARY KEY(source,namespace));
CREATE TABLE IF NOT EXISTS source_health(endpoint text PRIMARY KEY,state text NOT NULL,last_success timestamptz,last_attempt timestamptz NOT NULL DEFAULT clock_timestamp(),reason text NOT NULL DEFAULT '');
ALTER TABLE source_health ADD COLUMN IF NOT EXISTS last_new timestamptz;
ALTER TABLE source_health ADD COLUMN IF NOT EXISTS latest_observed timestamptz;
ALTER TABLE source_health ADD COLUMN IF NOT EXISTS source_generated timestamptz;
ALTER TABLE source_progress ADD COLUMN IF NOT EXISTS singletons bigint NOT NULL DEFAULT 0;
ALTER TABLE source_progress ADD COLUMN IF NOT EXISTS excluded bigint NOT NULL DEFAULT 0;
ALTER TABLE source_progress ADD COLUMN IF NOT EXISTS late_mutations bigint NOT NULL DEFAULT 0;
UPDATE source_progress p SET singletons=(SELECT count(*) FROM wildfire_observations w WHERE w.namespace=p.namespace AND w.active AND w.confidence<>'l' AND w.fire_id IS NULL),excluded=(SELECT count(*) FROM wildfire_observations w WHERE w.namespace=p.namespace AND w.active AND w.confidence='l') WHERE p.source='SATELIT';
ALTER TABLE raw_inputs ADD COLUMN IF NOT EXISTS attempts bigint NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS blocked_entities(source text NOT NULL,namespace text NOT NULL,entity_key text NOT NULL,message_id uuid NOT NULL,PRIMARY KEY(source,namespace,entity_key));
CREATE TABLE IF NOT EXISTS response_failures(message_id uuid PRIMARY KEY,source text NOT NULL,namespace text NOT NULL,entity text NOT NULL,received_at timestamptz NOT NULL DEFAULT clock_timestamp(),reason text NOT NULL,size bigint NOT NULL DEFAULT 0,sha256 text NOT NULL DEFAULT '',resolved boolean NOT NULL DEFAULT false);
CREATE TABLE IF NOT EXISTS response_failure_chunks(message_id uuid NOT NULL REFERENCES response_failures(message_id) ON DELETE CASCADE,ordinal integer NOT NULL,data bytea NOT NULL,PRIMARY KEY(message_id,ordinal));
-- Raw publication claims: short, expiring, owner-stamped. Order and priority are stored in the
-- row (filled by trigger from the immutable raw input) so a claim is one indexed read.
ALTER TABLE raw_outbox ADD COLUMN IF NOT EXISTS priority smallint NOT NULL DEFAULT 3;
ALTER TABLE raw_outbox ADD COLUMN IF NOT EXISTS received_at timestamptz;
ALTER TABLE raw_outbox ADD COLUMN IF NOT EXISTS position bigint NOT NULL DEFAULT 0;
ALTER TABLE raw_outbox ADD COLUMN IF NOT EXISTS generation bigint NOT NULL DEFAULT 0;
ALTER TABLE raw_outbox ADD COLUMN IF NOT EXISTS available_at timestamptz NOT NULL DEFAULT '-infinity';
ALTER TABLE raw_outbox ADD COLUMN IF NOT EXISTS claim_holder text;
ALTER TABLE raw_outbox ADD COLUMN IF NOT EXISTS claim_epoch bigint;
CREATE OR REPLACE FUNCTION raw_outbox_fill() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 SELECT i.received_at,i.position,coalesce((i.metadata->>'_replay_generation')::bigint,0) INTO NEW.received_at,NEW.position,NEW.generation FROM raw_inputs i WHERE i.message_id=NEW.message_id;
 NEW.priority:=CASE NEW.subject WHEN 'raw.coastal.critical' THEN 0 WHEN 'raw.field' THEN 1 WHEN 'raw.coastal.normal' THEN 2 ELSE 3 END;
 RETURN NEW;END $$;
DROP TRIGGER IF EXISTS raw_outbox_fill ON raw_outbox;
CREATE TRIGGER raw_outbox_fill BEFORE INSERT ON raw_outbox FOR EACH ROW EXECUTE FUNCTION raw_outbox_fill();
UPDATE raw_outbox o SET received_at=i.received_at,position=i.position,generation=coalesce((i.metadata->>'_replay_generation')::bigint,0),priority=CASE o.subject WHEN 'raw.coastal.critical' THEN 0 WHEN 'raw.field' THEN 1 WHEN 'raw.coastal.normal' THEN 2 ELSE 3 END FROM raw_inputs i WHERE i.message_id=o.message_id AND o.received_at IS NULL;
CREATE INDEX IF NOT EXISTS raw_outbox_claim ON raw_outbox(priority,received_at,position,message_id);
-- Canonical delivery claims (same pattern as the raw outbox).
ALTER TABLE canonical_outbox ADD COLUMN IF NOT EXISTS available_at timestamptz NOT NULL DEFAULT '-infinity';
ALTER TABLE canonical_outbox ADD COLUMN IF NOT EXISTS claim_holder text;
ALTER TABLE canonical_outbox ADD COLUMN IF NOT EXISTS claim_epoch bigint;
-- frontiers() asks for the first input that is not yet applied once per prepared input; without this
-- index that question walks every applied input from the oldest, so its cost grows with history.
CREATE INDEX IF NOT EXISTS raw_unapplied ON raw_inputs(source,namespace,position) WHERE state IN ('pending','prepared');
