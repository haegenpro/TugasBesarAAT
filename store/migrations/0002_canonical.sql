
-- Offline upgrade: stop all writers and publishers before invoking the migrator.
CREATE FUNCTION canonical_route(s text, r text) RETURNS text
 LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
 AS $$ SELECT '[' || to_json(s)::text || ',' || to_json(r)::text || ']' $$;
CREATE TABLE canonical_entities (
 route_key text COLLATE "C" NOT NULL, hazard_id uuid NOT NULL,
 source text NOT NULL, source_ref_id text NOT NULL,
 revision bigint NOT NULL CHECK(revision>0), active boolean NOT NULL,
 ingested_at timestamptz NOT NULL,
 source_namespace text, source_position bigint CHECK(source_position>=0),
 PRIMARY KEY(route_key,hazard_id), UNIQUE(route_key,source,source_ref_id),
 CHECK(route_key=canonical_route(source,source_ref_id)),
 CHECK((source_namespace IS NULL) = (source_position IS NULL))
);
INSERT INTO canonical_entities SELECT canonical_route(source,source_ref_id),hazard_id,source,source_ref_id,revision,true,ingested_at,NULL,NULL FROM hazard_events;
ALTER TABLE hazard_events ADD route_key text COLLATE "C";
UPDATE hazard_events SET route_key=canonical_route(source,source_ref_id);
ALTER TABLE hazard_events ALTER route_key SET NOT NULL;
ALTER TABLE hazard_events ADD active boolean NOT NULL DEFAULT true;
ALTER TABLE hazard_outbox DROP CONSTRAINT hazard_outbox_hazard_id_fkey;
ALTER TABLE hazard_outbox DROP CONSTRAINT hazard_outbox_pkey;
ALTER TABLE hazard_events DROP CONSTRAINT hazard_events_pkey;
DROP INDEX ux_hazard_events_source_ref;
ALTER TABLE hazard_events ADD PRIMARY KEY(route_key,hazard_id);
ALTER TABLE hazard_events ADD UNIQUE(route_key,source,source_ref_id);
ALTER TABLE hazard_events ADD CHECK(route_key=canonical_route(source,source_ref_id));
ALTER TABLE hazard_events ADD FOREIGN KEY(route_key,hazard_id) REFERENCES canonical_entities(route_key,hazard_id);
ALTER TABLE hazard_outbox ADD route_key text COLLATE "C";
UPDATE hazard_outbox o SET route_key=h.route_key FROM hazard_events h WHERE h.hazard_id=o.hazard_id;
ALTER TABLE hazard_outbox ALTER route_key SET NOT NULL;
ALTER TABLE hazard_outbox ADD PRIMARY KEY(route_key,hazard_id);
ALTER TABLE hazard_outbox ADD FOREIGN KEY(route_key,hazard_id) REFERENCES hazard_events(route_key,hazard_id) ON DELETE CASCADE;
CREATE TABLE canonical_operations (
 operation_key text COLLATE "C" PRIMARY KEY, source text NOT NULL, operation_id text NOT NULL,
 fingerprint text NOT NULL, batch_id text NOT NULL, result jsonb NOT NULL,
 CHECK(operation_key=canonical_route(source,operation_id))
);
CREATE TABLE canonical_batches (
 batch_id text COLLATE "C" PRIMARY KEY, fingerprint text NOT NULL, result jsonb NOT NULL
);
-- Small authoritative coordinator metadata. Production replication/decision durability is a Role 2 gate.
CREATE TABLE canonical_visibility (singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), generation bigint NOT NULL DEFAULT 0 CHECK(generation>=0));
INSERT INTO canonical_visibility(singleton) VALUES(true);
CREATE TABLE canonical_attempts (
 attempt_id uuid PRIMARY KEY, topology text NOT NULL, state text NOT NULL CHECK(state IN ('pending','committed','aborted')),
 created_at timestamptz NOT NULL DEFAULT now(), resolved_at timestamptz
);
CREATE INDEX pending_attempts ON canonical_attempts(created_at) WHERE state='pending';
CREATE TABLE canonical_attempt_commits (attempt_id uuid PRIMARY KEY);
ALTER TABLE hazard_outbox ADD attempt_id uuid;
ALTER TABLE report_lifecycle_outbox ADD attempt_id uuid;
CREATE TABLE canonical_lifecycle_events (
 route_key text COLLATE "C" NOT NULL, event_id uuid NOT NULL, hazard_id uuid NOT NULL,
 event_payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(route_key,event_id),
 FOREIGN KEY(route_key,hazard_id) REFERENCES canonical_entities(route_key,hazard_id)
);
CREATE TABLE canonical_lifecycle_outbox (
 route_key text COLLATE "C" NOT NULL, event_id uuid NOT NULL, queued_at timestamptz NOT NULL DEFAULT now(),
 traceparent text NOT NULL DEFAULT '', tracestate text NOT NULL DEFAULT '', attempt_id uuid,
 PRIMARY KEY(route_key,event_id), FOREIGN KEY(route_key,event_id) REFERENCES canonical_lifecycle_events(route_key,event_id)
);
-- Move only entity-owned hazard corrections; notices/status stay in the reporting lifecycle.
INSERT INTO canonical_lifecycle_events(route_key,event_id,hazard_id,event_payload,created_at)
 SELECT h.route_key,e.event_id,h.hazard_id,e.event_payload,e.created_at
 FROM report_lifecycle_events e JOIN hazard_events h ON h.hazard_id::text=e.record_id
 WHERE e.event_payload->>'event_type'='hazard_corrected';
INSERT INTO canonical_lifecycle_outbox(route_key,event_id,queued_at,traceparent,tracestate,attempt_id)
 SELECT e.route_key,o.event_id,o.queued_at,o.traceparent,o.tracestate,o.attempt_id
 FROM report_lifecycle_outbox o JOIN canonical_lifecycle_events e USING(event_id);
DELETE FROM report_lifecycle_outbox WHERE event_id IN(SELECT event_id FROM canonical_lifecycle_events);
DELETE FROM report_lifecycle_events WHERE event_id IN(SELECT event_id FROM canonical_lifecycle_events);
