-- Administrator-run, offline. State rows are immutable versions, distributed
-- independently of session ownership. No session payload/checkpoint blob lives
-- in the coordinator control tables.
CREATE FUNCTION wildfire_route(n text,k text,e text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT '['||to_json('SATELIT'::text)::text||','||to_json(n)::text||','||to_json(k)::text||','||to_json(e)::text||']' $$;
CREATE TABLE wildfire_runs (
 namespace text COLLATE "C" PRIMARY KEY, config_sha256 text NOT NULL,
 prepared_through bigint NOT NULL, applied_through bigint NOT NULL,
 virtual_now timestamptz, watermark timestamptz, late_count bigint NOT NULL DEFAULT 0,
 CHECK(applied_through<=prepared_through)
);
CREATE TABLE wildfire_jobs (
 namespace text COLLATE "C" NOT NULL, position bigint NOT NULL,
 phase text NOT NULL, checkpoint jsonb NOT NULL DEFAULT '{}', topology text NOT NULL,
 late boolean NOT NULL DEFAULT false, next_clock timestamptz, next_watermark timestamptz,
 record_count bigint NOT NULL DEFAULT 0, staged_bytes bigint NOT NULL DEFAULT 0,
 state_sha256 text, node_sha256 text, blocked_reason text,
 output_count bigint NOT NULL DEFAULT 0, batch_id text NOT NULL,
 sealed boolean NOT NULL DEFAULT false, output_sha256 text, output_bytes bigint,
 output_chunks integer, seal_cursor integer NOT NULL DEFAULT 0, applied boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(namespace,position), CHECK(position>=0)
);
CREATE TABLE wildfire_state (
 route_key text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 kind text COLLATE "C" NOT NULL, entity_key text COLLATE "C" NOT NULL,
 position bigint NOT NULL CHECK(position>=0), payload bytea NOT NULL,
 sha256 text NOT NULL, event_at timestamptz, active boolean NOT NULL DEFAULT false,
 reference text COLLATE "C" NOT NULL DEFAULT '',
 PRIMARY KEY(route_key,namespace,kind,entity_key,position),
 CHECK(route_key=wildfire_route(namespace,kind,entity_key))
);
CREATE INDEX wildfire_state_scan ON wildfire_state(namespace,kind,entity_key,position DESC);
CREATE INDEX wildfire_state_time ON wildfire_state(namespace,kind,event_at,entity_key,position DESC);
CREATE INDEX wildfire_state_reference ON wildfire_state(namespace,kind,reference,entity_key,position DESC);
CREATE TABLE wildfire_work (
 route_key text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 position bigint NOT NULL, node_key text COLLATE "C" NOT NULL,
 component text COLLATE "C" NOT NULL DEFAULT '', done boolean NOT NULL DEFAULT false,
 PRIMARY KEY(route_key,namespace,position,node_key),
 CHECK(route_key=wildfire_route(namespace,'work',node_key))
);
CREATE INDEX wildfire_work_queue ON wildfire_work(namespace,position,component,done,node_key);
CREATE TABLE wildfire_output_chunks (
 route_key text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 position bigint NOT NULL, ordinal integer NOT NULL CHECK(ordinal>=0),
 payload bytea NOT NULL, sha256 text NOT NULL,
 PRIMARY KEY(route_key,namespace,position,ordinal),
 CHECK(route_key=wildfire_route(namespace,'output',position::text||':'||ordinal::text))
);
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_extension WHERE extname='citus') THEN
  PERFORM create_distributed_function('wildfire_route(text,text,text)');
  PERFORM create_distributed_table('wildfire_state','route_key',colocate_with=>'none');
  PERFORM create_distributed_table('wildfire_work','route_key',colocate_with=>'wildfire_state');
  PERFORM create_distributed_table('wildfire_output_chunks','route_key',colocate_with=>'wildfire_state');
 END IF;
END $$;
