-- Offline only. Original input bytes and processing work are sharded by
-- source/namespace/position. Coordinator session rows contain frontiers only.
INSERT INTO canonical_ownership(scope,source) VALUES ('processor:BMKG','BMKG');
CREATE FUNCTION input_route(s text,n text,p bigint) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT '['||to_json(s)::text||','||to_json(n)::text||','||p::text||']' $$;
CREATE TABLE input_sessions (
 source text NOT NULL CHECK(source IN ('BMKG','SATELIT')),
 namespace text COLLATE "C" NOT NULL,
 initial_position bigint NOT NULL CHECK(initial_position>=0),
 received_through bigint NOT NULL,
 prepared_through bigint NOT NULL,
 applied_through bigint NOT NULL,
 PRIMARY KEY(source,namespace),
 CHECK(initial_position>0 OR received_through>=-1),
 CHECK(applied_through<=prepared_through AND prepared_through<=received_through)
);
CREATE TABLE input_history (
 route_key text COLLATE "C" NOT NULL,
 source text NOT NULL, namespace text COLLATE "C" NOT NULL, position bigint NOT NULL CHECK(position>=0),
 message_id text COLLATE "C" NOT NULL, fingerprint text NOT NULL,
 envelope jsonb NOT NULL, original_payload bytea NOT NULL,
 state text NOT NULL DEFAULT 'received' CHECK(state IN ('received','prepared','applied')),
 output_count integer CHECK(output_count>=0), preparation_topology text,
 PRIMARY KEY(route_key,source,namespace,position),
 CHECK(route_key=input_route(source,namespace,position))
);
CREATE TABLE input_outputs (
 route_key text COLLATE "C" NOT NULL, source text NOT NULL, namespace text COLLATE "C" NOT NULL,
 position bigint NOT NULL, ordinal integer NOT NULL CHECK(ordinal>=0),
 batch_id text COLLATE "C" NOT NULL, batch jsonb NOT NULL,
 state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','applied')),
 result jsonb, PRIMARY KEY(route_key,source,namespace,position,ordinal),
 FOREIGN KEY(route_key,source,namespace,position) REFERENCES input_history(route_key,source,namespace,position)
);
CREATE TABLE input_bmkg_observations (
 route_key text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 entity text NOT NULL, record_id text COLLATE "C" NOT NULL,
 history_sequence bigint NOT NULL CHECK(history_sequence>=0), payload jsonb NOT NULL,
 observed_at timestamptz NOT NULL,
 PRIMARY KEY(route_key,namespace,entity,record_id),
 CHECK(route_key=canonical_route('BMKG',record_id))
);
CREATE TABLE input_bmkg_checks(namespace text COLLATE "C" PRIMARY KEY,snapshot_position bigint NOT NULL,snapshot_started timestamptz NOT NULL,validated_position bigint);
CREATE TABLE input_cutovers(namespace text COLLATE "C" PRIMARY KEY,state text NOT NULL CHECK(state IN ('building','ready')));
CREATE TABLE input_schema_cache (schema_id integer PRIMARY KEY CHECK(schema_id>0), schema_text text NOT NULL, sha256 text NOT NULL);
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_extension WHERE extname='citus') THEN
  ALTER TABLE input_outputs DROP CONSTRAINT input_outputs_route_key_source_namespace_position_fkey;
  PERFORM create_distributed_function('input_route(text,text,bigint)');
  PERFORM create_distributed_table('input_history','route_key',colocate_with=>'none');
  PERFORM create_distributed_table('input_outputs','route_key',colocate_with=>'input_history');
  PERFORM create_distributed_table('input_bmkg_observations','route_key',colocate_with=>'canonical_entities');
  ALTER TABLE input_outputs ADD FOREIGN KEY(route_key,source,namespace,position) REFERENCES input_history(route_key,source,namespace,position);
 END IF;
END $$;
