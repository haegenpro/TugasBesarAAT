-- Candidate schema ONLY: not an application migration.
SET citus.shard_count = 4;
SET citus.shard_replication_factor = 1;
-- Citus 14.2 has removed multi_shard_commit_protocol; distributed writes use
-- its built-in 2PC path. The interrupted-COMMIT probe must observe PREPARE.
CREATE FUNCTION lab_route(s text, r text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT '[' || to_json(s)::text || ',' || to_json(r)::text || ']' $$;
SELECT create_distributed_function('lab_route(text,text)');
CREATE TABLE hazards (
 route_key text NOT NULL,
 hazard_id uuid NOT NULL,
 source text NOT NULL,
 source_ref_id text NOT NULL,
 ingested_at timestamptz NOT NULL DEFAULT '2026-11-11T00:00:00Z',
 active boolean NOT NULL DEFAULT true,
 revision bigint NOT NULL DEFAULT 1,
 severity text NOT NULL DEFAULT 'WASPADA',
 PRIMARY KEY(route_key,hazard_id),
 UNIQUE(route_key,source,source_ref_id),
 CHECK(route_key = lab_route(source,source_ref_id))
);
SELECT create_distributed_table('hazards','route_key');
CREATE TABLE operations (
 route_key text NOT NULL, operation_id text NOT NULL, hazard_id uuid NOT NULL,
 PRIMARY KEY(route_key,operation_id),
 FOREIGN KEY(route_key,hazard_id) REFERENCES hazards(route_key,hazard_id)
);
SELECT create_distributed_table('operations','route_key',colocate_with=>'hazards');
CREATE TABLE outbox (
 route_key text NOT NULL, event_id text NOT NULL, hazard_id uuid NOT NULL, revision bigint NOT NULL,
 PRIMARY KEY(route_key,event_id),
 FOREIGN KEY(route_key,hazard_id) REFERENCES hazards(route_key,hazard_id)
);
SELECT create_distributed_table('outbox','route_key',colocate_with=>'hazards');
CREATE TABLE lifecycle (LIKE outbox INCLUDING ALL);
ALTER TABLE lifecycle ADD FOREIGN KEY(route_key,hazard_id) REFERENCES hazards(route_key,hazard_id);
SELECT create_distributed_table('lifecycle','route_key',colocate_with=>'hazards');
CREATE TABLE batches (batch_id text PRIMARY KEY, fingerprint text NOT NULL, outcome text NOT NULL);
SELECT create_distributed_table('batches','batch_id',colocate_with=>'none');
CREATE TABLE snapshots (
 route_key text NOT NULL, snapshot_id uuid NOT NULL, hazard_id uuid NOT NULL,
 active boolean NOT NULL, revision bigint NOT NULL, severity text NOT NULL,
 PRIMARY KEY(route_key,snapshot_id,hazard_id)
);
SELECT create_distributed_table('snapshots','route_key',colocate_with=>'hazards');
-- Small capture metadata; never a coordinator copy of canonical rows.
CREATE TABLE captures (capture_id uuid PRIMARY KEY, epoch bigint NOT NULL, status text NOT NULL);
