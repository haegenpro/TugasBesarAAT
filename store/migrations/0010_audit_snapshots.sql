-- Offline additive migration. Frozen canonical rows inherit source-pair placement.
CREATE TABLE audit_snapshot_records (LIKE hazard_events INCLUDING DEFAULTS);
ALTER TABLE audit_snapshot_records ADD capture_id uuid NOT NULL;
ALTER TABLE audit_snapshot_records ADD PRIMARY KEY(route_key,capture_id,hazard_id);
ALTER TABLE audit_snapshot_records ADD CHECK(route_key=canonical_route(source,source_ref_id));
CREATE INDEX audit_snapshot_order ON audit_snapshot_records(capture_id,ingested_at,hazard_id);
CREATE INDEX audit_snapshot_refs ON audit_snapshot_records(capture_id,source_ref_id COLLATE "C");
CREATE TABLE audit_snapshot_markers (
 route_key text COLLATE "C" NOT NULL,capture_id uuid NOT NULL,source_shard bigint NOT NULL,
 record_count bigint NOT NULL,sha256 text NOT NULL,
 PRIMARY KEY(route_key,capture_id,source_shard)
);
CREATE TABLE audit_snapshot_units (
 capture_id uuid NOT NULL REFERENCES platform_audit_captures(capture_id),
 source_shard bigint NOT NULL, group_id integer NOT NULL,source_table text NOT NULL,
 target_table text NOT NULL,marker_table text NOT NULL,marker_route text NOT NULL,
 record_count bigint NOT NULL DEFAULT 0,sha256 text,hash_state bytea,
 last_time timestamptz,last_id uuid,complete boolean NOT NULL DEFAULT false,
 verified boolean NOT NULL DEFAULT false,verify_count bigint NOT NULL DEFAULT 0,
 verify_hash bytea,verify_time timestamptz,verify_id uuid,
 PRIMARY KEY(capture_id,source_shard)
);
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_extension WHERE extname='citus') THEN
  PERFORM create_distributed_table('audit_snapshot_records','route_key',colocate_with=>'hazard_events');
  PERFORM create_distributed_table('audit_snapshot_markers','route_key',colocate_with=>'hazard_events');
 END IF;
END $$;
