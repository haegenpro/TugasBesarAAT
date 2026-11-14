-- Administrator-only offline upgrade. Stop processors, writers and publishers.
-- Large membership and lineage collections remain on workers; control rows hold
-- only cursors, digests and scalar aggregates. Previous immutable rows stay intact.
CREATE TABLE wildfire_fire_versions (
 route_key text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 fire_id text COLLATE "C" NOT NULL, position bigint NOT NULL,
 member_position bigint NOT NULL, member_count bigint NOT NULL,
 member_sha256 text NOT NULL, header bytea NOT NULL, active boolean NOT NULL,
 birth bigint NOT NULL, earliest_key text COLLATE "C" NOT NULL, deactivated bigint, header_sha256 text NOT NULL,
 PRIMARY KEY(route_key,namespace,fire_id,position),
 CHECK(route_key=wildfire_route(namespace,'fire',fire_id))
);
CREATE INDEX wildfire_fire_candidates ON wildfire_fire_versions(namespace,member_count,member_sha256,deactivated DESC,birth,earliest_key,position);
CREATE TABLE wildfire_memberships (
 route_key text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 fire_id text COLLATE "C" NOT NULL, position bigint NOT NULL, member_key text COLLATE "C" NOT NULL,
 PRIMARY KEY(route_key,namespace,fire_id,position,member_key),
 CHECK(route_key=wildfire_route(namespace,'membership',canonical_route(fire_id,member_key)))
);
CREATE INDEX wildfire_members_by_fire ON wildfire_memberships(namespace,fire_id,position,member_key);
CREATE INDEX wildfire_members_by_key ON wildfire_memberships(namespace,member_key,fire_id,position);
CREATE TABLE wildfire_lineage (
 route_key text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 fire_id text COLLATE "C" NOT NULL, related_id text COLLATE "C" NOT NULL, position bigint NOT NULL,
 PRIMARY KEY(route_key,namespace,fire_id,related_id,position),
 CHECK(route_key=wildfire_route(namespace,'lineage',canonical_route(fire_id,related_id)))
);
CREATE INDEX wildfire_lineage_scan ON wildfire_lineage(namespace,fire_id,related_id,position);
CREATE TABLE wildfire_finalize (
 route_key text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 position bigint NOT NULL, kind text COLLATE "C" NOT NULL, entity_key text COLLATE "C" NOT NULL,
 reference text COLLATE "C" NOT NULL DEFAULT '', secondary text COLLATE "C" NOT NULL DEFAULT '',
 payload jsonb NOT NULL, done boolean NOT NULL DEFAULT false, charged_bytes bigint NOT NULL DEFAULT 0,
 PRIMARY KEY(route_key,namespace,position,kind,entity_key),
 CHECK(route_key=wildfire_route(namespace,'final:'||kind,entity_key))
);
CREATE INDEX wildfire_finalize_queue ON wildfire_finalize(namespace,position,kind,reference,done,entity_key);
CREATE INDEX wildfire_finalize_reverse ON wildfire_finalize(namespace,position,kind,secondary,entity_key);
ALTER TABLE wildfire_jobs ADD COLUMN finalization_version integer NOT NULL DEFAULT 1 CHECK(finalization_version IN(1,2));
ALTER TABLE wildfire_jobs ADD COLUMN seal_after text COLLATE "C" NOT NULL DEFAULT '';
ALTER TABLE wildfire_jobs ADD COLUMN seal_count bigint NOT NULL DEFAULT 0;
ALTER TABLE wildfire_jobs ADD COLUMN seal_hash bytea;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_extension WHERE extname='citus') THEN
  PERFORM create_distributed_table('wildfire_fire_versions','route_key',colocate_with=>'wildfire_state');
  PERFORM create_distributed_table('wildfire_memberships','route_key',colocate_with=>'wildfire_state');
  PERFORM create_distributed_table('wildfire_lineage','route_key',colocate_with=>'wildfire_state');
  PERFORM create_distributed_table('wildfire_finalize','route_key',colocate_with=>'wildfire_state');
 END IF;
END $$;
-- Legacy supported records were bounded by the old cache. Explicitly reject an
-- unsupported oversized baseline before expanding it, within this locked upgrade.
DO $$ DECLARE r record; d jsonb; h jsonb; k text; digest text; BEGIN
 IF EXISTS(SELECT 1 FROM wildfire_state WHERE kind='fire' AND octet_length(payload)>16777216) THEN
  RAISE EXCEPTION 'unsupported oversized legacy wildfire header; preserve baseline and inspect offline';
 END IF;
 FOR r IN SELECT namespace,entity_key,position,payload FROM wildfire_state WHERE kind='fire' ORDER BY namespace,entity_key,position LOOP
  d:=convert_from(r.payload,'UTF8')::jsonb;
  IF jsonb_typeof(d->'members') IS DISTINCT FROM 'array' OR jsonb_typeof(d->'related') IS DISTINCT FROM 'array' THEN
   RAISE EXCEPTION 'unsupported partial wildfire baseline';
  END IF;
  SELECT encode(sha256(convert_to(coalesce(string_agg(to_json(value)::text||E'\n','' ORDER BY value COLLATE "C"),''),'UTF8')),'hex') INTO digest FROM jsonb_array_elements_text(d->'members');
  h:=d-'members'-'related';
  IF h->'event' IS NOT NULL AND h->'event'<>'null'::jsonb THEN h:=jsonb_set(h,'{event,attributes,detection_keys}','[]'); END IF;
  INSERT INTO wildfire_fire_versions VALUES(wildfire_route(r.namespace,'fire',r.entity_key),r.namespace,r.entity_key,r.position,r.position,jsonb_array_length(d->'members'),digest,convert_to(h::text,'UTF8'),(d->>'active')::boolean,(d#>>'{derivation,birth_offset}')::bigint,d#>>'{derivation,earliest_key}',(d->>'deactivated_at')::bigint,encode(sha256(convert_to(h::text,'UTF8')),'hex'));
  FOR k IN SELECT value FROM jsonb_array_elements_text(d->'members') LOOP
   INSERT INTO wildfire_memberships VALUES(wildfire_route(r.namespace,'membership',canonical_route(r.entity_key,k)),r.namespace,r.entity_key,r.position,k);
  END LOOP;
  FOR k IN SELECT value FROM jsonb_array_elements_text(d->'related') LOOP
   INSERT INTO wildfire_lineage VALUES(wildfire_route(r.namespace,'lineage',canonical_route(r.entity_key,k)),r.namespace,r.entity_key,k,r.position) ON CONFLICT DO NOTHING;
  END LOOP;
 END LOOP;
 -- Only invisible finalization scratch is rebuilt. Graph/history, original job
 -- identities, activated frontiers and sealed/partially sealed bytes are retained.
 FOR r IN SELECT namespace,position FROM wildfire_jobs WHERE phase IN('resolve','verify','verify_nodes','activate') LOOP
  DELETE FROM wildfire_state WHERE namespace=r.namespace AND position=r.position AND kind IN('fire','intent');
  DELETE FROM wildfire_fire_versions WHERE namespace=r.namespace AND position=r.position;
  DELETE FROM wildfire_memberships WHERE namespace=r.namespace AND position=r.position;
  DELETE FROM wildfire_lineage WHERE namespace=r.namespace AND position=r.position;
  UPDATE wildfire_jobs SET phase='resolve',checkpoint='{}',state_sha256=NULL,node_sha256=NULL,output_count=0 WHERE namespace=r.namespace AND position=r.position;
  SELECT jsonb_build_object('records',count(*),'bytes',coalesce(sum(octet_length(payload)+octet_length(entity_key)+octet_length(reference)+256),0)) INTO d FROM wildfire_state WHERE namespace=r.namespace AND position=r.position;
  SELECT jsonb_build_object('bytes',coalesce(sum(octet_length(node_key)*2+256),0)) INTO h FROM wildfire_work WHERE namespace=r.namespace AND position=r.position;
  UPDATE wildfire_jobs SET record_count=(d->>'records')::bigint,staged_bytes=(d->>'bytes')::bigint+(h->>'bytes')::bigint WHERE namespace=r.namespace AND position=r.position;
 END LOOP;
 UPDATE wildfire_jobs SET finalization_version=2 WHERE phase<>'active';
END $$;
