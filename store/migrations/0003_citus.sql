
-- Explicit candidate conversion, never inferred from extension presence.
ALTER TABLE hazard_events DROP CONSTRAINT hazard_events_route_key_hazard_id_fkey;
ALTER TABLE hazard_outbox DROP CONSTRAINT hazard_outbox_route_key_hazard_id_fkey;
ALTER TABLE canonical_lifecycle_events DROP CONSTRAINT canonical_lifecycle_events_route_key_hazard_id_fkey;
ALTER TABLE canonical_lifecycle_outbox DROP CONSTRAINT canonical_lifecycle_outbox_route_key_event_id_fkey;
SET citus.shard_count=4;
SET citus.shard_replication_factor=1;
SELECT create_distributed_function('canonical_route(text,text)');
SELECT create_distributed_table('canonical_entities','route_key');
SELECT create_distributed_table('hazard_events','route_key',colocate_with=>'canonical_entities');
SELECT create_distributed_table('hazard_outbox','route_key',colocate_with=>'canonical_entities');
SELECT create_distributed_table('canonical_lifecycle_events','route_key',colocate_with=>'canonical_entities');
SELECT create_distributed_table('canonical_lifecycle_outbox','route_key',colocate_with=>'canonical_entities');
SELECT create_distributed_table('canonical_operations','operation_key',colocate_with=>'none');
SELECT create_distributed_table('canonical_batches','batch_id',colocate_with=>'none');

ALTER TABLE hazard_events ADD FOREIGN KEY(route_key,hazard_id) REFERENCES canonical_entities(route_key,hazard_id);
ALTER TABLE hazard_outbox ADD FOREIGN KEY(route_key,hazard_id) REFERENCES hazard_events(route_key,hazard_id) ON DELETE CASCADE;
ALTER TABLE canonical_lifecycle_events ADD FOREIGN KEY(route_key,hazard_id) REFERENCES canonical_entities(route_key,hazard_id);
ALTER TABLE canonical_lifecycle_outbox ADD FOREIGN KEY(route_key,event_id) REFERENCES canonical_lifecycle_events(route_key,event_id);

-- Remove retained physical coordinator copies after successful redistribution.
SELECT truncate_local_data_after_distributing_table('canonical_entities');
SELECT truncate_local_data_after_distributing_table('hazard_events');
SELECT truncate_local_data_after_distributing_table('hazard_outbox');
SELECT truncate_local_data_after_distributing_table('canonical_lifecycle_events');
SELECT truncate_local_data_after_distributing_table('canonical_lifecycle_outbox');
SELECT truncate_local_data_after_distributing_table('canonical_operations');
SELECT truncate_local_data_after_distributing_table('canonical_batches');
