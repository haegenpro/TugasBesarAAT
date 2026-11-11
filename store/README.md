# Canonical Store

PostgreSQL 16 (`postgres:16-alpine3.24`). Owned exclusively by the Aggregator —
no other service connects to this database directly.

`init.sql` initializes a fresh PostgreSQL volume through the bootstrap role. Aggregator runtime credentials have data access only; startup validates tables without issuing DDL. M2 adds the offline versioned upgrade below; startup requires the migrated schema.

## Tables

### `hazard_events`

The canonical `HazardEvent` table.

| Column | Notes |
| --- | --- |
| `hazard_id` | UUIDv4 assigned once; distributed primary key includes `route_key` |
| `source`, `source_ref_id` | Unique with the checked source-pair routing key |
| `hazard_type`, `severity`, `area_name` | Indexed for filtering |
| `occurred_at`, `ingested_at` | Indexed, `TIMESTAMPTZ` |
| `attributes` | `JSONB` — flexible per-source fields, including `confidence_level` once PVMBG starts emitting it |
| `raw_payload` | optional full source payload, for debugging |

### `volcano_reference`

Static `volcano_id -> area_name, latitude, longitude` lookup used by the
Aggregator's PVMBG mapping. Seeded with the same `volcano_id` values PVMBG's
mock uses, so every seeded volcanic report resolves.

### `hazard_outbox`

One pending publication per newly inserted hazard, keyed by `(route_key,hazard_id)`.
`event_payload` stores the original event snapshot and `correlation_id` keeps
its trace. The dispatcher removes a row only after JetStream acknowledges it.
Published messages may be duplicated after a crash; consumers use `hazard_id`
for idempotency.

## Verifying JSONB coexistence

```sql
-- Old-shape record (no confidence_level)
SELECT attributes FROM hazard_events WHERE source = 'PVMBG' AND attributes ? 'confidence_level' = false LIMIT 1;

-- New-shape record (has confidence_level)
SELECT attributes FROM hazard_events WHERE source = 'PVMBG' AND attributes ? 'confidence_level' LIMIT 1;
```

Both rows live in the same table, same column, with no migration between them.

## Reporting lifecycle

Validated observations, replay cursors, retry schedules, notices, transitions, and lifecycle outbox work are Aggregator-owned PostgreSQL data. Hazard corrections retain the hazard ID and increment a storage revision. See [Reporting Lifecycle](../docs/Reporting%20Lifecycle.md).

`failed_ingestions` preserves original failed source-response bytes (or an explicitly marked bounded prefix for oversized input), identity/digest, reason, retry classification, timestamps and attempts. It is not a complete M2 quarantine/replay API. Both outboxes persist traceparent/tracestate separately from canonical JSON.

## M2 offline migrations

Stop every Aggregator writer, replay job and publisher first. Set `MIGRATION_DATABASE_URL` to an administrator connection; runtime `DATABASE_URL` remains a restricted credential. From the repository root:

```powershell
rtk proxy go -C aggregator run ./cmd/migrate --offline --dir ../store/migrations --target postgres --runtime-role aggregator
```

Replace `aggregator` with the existing runtime role. Empty databases and the exact checked-in M1 schema are supported; fresh initialization applies `0001_m1.sql`, then the same M2 migration used for upgrade. Existing Compose bootstrap volumes already contain the M1 baseline and still require this command. `init.sql` is preserved as the historical M1 bootstrap, not an M2-only fresh-volume shortcut.

The administrator command holds a migration lock, validates checksums and baseline structure, preserves existing work, and commits the chain transactionally. A failed attempt rolls back: retain logs, correct the cause, rerun while services remain stopped. A recognized applied migration is skipped; modified checksums, partial unjournaled canonical schemas, unknown history and target downgrade fail. Start services only after success. Back up persistent data before an operational upgrade; no down migration or automatic data deletion is supplied.

`--target citus` additionally converts the canonical tables to the **experimental** four-shard single-copy candidate and removes retained coordinator heap copies. All worker databases, current authority, runtime roles/inter-node credentials and topology must already be provisioned. It is not the approved replicated deployment; Role 2 must integrate production durability, permissions and failover before activation.

`canonical_entities` retains UUID/time/revision/activity/source order, including tombstones without an active payload. `canonical_operations` enforces source-scoped operation identities; `canonical_batches` records retry outcomes. Entity-owned lifecycle events/outboxes are colocated with canonical rows; notices and source-status lifecycle remain in the reporting tables. Visibility generations, fenced attempts and commit markers are small coordinator control state requiring Role 2 durability integration. Outbox delivery checks terminal attempt completion before publishing.

See [M2 report](../docs/Role1-M2-Write-Core.md) for exact acceptance results, protocol limits and service API configuration.
