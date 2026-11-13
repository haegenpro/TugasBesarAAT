# Platform coordination contract v1

Owner: Platform, Coordination & Reliability. Integration partners: Distributed
State & Stream Processing and Reliable Ingestion & Delivery.

## Status and first handoff

The first implementation provides canonical ownership acquisition, renewal,
release and a transactional `store.Fence` adapter. It is an integration candidate,
not certification of four-node availability or another teammate's approval.
See the [executed local validation report](Platform-Coordination-Validation.md).
The subsequent [frozen-worker cleanup implementation](Platform-Recovery-Implementation.md)
is a development draft with tests deferred, not an additional passed guarantee.
Use this document with the existing [canonical adapter contract](Canonical-and-Storage-Contract.md#9-canonical-adapter-v1).

The first handoff covers **canonical delivery authority**. A subsequent
[acquisition draft](Platform-Acquisition-Implementation.md) adds private
source-wide election and fenced progress for the existing BMKG/PVMBG pollers;
its tests and deployment are deferred. Processor state/checkpoint protection
and audit ownership/expiry still need integration. Do not activate multiple
unguarded legacy pollers or infer exactly-once upstream requests from deduplication.

## Authority and deployment boundary

Use `canonical_ownership` in the Aggregator-owned PostgreSQL coordinator as the
authority for this first implementation. Its row locks serialize ownership
changes with protected writes in the same database transaction. This resolves
the existing `Fence.Protect` integration without a separate cross-store lease
check. PostgreSQL/Patroni remains responsible for one authoritative primary;
coordinator control rows require the same acknowledged-durability and promotion
protection as canonical operation/transaction-decision state.

An independent etcd lease is not used by this implementation. Patroni's DCS and
Kubernetes's datastore are separate infrastructure decisions; Kubernetes's
internal datastore is not an application API. Replacing the authority later
requires preserving these commit-boundary semantics and validating the new
cross-store protocol.

Only the Aggregator has canonical database credentials. Clients call its private
HTTP interface with `CANONICAL_SERVICE_TOKEN`; audit, admin and source credentials
do not grant access. This is a trusted service credential, not a per-human or
per-worker identity system. Any service holding it is trusted to participate in
ownership and must protect it accordingly. Do not expose these routes publicly.

## Scope and token rules

The offline migration provisions exactly four source-wide scopes:

| Scope | Permitted canonical mutation source |
| --- | --- |
| `canonical:BMKG` | `BMKG` |
| `canonical:PVMBG` | `PVMBG` |
| `canonical:SATELIT` | `SATELIT` |
| `canonical:SENSOR_PANTAI` | `SENSOR_PANTAI` |

A scope does not grant cross-source mutation authority. Partitioned processor,
acquisition, field-report and snapshot scopes are not implicitly authorized by
these rows. They need explicit target policy and their own protected boundaries.
Clients cannot create scopes, choose epochs, or configure lease durations.

The token is the existing `{scope, epoch, holder}` object. `holder` must be a new
random process-incarnation identifier at every start, e.g. a UUID, not a stable
pod name. Scope/holder are nonempty UTF-8 strings of at most 256 bytes, with no
outer whitespace or NUL. The epoch is a positive int64, monotonically increasing
within one scope. Rows/epochs are retained on release, expiry and restart; they
must not be deleted/reset during normal operation. Independent scopes' epochs
are not comparable.

## HTTP interface

All routes use `POST`, `Content-Type: application/json`,
`Authorization: Bearer <canonical-service-token>`, an 8 KiB request limit, and a
two-second request deadline. JSON rejects unknown and duplicate fields and
trailing values. The service owns timing; client time/TTL fields are invalid.

`POST /internal/ownership/acquire`:

```json
{"scope":"canonical:BMKG","holder":"worker-incarnation-uuid"}
```

Successful response, including when retrying a live acquisition:

```json
{
  "ownership":{"scope":"canonical:BMKG","epoch":1,"holder":"worker-incarnation-uuid"},
  "expires_at":"2026-11-13T08:00:15Z",
  "renew_after_ms":5000
}
```

An acquisition retry for the same live holder returns the existing epoch and
expiry; it does not renew. Another live holder gets `409 ownership_conflict`.
After release or expiry the next acquisition increments the epoch, including if
the same incarnation reacquires. Unknown scopes return `400 invalid_scope`.

`POST /internal/ownership/renew` takes the exact token:

```json
{"scope":"canonical:BMKG","epoch":1,"holder":"worker-incarnation-uuid"}
```

It returns a lease with the same token and a refreshed expiry only while that
token is current and live. Expired/replaced tokens get `403 stale_owner`.

`POST /internal/ownership/release` takes the same token and returns
`200 {"outcome":"released"}`. Repeating a release is harmless before a newer
epoch is issued. An old epoch cannot release a successor's authority.

Missing/wrong credentials return 401/403. Database/coordination failure, lock
timeout, or an unknown commit outcome returns `503 temporarily_unavailable`.
Errors use the existing `{error:{code,message}}` envelope without database
details. A timeout does not prove that acquisition/release rolled back: retry
with the original holder/token, never a newly invented acquisition identity.

## Timing and protected commits

The initial configuration is a 15-second TTL, renew advice at TTL/3, and a
two-second operation budget. Enabled TTL must be 6s..1m. These are starting
budgets, not measured VM takeover guarantees. The worker schedules renewal with
relative/monotonic timers and stops starting protected work after renewal
failure; the database still rejects stale work independently.

The authoritative coordinator samples `clock_timestamp()` **after** obtaining
the ownership row lock. Client wall clocks do not establish expiry. Database
clock changes may affect lease duration and availability; epoch/row-lock
serialization must continue to prevent old/new owners from committing across a
takeover. Authority-host clock shifts, frozen database primaries and HA promotion
are integration tests still required before claiming the assignment is met.

`Authority.Protect(ctx, tx, token, mutations)`:

1. Installs local statement/lock/idle-transaction timeouts and enables
   `synchronous_commit=on`. This setting alone is not a replication proof.
2. Takes a `FOR SHARE` ownership row lock in the **mutation transaction**.
3. Checks the current epoch, holder, fresh database time and every target source.
4. Registers a deferred ownership check in that same transaction.
5. Keeps the lock through commit/rollback. Acquisition, renewal and release use
   conflicting row locks and cannot overtake the transaction.
6. At COMMIT, the deferred constraint trigger rechecks expiry/current authority.
   It rejects expired work using SQLSTATE `P0F01`; canonical writes map this to
   `403 stale_owner`. Data, operation records and publication intents roll back
   together. Successful checks remove their own transient bookkeeping rows.

This explicitly covers an owner that keeps its transaction active past expiry,
even if no successor has acquired yet. An idle/frozen client loses its backend
through the database idle-transaction timeout. Worker-side cancellation is not
the sole cleanup mechanism. Multi-group prepared transactions still require
distributed recovery and the separate production durability adapter.

The owning runtime is trusted to use this adapter on every relevant transaction,
retain deferred checks through commit, and not weaken timeouts or authority.
The first implementation does not retrofit authority onto every legacy write,
replay or polling path. Runtime/admin database access is not delegated to API
callers. A malicious database owner can bypass application protocols and is
outside this service-to-service contract.

## Takeover and accepted work

Example: A holds epoch 12, then stops renewing. Once its lease expires and any
protected transaction is resolved, B acquires epoch 13. A's resumed request with
epoch 12 returns `stale_owner`. B delivers pending immutable output intents with
its current token and the original batch/mutation identities. Committed batch
retries still return their recorded results before current ownership checks, as
required by Canonical Adapter v1. An expired acquisition token is provenance;
it is not permission to delete accepted input.

## Enablement and verification

Apply the existing offline migration command, which now includes additive
`0004_ownership.sql`. Previously applied migration files/checksums are unchanged.

```sh
go -C aggregator run ./cmd/migrate --offline --dir ../store/migrations --target postgres --runtime-role aggregator_app
```

Set administrator `MIGRATION_DATABASE_URL` privately for this command. Use
`--target citus` only on the provisioned/validated Citus formation. Then enable
the private interfaces in the Aggregator configuration:

```dotenv
CANONICAL_ADAPTER_ENABLED=true
CANONICAL_SERVICE_TOKEN=<dedicated-service-secret>
CANONICAL_OWNERSHIP_ENABLED=true
OWNERSHIP_LEASE_TTL=15s
```

Startup wires the authority into the HTTP routes and `Store.Canonical.Fence`.
These settings alone **do not enable canonical batch success**: a durability
adapter is still required. The new [replicated storage draft](../deployment/storage/README.md)
installs one when `CANONICAL_DURABILITY_CONFIG` names a captured formation and
its group DSN secrets are supplied. It also confirms authority operations and
outbox publication. This draft and its verification scripts are implemented but
unexecuted; HA testing is deferred and does not block coding. Multi-entity Citus
validation remains a separate protocol milestone.

Run the isolated handoff proof (requires Go and Docker access):

```sh
python3 scripts/test-coordination.py
python3 scripts/test-coordination.py --race
```

The runner uses a generated loopback-only PostgreSQL container and credentials;
it never reads application env files. It removes only its labelled container and
temporary data. Redacted logs, image identity, runtime version and results live
under ignored `evidence/platform-ownership/<run-id>/`. Test-only single-copy
durability is used to exercise the actual canonical HTTP/store path. These
results do not establish synchronous replication, VM freeze recovery, upstream
single acquisition, full audit fencing or the 10,000-VU requirement.

References: [PostgreSQL row locks](https://www.postgresql.org/docs/16/explicit-locking.html),
[deferred triggers](https://www.postgresql.org/docs/16/sql-createtrigger.html),
[database statement/idle timeouts](https://www.postgresql.org/docs/16/runtime-config-client.html).
