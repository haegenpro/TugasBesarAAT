# Processor and audit coordination handoff

Status: implemented development draft, unbuilt and unexecuted. Platform provides
these primitives; Distributed State & Stream Processing owns processor records,
snapshot pinning/copying, unit-completeness proof and prescribed audit endpoints.

Apply offline migration 0007 after stopping old writers/publishers. It adds a
`processor:SATELIT` lease and small audit gate/capture metadata. No graph, full
session history or frozen snapshot rows are stored in these control tables.

Enable `PLATFORM_COORDINATION_ENABLED=true` only with replicated durability and
a dedicated `PROCESSOR_SERVICE_TOKEN` distinct from source/canonical/admin/audit
credentials. Processor leases use POST
`/internal/processor/ownership/{acquire,renew,release}` with the existing ownership
JSON contract, fixed 15-second TTL and `processor:SATELIT` scope. Canonical and
acquisition authorities reject processor tokens and vice versa.

Before updating graph/lineage/prepared progress/output intents, call
`Store.ProtectProcessorState(ctx, tx, owner)` inside the owning coordinator-origin
transaction. The helper enforces synchronous configuration and holds epoch and
expiry protection through COMMIT. The storage owner still supplies distributed
records, original idempotency identities, ambiguous-outcome reconciliation and
confirmation of its committed records. This helper alone does not acknowledge
prepared progress, implement a wildfire processor or turn a token preflight into
a cross-database fence. Preparation does not acquire the canonical audit gate.

Every existing admitted canonical mutation now checks `platform_audit_gate`
under a SHARE row lock through attempt registration. Closing takes UPDATE, so
the drain set includes every previously admitted writer. An expired gate opens
logically using database time without requiring its old process to resume.
Existing session/advisory locks and marker-based decision recovery are retained.

The storage owner calls `CaptureBoundary(ctx, holder, source, pin)` for an
explicit authorized snapshot only. It admits one live capture, closes canonical
admission, drains admitted/ambiguous work and prepared decisions, then invokes
the provided real view-pinning callback. The callback must actually pin every
required authoritative view and return their cleanup function. BEGIN alone is
insufficient. Topology and the exact gate epoch/holder/capture ID are rechecked
before confirming the cut and opening admission. The whole boundary has a
two-second context and a two-second database gate expiry. Failure aborts the
unfinished capture and cleans returned views; stale cleanup cannot open a
successor's gate. The caller retains and eventually closes successful views.

During copy, renew the capture owner at intervals below 15 seconds and use
`ProtectCaptureCopy` in each copy/progress transaction. Owner expiry is capped by
the immutable five-minute copy deadline. The existing default application pool
has idle transaction expiry; retained read views need a separately bounded pool
and explicit pinning configuration from the storage owner. Losing a required
view must abort, never substitute current data under the old snapshot ID.

`PublishCapture` requires a nonnil storage-owner completeness callback inside the
same fenced transaction. That callback must prove all required immutable units
and metadata complete, replicated and view-valid. Platform does not invent that
proof. Deferred database checks reject late publication at COMMIT. Unknown
commit results require reading the original capture identity, not recapture.
The metadata transition and WAL confirmation do not implement snapshot listing,
checksum, location, partial reads or cursor signing.

Only after authorized and filter-valid session access may the audit layer call
`TouchSnapshot`. Use `ProtectSnapshotRead` in the transaction reading frozen rows
and commit before emitting a page/checksum: it excludes GC and rejects completion
after expiry. Ready metadata has 15-minute idle retention capped by the
one-hour absolute lifetime. `ExpireCaptures` marks dead builds and expired ready
sessions and returns their exact IDs; storage deletes frozen rows by those IDs
idempotently, then acknowledges `MarkCaptureCleaned`. Terminal IDs are returned
again until that acknowledgement, allowing cleanup to recover after a crash.
Aborted/expired identities are never reused or resurrected.

Deferred checks: registration-versus-close race, timeout while draining a frozen
writer, process freeze during pin/copy/publication, late old-owner cleanup after
a successor closes the gate, renewal versus new capture, COMMIT expiry, lost
views/topology changes, correct callback completeness/replication proof, sliding
retention/absolute cap and source/token isolation. These APIs are integration
primitives, not completed audit acceptance or evidence of a two-second pause SLA.
