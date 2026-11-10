# Audit, Analytics, and Validation

Lead: Distributed State & Stream Processing. Collaborators: Platform, Coordination & Reliability for availability/fault scenarios and Reliable Ingestion & Delivery for `/unprocessed`, input progress, and replay evidence.

Selected design: immutable materialized snapshots for explicit audit sessions only, a short write barrier to pin read views, copying after writes resume, globally ordered pause-and-retry session pagination, shared session listing/checksum, sliding retention with a hard maximum, live authoritative-primary ordinary reads, and on-request canonical analytics.

See [Decisions and Readiness](Decisions-and-Readiness.md) for authority, the decision record, and final four-node/three-copy topology with two-copy acknowledgement.

## 1. Prescribed Audit Contract

Audit endpoints are accessed through Aggregator with the prescribed bearer token. Use `Content-Type: application/json`, UTC timestamps, and the assignment's exact field names. Secrets belong in Kubernetes Secrets, not documentation examples containing real credentials.

| Endpoint | Parameters | Required behavior |
| --- | --- | --- |
| `GET /internal/audit/events` | `source`, `since`, `limit`, `cursor` | All canonical fields; `ingested_at >= since`; order by `ingested_at ASC`, then `hazard_id ASC`. |
| `GET /internal/audit/checksum` | `source` | Record count and exact prescribed checksum of `source_ref_id` values. |
| `GET /internal/audit/location` | Required `hazard_id` | Actual Kubernetes node names, unit names, and primary/replica roles; 404 only for established absence. |
| `GET /internal/audit/unprocessed` | `source`, `limit`, `cursor` | Reliable Ingestion & Delivery owns this endpoint; status is retrying/parked and recovered messages disappear. |

For paginated endpoints, default limit is 100, maximum 1,000. Omitted `source` means all sources. `cursor` is opaque; `next_cursor` is null only at established exhaustion. Apply the exact error envelope from Appendix A for its specified HTTP errors. An invalid/expired cursor is an explicit error, never a silent restart on current data.

The M1 client query's descending `occurred_at` ordering and exclusive occurrence-time filter are not the audit contract. Implement a separate query path while preserving legitimate existing client behavior.

The canonical event wire shape remains unchanged. Lifecycle/tombstone metadata stays internal unless a permitted response extension is explicitly reviewed. Whether inactive historical fire identities belong in the evaluator's audit population is still unresolved; keep that inclusion policy explicit and common to events, count, and checksum.

## 2. Immutable Materialized Snapshot

A snapshot is an immutable, distributed copy of the selected canonical state at one coordinated committed boundary. Include only canonically applied state, not prepared-but-undelivered processor output. State how other sources participate in that boundary; a satellite offset alone cannot define a snapshot covering BMKG, PVMBG, and coastal writes.

### Explicit sessions and live requests

Only an authorized explicit audit-session request initiates coordinated capture. Ordinary client reads and audit requests without a session reference do not silently invoke a write barrier; they remain live. An opt-in interface is a proposed extension whose exact shape and evaluator compatibility need review. Preserve every prescribed endpoint and default request shape.

Live pagination retains deterministic ordering and safe missing-unit handling, but pages can reflect different canonical states. A non-session listing and checksum reconcile exactly only at a stable processing point. Do not label a live request as an immutable snapshot or imply that its timestamp identifies a global cut.

### Snapshot lifecycle: selected short-barrier protocol

1. Allocate a capture identity and expected scope/units; state is building, not yet a published snapshot.
2. Acquire the shared fenced canonical-write admission gate. All sources, canonical output batches, replay, and bulk loaders must honor it. Existing readers continue; inputs may remain durably queued.
3. Drain/resolve previously admitted mutations and distributed transaction decisions. Unknown outcomes or unavailable participants prevent claiming a safe cut; abort within a bounded deadline rather than waiting indefinitely.
4. While mutation admission is closed, establish and actually pin a consistent read view on every participating authoritative primary. Record stable shard ownership, scope, and capture metadata. Independently timed views are comparable here only because no canonical mutations can commit during their establishment.
5. Once all required views are confirmed, release the write gate immediately. Canonical commits resume before the full copy finishes.
6. Materialize immutable records from those retained views into distributed replicated snapshot storage. Snapshot-copy writes are separate from canonical mutations. Persist copy progress without pretending a lost read view can be reconstructed from that progress alone.
7. After all required units and their durable metadata are complete, publish the snapshot manifest. Serve listing/checksum from frozen records until expiry.
8. Release retained transactions/views in success and failure paths. Renew published-session retention on use within the absolute cap; clean up expired/aborted capture data idempotently.

This is a selected application-level capture strategy, **not a proven Citus feature**. Verify transaction/snapshot handles, connection routing, shard access, distributed transaction recovery, and failover behavior with pinned versions. BEGIN alone may not pin a lazy read view; explicitly establish it before releasing the gate. A coordinator transaction without this protocol is not sufficient evidence of a common cut.

Long-lived read views can retain row versions and database connections. Bound concurrent captures, total copy duration, and resource usage. Abort if acquisition/drain/view establishment exceeds the permitted write-pause budget. Platform, Coordination & Reliability must provide owner-fenced gate cleanup so an audit coordinator crash cannot leave canonical writes blocked indefinitely. Never keep the gate closed merely because background copying is slow.

### Failure before publication versus failure after publication

If any required retained read view is lost before materialization completes, **abort that unfinished capture explicitly**. Primary failover, required shard movement, expired transaction handles, or audit-process failure can invalidate views. Do not fill the missing unit from current data, promote a replica and assume its new view is identical, or silently retry under the same snapshot identity. A new capture uses a new identity and requires a new coordinated boundary.

After publication, loss of a storage unit is a different problem: frozen snapshot records already exist. An eligible replica can serve them; otherwise the existing partial-response/pause-and-retry policy applies. Published snapshots are not recaptured at a newer boundary because of availability loss.

Materialization does not authorize storing all snapshot rows on the coordinator, in an audit-service cache, or in a single exported file on one node. Temporary sorting/copy staging must respect the same data-distribution constraint. Snapshot manifests and small progress summaries may be replicated separately; measure their size.

Snapshot rows are frozen, but metadata about unavailable units is request-time availability. A frozen snapshot can still be temporarily unreadable. A partially built snapshot cannot be labeled globally complete. If the cut cannot be established or a required retained view is lost, abort the unfinished capture with an explicit bounded result according to a compatible session API design; never silently publish a later state as that unit's contribution.

### Optional extension for shared checksum/listing

Within explicit audit sessions, the user selected a shared snapshot reference. A proposed backwards-compatible extension returns a snapshot reference in a response header and accepts it as an optional request parameter/header for checksum and subsequent audit operations. Exact naming is not prescribed here. Required fields and existing valid requests must remain supported. Verify that the evaluator tolerates optional extensions before adopting them.

The cursor internally binds snapshot identity, scope/filters, ordering position, and format version. Opaque does not mean trusted: validate it, bind it to authorized scope, and reject tampering or changed filters. Exact encoding and signing choices are implementation details with interoperability/security rationale, not design aesthetics.

### Retention

Maintain last-use expiry and an immutable absolute expiry. Successful authorized access can extend last-use expiry only to the absolute cap. Do not renew based on arbitrary malformed/unauthorized requests. Choose timing/expiry authority with Platform, Coordination & Reliability so node clock-skew tests do not create conflicting lifetimes. Numeric idle and maximum lifetimes must exceed measured normal traversal/recovery needs within the storage budget; none is invented here.

Expired snapshots return an explicit restart-required error using a permitted error envelope. Never translate expiry into `next_cursor: null` or invisibly recreate the snapshot. Garbage collection must not remove rows still protected by a valid lease, and replicas must agree on expiry state.

## 3. Global Ordering and Partial Pagination

For published audit sessions, read one logical copy of each frozen partition from an authoritative primary or a verified snapshot-equivalent replica during failover. This replica fallback does not recreate a lost in-progress capture view. Do not double-count physical copies. The following session rules also define safe ordering boundaries for live reads, but live requests do not gain cross-page immutability.

Order globally by `(ingested_at, hazard_id)` ascending. Persist sufficient per-unit ordering/progress metadata to merge records safely. Preserve database precision or define cursor comparisons so timestamp serialization does not skip equal-looking values. UUID string comparison must have one documented consistent ordering across services.

When a required logical unit is unavailable:

- Attempt an eligible copy serving the same immutable snapshot.
- Use a bounded request deadline.
- Return only the globally ordered prefix that can be proven safe.
- Set `partial: true` and list unavailable units using the agreed audit naming.
- Preserve a continuation cursor at the last emitted position; retry may return no new records while the boundary is blocked.
- Do not advance past unseen records or claim exhaustion.

Without a trustworthy lower bound for an unavailable unit's next key, the safe prefix may be empty. Metadata can permit progress only when it proves that hidden keys cannot precede emitted records. A partition timeout alone is not such proof.

**Derived conservative default:** `partial` means required logical data is missing, rather than merely that an extra replica is down. Appendix A also asks for names of unreachable storage units. Agree whether names represent logical partitions or physical replica units and verify the evaluator's interpretation; do not hide physical failures in metrics or equate a healthy process with current data.

If all required units are unavailable, return a bounded partial result under the data endpoint contract rather than fabricating a complete empty dataset. Distinguish validation/authentication failures from storage incompleteness.

## 4. Exact Checksum

For the endpoint's source scope and canonical inclusion policy:

1. Read each logical HazardEvent once from the selected snapshot.
2. Extract its `source_ref_id`.
3. Sort strings by byte order, not locale-dependent database collation.
4. Join with one newline between entries and no trailing newline.
5. Compute SHA-256 of the resulting bytes.
6. Return 64 lowercase hexadecimal characters and `algorithm: "sha256"`.

Count logical records, not distinct identifier strings across all sources: different sources may legitimately have the same `source_ref_id`. Empty input hashes the empty byte sequence. Use a verified UTF-8 encoding convention and reject identifier characters that would make the prescribed serialization ambiguous only if the source contract permits that validation; do not silently rewrite source identifiers.

The assignment specifies source filtering, but not `since` on checksum. Compare a checksum with an unfiltered listing of the same source/snapshot; a `since`-filtered listing is not the same population.

For a session checksum, `as_of` describes the snapshot capture boundary, not when computation finishes. For a live checksum, document its observation semantics without claiming a common retained snapshot; compare at a stable processing point. Reusing a recorded valid full checksum is possible when rows are frozen. However storage incompleteness must still be marked as prescribed; do not present a reachable-subset digest as the full snapshot's checksum. State which population a partial count/digest describes.

## 5. Location Reporting

Resolve the canonical identity to actual logical shard placement and current physical replicas. Return Kubernetes node names as shown by `kubectl get nodes`, not Pod names or database aliases. Role is current primary/replica, not the initial placement role in a static README.

Include enough metadata to distinguish a rebuilding replica from a complete copy; adding a location must not imply full durability before catch-up. Exact optional fields require contract review. A metadata outage is not proof that the HazardEvent does not exist; report incompleteness rather than a false 404.

Location is current physical placement, whereas event listing/checksum can describe a historical snapshot. Do not imply that old snapshot placement is current canonical placement.

## 6. Analytics and Ordinary Reads

Ordinary reads use current authoritative primaries. The dashboard aggregates canonical rows across partitions on request; it is separate from the operational metrics dashboard.

Required analytics:

- Counts over time per hazard type, with selectable time range.
- Counts by severity.
- Counts by `area_name`.

Use exact counts, common filters, and the same active/inactive membership rule as the corresponding canonical view. Preserve M1's treatment of tsunami-related data until its mapping and the assignment's requested display categories are reconciled; do not invent a new enum just to label a chart.

**Proposed default:** event-time (`occurred_at`) buckets in documented UTC intervals, clearly distinguished from audit's `ingested_at` filter. Use half-open bucket boundaries to avoid double-counting endpoints. The concrete interval set is a UI/configuration detail, not fixed in this contract.

Dashboard figures match audit when computed from the same snapshot or a proven stable processing point. On-request live queries during concurrent updates need not equal an earlier audit snapshot; display their data boundary rather than claiming they do. Exact cross-panel consistency requires a common read boundary as well.

During unavailable partitions, show available aggregates with a visible incomplete indicator and unavailable-unit context. Do not fill missing partitions with zero or add primary and replica totals. Apply bounded queries and resource/concurrency limits so analytics cannot starve critical ingestion or notifications. Add caching or derived counters only if measurements justify revisiting decision 25.

## 7. Remaining Validation Gates

| Gate | Why it is necessary | Lead and contributors |
| --- | --- | --- |
| Four-node single-loss availability | Demonstrate canonical writes resume after bounded failover on the two surviving copies before replica rebuilding completes; coordinator and DCS must remain usable. | Distributed State & Stream Processing + Platform, Coordination & Reliability + Reliable Ingestion & Delivery |
| Citus placement and resource fit | Twelve worker instances plus coordinator replicas and other services must fit; no node may hold the whole dataset, including transient files. | Platform, Coordination & Reliability + Distributed State & Stream Processing |
| Safe synchronous failover | Strict two-copy acknowledgement, correct promotion, and ambiguous outcomes must survive faults. | Distributed State & Stream Processing + Platform, Coordination & Reliability |
| Processor ownership/fencing | Database HA alone does not fence stale application workers. | Platform, Coordination & Reliability + Distributed State & Stream Processing + Reliable Ingestion & Delivery |
| Short-barrier session capture | Every canonical writer honors the gate; read views are pinned before release; copying continues after commits resume. | Distributed State & Stream Processing + Platform, Coordination & Reliability |
| Unfinished capture failure | Loss of a required view aborts capture; gate/view cleanup is bounded and fenced. Completed snapshots retain retry semantics. | Distributed State & Stream Processing + Platform, Coordination & Reliability |
| Degraded read implementation | A normal Citus distributed query may fail as a whole; design an authorized per-unit fallback within the storage-owning service. | Distributed State & Stream Processing + Platform, Coordination & Reliability |
| Snapshot API compatibility | Optional shared-snapshot extension must preserve the tester's contract. | Distributed State & Stream Processing |
| Reference identity semantics | Validate stream-history-derived IDs and ambiguous historical restoration against reference expectations. | Distributed State & Stream Processing + Reliable Ingestion & Delivery |
| Geography/reference conventions | Check distance model, center formula, province data, and ties. | Distributed State & Stream Processing |
| Inactive audit membership | Assignment does not explicitly settle tombstone inclusion in the uploaded audit examples. | Distributed State & Stream Processing |
| Feed-clock and operation behavior | Confirm namespace, reconnect, confidence, updates/retractions, `virtual_now`, and recorded replay semantics. | Reliable Ingestion & Delivery + Distributed State & Stream Processing |
| Measured tuning | Select lateness, snapshot lifetimes, deadlines, partition count, batch limits, and memory budgets from evidence. | Each owner, coordinated by Platform, Coordination & Reliability |

Do not describe these gates as completed or change selected policies silently when a gate fails. Record the evidence and reopen only the affected decision.

## 8. Acceptance Evidence Checklist

- [ ] Four nonempty worker groups have three copies each across four separate-kernel nodes; no node stores the complete dataset.
- [ ] Distribution and write load are measured during historical load and regional skew.
- [ ] Duplicate source writes, stale revisions, stale owners, and timeout retries preserve identity/state correctly.
- [ ] Worker and disk loss preserves every acknowledged HazardEvent.
- [ ] Canonical commits resume after one-node loss on two surviving copies before replica rebuilding finishes; failover pause and lag are measured.
- [ ] If two eligible durable copies cannot be reached, input remains pending rather than acknowledging one-copy success.
- [ ] An empty replacement node joins, replicas rebuild, load redistributes, and three-copy placement is restored.
- [ ] A subsequent supported node loss can again be survived after repair.
- [ ] Events paginate in exact audit order with inclusive ingestion-time filtering and correct page boundaries.
- [ ] Only explicit audit sessions trigger capture; ordinary reads do not acquire a capture write barrier.
- [ ] All canonical mutation paths honor the gate; in-flight/ambiguous transaction decisions resolve before views are pinned.
- [ ] Writes resume after all required read views are established and before the full snapshot copy completes.
- [ ] Gate acquisition/drain failure and audit-owner crash release admission safely within the measured deadline.
- [ ] Loss of a required read view aborts an unfinished capture; it never substitutes newer rows.
- [ ] Concurrent creates/corrections/retractions do not mutate a completed materialized snapshot.
- [ ] Unavailable snapshot units pause safe ordered continuation; recovery resumes without missing or duplicated rows.
- [ ] Event listing, record count, and checksum reconcile for the same snapshot and source scope.
- [ ] Byte ordering, no trailing newline, empty dataset, duplicate IDs across sources, and maximum page size have fixtures.
- [ ] Snapshot renewal, hard expiry, failover, cleanup, invalid cursors, and clock changes behave explicitly.
- [ ] Dashboard time/severity/area counts match direct audit calculation at the same data boundary.
- [ ] Dashboard remains visible with clear incompleteness during storage failure.
- [ ] Evidence in [Wildfire Processing Contract](Wildfire-Processing-Contract.md#8-required-evidence) passes, including restart, live reconnect, 60x playback, and late correction.
- [ ] Traces and correlation IDs link input receipt, preparation, canonical application, and publication.
- [ ] Report distinguishes selected design, demonstrated guarantee, observed limitation, and unresolved evaluator interpretation.

## 9. Recommended Implementation Sequence

1. Review cross-component contracts and resolve readiness gates affecting schema or deployment.
2. Pin versions and prove the four-node/three-copy Citus/Patroni layout and two-copy acknowledgement with a small distributed dataset.
3. Migrate source-key constraints, idempotent batch application, outboxes, and fencing checks.
4. Deliver a minimal source-to-canonical-to-read path before adding the full wildfire algorithm.
5. Implement wildfire state, lineage fixtures, progress recovery, and automatic late corrections.
6. Prove explicit-session short-barrier capture and abort/cleanup behavior, then add published listing/checksum continuation and expiry.
7. Add exact on-request analytics, run integrated failure tests, and select measured configuration values.

This sequence is implementation guidance, not evidence of completed work. Only documentation consistency and source review were performed while generating this document set; no runtime, load, or fault tests have been executed.
