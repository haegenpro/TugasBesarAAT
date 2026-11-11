# Canonical and Storage Contract

Lead: Distributed State & Stream Processing. Collaborators: Platform, Coordination & Reliability for placement/coordination and Reliable Ingestion & Delivery for ingestion/replay.

This contract records the selected canonical storage design. The [M2 assignment](IF4031%20-%20Tugas%20Besar%20-%20M2.md) is authoritative; proposed implementation details and unverified guarantees are identified below. Deployment and evaluator compatibility remain to be demonstrated.

## 1. Preserve the Existing Canonical Shape

| Field | Type and meaning | Rule |
| --- | --- | --- |
| `hazard_id` | UUIDv4 string | Generate once for a new canonical identity; persist and reuse on correction/restoration. |
| `source` | Source enum string | Preserve BMKG/PVMBG and add SATELIT/SENSOR_PANTAI as required by M2. |
| `source_ref_id` | Source entity identifier | Canonical deduplication scope is the pair `(source, source_ref_id)`. |
| `hazard_type` | Hazard enum string | Preserve existing mappings; add WILDFIRE/COASTAL. Do not invent another tsunami representation without reviewing M1. |
| `severity` | NORMAL/WASPADA/SIAGA/AWAS | Apply prescribed source-specific mappings. |
| `area_name` | String | Use the agreed source mapping and static reference data; do not fabricate missing geography. |
| `latitude` | Finite JSON number | Validate range [-90, 90]. |
| `longitude` | Finite JSON number | Validate range [-180, 180]. |
| `occurred_at` | UTC timestamp | Event time, not receipt time; earliest included detection for wildfire. |
| `ingested_at` | UTC timestamp | Preserve first canonical ingestion value through normal corrections and retries, following existing store behavior. |
| `attributes` | Typed source-specific JSON object | Allow required arrays and documented structured fields. |

Retain `snake_case`, uppercase existing enum strings, and UTC `Z` timestamp serialization because these are existing interoperability contracts, not a new aesthetic preference. Retaining millisecond output precision is a compatibility default; internal comparisons must not silently lose precision needed for cursor ordering.

The [wire-format specification](HazardEvent%20Representation.md#m2-field-constraints) includes M2 enum and structured-attribute extensions. M2 requires arrays such as `satellites` and `detection_keys`. Keep revision, tombstone, ownership, lineage, and freshness metadata in internal tables or existing lifecycle envelopes unless the public contract explicitly permits additions.

Raw observation payloads are distinct from canonical attributes. A parsed JSON object or reserialized floating-point value is not a lossless substitute for original bytes or identity components.

## 2. Identity, Revisions, and Tombstones

| Input condition | State change | Identity/revision outcome |
| --- | --- | --- |
| First accepted source entity | Insert canonical row and publication intent | Allocate one UUIDv4; initial revision. |
| Identical retry | No new logical change | Return existing identity; no revision increment. |
| Newer material update | Update mutable fields and publication intent atomically | Preserve UUID/source identity; increment canonical revision. |
| Older source operation | Do not overwrite current state | Explicit stale/no-change outcome. |
| Retraction or nonqualifying fire | Retain internal tombstone/lineage | Remove from active view; retain identity and ordering history. |
| Exact historical restoration | Reactivate the historical identity | Reuse original UUID; publish a new lifecycle revision. |

Canonical revision is not the same as source offset, source revision, or polling ownership token. Document their separate meanings. Equality checks compare typed canonical content, not arbitrary JSON property order. Repeated input may update observation metadata without incrementing the logical event revision.

Inactive rows are excluded from current client/analytics results and from audit listing/count/checksum. Tombstones, revisions, and lineage remain internal. This is the selected project population rule; if evaluator evidence requires a different population, revise all three audit operations together.

UUIDv4 is an opaque canonical identity choice, not a deduplication mechanism. Database constraints, stable routing, revision checks, and retry identities remain necessary.

## 3. Distribution and Constraints

Selected routing principle: hash of the composite source identity, stable logical partitions, explicit placement metadata.

Under Citus, use its hash distribution and shard placement catalogs as the authority. Do not maintain a competing application hash ring. Coordinate updates leave `(source, source_ref_id)` unchanged, so they leave routing unchanged.

**Distribution-key encoding v1:** UTF-8 bytes of the compact JSON array `[source,source_ref_id]`, without insignificant whitespace; use standard JSON string escaping, preserve Unicode code points without normalization, and emit non-ASCII characters as UTF-8. Distribute its text value with Citus and retain the exact original pair. This unambiguous encoding separates source and identifier even when either contains delimiters; hash collisions never replace exact pair comparison.

Citus distributed uniqueness constraints generally need the distribution column included. Therefore the M1 UUID-only primary key, `(source, source_ref_id)` unique index, outbox foreign keys, and related lifecycle constraints cannot simply be copied unchanged. Design and test constraints including the routing key, with a consistency check binding it to the source pair. Hash collisions must never cause two distinct source identities to be treated as one entity.

Lookup by `hazard_id` must still work without a caller knowing the source pair. Use a correct distributed lookup or a durable mapping; do not assume a single-shard UUID index is global. The concrete index strategy remains an implementation detail to benchmark.

## 4. Selected Durability and Failover Semantics

- Three copies of each worker group's shards on three distinct Kubernetes nodes.
- A canonical write succeeds only after each participating group's primary and at least one eligible standby have durably flushed the write.
- Two of the three copies are required for acknowledgement. The third may catch up later; report replica lag and groups unable to meet the two-copy condition.
- When this acknowledgement condition cannot be established, do not return canonical success. Input stays durably pending at the ingestion/output stage and retries automatically.
- For multi-group correction batches, replication and transaction completion requirements apply to every participant and any durable transaction-decision state. One group's successful commit is not success for the whole batch.
- Ambiguous client timeout means unknown outcome, not guaranteed rollback. Retry the same operation identity and inspect its committed outcome.

Patroni manages database leadership and Citus worker/coordinator metadata integration. A configuration candidate consistent with the selected two-of-three acknowledgement policy is quorum synchronous mode, one synchronous standby acknowledgement, strict synchronous mode, and PostgreSQL durable synchronous commit. This is configuration guidance to validate against pinned versions, not a ready-to-apply manifest. Patroni defaults must not be assumed safe: ordinary synchronous mode can permit unreplicated writes when suitable standbys disappear unless strict behavior is enabled.

Prevent session-level weakening of durability. Promotion must preserve acknowledged WAL and respect Patroni's coordinated eligibility rules. Do not promote the most reachable replica merely because its process is healthy. Do not bypass the manager with manual promotion during automated failure recovery.

Database primary ownership and Aggregator/processor ownership are distinct. Database HA does not reject a stale polling worker that still has legitimate database connectivity. Platform, Coordination & Reliability supplies the application ownership/fencing mechanism; Distributed State & Stream Processing validates its current token in the same serialized boundary as protected state/output preparation. Token propagation, already-accepted queued work, and takeover semantics need cross-component agreement. A separate preflight token check followed by an unprotected write is insufficient.

## 5. Placement on Four Kubernetes Nodes

The selected topology uses four nodes, three copies, and two-copy durable acknowledgement. The following is a derived minimum-size example. `P` means initial primary, `R` standby, and `absent` no copy. Failover may change primary roles without changing copy placement.

| Worker group | Node A | Node B | Node C | Node D |
| --- | --- | --- | --- | --- |
| G1 | absent | P | R | R |
| G2 | R | absent | P | R |
| G3 | R | R | absent | P |
| G4 | P | R | R | absent |

All four groups must own nonempty portions of the relevant dataset. Each node lacks one group; no node stores every canonical record. Balance group sizes and primary load. More logical shards may exist within each group; shard count is not replica count or worker-group count.

This layout needs 12 worker database instances: 6 GiB total requests and 12 GiB combined limits under M2's 512 MiB request/1 GiB limit per storage instance. A proposed three-member coordinator group adds 1.5 GiB requests and 3 GiB limits. Provision other services and node overhead separately. Physical-host colocation still creates correlated-failure exposure even when nodes have separate kernels.

Control-plane topology, DCS backend, coordinator replica count, CPU allocation, and version pins are shared deployment work with Platform, Coordination & Reliability. The table does not settle them. A four-node Kubernetes cluster does not itself guarantee an available control plane or coordination store. To meet the selected single-node-loss write objective, the coordinator and DCS must also survive that loss with safe leadership and a usable durability path. A single control-plane/DCS endpoint must not be assumed sufficient.

No-full-dataset placement applies to stored replicas, materialized snapshots, backups, history, and processor state as well as primary canonical rows. Avoid a processor checkpoint containing a complete canonical dataset on one node. Keep large history/observation collections distributed; retain only bounded working state in memory. Assess local WAL, snapshot staging, and retained files during migration as well as active table ownership.

## 6. Recovery and Rebalancing

1. Detect failure through the coordinated HA mechanism; stop treating the former primary as authoritative.
2. Promote only an eligible survivor, update Citus routing, and reconnect clients with bounded retries.
3. Continue two-copy writes only where the selected durability condition can still be met. Keep other work pending.
4. Rejoin or provision a fourth eligible Kubernetes node with empty local storage.
5. Rebuild replicas and redistribute workload without copying the entire dataset onto every surviving node.
6. Verify checksum, copy count, placement, and catch-up before declaring recovery complete.

With only three surviving nodes, restoring three copies of every partition would put the complete dataset on each survivor. Do not use that as the final recovered layout. Restore a fourth node first. Recovery to three-copy protection is distinct from continuing writes on the two remaining copies.

The selected objective is continued canonical-write progress after one node loss, with bounded safe-failover interruption. Validate coordinator/DCS availability, eligible promotion, synchronous standby selection, routing, and transaction recovery. Do not substitute durable input buffering for proof that canonical commits resume before replica rebuilding finishes. If fewer than two eligible copies are reachable, retain durable input pending instead of weakening acknowledgement.

Use Citus online shard movement where supported, with replica identity configured for updates/retractions. Test routing/constraint correctness during movement and transient copies on destination nodes. Do not presume the default rebalancer accounts for whole-worker replica placement on Kubernetes hosts. Its placement plan must be checked against the per-node data constraint.

## 7. Read and Write Outcomes

Logical outcomes below are requirements for adapter design, not newly prescribed public HTTP codes.

| Outcome | Caller action |
| --- | --- |
| Applied | Record canonical batch progress. |
| Duplicate/no change | Treat as successful idempotent completion. |
| Stale operation | Preserve current state; report explicit no-change result. |
| Stale owner | Stop protected processing and refresh ownership. |
| Invalid input | Preserve/quarantine via Reliable Ingestion & Delivery's agreed failure contract. |
| Temporary storage/coordination failure | Keep durable work pending; bounded retry with backoff. |
| Unknown commit outcome | Retry the same identity; resolve against committed operation state. |

Ordinary reads use authoritative primaries. During uncertain ownership or unavailability, answer promptly with explicit partial metadata where the endpoint allows it. Primary routing alone does not establish a common cross-partition snapshot or a linearizable read during a partition; state only guarantees actually demonstrated.

Only the storage-owning service uses database credentials. An authorized distributed-read component may query multiple units owned by that service; unrelated services must not gain direct storage access. Platform isolation must permit required Citus/Patroni internals while preserving service ownership and leaving the prescribed Tester Agent unobstructed.

## References

- [Citus DDL/distribution constraints](https://docs.citusdata.com/en/stable/develop/reference_ddl.html)
- [Citus cluster management](https://docs.citusdata.com/en/stable/admin_guide/cluster_management.html)
- [Patroni Citus integration](https://patroni.readthedocs.io/en/latest/citus.html)
- [Patroni replication modes](https://patroni.readthedocs.io/en/latest/replication_modes.html)
- [PostgreSQL WAL/durable acknowledgement](https://www.postgresql.org/docs/16/runtime-config-wal.html)

Project authority: [M2 assignment](IF4031%20-%20Tugas%20Besar%20-%20M2.md). Existing identity/storage behavior is defined in [HazardEvent Representation](HazardEvent%20Representation.md), `aggregator/internal/domain/hazard.go`, `aggregator/internal/store/`, and `store/init.sql`. No deployment benchmark has been run for this document.

## 8. Consistency Boundaries and Audit Write Admission

| Operation | Intended guarantee |
| --- | --- |
| One source-entity mutation | Serialized identity/revision checks; stale operations cannot overwrite newer state. |
| Successful canonical commit | Durable on at least two copies per participating group, with safe promotion preserving acknowledgements. |
| Ordinary read | Current authoritative-primary result; no automatic cross-partition snapshot or blanket linearizability claim. |
| Merge/split correction batch | Related canonical changes and publication intents become visible as one logical batch. |
| Processor output delivery | Eventual idempotent application; prepared progress is not canonical completion. |
| Completed audit session | Immutable records from one coordinated capture boundary, listing/checksum scope matched. |

For explicit snapshot capture, every canonical mutation path must honor a shared fenced admission gate. Include all sources, output appliers, replay/admin jobs, and bulk imports. Gate entry is outside mutation transactions; drain or resolve transactions already admitted, including prepared/ambiguous distributed transactions, before declaring the cut stable. Continue durable input acceptance only where its own reliability/resource requirements hold.

Once each required primary has established and pinned its read view while no canonical mutations can commit, release the admission gate and materialize from those views. Reads during a barrier may use already committed live state; no new global-snapshot guarantee applies to ordinary reads. The gate has an acquisition/drain deadline and an owner-fenced cleanup path; failed or slow capture must not stop writes indefinitely. Abort the capture before releasing an incomplete boundary. Platform, Coordination & Reliability and Distributed State & Stream Processing must validate coordinator crash and stale-owner behavior of the gate.

This application protocol and its Citus integration remain a validation gate. Read-view establishment must actually occur before barrier release; merely issuing BEGIN does not establish a lazy MVCC snapshot. Long-lived views can retain old versions and consume connections. Measure that cost and bound capture concurrency/duration.

## 9. Canonical Adapter v1

The Aggregator/storage boundary exposes `POST /internal/canonical/batches` to authorized ingestion and processing callers. This is a private service interface, not a new client API. Only its owning service accesses canonical database credentials. JSON uses the error envelope in Appendix A. The bearer credential is a service credential; the audit token grants no write permission. An in-process implementation must preserve the same request/outcome semantics.

Required request fields:

| Field | Contract |
| --- | --- |
| `contract_version` | Integer `1`. |
| `batch_id` | Stable, caller-persisted operation identity; unchanged across retries or delivery-owner changes. |
| `ownership` | `{scope, epoch, holder}`; scope string, positive int64 epoch, holder string. Platform supplies authoritative current ownership. |
| `mutations` | Nonempty array of distinct `(source, source_ref_id)` entities. |
| Mutation `operation_id` | Stable identity for this logical mutation, scoped by source; a correction has a new identity. |
| Mutation `source`, `source_ref_id` | Canonical source and stable entity identity. |
| Mutation `operation` | `upsert` or `retract`. |
| Mutation `source_order` | `{namespace, position}`; nonnegative int64 position ordered within a stable namespace. |
| Mutation `expected_revision` | Nonnegative canonical compare-and-set value, `0` for a new identity. |
| Mutation `event` | Required for upsert: canonical mutable fields (`hazard_type`, `severity`, `area_name`, coordinates, `occurred_at`, `attributes`). Omitted for retract. |

Storage assigns/persists `hazard_id` and first `ingested_at` on creation. Neither is a mutable caller field. Ingestion assigns a durable monotonic acquisition position for polling sources; satellite positions are original feed offsets. Processor output uses a durable output sequence in its namespace. Never compare offsets from different namespaces; an explicit serialized source-session transition is required before accepting a new namespace for an existing identity. A newer source position with equal typed content advances source ordering without incrementing the canonical revision. Retractions preserve the source-order high-water mark, identity, and tombstone.

Check authorization first. Then resolve a previously committed batch/mutation identity before applying current revision checks: a legitimate retry must return its recorded outcome. Same identity with different mutation content is `409 identity_conflict`. Ownership/trace delivery metadata may change on a successor retry; logical mutation content may not. New work requires current fenced authority inside the protected commit boundary. No request may choose a weaker acknowledgement policy.

| HTTP / code | Meaning and caller behavior |
| --- | --- |
| `200`, `outcome: applied` | All mutations, operation records, and outgoing intents committed atomically under two-copy durability. Return `batch_id` and per-entity `hazard_id`, `revision`, and outcome. |
| `200`, `outcome: duplicate` | Batch already committed; return its recorded result without new effects. |
| `200`, `outcome: no_change` | No canonical content changed; ordering/operation metadata still commits. |
| `409 stale_operation` | At least one mutation precedes its entity's stored source order; none of this new batch commits. Preserve state; resolve/reconcile producer progress. |
| `409 revision_conflict` | New operation used an outdated expected canonical revision; none commits. Re-read/reconcile rather than blindly incrementing a revision. |
| `403 stale_owner` | Caller lacks current authority; stop protected work and acquire valid ownership. |
| `400 invalid_request` | Unsupported version, malformed fields, invalid coordinates, duplicate target, or invalid mutation. Preserve failed acquired input through ingestion. |
| `503 temporarily_unavailable` | Required storage, coordination, or audit-write admission unavailable by the request deadline. Retain input/output intent and retry with the same identity. |

Authentication failures use `401 unauthorized` or `403 forbidden`. An ambiguous timeout/503 can follow a committed transaction; retry the original batch identity to discover the result. A multi-record merge/split is never partly acknowledged: any stale/conflicting member rejects the new batch as a whole. Already committed output intents may be delivered by a successor with current delivery authority; an expired acquisition token is provenance, not a reason to delete accepted work.

The initial adapter request deadline is 2 seconds and inline request limit is 1 MiB. Larger corrections use the same endpoint with `mutations_ref` instead of inline `mutations`: an opaque reference to an immutable, fully prepared batch in the storage/processor-owned output-intent store, including mutation count and a SHA-256 digest. Only the owning service resolves it; no caller-provided filesystem path/URL or unrelated database credential is accepted. Both forms identify the same logical batch, follow the same validation rules, and commit all visible effects atomically. Bound the reader buffer and configure a measured total correction-size quota; if the batch cannot currently execute safely, keep it pending rather than exposing part of a split. These initial budgets require resource/load validation and are not measured performance claims.


The private authenticated reconciliation read is `GET /internal/canonical/entities?source=...&source_ref_id=...`, returning `{source,source_ref_id,hazard_id,revision,active,source_order,event}` with `event` null for an inactive identity. Established absence returns 404; uncertain storage returns 503 rather than false absence. Its credentials are the same scoped producer/recovery identity as canonical writes, not Media or audit credentials. A revision-conflict response includes the conflicting source identities in safe error details so the caller can reconcile through this read. A repeated stale operation is not rewritten with a new identity or a guessed higher revision.
