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

The old scalar-only description of `attributes` must be corrected. M2 requires arrays such as `satellites` and `detection_keys`. Keep revision, tombstone, ownership, lineage, and freshness metadata in internal tables or existing lifecycle envelopes unless the public contract explicitly permits additions.

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

Inactive rows are excluded from current client/analytics results. Whether the evaluator expects inactive identities in audit events/checksum remains a validation gate; do not silently implement either interpretation as an assignment fact.

UUIDv4 is an opaque canonical identity choice, not a deduplication mechanism. Database constraints, stable routing, revision checks, and retry identities remain necessary.

## 3. Distribution and Constraints

Selected routing principle: hash of the composite source identity, stable logical partitions, explicit placement metadata.

Under Citus, use its hash distribution and shard placement catalogs as the authority. Do not maintain a competing application hash ring. Coordinate updates leave `(source, source_ref_id)` unchanged, so they leave routing unchanged.

**Proposed implementation detail:** encode the source pair into a collision-free distribution value, distribute on that value, and retain the original pair for exact identity checks. An unambiguous structured or length-prefixed encoding avoids delimiter collisions. Specify and version the encoding before migration; do not choose an unexplained hash or identifier style.

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
