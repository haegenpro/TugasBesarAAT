# Decisions and Implementation Readiness

Status: user-selected design; implementation and evaluator compatibility are not yet proven.

This document records the interview decisions. It supersedes the generic Distributed State & Stream Processing choices in the earlier preimplementation checklist. Contracts for the other areas of responsibility still require joint review. No application code or deployment was changed while preparing these documents.

## Document Set

| Document | Contents |
| --- | --- |
| [Canonical and Storage Contract](Canonical-and-Storage-Contract.md) | Canonical schema, writes, Citus distribution, replication, failover, placement, and ownership boundaries. |
| [Wildfire Processing Contract](Wildfire-Processing-Contract.md) | Observation identity, connectivity, lifecycle, deterministic replay, lateness, and checkpoint/output delivery. |
| [Audit, Analytics, and Validation](Audit-Analytics-and-Validation.md) | Snapshot reads, exact audit rules, degraded behavior, analytics, test evidence, and remaining validation gates. |

## Authority and Status Labels

- **Required:** prescribed by the uploaded M2 assignment.
- **Selected:** explicitly chosen by the user during the decision interview.
- **Derived requirement:** necessary to preserve a selected guarantee; its implementation still needs verification.
- **Proposed implementation detail:** reasoned guidance, not an additional user-approved architectural choice.
- **Validation gate:** unresolved compatibility, feasibility, or measurement work. Do not report it as implemented or proven.

The assignment prevails over a design preference. A discovered conflict must be documented and brought back for a decision; it must not silently change the selected policy.

## Decision Register

Numbers follow the interview, including refinements and superseded entries.

| Decision | Selected outcome | Qualification |
| --- | --- | --- |
| 1 | B — more than three nodes | Refined to four nodes; restored in consistency review A2. |
| 2 | Final revision — four nodes with separate kernels | Three-node interim revision superseded by review A2. Node count is not database-instance count. |
| 3 | Compare storage approaches; then deferred | Resolved by decision 26. |
| 4 | A — UUIDv4 canonical `hazard_id`, assigned once and persisted | Preserve it on corrections and restoration. |
| 5 | A — internal tombstones; inactive fires excluded from current results | Exclusion from audit events/checksum remains conditional on evaluator compatibility. |
| 6 | A — merged identity survives from the fire containing the earliest detection | Deterministic detection-key tie-break. |
| 7 | A — on split, earliest remaining qualifying group retains identity | Nonqualifying singletons cannot retain an active fire identity. |
| 8 | A — exact restoration of historical detection membership reactivates the old identity | Requires retained lineage and unambiguous matching. |
| 9 | B, narrowed by review B1 — fixed snapshots in explicit audit sessions only | Ordinary reads remain live; a local transaction on each worker alone does not prove a common snapshot. |
| 10 | B — pause ordered audit pagination at unavailable data | Bounded response; retryable continuation, not an indefinitely open request. |
| 11 | A — listing and checksum can share a snapshot | Optional API extension; required assignment fields remain unchanged. |
| 12 | B — extend retention on use, with absolute maximum lifetime | Expired cursors fail explicitly. Durations require measurement. |
| 13 | B — automatic correction path for inputs beyond allowed lateness | Correction work is bounded and yields to live work. |
| 14 | B — watermark based on feed `virtual_now` | Feed-clock/session semantics require validation and journaling. |
| 15 | A — fixed configurable lateness allowance per test run | Select from observations; no arbitrary duration is approved. |
| 16 | A — one active logical wildfire processor, with automatic takeover | Explicitly revised from B. The earlier parallel-reconciliation question is withdrawn. |
| 17 | A — initial fire source identity anchored to earliest detection key | Refined by decision 31 to disambiguate later births. |
| 18 | A — retain complete satellite history and lineage for the evaluation session | Durable retention does not imply unbounded RAM. |
| 19 | A — hash partitioning based on `(source, source_ref_id)` | Coordinate corrections do not change the routing key. |
| 20 | A — stable logical partitions and explicit placement metadata | Use Citus metadata rather than an independent conflicting application map. |
| 21 | Final revision — three copies per partition on distinct Kubernetes nodes | Restored by review A3; one primary and two standbys per worker group. |
| 22 | A — success after two of three copies durably persist each participating write | Safe ordering and promotion remain necessary; the third copy may catch up. |
| 23 | A — keep input pending when required replication is unavailable | Do not downgrade to one-copy canonical success. |
| 24 | A — authoritative-primary ordinary reads | Not a blanket claim of distributed linearizability. |
| 25 | A — dashboard aggregates canonical records on request | Exact counts, explicit completeness, no separate derived-counter pipeline initially. |
| 26 | B — self-hosted Citus | Version compatibility and resource fit need validation. |
| 27 | A — Patroni with Citus integration | Platform, Coordination & Reliability operates coordination infrastructure; Distributed State & Stream Processing verifies data safety. |
| 28 | B — atomic processor state/progress/output intent, then idempotent canonical application | Track prepared and applied progress separately. |
| 29 | B — WGS84 ellipsoidal geodesic distance | Evaluator metric compatibility is not yet confirmed. |
| 30 | B — equal-weight spherical vector mean for center | Evaluator formula compatibility is not yet confirmed. |
| 31 | A — earliest detection key + birth feed offset + stable session namespace | Reconnects/replay must preserve the namespace and original offsets. |
| 32 | A, narrowed by review B1/B2 — immutable snapshots for explicit audit sessions | Brief write barrier establishes retained read views; copying proceeds after writes resume. |

## Important Combined Consequences

### Final topology: four nodes and three copies

Consistency review reopened only write availability and audit snapshots. Review A1 selected continued canonical-write progress after one node fails; A2 restored four Kubernetes nodes; A3 restored three copies per partition with two-copy durable acknowledgement and retained Citus plus Patroni. The interim three-node/two-copy design is superseded. Continuous operation permits a bounded safe-failover interruption; zero interruption is not promised.

Under whole-worker streaming replication, at least four nonempty worker groups are needed in the symmetric layout to keep some canonical data absent from every node. [Canonical and Storage Contract](Canonical-and-Storage-Contract.md#5-placement-on-four-kubernetes-nodes) uses four groups, each omitting a different node. This requires **12 worker database instances**. A proposed three-member coordinator group would bring the total to **15 database instances**; coordinator count remains a shared deployment proposal rather than a separate user choice.

The workers request 6 GiB and have 12 GiB combined memory limits under M2. With three coordinators, totals become 7.5 GiB requests and 15 GiB limits. Other services, coordination, sidecars, operating systems, and Kubernetes require additional capacity. This resource fit has not been benchmarked.

After one node fails, each affected group has two copies left. Writes can resume after safe promotion, synchronous-standby reconfiguration, and routing recovery, provided those copies, the coordinator, and the coordination backend can communicate. Losing coordination or a second needed copy can still block writes; no acknowledgement downgrade is allowed.

Do not restore three copies of every partition onto only the three surviving nodes: that would put the complete canonical dataset on each survivor. Restore a fourth eligible node before returning to the intended placement. Continuing two-copy writes is distinct from completing redundancy repair.

### Audit scope and capture revised

Review B1 selected reproducible snapshots only for explicitly initiated audit sessions. Review B2 selected a short write-admission barrier while establishing retained read views, followed by background materialization after canonical writes resume. If a necessary read view is lost before copying completes, abort that unfinished capture explicitly. Do not restart one unit at a newer boundary under the same snapshot identity.

Ordinary client reads and non-session audit requests remain live. Within a completed snapshot session, the original listing/checksum reconciliation, ordered partial pagination, and sliding expiry with hard maximum still apply. The concrete Citus read-view and barrier integration remains unproven; selecting this protocol does not mean it is already supported by an ordinary distributed transaction.

### Audit and processing guarantees need separate mechanisms

Citus sharding and Patroni failover do not themselves supply the chosen audit snapshot, partial-pagination, processor fencing, or historical identity semantics. Those belong to application contracts and validation work.

UUIDv4 canonical IDs are stable in the persisted system, but a full rebuild without the identity registry may create different UUIDs. Fire `source_ref_id` values are reproducible for the same ordered, namespaced operation stream. This is not a promise of order-independent identity from a final unordered set of detections.

## Shared Ownership

| Boundary | Distributed State & Stream Processing | Platform, Coordination & Reliability | Reliable Ingestion & Delivery |
| --- | --- | --- | --- |
| Citus/Patroni | Distribution schema, data safety, recovery assertions | Deployment, DCS, networking, placement, operational recovery | Uses agreed acceptance/retry outcomes |
| Polling/processor fencing | Enforce tokens with protected mutations | Define ownership source and takeover mechanism | Carry ownership context and stop stale acquisition |
| Satellite handoff | Interpret operations and create fires | Operate broker/storage infrastructure | Preserve raw operations, namespace, offsets, and feed-clock context |
| Progress | Prepared/applied progress and state recovery | Expose lag and failure signals | Durably received progress and reconnect cursor |
| Audit | Events, checksum, location, snapshots | Infrastructure availability and failure injection | `/unprocessed` and pending-message interpretation |
| Evidence | Correctness and processing results | Shared harness and operational evidence | Input/replay/delivery evidence |

## Readiness Checklist

- [x] User architectural choices recorded, including the decision 16 revision.
- [x] Required assignment behavior separated from preferences and proposals.
- [x] Combined topology/resource implications documented.
- [ ] Platform, Coordination & Reliability confirms host capacity, control-plane/DCS availability, and placement feasibility.
- [ ] Citus/PostgreSQL/Patroni versions are pinned and compatibility is demonstrated.
- [ ] Distribution keys and constraints are migrated without weakening deduplication.
- [ ] Strict synchronous durability and safe promotion are demonstrated under faults.
- [ ] The single processor's durable state and history placement preserves the no-full-dataset-per-node requirement.
- [ ] Short-barrier capture, write resumption before copying completes, explicit capture abort, partial reads, expiry, and completed-snapshot recovery are demonstrated.
- [ ] All sources and bulk loaders honor the fenced snapshot write-admission barrier; bounded release after capture failure is demonstrated.
- [ ] Evaluator compatibility for identity, inactive audit membership, distance, and center is checked.
- [ ] Cross-component envelope, token, acceptance, and progress contracts are reviewed.
- [ ] Measured parameter values and acceptance evidence are recorded.

These unchecked items are implementation or compatibility gates, not additional arbitrary preference questions. If evidence invalidates a selected choice, reopen that specific decision.

## Sources

Primary project sources: `upload/IF4031 - Tugas Besar - M2.md` and the M1 archive `upload/TugasBesarAAT.zip`. Relevant archive paths include `docs/HazardEvent Representation.md`, `aggregator/internal/domain/hazard.go`, `aggregator/internal/store/store.go`, `aggregator/internal/store/lifecycle.go`, `aggregator/internal/store/pagination.go`, and `store/init.sql`.

Technology references were checked while documenting the design; their existence is not deployment validation:

- [Citus cluster management](https://docs.citusdata.com/en/stable/admin_guide/cluster_management.html)
- [Citus distributed DDL and constraints](https://docs.citusdata.com/en/stable/develop/reference_ddl.html)
- [Patroni Citus integration](https://patroni.readthedocs.io/en/latest/citus.html)
- [Patroni replication modes](https://patroni.readthedocs.io/en/latest/replication_modes.html)
- [PostgreSQL 16 WAL and synchronous commit](https://www.postgresql.org/docs/16/runtime-config-wal.html)
- [GeographicLib geodesics](https://geographiclib.sourceforge.io/html/python/geodesics.html)
