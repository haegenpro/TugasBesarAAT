# M2 Preimplementation Agreement Checklist

## Purpose

Agree on the shared schemas, interfaces, and correctness rules that allow the three areas of responsibility to implement independently. This checklist records both documented design decisions and the agreements still needed for integration. It does not imply that the implementation already satisfies them.

Use the [M2 specification](IF4031%20-%20Tugas%20Besar%20-%20M2.md) as the authority for prescribed endpoints, ordering, checksums, processing parameters, and acceptance thresholds. Confirm those details before freezing the corresponding agreements.

## Current Status — 11 November 2026

Core designs are documented for Distributed State & Stream Processing (**1, 2, 4, 7, 9, 12, 13**) and Reliable Ingestion & Delivery (**3, 6, 8, 10, 11, 14**). Platform-led agreements (**5, 15, 16**) have contributions but still need their shared contracts completed. The 13 storage/processing and ingestion/delivery written agreements are finalized as the implementation baseline in this session: concrete interfaces, two Avro schemas, broker defaults, and expected-output fixtures are specified and checked for consistency. The user selected historical-restoration precedence, a separate field-report workflow, and active-only audit membership. Platform-led agreements **5, 15, and 16** remain open. External resources are not set up locally; deployment, source characterization, reference compatibility, and runtime evidence are implementation/integration gates, not claimed results or implied sign-off by absent teammates.

| Document | Existing contract coverage |
| --- | --- |
| [Canonical and Storage Contract](Canonical-and-Storage-Contract.md) | Items 1, 2, 4, and 7; storage-side requirements for items 5 and 6. |
| [Wildfire Processing Contract](Wildfire-Processing-Contract.md) | Item 9; processor-side requirements for items 3, 6, and 8. |
| [Audit, Analytics, and Validation](Audit-Analytics-and-Validation.md) | Items 12 and 13; analytics behavior and acceptance scenarios contributing to item 16. |
| [Ingestion and Messaging Contract](Ingestion-and-Messaging-Contract.md) | Items 3, 6, 8, and 10; report idempotency, input durability, capacity, and ownership context. |
| [Validation and Replay Contract](Validation-and-Replay-Contract.md) | Item 11; schema policy, failed-input preservation, ordered replay, authorization, and `/unprocessed` contributions to item 12. |
| [Coastal Delivery and Validation](Coastal-Delivery-and-Validation.md) | Item 14; sensor resilience, durable recipient receipt, scheduling, and acceptance evidence contributing to item 16. |

The selected storage baseline is **four separate-kernel Kubernetes nodes, Citus with Patroni, three copies per worker group, and durable acknowledgement from two copies per participating group**. Audit snapshots are initiated only by explicit sessions; ordinary reads remain live. These decisions supersede generic alternatives in the earlier checklist and responsibility plan. Deployment feasibility and guarantees still require implementation evidence.

## Responsibilities

Platform's first implementation handoff is now in the
[coordination contract](Platform-Coordination-Contract.md),
[deployment contract](Platform-Deployment-Contract.md), and
[acceptance plan](Platform-Acceptance-Plan.md). The ownership API and transactional
canonical fence have an isolated PostgreSQL integration runner. Source-election,
processor/audit ownership, production replicated durability, Kubernetes and full
failure/load acceptance remain pending. Items 5, 15 and 16 below stay open for
joint review and the remaining boundaries; this is not absent-teammate sign-off.

| Area | Responsibility |
| --- | --- |
| **Distributed State & Stream Processing** | Canonical storage, distributed reads, audit endpoints except `/unprocessed`, wildfire processing, and analytics dashboard. |
| **Platform, Coordination & Reliability** | Kubernetes, ownership and fencing mechanism, shared infrastructure, observability, load tests, and failure-test orchestration. |
| **Reliable Ingestion & Delivery** | Input acquisition, validation, durable acceptance, schema handling, retry/replay, `/unprocessed`, sensor integration, and notifications. |

The lead drafts each agreement. Collaborators review the parts their components must produce or consume. Each component owner remains responsible for its own service manifests, instrumentation, tests, and documentation.

## Completion Rule

Each item tracks documented coverage and the written implementation baseline separately. **Agreement finalized** means the application-side contract has concrete decisions, examples and a consistency review in this session, including the user’s answers where a policy choice was needed. It does not certify deployment, external compatibility, runtime correctness, or another teammate’s review. The baseline contains:

- Concrete schema, interface, or behavioral rules.
- At least one valid example or representative configuration.
- Relevant duplicate, stale-update, ordering, and failure behavior.
- Expected results that can become shared fixtures or integration assertions.
- No unresolved application-side interface or policy decision that blocks independent storage/processing or ingestion/delivery implementation. Required platform capabilities are explicit dependencies.

The checked documentation milestones below refer to the linked contracts, including their stated qualifications. [Shared fixtures](fixtures/m2-contract-examples.json) and the [raw Avro schema](fixtures/raw-input-v1.avsc) make those interfaces inspectable. Platform-led agreements remain unchecked until their shared mechanism/deployment/observability decisions are supplied. Runtime acceptance checklists are separate and do not need to pass before design work is recorded.

## P0 — Agree Before Implementing Shared Boundaries

### 1. Canonical HazardEvent Schema

- [x] **Core design documented** — [Canonical and Storage Contract, §1](Canonical-and-Storage-Contract.md#1-preserve-the-existing-canonical-shape) defines canonical fields, types, enums, UTC timestamps, source identity, and structured attributes.
- [x] **Agreement finalized**
  - **Implementation / integration follow-up:** Use the M2 wire format and shared examples. Production coastal validation and province assignment require actual sensor behavior and the immutable production reference table; synthetic fixture geography is for tests only.
  - **Lead:** Distributed State & Stream Processing.
  - **Collaborator:** Reliable Ingestion & Delivery.
  - **Description:** Define the canonical representation stored and returned by the system. Distinguish raw source observations from derived HazardEvents.
  - **Decisions:** Required and optional fields; field types; event category and status; severity values; location format and coordinate system; source references; timestamp formats, timezone, precision, and meaning; schema versioning.
  - **Completion evidence:** Written schema, example source observation, and example canonical HazardEvent with field definitions.

### 2. Identity and Revision Rules

- [x] **Core design documented** — [Canonical and Storage Contract, §2](Canonical-and-Storage-Contract.md#2-identity-revisions-and-tombstones) defines UUID preservation, duplicate/stale outcomes, revisions, and tombstones; [Wildfire Processing Contract, §5](Wildfire-Processing-Contract.md#5-fire-identity-and-lineage) defines birth, merge, split, and restoration identity.
- [x] **Agreement finalized**
  - **Implementation / integration follow-up:** Implement restoration-first matching with deterministic multiple-match ties and unmatched-child assignment. Audit active records only; retain tombstones internally. The shared lifecycle fixtures cover merge, split, restoration collisions, duplicates, stale updates, and retractions.
  - **Lead:** Distributed State & Stream Processing.
  - **Collaborator:** Reliable Ingestion & Delivery.
  - **Description:** Ensure independent components recognize the same observation or event and apply updates consistently.
  - **Decisions:** Message, observation, and HazardEvent identifiers; uniqueness scope; source-to-event mapping; duplicate detection; revision ordering; conflicting or older updates; retraction representation and retention.
  - **Completion evidence:** Examples covering duplicate delivery, a newer revision, a stale revision, and a retraction, with expected stored state.

### 3. Common Input Message Envelope

- [x] **Core design documented** — [Ingestion and Messaging Contract, §3](Ingestion-and-Messaging-Contract.md#3-raw-envelope) defines a separate registry-framed Avro raw envelope with exact payload bytes, stable operation/source identity, namespace/position, time, feed-clock, trace, and ownership context.
- [x] **Agreement finalized**
  - **Implementation / integration follow-up:** Implement the exact Raw Input v1 Avro schema, per-source rules, framing, byte/reference preservation, and identity algorithm. Validate codec integration and durable cached-schema cold starts; no field names/types remain undecided.
  - **Lead:** Reliable Ingestion & Delivery.
  - **Collaborator:** Distributed State & Stream Processing.
  - **Description:** Establish one transport envelope for inputs entering validation and processing.
  - **Decisions:** Source; message identity; observation/source reference; operation such as `create`, `update`, or `retract`; schema version; original payload preservation; source position; event and receipt timestamps; correlation/trace context. Specify which fields are mandatory for each source.
  - **Completion evidence:** Versioned envelope schema and representative messages for the supported operation types.

### 4. Canonical Write Contract

- [x] **Core design documented** — [Canonical and Storage Contract, §§4, 7–8](Canonical-and-Storage-Contract.md#4-selected-durability-and-failover-semantics) and [Wildfire Processing Contract, §7](Wildfire-Processing-Contract.md#7-durable-state-progress-and-output) define two-copy success, logical write outcomes, idempotent correction batches, atomic publication intent, and write admission.
- [x] **Agreement finalized**
  - **Implementation / integration follow-up:** Implement Canonical Adapter v1, reconciliation reads, atomic large-batch references, and the shared write outcomes. Platform supplies current ownership validation at the protected commit boundary; no stale-token preflight-only workaround is permitted.
  - **Lead:** Distributed State & Stream Processing.
  - **Collaborators:** Platform, Coordination & Reliability and Reliable Ingestion & Delivery.
  - **Description:** Define how ingestion and processing safely change canonical state.
  - **Decisions:** API or message interface; upsert and retract requests; idempotency key; revision checks; fencing-token placement; success and error responses; retry behavior; atomicity between canonical changes and outgoing events; behavior when only part of a multi-record operation succeeds.
  - **Completion evidence:** Request/response or message examples for success, duplicate, stale revision, stale token, and temporary storage failure.

### 5. Ownership and Fencing Contract

Implementation candidate: [canonical authority v1](Platform-Coordination-Contract.md)
defines scope/epoch/holder, authoritative expiry, takeover, pending-intent handling
and transaction/commit checks. `python3 scripts/test-coordination.py` runs its
local proof; other protected boundaries and authority-host/HA faults are pending.

- [x] **Contribution documented** — [Canonical and Storage Contract, §§4 and 8](Canonical-and-Storage-Contract.md#4-selected-durability-and-failover-semantics) requires serialized stale-owner rejection and a fenced snapshot admission gate; [Wildfire Processing Contract, §7](Wildfire-Processing-Contract.md#7-durable-state-progress-and-output) separates takeover from delivery of committed intents.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Platform must define ownership scope, mechanism, token format/order, renewal/expiry, authoritative time, takeover, cleanup, and authorization of queued work. Jointly review the stale-owner sequence.
  - **Lead:** Platform, Coordination & Reliability.
  - **Collaborators:** Distributed State & Stream Processing and Reliable Ingestion & Delivery.
  - **Description:** Prevent an expired or resumed former owner from producing accepted stale writes.
  - **Decisions:** Ownership scope; coordination mechanism; token generation and ordering; acquire, renew, expire, and takeover behavior; token propagation; validation boundary; unavailable-coordinator behavior; clock assumptions; treatment of previously accepted queued work after takeover.
  - **Ownership boundary:** Platform, Coordination & Reliability supplies ownership and tokens. Reliable Ingestion & Delivery attaches the required ownership context. Distributed State & Stream Processing enforces rejection at the canonical write boundary.
  - **Completion evidence:** Sequence and expected outcomes for takeover followed by a stale owner's write, including suspended/resumed worker behavior.

### 6. Durability and Acknowledgement Rules

- [x] **Core design documented** — [Ingestion and Messaging Contract, §§1–2 and 4–5](Ingestion-and-Messaging-Contract.md#1-acceptance-and-completion-boundaries) defines broker-first 202 acceptance, scoped durable report identity, reject-new capacity behavior, and commit-before-handoff acknowledgement. [Coastal Delivery and Validation, §3](Coastal-Delivery-and-Validation.md#3-durable-critical-path) defines durable warning intent and recipient commit before acknowledgement.
- [x] **Agreement finalized**
  - **Implementation / integration follow-up:** Implement the specified broker-first admission/ledger recovery, stable field-report key/receipt, separate report projection, crash table and service-owned durability. Verify client integration and actual persistence boundaries in implementation tests.
  - **Lead:** Reliable Ingestion & Delivery.
  - **Collaborators:** Distributed State & Stream Processing and Platform, Coordination & Reliability.
  - **Description:** Define the persistent state required before any component claims that work has succeeded.
  - **Decisions:** Persistence required before HTTP `202`; broker acknowledgement points; received and processed offset advancement; transaction/outbox boundaries; delivery completion; crash recovery; duplicate-effect prevention after redelivery.
  - **Completion evidence:** A boundary-by-boundary acceptance table and expected recovery behavior for crashes immediately before and after each acknowledgement.

### 7. Storage Topology and Failure Model

- [x] **Core design documented** — [Canonical and Storage Contract, §§3–6](Canonical-and-Storage-Contract.md#3-distribution-and-constraints) selects source-key hash distribution, Citus/Patroni, four nodes, three copies, two-copy acknowledgement, placement, single-loss behavior, and replacement/rebalancing rules.
- [x] **Agreement finalized**
  - **Implementation / integration follow-up:** Implement the four-node, three-copy, two-copy-acknowledgement contract and v1 source-pair distribution key. Platform must supply feasible resources, coordinator/control-plane/DCS availability, version/placement configuration and recovery orchestration under item 15. Do not describe that platform work as complete.
  - **Lead:** Distributed State & Stream Processing.
  - **Collaborator:** Platform, Coordination & Reliability.
  - **Description:** Choose a deployment and data-distribution design capable of meeting the required storage and recovery constraints.
  - **Decisions:** Node count; partition unit and routing; replication factor; replica placement; write/read consistency; storage isolation; tolerated node and disk failures; replacement-node recovery; rebalancing; skew handling; degraded availability.
  - **Completion evidence:** Topology and placement example plus a failure matrix showing which reads and writes remain possible. Verify that replication and placement meet the specification's data-distribution requirements.

## P1 — Agree Before Implementing Each Affected Feature

These agreements can be drafted in parallel with P0. Finalize each one before its producers and consumers implement their shared boundary.

### 8. Satellite Acquisition-to-Processing Contract

- [x] **Core design documented** — [Ingestion and Messaging Contract, §5](Ingestion-and-Messaging-Contract.md#5-satellite-handoff-and-retention) defines original namespace/offset/payload preservation, gap detection, persistence-before-receipt-progress, and deletion only after durable processor handoff. [Wildfire Processing Contract, §7](Wildfire-Processing-Contract.md#7-durable-state-progress-and-output) separates prepared and applied progress.
- [x] **Agreement finalized**
  - **Implementation / integration follow-up:** Implement the specified history append/replay/receipt interface and contiguous progress. No feed credential/session/reference setup exists locally: characterize actual reconnect/start-offset/update/confidence/clock semantics before enabling retirement or claiming live compatibility.
  - **Lead:** Reliable Ingestion & Delivery.
  - **Collaborator:** Distributed State & Stream Processing.
  - **Description:** Define the durable, replayable satellite input supplied to wildfire processing.
  - **Decisions:** Detection/update/retract payloads; identity and ordering guarantees; reconnect cursor; duplicate and gap handling; retention/replay expectations; separate durably received and successfully processed positions; checkpoint ownership.
  - **Completion evidence:** Example stream with reconnect, duplicate delivery, update, and retraction, including expected progress positions.

### 9. Wildfire Processing Semantics

- [x] **Core design documented** — [Wildfire Processing Contract](Wildfire-Processing-Contract.md) defines event time, transitive 1 km/360-minute connectivity, qualification, canonical mapping, stable identities, lineage, watermark policy, late corrections, and transactional state/output preparation.
- [x] **Agreement finalized**
  - **Implementation / integration follow-up:** Implement the deterministic graph/identity/numeric/lifecycle rules and golden fixtures. Configure and record allowed lateness per run; obtain the real province/reference artifacts and feed evidence. Reference, accelerated-playback, restart and live-feed proof remain implementation acceptance work.
  - **Lead:** Distributed State & Stream Processing.
  - **Collaborator:** Reliable Ingestion & Delivery.
  - **Description:** Make derived wildfire results deterministic across normal execution, replay, and restart.
  - **Decisions:** Event-time field; spatial and temporal grouping; watermark and allowed lateness; deduplication; stable wildfire identity; update/retraction effects; merge/split behavior; state and progress checkpointing. Use specification-prescribed parameters where provided.
  - **Completion evidence:** Small reference input sequences with expected wildfire identities and outputs, including late input, merge, split, removal, and restart.

### 10. Broker Topology and Consumer Configuration

- [x] **Core design documented** — [Ingestion and Messaging Contract, §4](Ingestion-and-Messaging-Contract.md#4-streams-replication-and-capacity) separates satellite, coastal, and report raw streams, requires disk flushing plus replication, bounded consumption and reject-new capacity behavior. [Coastal Delivery and Validation, §3](Coastal-Delivery-and-Validation.md#3-durable-critical-path) specifies a dedicated durable recipient consumer.
- [x] **Agreement finalized**
  - **Implementation / integration follow-up:** Apply the fixed stream/subject/consumer, retention, fsync, replication, acknowledgement and retry contract. Platform allocates capacity and pins broker configuration. Validate initial numeric limits and fault recovery rather than treating defaults as benchmark results.
  - **Lead:** Reliable Ingestion & Delivery.
  - **Collaborator:** Platform, Coordination & Reliability.
  - **Description:** Align application messaging guarantees with the deployed broker's persistence and resource configuration.
  - **Decisions:** Streams and subjects; publishers and consumers; durable consumer names; retention and replication; ordering scope; acknowledgement policy; acknowledgement timeout; retry/backoff limits; redelivery; capacity and backpressure behavior.
  - **Ownership boundary:** Platform, Coordination & Reliability operates broker infrastructure. Reliable Ingestion & Delivery owns application stream and consumer semantics.
  - **Completion evidence:** Messaging topology/configuration table linked to the durability agreement and a broker-restart scenario.

### 11. Failure, Quarantine, and Replay Contract

- [x] **Core design documented** — [Validation and Replay Contract](Validation-and-Replay-Contract.md) defines compatible additions/lossless promotions, parking for incompatible changes, complete original-byte failure records, automatic transient retries, Ops-only replay, source-entity ordering, and repeated-failure handling.
- [x] **Agreement finalized**
  - **Implementation / integration follow-up:** Implement the failure database/attempt records, Ops APIs, source/namespace fallback gate, entity ordering, durable oversized references, and replay fixtures. Validate quotas, byte preservation, auth, crash recovery and replay/live serialization.
  - **Lead:** Reliable Ingestion & Delivery.
  - **Collaborator:** Distributed State & Stream Processing.
  - **Description:** Preserve failed input and recover it without blocking unrelated work or creating duplicate effects.
  - **Decisions:** Accepted versus incompatible schema changes; retryable versus parked failures; original payload and failure metadata; quarantine representation; inspection and replay authorization; ordering per `source_ref_id`; replay identity; poison-message isolation; behavior when one replayed message fails again.
  - **Completion evidence:** Examples of malformed input, incompatible schema, temporary failure, and successful replay with expected canonical effects.

### 12. Read and Audit API Contract

- [x] **Core design documented** — [Audit, Analytics, and Validation, §§1–5](Audit-Analytics-and-Validation.md#1-prescribed-audit-contract) defines endpoint parameters, ordering, filtering, pagination, checksum serialization, current location reporting, and explicit-session snapshot semantics.
- [x] **Agreement finalized**
  - **Implementation / integration follow-up:** Implement the prescribed endpoint schemas plus the optional explicit-session interface, signed cursors and active-only inclusion rule. Use the fixed ordering/checksum/unprocessed fixtures. Verify optional evaluator compatibility during integration without changing default requests.
  - **Lead:** Distributed State & Stream Processing.
  - **Collaborator:** Reliable Ingestion & Delivery.
  - **Description:** Define externally observable reads and audit evidence consistently across services.
  - **Decisions:** Endpoint methods, parameters, filtering, response schemas, status codes, global ordering, tie-breakers, pagination and concurrent-update behavior; exact checksum serialization and scope; `/location` meaning; `/unprocessed` representation. Copy prescribed rules from the M2 specification.
  - **Ownership boundary:** Distributed State & Stream Processing owns `/events`, `/checksum`, and `/location`. Reliable Ingestion & Delivery owns `/unprocessed`.
  - **Completion evidence:** Example responses and a fixed dataset with expected ordering, pages, checksum, and unprocessed entries.

### 13. Partial-Result and Completeness Contract

- [x] **Core design documented** — [Audit, Analytics, and Validation, §§2–3 and 6](Audit-Analytics-and-Validation.md#2-immutable-materialized-snapshot) defines bounded partial reads, safe ordered-prefix continuation, snapshot-copy fallback, and visible dashboard incompleteness.
- [x] **Agreement finalized**
  - **Implementation / integration follow-up:** Implement physical unavailable-unit reporting, safe-prefix continuation, snapshot-copy fallback and initial budgets. Platform supplies authoritative expiry/fencing. Prove cleanup, fallback and timing budgets under faults.
  - **Lead:** Distributed State & Stream Processing.
  - **Collaborator:** Platform, Coordination & Reliability.
  - **Description:** Expose incomplete reads clearly when storage units are unavailable.
  - **Decisions:** Meaning and format of `partial` and `unavailable_units`; unit identifiers; timeout behavior; completeness determination with replicas; propagation through pagination, aggregations, audit responses, and dashboard displays.
  - **Completion evidence:** Complete and degraded response examples, with expected dashboard behavior and consistency with the read API contract.

### 14. Critical Notification Contract

- [x] **Core design documented** — [Coastal Delivery and Validation](Coastal-Delivery-and-Validation.md) defines sensor validation/isolation, critical warning delivery after durable raw acceptance independently of canonical writes, dedicated recipient consumption, transactional receipt/deduplication, fresh/backlog scheduling, and zero-loss/p95 ≤5-second measurement requirements.
- [x] **Agreement finalized**
  - **Implementation / integration follow-up:** Implement the typed warning schema, stable notification identity, separate canonical/warning intents, transactional recipient receipt, fair scheduling and loss/latency accounting. The manifest is present, but no local sensor is configured; characterize the prescribed image before claiming its quality/cursor/critical behavior verified.
  - **Lead:** Reliable Ingestion & Delivery.
  - **Collaborators:** Distributed State & Stream Processing and Platform, Coordination & Reliability.
  - **Description:** Define the critical coastal-warning path and how delivery reliability is measured.
  - **Decisions:** Critical-event criteria; event/revision triggering rules; notification payload; recipient routing; delivery identity; durable handoff; recipient acknowledgement; retries and duplicate handling; latency start/end points; loss accounting; reliability target and error-budget accounting.
  - **Completion evidence:** Example critical event and notification, expected retry behavior after recipient failure, and precise measurement definitions tied to the specification's thresholds.

### 15. Service and Deployment Boundaries

First platform handoff: [portable four-node deployment contract](Platform-Deployment-Contract.md).
The [QEMU/KVM k3s bootstrap](../deployment/platform/README.md) now supplies a
light four-node environment, fixed host API and the unchanged Tester Agent;
[local validation](Platform-Bootstrap-Validation.md) includes one control-plane
VM-pause experiment. Full application/storage manifests, capacity, durability
and recovery remain implementation work. The 7 GiB bootstrap is not a claim that
the selected canonical database topology fits this laptop.

- [x] **Contribution documented** — [Canonical and Storage Contract, §5](Canonical-and-Storage-Contract.md#5-placement-on-four-kubernetes-nodes) provides canonical placement and resource requirements. [Ingestion and Messaging Contract, §4](Ingestion-and-Messaging-Contract.md#4-streams-replication-and-capacity) and [Coastal Delivery and Validation, §3](Coastal-Delivery-and-Validation.md#3-durable-critical-path) add broker isolation and recipient-storage needs.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Platform must complete the service inventory, capacity and HA plan, ports/configuration/Secrets, probes, placement, shutdown/rollout and admission conventions, required images, and reproducible startup procedure.
  - **Lead:** Platform, Coordination & Reliability.
  - **Collaborators:** Distributed State & Stream Processing and Reliable Ingestion & Delivery.
  - **Description:** Allow all component owners to build services that deploy together without incompatible operational assumptions.
  - **Decisions:** Service ownership; dependencies and discovery; ports; configuration and Secret names; persistent volumes; resource budgets; placement; startup/readiness/liveness probes; graceful shutdown; rolling-update expectations; required external/tester images; admission-control ownership and priority rules.
  - **Completion evidence:** Service inventory, configuration template, deployment conventions, and a reproducible startup procedure. Each component owner implements its own service manifests under these conventions.

### 16. Observability and Acceptance Evidence

First platform handoff: [acceptance and evidence matrix](Platform-Acceptance-Plan.md).
The local coordination runner captures its own results; Kubernetes telemetry and
the integrated load/failure scenarios remain implementation work.

- [x] **Contribution documented** — [Audit, Analytics, and Validation, §§7–8](Audit-Analytics-and-Validation.md#7-remaining-validation-gates) and [Wildfire Processing Contract, §8](Wildfire-Processing-Contract.md#8-required-evidence) list storage/audit/processing scenarios. [Validation and Replay Contract, §6](Validation-and-Replay-Contract.md#6-evidence) and [Coastal Delivery and Validation, §§5–7](Coastal-Delivery-and-Validation.md#5-delivery-evidence-and-reliability-target) add byte preservation, replay/auth, acceptance, recipient receipt, latency, and loss-reconciliation evidence.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Platform must assemble the shared metric/evidence matrix, names/labels, log/trace conventions, harness ownership, and integrated scenarios with ingestion/delivery. Listed scenarios are not executed results.
  - **Lead:** Platform, Coordination & Reliability.
  - **Collaborators:** Distributed State & Stream Processing and Reliable Ingestion & Delivery.
  - **Description:** Make integrated correctness, performance, and failure behavior measurable from the beginning.
  - **Decisions:** Trace/correlation propagation; structured log fields; timestamp conventions; essential counters and latency histograms; queue lag; ownership changes; stale-write rejection; storage completeness; replay results; notification outcomes; test assertions and evidence ownership.
  - **Ownership boundary:** Platform, Coordination & Reliability owns shared observability infrastructure and the end-to-end test harness. Each component owner supplies service instrumentation, scenarios, assertions, and interpretation for its area.
  - **Completion evidence:** Metric/evidence matrix assigning each acceptance criterion to a measurement, scenario, and responsible owner.

## Implementation Baseline and Remaining Dependencies

- [x] Storage/processing and ingestion/delivery contracts **1–4, 6–14** are written with concrete interfaces, errors, identity/order rules and shared fixtures.
- [x] Shared HazardEvent documentation includes M2 enums, structured attributes and canonical versioning; raw-input and coastal-warning Avro schemas are available in `docs/fixtures/`.
- [x] Historical restoration wins before ordinary identity survival; field reports stay in a separate durable workflow; audit listing/count/checksum include active HazardEvents only.
- [x] Documentation/schema/fixture consistency is reviewed in this session. No runtime or absent-teammate approval is implied.
- [ ] Platform must finalize **5, 15, and 16**: authoritative ownership/fencing/expiry, viable deployment and capacity, and shared observability/evidence conventions. Their application-side requirements are already specified.
- [ ] Obtain/configure the satellite group token, actual feed/session behavior, reference wildfire results, province table, and local Sensor Pantai environment. The user confirmed these are not set up locally. Keep credentials in Secrets; no tokens belong in fixtures or docs.
- [ ] During implementation, validate versions, tune starting limits, prove byte/durability/atomicity guarantees, run the reference/restart/failure scenarios, and demonstrate the first traced source-to-canonical/read path plus the independent critical-warning path.

Contract changes update the relevant retained document and shared fixtures before integration. Source/reference evidence that conflicts with this selected baseline reopens the affected rule explicitly. Pending platform work is not silently assigned to storage/ingestion, and later validation must not be reported as already passed.
