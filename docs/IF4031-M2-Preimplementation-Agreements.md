# M2 Preimplementation Agreement Checklist

## Purpose

Agree on the shared schemas, interfaces, and correctness rules that allow the three areas of responsibility to implement independently. This checklist records both documented design decisions and the agreements still needed for integration. It does not imply that the implementation already satisfies them.

Use the [M2 specification](IF4031%20-%20Tugas%20Besar%20-%20M2.md) as the authority for prescribed endpoints, ordering, checksums, processing parameters, and acceptance thresholds. Confirm those details before freezing the corresponding agreements.

## Current Status — 11 November 2026

Core designs are documented for Distributed State & Stream Processing (**1, 2, 4, 7, 9, 12, 13**) and Reliable Ingestion & Delivery (**3, 6, 8, 10, 11, 14**). Platform-led agreements (**5, 15, 16**) have contributions but still need their shared contracts completed. Concrete wire schemas/configuration, integration fixtures, cross-component review, and the compatibility questions listed below remain open. Documented design does not imply joint sign-off or runtime validation.

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

| Area | Responsibility |
| --- | --- |
| **Distributed State & Stream Processing** | Canonical storage, distributed reads, audit endpoints except `/unprocessed`, wildfire processing, and analytics dashboard. |
| **Platform, Coordination & Reliability** | Kubernetes, ownership and fencing mechanism, shared infrastructure, observability, load tests, and failure-test orchestration. |
| **Reliable Ingestion & Delivery** | Input acquisition, validation, durable acceptance, schema handling, retry/replay, `/unprocessed`, sensor integration, and notifications. |

The lead drafts each agreement. Collaborators review the parts their components must produce or consume. Each component owner remains responsible for its own service manifests, instrumentation, tests, and documentation.

## Completion Rule

Each item tracks two separate milestones: a checked **Core design documented** or **Contribution documented** records existing written work; **Agreement finalized** remains unchecked until the lead and collaborators have reviewed a written decision containing:

- Concrete schema, interface, or behavioral rules.
- At least one valid example or representative configuration.
- Relevant duplicate, stale-update, ordering, and failure behavior.
- Expected results that can become shared fixtures or integration assertions.
- No unresolved decision that blocks another component owner's implementation.

The checked documentation milestones below refer to the linked contracts, including their stated qualifications. Runtime acceptance checklists remain in those contracts and are separate from agreement sign-off.

## P0 — Agree Before Implementing Shared Boundaries

### 1. Canonical HazardEvent Schema

- [x] **Core design documented** — [Canonical and Storage Contract, §1](Canonical-and-Storage-Contract.md#1-preserve-the-existing-canonical-shape) defines canonical fields, types, enums, UTC timestamps, source identity, and structured attributes.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Review source-specific mappings, canonical schema versioning, and representative source/canonical JSON examples against the [external-schema acceptance policy](Validation-and-Replay-Contract.md#1-schema-acceptance-policy). Align the older wire-format document with M2 arrays and source/type additions.
  - **Lead:** Distributed State & Stream Processing.
  - **Collaborator:** Reliable Ingestion & Delivery.
  - **Description:** Define the canonical representation stored and returned by the system. Distinguish raw source observations from derived HazardEvents.
  - **Decisions:** Required and optional fields; field types; event category and status; severity values; location format and coordinate system; source references; timestamp formats, timezone, precision, and meaning; schema versioning.
  - **Completion evidence:** Written schema, example source observation, and example canonical HazardEvent with field definitions.

### 2. Identity and Revision Rules

- [x] **Core design documented** — [Canonical and Storage Contract, §2](Canonical-and-Storage-Contract.md#2-identity-revisions-and-tombstones) defines UUID preservation, duplicate/stale outcomes, revisions, and tombstones; [Wildfire Processing Contract, §5](Wildfire-Processing-Contract.md#5-fire-identity-and-lineage) defines birth, merge, split, and restoration identity.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Resolve ambiguous historical-restoration precedence and inactive audit membership. Align source-operation ordering and report/operation identity with [report idempotency](Ingestion-and-Messaging-Contract.md#2-report-idempotency) and [ordered replay](Validation-and-Replay-Contract.md#5-ordered-replay); add concrete lifecycle fixtures with expected state.
  - **Lead:** Distributed State & Stream Processing.
  - **Collaborator:** Reliable Ingestion & Delivery.
  - **Description:** Ensure independent components recognize the same observation or event and apply updates consistently.
  - **Decisions:** Message, observation, and HazardEvent identifiers; uniqueness scope; source-to-event mapping; duplicate detection; revision ordering; conflicting or older updates; retraction representation and retention.
  - **Completion evidence:** Examples covering duplicate delivery, a newer revision, a stale revision, and a retraction, with expected stored state.

### 3. Common Input Message Envelope

- [x] **Core design documented** — [Ingestion and Messaging Contract, §3](Ingestion-and-Messaging-Contract.md#3-raw-envelope) defines a separate registry-framed Avro raw envelope with exact payload bytes, stable operation/source identity, namespace/position, time, feed-clock, trace, and ownership context.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Freeze the Avro field names/types, per-source mandatory/optional rules, schema version, and representative create/update/retract/control frames. Verify compatibility with existing codecs and cached-schema startup behavior.
  - **Lead:** Reliable Ingestion & Delivery.
  - **Collaborator:** Distributed State & Stream Processing.
  - **Description:** Establish one transport envelope for inputs entering validation and processing.
  - **Decisions:** Source; message identity; observation/source reference; operation such as `create`, `update`, or `retract`; schema version; original payload preservation; source position; event and receipt timestamps; correlation/trace context. Specify which fields are mandatory for each source.
  - **Completion evidence:** Versioned envelope schema and representative messages for the supported operation types.

### 4. Canonical Write Contract

- [x] **Core design documented** — [Canonical and Storage Contract, §§4, 7–8](Canonical-and-Storage-Contract.md#4-selected-durability-and-failover-semantics) and [Wildfire Processing Contract, §7](Wildfire-Processing-Contract.md#7-durable-state-progress-and-output) define two-copy success, logical write outcomes, idempotent correction batches, atomic publication intent, and write admission.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Freeze the concrete canonical adapter schema, operation identity, revision/token fields, error mapping, and success/failure examples against [acceptance boundaries](Ingestion-and-Messaging-Contract.md#1-acceptance-and-completion-boundaries) and [replay completion](Validation-and-Replay-Contract.md#5-ordered-replay).
  - **Lead:** Distributed State & Stream Processing.
  - **Collaborators:** Platform, Coordination & Reliability and Reliable Ingestion & Delivery.
  - **Description:** Define how ingestion and processing safely change canonical state.
  - **Decisions:** API or message interface; upsert and retract requests; idempotency key; revision checks; fencing-token placement; success and error responses; retry behavior; atomicity between canonical changes and outgoing events; behavior when only part of a multi-record operation succeeds.
  - **Completion evidence:** Request/response or message examples for success, duplicate, stale revision, stale token, and temporary storage failure.

### 5. Ownership and Fencing Contract

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
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Freeze report receipt/error examples and the admission-ledger/publication recovery sequence. Verify evaluator support for mandatory client report IDs; agree acceptance tables and crash fixtures across input, processor, canonical, and recipient boundaries. Pinned broker disk-flush/replication behavior still needs proof.
  - **Lead:** Reliable Ingestion & Delivery.
  - **Collaborators:** Distributed State & Stream Processing and Platform, Coordination & Reliability.
  - **Description:** Define the persistent state required before any component claims that work has succeeded.
  - **Decisions:** Persistence required before HTTP `202`; broker acknowledgement points; received and processed offset advancement; transaction/outbox boundaries; delivery completion; crash recovery; duplicate-effect prevention after redelivery.
  - **Completion evidence:** A boundary-by-boundary acceptance table and expected recovery behavior for crashes immediately before and after each acknowledgement.

### 7. Storage Topology and Failure Model

- [x] **Core design documented** — [Canonical and Storage Contract, §§3–6](Canonical-and-Storage-Contract.md#3-distribution-and-constraints) selects source-key hash distribution, Citus/Patroni, four nodes, three copies, two-copy acknowledgement, placement, single-loss behavior, and replacement/rebalancing rules.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Platform must confirm capacity, coordinator/control-plane/DCS availability, version compatibility, and concrete placement/configuration. Add a joint read/write failure matrix; keep deployment and fault demonstrations as later validation gates.
  - **Lead:** Distributed State & Stream Processing.
  - **Collaborator:** Platform, Coordination & Reliability.
  - **Description:** Choose a deployment and data-distribution design capable of meeting the required storage and recovery constraints.
  - **Decisions:** Node count; partition unit and routing; replication factor; replica placement; write/read consistency; storage isolation; tolerated node and disk failures; replacement-node recovery; rebalancing; skew handling; degraded availability.
  - **Completion evidence:** Topology and placement example plus a failure matrix showing which reads and writes remain possible. Verify that replication and placement meet the specification's data-distribution requirements.

## P1 — Agree Before Implementing Each Affected Feature

These agreements can be drafted in parallel with P0. Finalize each one before its producers and consumers implement their shared boundary.

### 8. Satellite Acquisition-to-Processing Contract

- [x] **Core design documented** — [Ingestion and Messaging Contract, §5](Ingestion-and-Messaging-Contract.md#5-satellite-handoff-and-retention) defines original namespace/offset/payload preservation, gap detection, persistence-before-receipt-progress, and deletion only after durable processor handoff. [Wildfire Processing Contract, §7](Wildfire-Processing-Contract.md#7-durable-state-progress-and-output) separates prepared and applied progress.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Verify actual feed reconnect inclusion/exclusion, namespace, confidence, update/retract, and clock semantics. Freeze the durable handoff acknowledgement and authorized history-replay interface; supply reconnect/duplicate/update/retract fixtures before enabling transport deletion.
  - **Lead:** Reliable Ingestion & Delivery.
  - **Collaborator:** Distributed State & Stream Processing.
  - **Description:** Define the durable, replayable satellite input supplied to wildfire processing.
  - **Decisions:** Detection/update/retract payloads; identity and ordering guarantees; reconnect cursor; duplicate and gap handling; retention/replay expectations; separate durably received and successfully processed positions; checkpoint ownership.
  - **Completion evidence:** Example stream with reconnect, duplicate delivery, update, and retraction, including expected progress positions.

### 9. Wildfire Processing Semantics

- [x] **Core design documented** — [Wildfire Processing Contract](Wildfire-Processing-Contract.md) defines event time, transitive 1 km/360-minute connectivity, qualification, canonical mapping, stable identities, lineage, watermark policy, late corrections, and transactional state/output preparation.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Resolve restoration collisions and reference-compatible geography/identity rules. Select lateness from measurements, freeze feed-clock behavior, and turn the logical sequence table into concrete input/output fixtures. Restart/load evidence remains an implementation gate.
  - **Lead:** Distributed State & Stream Processing.
  - **Collaborator:** Reliable Ingestion & Delivery.
  - **Description:** Make derived wildfire results deterministic across normal execution, replay, and restart.
  - **Decisions:** Event-time field; spatial and temporal grouping; watermark and allowed lateness; deduplication; stable wildfire identity; update/retraction effects; merge/split behavior; state and progress checkpointing. Use specification-prescribed parameters where provided.
  - **Completion evidence:** Small reference input sequences with expected wildfire identities and outputs, including late input, merge, split, removal, and restart.

### 10. Broker Topology and Consumer Configuration

- [x] **Core design documented** — [Ingestion and Messaging Contract, §4](Ingestion-and-Messaging-Contract.md#4-streams-replication-and-capacity) separates satellite, coastal, and report raw streams, requires disk flushing plus replication, bounded consumption and reject-new capacity behavior. [Coastal Delivery and Validation, §3](Coastal-Delivery-and-Validation.md#3-durable-critical-path) specifies a dedicated durable recipient consumer.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Freeze subjects, stream and consumer names, retention/deletion mode, replica placement, acknowledgement timeout, retry/backoff and in-flight/resource limits with platform. Three input replicas are a proposed topology; verify pinned broker publish-ack durability and restart/fault behavior. M1 messaging remains the baseline, not the completed M2 configuration.
  - **Lead:** Reliable Ingestion & Delivery.
  - **Collaborator:** Platform, Coordination & Reliability.
  - **Description:** Align application messaging guarantees with the deployed broker's persistence and resource configuration.
  - **Decisions:** Streams and subjects; publishers and consumers; durable consumer names; retention and replication; ordering scope; acknowledgement policy; acknowledgement timeout; retry/backoff limits; redelivery; capacity and backpressure behavior.
  - **Ownership boundary:** Platform, Coordination & Reliability operates broker infrastructure. Reliable Ingestion & Delivery owns application stream and consumer semantics.
  - **Completion evidence:** Messaging topology/configuration table linked to the durability agreement and a broker-restart scenario.

### 11. Failure, Quarantine, and Replay Contract

- [x] **Core design documented** — [Validation and Replay Contract](Validation-and-Replay-Contract.md) defines compatible additions/lossless promotions, parking for incompatible changes, complete original-byte failure records, automatic transient retries, Ops-only replay, source-entity ordering, and repeated-failure handling.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Add concrete malformed/incompatible/transient/replay fixtures and record/API schemas. Resolve ordering for unparseable identities, oversized-input preservation, failure-store durability, and replay/live-operation serialization. Agree operational payload access separately from audit metadata access.
  - **Lead:** Reliable Ingestion & Delivery.
  - **Collaborator:** Distributed State & Stream Processing.
  - **Description:** Preserve failed input and recover it without blocking unrelated work or creating duplicate effects.
  - **Decisions:** Accepted versus incompatible schema changes; retryable versus parked failures; original payload and failure metadata; quarantine representation; inspection and replay authorization; ordering per `source_ref_id`; replay identity; poison-message isolation; behavior when one replayed message fails again.
  - **Completion evidence:** Examples of malformed input, incompatible schema, temporary failure, and successful replay with expected canonical effects.

### 12. Read and Audit API Contract

- [x] **Core design documented** — [Audit, Analytics, and Validation, §§1–5](Audit-Analytics-and-Validation.md#1-prescribed-audit-contract) defines endpoint parameters, ordering, filtering, pagination, checksum serialization, current location reporting, and explicit-session snapshot semantics.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Finalize the optional snapshot API shape, inactive audit population, cursor/error details, concrete responses and fixed checksum dataset. The [Validation and Replay Contract, §4](Validation-and-Replay-Contract.md#4-inspection-and-unprocessed) now defines failed states, recovery removal, and audit-versus-Ops access; add concrete Appendix A response/pagination/error fixtures and freeze the service interface.
  - **Lead:** Distributed State & Stream Processing.
  - **Collaborator:** Reliable Ingestion & Delivery.
  - **Description:** Define externally observable reads and audit evidence consistently across services.
  - **Decisions:** Endpoint methods, parameters, filtering, response schemas, status codes, global ordering, tie-breakers, pagination and concurrent-update behavior; exact checksum serialization and scope; `/location` meaning; `/unprocessed` representation. Copy prescribed rules from the M2 specification.
  - **Ownership boundary:** Distributed State & Stream Processing owns `/events`, `/checksum`, and `/location`. Reliable Ingestion & Delivery owns `/unprocessed`.
  - **Completion evidence:** Example responses and a fixed dataset with expected ordering, pages, checksum, and unprocessed entries.

### 13. Partial-Result and Completeness Contract

- [x] **Core design documented** — [Audit, Analytics, and Validation, §§2–3 and 6](Audit-Analytics-and-Validation.md#2-immutable-materialized-snapshot) defines bounded partial reads, safe ordered-prefix continuation, snapshot-copy fallback, and visible dashboard incompleteness.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Agree logical versus physical unavailable-unit identifiers and exact response examples with platform. Set measured request deadlines and snapshot lifetimes; validate per-unit fallback and capture cleanup during implementation.
  - **Lead:** Distributed State & Stream Processing.
  - **Collaborator:** Platform, Coordination & Reliability.
  - **Description:** Expose incomplete reads clearly when storage units are unavailable.
  - **Decisions:** Meaning and format of `partial` and `unavailable_units`; unit identifiers; timeout behavior; completeness determination with replicas; propagation through pagination, aggregations, audit responses, and dashboard displays.
  - **Completion evidence:** Complete and degraded response examples, with expected dashboard behavior and consistency with the read API contract.

### 14. Critical Notification Contract

- [x] **Core design documented** — [Coastal Delivery and Validation](Coastal-Delivery-and-Validation.md) defines sensor validation/isolation, critical warning delivery after durable raw acceptance independently of canonical writes, dedicated recipient consumption, transactional receipt/deduplication, fresh/backlog scheduling, and zero-loss/p95 ≤5-second measurement requirements.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Verify sensor critical/quality/correction and cursor/retention semantics. Freeze warning identity/payload, recipient routing/receipt schema, retry configuration, scheduling rule, and evaluator-compatible latency denominator/origin/window and error-budget accounting. Supply critical/retry examples and prove recipient durability.
  - **Lead:** Reliable Ingestion & Delivery.
  - **Collaborators:** Distributed State & Stream Processing and Platform, Coordination & Reliability.
  - **Description:** Define the critical coastal-warning path and how delivery reliability is measured.
  - **Decisions:** Critical-event criteria; event/revision triggering rules; notification payload; recipient routing; delivery identity; durable handoff; recipient acknowledgement; retries and duplicate handling; latency start/end points; loss accounting; reliability target and error-budget accounting.
  - **Completion evidence:** Example critical event and notification, expected retry behavior after recipient failure, and precise measurement definitions tied to the specification's thresholds.

### 15. Service and Deployment Boundaries

- [x] **Contribution documented** — [Canonical and Storage Contract, §5](Canonical-and-Storage-Contract.md#5-placement-on-four-kubernetes-nodes) provides canonical placement and resource requirements. [Ingestion and Messaging Contract, §4](Ingestion-and-Messaging-Contract.md#4-streams-replication-and-capacity) and [Coastal Delivery and Validation, §3](Coastal-Delivery-and-Validation.md#3-durable-critical-path) add broker isolation and recipient-storage needs.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Platform must complete the service inventory, capacity and HA plan, ports/configuration/Secrets, probes, placement, shutdown/rollout and admission conventions, required images, and reproducible startup procedure.
  - **Lead:** Platform, Coordination & Reliability.
  - **Collaborators:** Distributed State & Stream Processing and Reliable Ingestion & Delivery.
  - **Description:** Allow all component owners to build services that deploy together without incompatible operational assumptions.
  - **Decisions:** Service ownership; dependencies and discovery; ports; configuration and Secret names; persistent volumes; resource budgets; placement; startup/readiness/liveness probes; graceful shutdown; rolling-update expectations; required external/tester images; admission-control ownership and priority rules.
  - **Completion evidence:** Service inventory, configuration template, deployment conventions, and a reproducible startup procedure. Each component owner implements its own service manifests under these conventions.

### 16. Observability and Acceptance Evidence

- [x] **Contribution documented** — [Audit, Analytics, and Validation, §§7–8](Audit-Analytics-and-Validation.md#7-remaining-validation-gates) and [Wildfire Processing Contract, §8](Wildfire-Processing-Contract.md#8-required-evidence) list storage/audit/processing scenarios. [Validation and Replay Contract, §6](Validation-and-Replay-Contract.md#6-evidence) and [Coastal Delivery and Validation, §§5–7](Coastal-Delivery-and-Validation.md#5-delivery-evidence-and-reliability-target) add byte preservation, replay/auth, acceptance, recipient receipt, latency, and loss-reconciliation evidence.
- [ ] **Agreement finalized**
  - **Remaining before finalization:** Platform must assemble the shared metric/evidence matrix, names/labels, log/trace conventions, harness ownership, and integrated scenarios with ingestion/delivery. Listed scenarios are not executed results.
  - **Lead:** Platform, Coordination & Reliability.
  - **Collaborators:** Distributed State & Stream Processing and Reliable Ingestion & Delivery.
  - **Description:** Make integrated correctness, performance, and failure behavior measurable from the beginning.
  - **Decisions:** Trace/correlation propagation; structured log fields; timestamp conventions; essential counters and latency histograms; queue lag; ownership changes; stale-write rejection; storage completeness; replay results; notification outcomes; test assertions and evidence ownership.
  - **Ownership boundary:** Platform, Coordination & Reliability owns shared observability infrastructure and the end-to-end test harness. Each component owner supplies service instrumentation, scenarios, assertions, and interpretation for its area.
  - **Completion evidence:** Metric/evidence matrix assigning each acceptance criterion to a measurement, scenario, and responsible owner.

## Remaining Shared Work

- [ ] Freeze concrete schemas, request/response examples, broker configuration, and cross-service fixtures for the documented designs.
- [ ] Complete platform ownership/fencing, deployment, and observability agreements: **5, 15, and 16**.
- [ ] Resolve the compatibility questions listed per item, including report identity, feed/sensor behavior, wildfire lineage, and audit inclusion/extensions.
- [ ] Review the shared contracts with their producers and consumers; check **Agreement finalized** only when the completion rule is met.
- [ ] Make the first integration path reproducible: **one source → durable input → canonical storage → read or notification**, with owners, fixtures, assertions, and traceable evidence.

For contract changes, the proposing owner updates the affected contract; affected component owners review it and update shared fixtures before integration. Runtime, load, and fault results belong in the retained contracts' validation/evidence checklists and must remain unchecked until demonstrated.
