# Wildfire Processing Contract

Lead: Distributed State & Stream Processing. Collaborator: Reliable Ingestion & Delivery for the durable satellite handoff; Platform, Coordination & Reliability for processor ownership, infrastructure, and takeover.

Selected design: one active logical processor, durable session history and lineage, automatic late corrections, and transactional state/output preparation followed by idempotent canonical application. The [M2 assignment](IF4031%20-%20Tugas%20Besar%20-%20M2.md) is authoritative. Implementation evidence and evaluator compatibility remain open as specified below.

## 1. Input Boundary

Reliable Ingestion & Delivery provides a durable, replayable stream containing the original feed operations. The handoff must preserve:

| Information | Required meaning |
| --- | --- |
| Stable feed/session namespace | Distinguishes independent feed histories; unchanged on reconnect or replay of the same session. |
| Original feed offset | Ordered operation position, not a newly assigned delivery counter. |
| Operation type | Detection, update, retract, or heartbeat according to the feed contract. |
| Original payload and identity components | Preserve bytes/lexical values needed for identity and audit; never rebuild detection keys from rounded floats. |
| Feed clock observations | Validated `virtual_now` samples and their ordered association with processing progress. |
| Receipt timestamp | For operational latency; not satellite event time. |
| Correlation/trace context | Links acquisition, processing, storage, and publication. |
| Ownership context | The applicable application fencing token under the shared contract. |

The assignment describes feed offsets as monotonically increasing without gaps. Reliable Ingestion & Delivery must verify reconnect inclusion/exclusion semantics, update/retract references, confidence representation, and feed-clock behavior using actual responses. Do not infer these details from endpoint names. A detected gap is visible and blocks progress that would silently skip operations.

Process source mutations in feed order. Duplicate operation positions are idempotent. A newer position referring to an existing observation is not a duplicate merely because the observation key is unchanged.

## 2. Detection Identity and Time

The prescribed detection identity is:

```text
satellite|acq_date|acq_time|latitude|longitude
```

Preserve each component exactly as supplied for the audit `detection_keys` array. Maintain parsed coordinates and event time separately. For example, a time value representing `243` may be interpreted as 02:43 UTC for computation without rewriting its identity representation to `0243`.

Detection time is `acq_date` plus `acq_time` in UTC. Receipt time, processing time, and a Kubernetes node's wall clock do not determine connectivity. Validate finite coordinates, valid dates/times, confidence values, and FRP according to the feed contract. Invalid input is preserved with a reason through Reliable Ingestion & Delivery's failure handling, rather than fabricated into a valid detection.

Retain excluded low-confidence detections in durable history as required for reproducibility; exclude them from the qualifying detection graph. Verify the feed's encoding of low/nominal/high confidence before assigning numerical or lexical rankings.

## 3. Connectivity and Qualification

Two included detections have an edge if both conditions hold:

- WGS84 ellipsoidal geodesic distance is at most 1,000 metres.
- Absolute detection-time difference is at most 360 minutes.

Distance uses the selected geodesic method, with evaluator compatibility still to be checked. Compare the unrounded computed distance to the threshold. Do not add an undocumented tolerance that changes membership.

A fire is a connected component with at least two detections, including detections from different satellites. Connectivity is transitive: A-B-C can form one fire even when A-C has no direct edge. Count singletons separately. An implementation using geographic indexes or time windows must preserve cross-boundary edges and transitive components; a fixed window is not the fire definition.

Updates can add or remove edges; retractions can shrink, split, or eliminate a component. An insertion-only union-find structure alone is insufficient for all these operations. The exact index and affected-component recomputation algorithm are implementation details to benchmark against a simple reference calculation.

## 4. Canonical Wildfire Mapping

| Field | Rule |
| --- | --- |
| `source` | SATELIT. |
| `hazard_type` | WILDFIRE. |
| `source_ref_id` | Stable fire identity under section 5. |
| `hazard_id` | Persisted UUIDv4 for that identity. |
| `occurred_at` | Earliest included member detection time. |
| `latitude`, `longitude` | Equal-weight spherical vector mean of unique included member detections. |
| `area_name` | Province whose static reference center is nearest the calculated fire center; reference dataset and tie behavior must be documented. |
| `severity` | AWAS if count >=20 or total FRP >=100 MW; otherwise SIAGA if count >=6 or total FRP >=30 MW; otherwise WASPADA. |
| `attributes.detection_count` | Count of unique included members. |
| `attributes.satellites` | Unique contributing satellites. |
| `attributes.max_confidence` | Highest contributing confidence under the verified source ranking. |
| `attributes.total_frp_mw` | Sum of included members' current FRP in MW. |
| `attributes.detection_keys` | Exact preserved member keys. |

For the selected center, convert latitude/longitude to unit vectors, sum them with equal weights, and convert the mean direction back to latitude/longitude. This is a representative spherical center, not an ellipsoidal centroid. FRP is not a weight. If the mean vector is degenerate, do not fabricate a center: make the case explicit and resolve a reference-compatible rule before supporting it.

Use deterministic detection-key ordering for aggregation and array output. Preserve numeric source precision and define a stable FRP summation method; round only for presentation, not severity decisions. Pin the province reference data and numerical implementation versions for reference comparisons. Center formula and distance-model compatibility remain validation gates because the assignment does not prescribe their detailed algorithms in the uploaded text.

## 5. Fire Identity and Lineage

### New identity

Derivation inputs are:

1. The stable feed/session namespace.
2. The earliest qualifying member detection key at identity creation, with detection-key ordering breaking equal-time ties.
3. The original feed offset of the operation that creates that identity.

Encode the tuple unambiguously. Its opaque serialization and optional digest algorithm are implementation details to document and version; no particular prefix, digest length, or UUID variant was approved for `source_ref_id`. The representation must not allow delimiter ambiguity or silently conflate different tuples.

Freeze the identity after creation. An earlier late detection or changed member set does not recompute it. Replaying the same ordered, namespaced operations with the same algorithm and recorded clock decisions reproduces fire source identities. This does not promise the same identities for an arbitrarily reordered final set or differently segmented source sessions.

`hazard_id` remains the selected UUIDv4 assigned once and preserved in the identity registry. A rebuild that discards that registry need not reproduce the same UUIDs.

### Merge

The predecessor fire containing the earliest detection retains its identity. Equal-time ties use detection-key comparison, not random UUID ordering. Other predecessors become inactive and link internally to the survivor. Recompute the survivor's canonical content and publish the complete correction batch.

### Split

For a split without historical restoration, the qualifying child containing the earliest remaining detection retains the current identity. Only children of size at least two qualify. Other qualifying children receive new identities using the split operation's original offset and their own earliest keys. Singletons are counted without inventing active fires.

### Exact historical restoration

Before allocating a new child identity, check retained inactive lineage. Exact restoration means equality of the full detection-key membership set, not equal centroid, equal count, or approximate geographic overlap. If that set uniquely restores a historical fire in the relevant lineage, reactivate its existing source identity and UUID. Recompute attributes from the latest accepted observation values; equal membership does not imply equal FRP or severity.

Historical restoration takes precedence over creating a new identity. If a historical match conflicts with the ordinary surviving identity or more than one historical identity matches the same set, the precise precedence is unresolved: record a fixture and bring back the ambiguity rather than silently choosing by UUID. The selected policies do not authorize arbitrary identity reassignment. These collision cases must be resolved before declaring lifecycle semantics complete.

### Elimination

When no qualifying child remains, retain a tombstone and lineage. Exclude the fire from active client/analytics results. Preserve ordering information so older operations cannot resurrect it. Audit membership of inactive fires must be validated against the evaluator.

### Shared fixtures

| Sequence | Expected logical result |
| --- | --- |
| Same detection operation delivered twice | One observation and no duplicate fire effect. |
| Two eligible neighboring detections | One fire. |
| Two fires joined by a new bridge | One survivor using earliest-detection rule; loser inactive and linked. |
| Bridge retracted | Qualifying children restored/split according to lineage; no extra active parent. |
| Previously merged membership exactly restored | Old source identity and UUID reactivated when matching is unambiguous. |
| Remaining component has one member | No active fire; singleton reported. |
| Correction followed by an older redelivery | Newer state preserved. |

## 6. Watermark and Late Corrections

Selected watermark basis is validated feed `virtual_now` minus a fixed configurable lateness allowance. Before relying on it, determine session reset semantics, timestamp trustworthiness, behavior during silence, and behavior at accelerated playback. Journal the clock observations used for classification so replay does not depend on new HTTP timing or a shifted worker clock. Watermark progress within a session must not move backward because of an out-of-order sample; session changes require an explicit reset protocol.

Select the allowance from measured feed lateness and document the coverage/margin. Keep it unchanged during a test run. No numeric value is fixed here. A watermark is a progress estimate, not permission to permanently discard observations or declare a connected fire impossible to change.

Inputs beyond the allowance are retained and counted, then automatically reconciled by bounded correction work. Live processing and critical warning paths have priority. The correction queue belongs to the same single logical state owner or an explicitly serialized command path; it is not an uncoordinated second writer to lineage state.

Measure pending correction count, oldest pending age, and correction completion. Report any reference differences caused by an unfinished correction backlog rather than claiming complete results prematurely.

## 7. Durable State, Progress, and Output

Track at least these separate positions:

| Position | Meaning |
| --- | --- |
| Durably received | Reliable Ingestion & Delivery has persisted raw input through this contiguous source position. |
| Prepared | Processor state, lineage, and output intents are committed through this position. |
| Canonically applied | Every required output batch through this contiguous position has been applied. |

Persist no-output progress as well. A heartbeat or duplicate may produce no canonical change but still participates in source replay/progress rules. A completed later batch must not jump the contiguous applied frontier over an earlier failed batch.

The selected transaction boundary covers processing state changes, lineage, prepared progress, and output outbox entries. Persist affected records in distributed storage, with small checkpoint metadata separate from the full session history. One active owner does not require one node to retain a complete history blob.

The output delivery stage applies deterministic operation identities and monotonically checked revisions. A merge/split's canonical changes, applied-operation record, and downstream publication intents must commit as one logical correction batch. Cross-shard atomicity may still require Citus distributed transactions. Do not describe the staged outbox design as eliminating those transactions.

For duplicate output delivery, return the committed batch result without another revision or notification effect. A commit timeout is ambiguous and must be resolved by replaying the same batch identity. Publication to a broker may occur more than once after an acknowledgement/commit race; downstream deduplication remains necessary.

On processor failure, Platform, Coordination & Reliability's ownership mechanism allows one successor. The successor restores durable state, resumes from committed prepared progress, and drains pending outputs. Protected state commits must reject stale owners. Already committed output intents are durable work, not automatically invalid merely because the preparing owner later lost its lease; define delivery authorization accordingly.

During an explicit audit capture, the canonical output applier honors the shared admission gate. Preparation and durable input receipt may continue within resource limits; neither is reported as applied canonical progress while commits are gated. Resume output application after the capture views are pinned and the barrier is released.

Retain complete session input and lineage durably. Retention after the evaluation session is configurable and must respect active snapshots/recovery needs. Bound caches, queues, batch sizes, and memory; choose values by measurement rather than assuming the complete graph fits the event-consumer pod limit.

## 8. Required Evidence

- [ ] Baseline/reference graph agrees on identity-independent membership and aggregate values.
- [ ] Identity fixtures cover late older anchors, merge, split, exact restoration, and tombstones.
- [ ] Ambiguous historical-restoration precedence is resolved explicitly.
- [ ] Crash before preparation commit leaves no advanced prepared position.
- [ ] Crash after preparation commit preserves state and pending outputs.
- [ ] Crash after canonical commit but before delivery acknowledgement causes no duplicate effect.
- [ ] Stale processor cannot mutate state after takeover.
- [ ] 60x playback loses no input and catches up within the required 60 seconds after the burst.
- [ ] Entry-to-canonical-store p95 latency and lag are visible.
- [ ] Uninterrupted and restarted runs produce the same final logical results; compare UUIDs only when using the same retained identity registry.
- [ ] Live-feed reconnect resumes at the verified position without gaps or duplicate effects.
- [ ] Correction work remains bounded and does not starve live work or critical notifications.

## References

Assignment authority: M2's Definisi Kebakaran, Aturan Pemetaan Tambahan, Problem 4, and Appendix A. See [Canonical and Storage Contract](Canonical-and-Storage-Contract.md) for identity, persistence, and storage boundaries.

[GeographicLib geodesic definitions](https://geographiclib.sourceforge.io/html/python/geodesics.html) support the chosen ellipsoidal distance terminology; they do not establish what the evaluator uses.
