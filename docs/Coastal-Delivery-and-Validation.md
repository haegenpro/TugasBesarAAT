# Coastal Warning Delivery and Validation

Selected policies: validate and durably accept coastal observations before warning delivery; do not wait for canonical persistence; Tim Lapangan backend uses a dedicated durable JetStream consumer.

## 1. Characterize the Sensor Before Tuning

Run the prescribed unmodified Sensor Pantai image. Do not hard-code training IDs, seeds, ordering patterns, or profiles. Use only supported interfaces and observed contract behavior.

The supplied basic contract includes authenticated station/observation endpoints, an unauthenticated health endpoint, observation ordering by `(observed_at, observation_id)`, and opaque cursor pagination. It shows station identity/geography and observation identity, time, sea-level anomaly, wave height, quality, and critical flag. The example is not a full declaration of every valid numeric range, quality enumeration, correction rule, or retention period.

Characterize and record:

- Cursor stability, replay retention, inclusive/exclusive boundaries, duplicate pages, and session resets.
- Quiet periods, bursts, partial/malformed responses, latency, outages, and recovery behavior.
- Station metadata changes and safe cached metadata use.
- Freshness of `observed_at` versus `generated_at`, future timestamps, and clock assumptions.
- Valid types, ranges, quality meanings, critical-flag semantics, and update/retraction identity if supported.

Do not treat `/healthz` success as evidence that observation data is fresh. Verify the basic contract before deciding a source critical flag is valid. An apparent conflict between invalid critical input and the requirement to deliver all critical events must be clarified with evidence, not hidden by dropping it or inventing valid HazardEvents.

## 2. Breakers, Throttling, and Last-Known Data

Use endpoint-specific circuit breakers plus source-wide rate/concurrency limits. A failing observation endpoint need not invalidate usable cached station metadata. Conversely, a host-wide outage must not cause independent endpoints to create a retry storm.

Classify failures before counting them toward a breaker: invalid client authentication/configuration is not simply an intermittent network fault. Open breakers prevent ordinary repeated calls; bounded half-open probes test recovery automatically. Shared throttling also limits probes. Do not use arbitrary health success to reset the observation endpoint's failure/freshness state.

Choose deadlines, failure thresholds, probe cadence, freshness bounds, and retry backoff from characterization results. Existing M1 dated last-known/freshness envelopes are a starting point. Keep freshness metadata outside canonical HazardEvent attributes and retain server-side Media projection rules.

Serve last-known data with its actual observation/check times and stale/unavailable status, or a prompt explicit unavailable result. Never hold a client request until a dead source's long timeout expires. Preserve source separation so Sensor Pantai failures do not consume all workers/connections needed by BMKG, PVMBG, satellite, or reports.

## 3. Durable Critical Path

Logical flow:

Acquire and persist the raw observation, validate it, and publish a durable critical warning. The recipient consumes it, commits receipt and deduplication state transactionally, then acknowledges the delivery.

This is a sequence description, not a claim of one atomic transaction across all stages. Every handoff must be recoverable by a stable operation identity. Canonical mapping runs separately and may catch up later.

A raw validation worker cannot acknowledge away its input after an in-memory notification send. It must durably record/publish the warning intent, or verify an existing matching one, before retiring its obligation. Crash between warning publication and input acknowledgement may redeliver; retry the same notification identity. Broker publication deduplication has a finite window, so recipient durable deduplication remains required.

Warning identity is derived from verified source/session and observation/operation identity, with receiver/recipient scope as appropriate. It is distinct from a not-yet-created canonical UUID. Include provenance, observation time, validation context, critical status, and any known canonical link. Label the warning as a validated source observation; do not falsely claim canonical persistence. Later canonical completion does not send another initial warning; genuine corrections use distinct ordered operation identities.

The recipient backend consumes a dedicated durable JetStream consumer with scoped credentials and explicit acknowledgement. It commits receipt identity, payload/integrity evidence, and the intended recipient-visible inbox effect in one durable transaction before acknowledging. A restart after commit repeats acknowledgement without repeating the inbox effect. The recipient's database and credentials are service-owned and its availability/durability must be demonstrated separately from canonical Citus.

At-least-once transport plus transactional idempotent receipt gives duplicate-resistant logical delivery, not universal exactly-once networking. If the product later requires device display/human receipt, that is an additional acknowledgement boundary and must not be conflated with backend durable receipt.

Avoid dependencies on the canonical audit gate, canonical identity lookup, an unavailable registry for already-known codecs, or the failed-input database for every valid critical observation. Shared physical resources can still couple these paths; reserve capacity and measure interference. Platform, Coordination & Reliability owns global Tim Lapangan versus Media admission priority, while Reliable Ingestion & Delivery owns warning stream/consumer capacity and scheduling.

## 4. Fresh Warnings and Backlog

Prioritize fresh critical warnings while reserving finite capacity for older critical backlog. Establish a deterministic scheduling rule and expose backlog age/progress; exact weights are measured configuration. Do not allow fresh traffic to starve old warnings indefinitely.

Preserve operation order for the same observation identity, including corrections. Across different observations, out-of-order receipt is allowed under this scheduling policy. Display actual observation times and mark delayed/replayed warnings. Do not rewrite old observations to appear fresh.

Source acquisition must still preserve cursor continuity. This policy does not authorize jumping a sensor cursor to seek newer observations or dropping old pages. If the source exposes only chronological pages, acquisition itself may limit how quickly fresh observations can be discovered. Characterization and capacity must establish feasibility.

During downstream outage, critical warnings remain durable. Do not expire them merely because five seconds passed. On recovery, deliver them and retain the measured delay. If capacity is exhausted, the selected reject-new policy applies; a source without sufficient replay retention then creates a real end-to-end-loss risk that configuration alone cannot solve.

## 5. Delivery Evidence and Reliability Target

The assignment requires zero critical events lost and p95 latency at most five seconds. Define denominator, identity set, start/end boundaries, evaluation window, and in-flight accounting before testing. Match the evaluator's convention when supplied.

Record both source-observation-to-receipt and BNPB-first-acquisition-to-receipt latency, plus intermediate acquisition, validation, broker, queue, and recipient-commit spans. Neither metric may be silently substituted for the other. Retries/replay retain the original timing origin. Clock-skew tests require a verified common timing basis and explicit uncertainty; wall-clock subtraction across skewed hosts alone is not valid evidence.

Count delivered only at recipient durable commit. A broker publish acknowledgement or consumer fetch is not delivery. Reconcile source critical identities with durable accepted warnings and recipient receipts. Deduplicate counts by logical identity while retaining physical attempt metrics. Report pending/unreachable observations separately; absence from the receipt table is not automatically proven permanent loss, nor may it be excluded from the denominator without explanation.

Zero loss is a correctness target. The p95 target permits a tail but does not make outages disappear: report late fraction, tail/max, backlog, and catch-up alongside p95. If the actual failure scenario makes the target unattainable, document measured impact rather than resetting clocks or excluding old warnings.

Define warning-path SLI/error budget from the agreed target and evaluation window; record good/late/failed/pending outcomes and denominator. No unsupported availability percentage or budget duration is chosen here.

## 6. Integration and Fault Evidence

| Scenario | Expected evidence | Owner |
| --- | --- | --- |
| HTTP response lost after durable acceptance | Retry returns same receipt; no duplicate effect. | Reliable Ingestion & Delivery |
| Receiver crashes immediately after 202 | Accepted report remains recoverable. | Reliable Ingestion & Delivery + Platform, Coordination & Reliability |
| Concurrent same-key/different-content reports | One accepted content; explicit conflict for the other. | Reliable Ingestion & Delivery |
| Broker node/disk loss | Required accepted input remains recoverable; ack durability demonstrated. | Platform, Coordination & Reliability + Reliable Ingestion & Delivery |
| Stream at capacity | New acceptance fails explicitly; accepted unprocessed data is not evicted/expired. | Reliable Ingestion & Delivery |
| Canonical failover or explicit audit barrier | Critical backend receipts continue; applied canonical lag is visible separately. | Storage, platform, and ingestion owners |
| Warning published, worker crashes before input ack | Repeated publication creates one recipient inbox effect. | Reliable Ingestion & Delivery |
| Recipient commits, crashes before consumer ack | Redelivery repeats ack without duplicate receipt effect. | Reliable Ingestion & Delivery |
| Sensor outage/stale healthy response | Calls remain bounded; clients respond promptly with truthful freshness state. | Reliable Ingestion & Delivery |
| Critical backlog plus fresh warnings | Both make bounded progress; same-observation order holds. | Reliable Ingestion & Delivery |
| Registry unavailable | Known cached frames continue; unknown frames remain safely pending. | Reliable Ingestion & Delivery |
| Satellite reconnect/60x burst | No skipped operations; handoff/canonical lag measured separately with the storage owner. | Storage and ingestion owners |
| Combined failure game day | Automatic transient recovery, source isolation, receipt reconciliation, latency and budget evidence. | Platform, Coordination & Reliability harness; all component owners contribute |

## 7. Completion Checklist

- [ ] Characterization report derives validation and resilience parameters from observed behavior.
- [ ] Source cursor/retention behavior supports the claimed no-loss path.
- [ ] Broker durability and recipient-store failover are demonstrated with pinned versions.
- [ ] Satellite history replay works after broker retention removes handed-off messages.
- [ ] Failure inspection and replay authorization are enforced.
- [ ] Valid critical delivery remains independent of canonical write/snapshot delays.
- [ ] Full identity reconciliation proves no critical event lost in the tested window.
- [ ] Evaluator-compatible p95 measurement, tail, backlog, and recovery evidence are reported.
- [ ] Streams, retry queues, caches, spool files, failure records, and deduplication ledgers have explicit resource/retention bounds.
- [ ] Runtime/load/fault results are recorded before guarantees are called proven.

Only source and documentation consistency review was performed while generating this set. No source code, deployment, runtime test, or delivery experiment was executed.

## 8. Coastal Mapping and Warning v1

The local deployment manifest is [sensor-pantai.yaml](../deployment/sensor-pantai.yaml), using the prescribed immutable image and Secret reference. Its presence does not prove the image has been characterized. Station metadata supplies province and coordinates; observation metadata supplies identity/time and measured values. A station lookup failure keeps work retrying rather than fabricating geography.

The initial adapter maps `source=SENSOR_PANTAI`, `hazard_type=COASTAL`, `source_ref_id=observation_id`, `area_name=station.province`, station coordinates, and `occurred_at=observed_at`. It preserves station ID, sea-level anomaly, wave height, quality, and the critical flag in typed `attributes`. Critical observations map to AWAS; noncritical observations map to NORMAL, with no invented SIAGA/WASPADA numeric thresholds. This is a project mapping, not an additional claim about Appendix C. Validate required types, finite coordinates/measurements, valid UTC time, boolean critical, and a known station. Quality meanings and operation/version semantics still require actual source evidence; do not infer a new quality enum from the one example.

The canonical UUID/first ingestion time belongs to canonical storage and is not required for warning publication. Repeated observations under the same identity/current content do not cause another warning or canonical revision. Any supported correction has a distinct source operation position and preserves per-observation order.

Warning JSON schema v1 fields: `warning_version:1`, `notification_id`, `recipient:"TIM_LAPANGAN"`, `source:"SENSOR_PANTAI"`, `source_namespace`, `observation_id`, `operation_id`, `operation_position` (int64), `operation` (`create`, `update`, or `retract`), `observed_at`, `first_received_at`, `critical`, station ID/province/coordinates, typed measurement/quality values, original payload digest, correlation/trace context, and nullable `hazard_id`. Raw source bytes remain behind ingestion authorization; the warning contains validated fields. The exact fields/types/null defaults are in [coastal-warning-v1.avsc](fixtures/coastal-warning-v1.avsc), registered under `bnpb.coastal-warning-value` and using M1 registry framing. Station and measurement fields are top-level as named in that schema.

`notification_id` is `warning-v1:` plus SHA-256 of UTF-8 compact JSON `["TIM_LAPANGAN",source_namespace,observation_id,operation_id]`. Retrying the same operation retains the identity, payload, and timing origin. Publish with `Nats-Msg-Id=notification_id` to `bnpb.warning.coastal`; recipient consumer is `tim-lapangan-v1`. The recipient transaction inserts its unique notification receipt and inbox effect together, then confirms the broker ack. Changed payload under a known notification identity is an integrity conflict and remains visible for investigation; it is not treated as successful duplicate receipt.

The validation stage persists both canonical-work and warning intents before retiring raw input. For noncritical valid input it records the explicit no-warning result. Recipient commit is delivery success; canonical completion cannot create a second initial warning. Preserve accepted critical warnings indefinitely until their recipient obligation finishes during the evaluation window.

Scheduling uses weighted round-robin: at most three ready fresh-observation deliveries, then at least one ready backlog delivery whenever both classes contain work. A waiting predecessor for the same observation is served before its correction regardless of class. Retry delays release the worker and do not block other identities. These initial weights can be tuned without changing no-starvation or same-observation ordering guarantees. No scheduling policy jumps a source cursor.

Measure timeliness primarily from first BNPB acquisition to durable recipient commit and additionally from source `observed_at`; publish both until evaluator timing is confirmed. The reliability target is zero missing critical logical operations and at least 95% delivered within five seconds over the declared game-day window. The timely-delivery budget is 5% of distinct required critical operations; pending-at-window-end operations count as not timely unless later evidence establishes an on-time receipt. Missing/late events are not removed from the denominator. Zero-loss reconciliation remains separate from the latency budget. Keep original timing across retries and include clock uncertainty. This defines a measurement contract; it does not claim achieved results or override a supplied evaluator convention.
