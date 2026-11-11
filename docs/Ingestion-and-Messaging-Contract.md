# Ingestion and Messaging Contract

This contract defines ingestion and messaging behavior. Final schemas, stream configuration, and durability evidence remain implementation work.

## 1. Acceptance and Completion Boundaries

| Boundary | What has succeeded | What is not implied |
| --- | --- | --- |
| Field-report HTTP 202 | The original submission and identity are durably accepted in the input path. | Validation completion, canonical commit, or notification delivery. |
| Satellite received progress | All source operations through a contiguous position are durably recoverable. | Wildfire preparation or canonical application through that position. |
| Processing handoff | Required history/state/output intent is durably committed by the owning processor. | Every output is already canonically applied. |
| Canonical applied | Required canonical effect has committed under the canonical storage service’s contract. | Recipient receipt. |
| Warning delivered | Tim Lapangan backend has durably committed the receipt and deduplication state. | Human viewing or device display. |

Use broker-first acceptance for field reports. Do not return 202 on an in-memory queue insertion, ordinary Core NATS fire-and-forget publish, or a publish whose required durable outcome is unknown. If persistence fails or capacity is exhausted, return an explicit bounded retryable failure. Unknown publish outcome is reconciled through the same idempotency identity; it is not proof the report was absent.

The HTTP path authenticates, enforces admission/size limits, and establishes a stable request identity. It does not wait for canonical processing. Exact response fields and error codes must respect the assignment and existing API conventions. A receipt may expose accepted/pending status; never mislabel it as a committed HazardEvent.

## 2. Report Idempotency

Require a stable client-generated report ID or key, scoped to the authenticated submitting identity. Preserve it across retries, including when the original HTTP response is lost. Header/body placement must be documented and checked against evaluator clients before becoming mandatory.

For the same scoped key:

- Same accepted content returns the existing receipt/outcome without another logical effect.
- Different content produces an explicit conflict; it must not silently overwrite the original report.
- An in-progress/unknown attempt is resolved, not automatically treated as a new report.

Define a versioned request-content equality rule. Do not fingerprint transient headers or authentication token bytes. Do not deduplicate independent reports merely because their bodies happen to match.

**Derived requirement:** retain a durable admission ledger beyond JetStream's duplicate-suppression window. It records scoped identity, content fingerprint, original receipt, and acceptance state, with serialized reservation of concurrent attempts. A pending reservation is not permission to send 202. Reconcile the reservation and broker publication using the same stable message identity so a crash between either step can be recovered safely.

The exact ledger storage/transaction sequence is an implementation gate, not an already demonstrated cross-system transaction. Broker-first describes the durable payload acceptance boundary; it does not remove ledger dependencies required for conflict detection. If ledger availability is required to establish identity safely, reject or defer that request rather than accepting conflicting payloads. For the evaluation session, retaining compact identity records is a proposed conservative default; do not silently expire the guarantee with the broker retention window.

Downstream consumers also deduplicate transactionally with their effects. Duplicate suppression at publication is insufficient for an acknowledgement lost after a database commit. M1's use of `hazard_id` for initial canonical messages must not be reused for every revision: operation identity must distinguish legitimate corrections.

## 3. Raw Envelope

Reuse M1's registry-framed Avro convention: magic byte, schema identifier, then datum, with exact framing validated against existing codecs. The raw envelope is a separate schema from the canonical HazardEvent envelope.

| Logical field | Meaning |
| --- | --- |
| Envelope/schema version | Evolves transport metadata independently from external payloads. |
| Source and source entity | Identifies the external producer and record kind. |
| Operation identity | Stable across delivery/replay; distinct for legitimate updates/retractions. |
| Entity/source reference | Identity whose ordered mutations must be preserved, when extractable. |
| Operation type | Detection/create, update, retract, or source-specific control operation. |
| Original payload bytes | Opaque exact input, including invalid UTF-8 or malformed JSON. |
| Payload/content metadata | Source schema identifier/content type and a byte-integrity digest where available. |
| Source namespace and position | Original satellite namespace/offset, sensor cursor context, or report identity. |
| Event/receipt time | Separate source-observation and BNPB receipt semantics; unknown event time is explicit. |
| Feed clock context | Recorded `virtual_now` context when applicable; not a worker-clock substitute. |
| Trace/correlation context | Continues M1 cross-service evidence. |
| Ownership context | Applicable source token; not a replacement for broker credentials or current write fencing. |

These are logical fields to finalize in a shared schema, not an invented mandatory naming style. Never require successful payload parsing to preserve the bytes. If entity identity cannot be safely extracted from malformed data, preserve transport-level identity and quarantine context rather than guessing a source reference.

A stable raw envelope prevents each external schema change from forcing a new transport schema. Pre-register and durably cache the envelope codec/schema identity. Registry outage can use known cached schemas; an unknown frame is retained for retry/inspection rather than decoded by guesswork. The critical path must not perform unnecessary registry lookups for already known codecs. Cold-start behavior without any verified cache remains a readiness test.

## 4. Streams, Replication, and Capacity

Separate raw streams for satellite, coastal observations, and field reports. Critical-warning output uses an independently controlled durable delivery path; canonical output keeps its existing contract. Names are configuration, not chosen conventions in this document.

Each raw stream has independent capacity, retention, consumer limits, and monitoring. Separate streams still share CPU, disk, network, and broker processes; Platform, Coordination & Reliability must enforce budgets. Preserve other sources during a sensor flood. Pull/batch consumption with bounded in-flight work is a proposed default because it makes backpressure explicit.

Selected persistence policy requires synchronous disk flushing plus replication. A proposed starting topology is three replicas per input stream on distinct nodes, chosen for single-node-loss tolerance; it is not a separate user-selected topology and must fit the four-node cluster's budgets. Verify pinned NATS support and exactly which writes/replicas are flushed before publish acknowledgement. Do not claim quorum receipt alone proves synchronous disk persistence. No runtime benchmark or fault proof is supplied here.

At capacity reject new publications instead of evicting unprocessed accepted messages. No age/count/byte retention limit may silently override that requirement. Expiry and discard configuration must be reviewed together; a discard-new policy alone does not prevent age-based deletion. Connector retry state must not advance source progress on a failed publication. HTTP 202 is unavailable when required acceptance storage is unavailable.

For nonreplayable sources, pausing acquisition can lose upstream observations even though the broker lost none. Characterize the source and provision capacity/recovery accordingly; do not assert zero end-to-end loss from broker configuration alone.

## 5. Satellite Handoff and Retention

Preserve detection/update/retract/control events, original namespace, monotonically ordered source positions, original detection-key components, and recorded feed-clock context. Determine the feed's exact `from`/Last-Event-ID inclusion rule experimentally. Detect gaps; never advance a contiguous position over a missing operation.

Advance durably received progress only after confirmed recoverable persistence. A crash after publication but before cursor update may cause redelivery, which is safe only with stable operation identity. A cursor written before persistence can cause loss and is prohibited.

The wildfire processor retains complete session history and processing state. Remove satellite transport messages only after that owner confirms the required durable handoff, including raw operations and replay context. Acknowledge after commit, not on callback entry. If other required durable consumers need the same message, their obligations must also be met before deletion. The concrete retention mode must implement this lifecycle rather than merely relying on a short age limit.

Once broker copies are removed, historical replay uses the wildfire processor’s authorized history interface; the ingestion service does not receive canonical database credentials. That interface must exist and be tested before removal is enabled. Eventual canonical-output application remains distinct from durable handoff.

## 6. Fencing and Snapshot Interaction

Coordinate source polling ownership with Platform, Coordination & Reliability and carry context to the canonical storage service. A broker accepting an authenticated publication does not prove the publisher is still the source owner. Canonical and processor state mutations require current fenced authority at their commit boundary.

Already accepted valid input survives owner takeover. Define which stage validates old acquisition tokens and how a successor can process legitimate queued work; do not indiscriminately discard it because the producing lease expired.

An explicit canonical audit barrier can delay canonical output application while raw acceptance remains available. Critical coastal delivery must not acquire that canonical gate. Respect resource bounds: a prolonged backlog does not justify false 202 responses, silent eviction, or claiming received progress is applied progress.
