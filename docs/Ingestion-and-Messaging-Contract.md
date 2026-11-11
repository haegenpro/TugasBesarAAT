# Ingestion and Messaging Contract

This contract defines ingestion and messaging behavior. The interfaces below are documentation contracts; codec integration, configuration rollout, and durability evidence remain implementation work.

## 1. Acceptance and Completion Boundaries

| Boundary | What has succeeded | What is not implied |
| --- | --- | --- |
| Field-report HTTP 202 | The original submission and identity are durably accepted in the input path. | Validation completion, canonical commit, or notification delivery. |
| Satellite received progress | All source operations through a contiguous position are durably recoverable. | Wildfire preparation or canonical application through that position. |
| Processing handoff | Original history and its pending-processing obligation are durably committed by the owning processor; preparation is a separate later transaction. | Every output is already canonically applied. |
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

Request-content equality v1 is specified in §10. Do not fingerprint transient headers or authentication token bytes. Do not deduplicate independent reports merely because their bodies happen to match.

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

The logical field table is implemented by the exact Raw Input v1 schema in §7. Never require successful payload parsing to preserve the bytes. If entity identity cannot be safely extracted from malformed data, preserve transport-level identity and quarantine context rather than guessing a source reference.

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

The wildfire processor retains complete session history and processing state. Remove satellite transport messages only after that owner confirms the required durable handoff, including raw operations and replay context. Acknowledge after durable history receipt, not on callback entry or only after canonical application. If other required durable consumers need the same message, their obligations must also be met before deletion. The concrete retention mode must implement this lifecycle rather than merely relying on a short age limit.

Once broker copies are removed, historical replay uses the wildfire processor’s authorized history interface; the ingestion service does not receive canonical database credentials. That interface must exist and be tested before removal is enabled. Eventual canonical-output application remains distinct from durable handoff.

## 6. Fencing and Snapshot Interaction

Coordinate source polling ownership with Platform, Coordination & Reliability and carry context to the canonical storage service. A broker accepting an authenticated publication does not prove the publisher is still the source owner. Canonical and processor state mutations require current fenced authority at their commit boundary.

Already accepted valid input survives owner takeover. Define which stage validates old acquisition tokens and how a successor can process legitimate queued work; do not indiscriminately discard it because the producing lease expired.

An explicit canonical audit barrier can delay canonical output application while raw acceptance remains available. Critical coastal delivery must not acquire that canonical gate. Respect resource bounds: a prolonged backlog does not justify false 202 responses, silent eviction, or claiming received progress is applied progress.

## 7. Raw Input v1 and Broker Configuration

The exact Avro field names, types, and null defaults are in [raw-input-v1.avsc](fixtures/raw-input-v1.avsc). Frame the datum with M1's zero magic byte and four-byte big-endian registry ID. Envelope version is `1`; its registry subject is `bnpb.raw-input-value`, separate from the canonical envelope. Registry registration/caching uses the existing compatibility workflow. `payload` is Avro bytes; JSON examples encode those bytes as base64 for readability, not as a replacement wire type. `payload_sha256` is the lowercase SHA-256 of the exact captured bytes, and `payload_size` is their nonnegative byte count. Normally `payload_ref` is null and payload bytes are inline. For an oversized acquired input, `payload_ref` is an opaque ingestion-owned durable object reference, inline `payload` is empty, and digest/size still describe the original bytes; consumers resolve through the authorized ingestion service, never direct storage credentials. All time strings are UTC RFC 3339. Nullable fields use explicit Avro nulls.

Common required fields are version, source, message identity, namespace, operation, received time, original payload or durable reference, digest, byte count, and content type, correlation ID, and acquisition ownership scope/epoch/holder. `traceparent` and source schema ID are nullable. `event_time` is null when unsafe to extract. No failed parse may prevent original-byte preservation. A missing or untrusted entity reference is null, never fabricated.

| Source | Operation / required source context |
| --- | --- |
| `BMKG`, `PVMBG` | `create`, `update`, or `retract`; source reference when parseable; producer-persisted acquisition position within the connector namespace. Preserve existing seismic/warning lifecycle distinctions. |
| `SATELIT` | `detection`, `update`, `retract`, or `heartbeat`; original feed offset in `source_position`; stable configured session namespace. Original body bytes preserve detection-key lexical components. Preserve observed `virtual_now` when present. |
| `SENSOR_PANTAI` | `create`, `update`, or `retract` only as supported by the verified sensor contract; observation ID when parseable; original page cursor in `source_cursor`, local durable acquisition sequence in `source_position`. |
| `FIELD_REPORT` | `create`; authenticated submitter/report scope, stable report ID, receipt identity. This is a transport source, not an added HazardEvent source enum. It remains in the separate durable report workflow; it does not create canonical HazardEvents. |

`message_id` is `raw-v1:` followed by SHA-256 of the UTF-8 compact JSON array `[source,source_namespace,source_position,source_ref_id,operation]`; JSON nulls remain null. `source_position` must identify a distinct logical operation and is persisted before retry; it is never a broker redelivery counter. An envelope retry preserves message ID, bytes, receipt origin, and source context. Digest equality alone does not identify two operations. Same message ID with different logical content is quarantined as an identity conflict, not overwritten. Use the same message ID as `Nats-Msg-Id`.

| Stream | Subject | Required durable consumer / completion |
| --- | --- | --- |
| `BNPB_RAW_SATELLITE` | `bnpb.raw.satellite` | `wildfire-handoff-v1`; ack after durable history append under §8. |
| `BNPB_RAW_COASTAL` | `bnpb.raw.coastal` | `coastal-validation-v1`; ack after complete validated fan-out intents or durable quarantine. |
| `BNPB_RAW_REPORTS` | `bnpb.raw.reports` | `field-reports-v1`; ack after the durable report processing outcome. |
| `BNPB_CRITICAL` | `bnpb.warning.coastal` | `tim-lapangan-v1`; ack after recipient inbox commit. |

Raw streams and warnings use file storage, interest retention with all required durable consumers provisioned before publishers start, discard-new, no age expiry (`MaxAge=0`), and three replicas on distinct nodes. Application readiness fails if required consumers/configuration are missing; do not publish into a stream with no retaining consumer. Satellite has one required handoff consumer. Coastal validation persists canonical and warning fan-out obligations before input ack; it does not rely on a second raw consumer appearing later. Once the handoff owner has committed, that owner retains recovery obligations even if transport data is deleted.

Initial per-stream limits are 1 GiB, maximum message size 1 MiB, no separate message-count eviction, duplicate window 120 seconds, explicit ack, `AckWait=30s`, `MaxDeliver=-1`, `MaxAckPending=128`, pull batches at most 32, and fetch wait at most 1 second. These are configurable starting values, not evidence of adequacy; platform budgets and observed feed rates determine the deployed limits. Require server-side `jetstream { sync_interval: always }` on all replica hosts plus the three-replica stream policy; pin the deployed broker image and prove its publish-acknowledgement boundary before enabling durable HTTP acceptance. The [official NATS persistence guidance](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/README.md) distinguishes OS writes from fsync; the [configuration reference](https://docs.nats.io/reference/config/) documents the server setting. Missing capability is a deployment blocker, never permission to weaken 202 semantics. Known codecs must work from durable cache; unknown frames remain pending during registry outage.

Application transient retries use full-jitter capped exponential delay from 250 ms to 30 seconds, releasing workers between attempts. Retry indefinitely while classified transient; persistent age raises alerts, not silent deletion. Processing longer than AckWait sends in-progress acknowledgements only while the worker still owns the work. Consumer deduplication is durable beyond the 120-second publication window. Stream limits reject new acceptance without expiring already accepted pending records.

## 8. Satellite History Handoff v1

The wildfire/history-owning Aggregator exposes private authenticated `POST /internal/satellite/history` with `{contract_version:1, ownership:{scope,epoch,holder}, messages:[RawInput...]}`. JSON transport uses the §7 logical field names and base64 payloads. A request contains one namespace and source `SATELIT`, at most 32 operations in source order. Public clients and audit tokens cannot append history or read raw payload history.

Return `200` only after original operations, byte-integrity checks, receipt frontier, and a durable pending-processing record commit with the history owner's required replication. Response: `{namespace, durable_through, items:[{message_id,outcome}]}`, with outcomes `stored` or `duplicate`. Raw history receipt does not imply advanced prepared state or canonical application. The processor can restart from committed history without asking the source connector to reproduce retired broker copies.

An already stored identical operation is a duplicate. Same namespace/offset with different bytes/operation identity returns `409 identity_conflict`; a gap returns `409 source_gap` with `expected_position`. Neither advances the contiguous frontier. Namespace changes require an explicit session transition; they are not inferred from reconnect. Invalid data is preserved/quarantined without falsely declaring an offset processed. `403 stale_owner` and `503 temporarily_unavailable` preserve pending broker work; timeout is ambiguous and retries use identical operation identities. The two-second initial request deadline is independent of the lifetime of retained work.

After success, ingestion advances only its contiguous confirmed frontier, then acknowledges the corresponding broker messages. Crash before broker ack repeats the append and gets duplicates. Crash before durable history commit produces no success/frontier advance. The first expected offset is recorded from the configured session start; do not guess zero or one before inspecting the feed.

Authorized replay uses `GET /internal/satellite/history?namespace=...&after=...&limit=...` through the owning service, returning the same raw logical envelopes ordered by original offset and a nullable `next_after`. `after` is exclusive, initial position is omitted, default limit 100 and maximum 1,000. History remains distributed and retained for the evaluation session. Its service credential is ingestion/recovery-scoped; credentials for canonical tables are never shared. Transport retirement stays disabled until this recovery path is implemented and verified.

## 9. Acceptance and Crash Outcomes

| Durable boundary | Crash immediately before | Crash immediately after |
| --- | --- | --- |
| Admission-ledger reservation | Client may retry same scoped key. | Reservation alone gives no 202; recovery completes publication with original identity. |
| Required broker publish acknowledgement | No 202; unknown outcome reconciled with stable identity. | Payload remains recoverable; lost HTTP response retries return the original receipt after ledger reconciliation. |
| Failure database commit | Original broker delivery stays unacked. | Redelivery verifies the matching complete failure record before ack. |
| Satellite history receipt | Broker retains operation; frontier unchanged. | Retry is a duplicate; history can drive preparation independently. |
| Processor preparation | Prepared frontier unchanged. | State, lineage, and output intent recover together. |
| Canonical batch commit | Pending intent remains, no success claimed. | Retry returns the recorded result; no second revision/publication intent. |
| Recipient inbox commit | Warning delivery remains pending. | Redelivery repeats ack, not the recipient-visible effect. |

For field reports, serialize admission-ledger reservation by authenticated submitter plus stable report key, persist original payload and content fingerprint, then publish that exact identity and mark acceptance after the required broker acknowledgement. A pending reservation retains enough data for automatic recovery. A durable admission-outbox worker may republish after ambiguous outcomes; downstream durable identity checks make this safe beyond the broker duplicate window. Return 202 only after the broker acceptance condition and recorded receipt are known; ledger or broker unavailability returns a bounded retryable failure. Same-key/different-content requests conflict before publication. This sequence is a contract to implement and test, not a claim of a distributed transaction.

## 10. Service-Owned Durability and Field Receipt

Ingestion ledger/failure storage and recipient inbox storage each require their own durable primary plus at least one synchronously flushed replica on another node before claiming their respective durable commits. They use service-owned databases/credentials. Canonical Citus availability does not substitute for these boundaries. Platform may share infrastructure only where isolation, replica placement, and capacity remain correct; it must not grant unrelated services direct table access. If the needed durability is unavailable, retain the prior durable obligation and return a retryable result. Ledger/failure/outbox/inbox recovery and retention must be included in the platform budget.

Field-report idempotency is `Idempotency-Key` scoped by stable authenticated submitter identity, not access-token bytes. Initial allowed key length is 1–128 printable ASCII characters. Request-content equality v1 compares the exact accepted body bytes plus normalized media type; changing whitespace is different content under the same key and yields `409 identity_conflict`. The client persists the key and original submission across retries. Keep the scoped key, fingerprint, and original receipt for the evaluation session, beyond broker deduplication. A supplied report ID can be adapted to this key only through a documented evaluator-client adapter; do not make incompatible required headers silently mandatory.

Successful new or duplicate accepted submissions return `202 {report_id, receipt_id, status:"accepted", received_at}` with the same receipt and first receipt timestamp for retries. `report_id` is the stable client key in its submitter scope; `receipt_id` is an opaque server-generated UUID persisted in the admission ledger. Pending/unknown acceptance returns `503 acceptance_pending`, not false success. Other errors use the standard envelope: 400 invalid request/key, 401/403 authentication/authorization, 409 identity conflict, 413 admission size limit, 429 overload, and 503 unavailable durability. Initial HTTP acceptance deadline is two seconds. Valid warning processing does not require accessing this report ledger.

Field reports remain separate from canonical HazardEvents: no canonical source/severity enum is added. The authenticated Tim Lapangan client submits a required `Idempotency-Key` and JSON body `{report_type, occurred_at, latitude, longitude, description, attributes}`. `report_type` is a nonempty client report-category string, not a canonical hazard enum; `occurred_at` is valid UTC time; coordinates are finite and in the canonical geographic ranges; `description` is a nonempty UTF-8 string; `attributes` is an object, default empty. The initial body limit is 1 MiB and depth limit 64; syntax/auth/admission failures before acceptance return 400/401/403/413, not 202. Domain processing remains asynchronous.

The consumer transactionally inserts the report projection and its unique scoped report/operation identity; then it acknowledges. The owning service exposes `GET /field-reports/{receipt_id}` to the submitting identity and Internal Ops, returning `{receipt_id, report_id, status, received_at}` with status `accepted`, `processed`, or `parked`. Report inspection never grants canonical database access. A normal accepted backlog is not parked; malformed/domain-incompatible accepted work follows the failure/replay workflow and remains recoverable. API retries preserve original body bytes/key; own clients and test adapters supply that identity. No report automatically changes canonical event counts or generates a coastal warning.
