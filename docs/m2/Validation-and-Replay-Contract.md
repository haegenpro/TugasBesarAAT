# Validation and Replay Contract

Selected policies: compatible changes accepted automatically; complete failed input in an ingestion-owned database; transient retries recover automatically; incompatible-schema replay starts through Internal Ops.

## 1. Schema Acceptance Policy

| Change | Behavior | Reason |
| --- | --- | --- |
| Additional field with required mappings unchanged | Accept if the declared reader/adapter compatibility checks pass; retain original bytes. | Additional information need not break existing interpretation. |
| Explicitly supported lossless type promotion | Accept only with validation and mapping fixtures. | Schema-level compatibility alone may not establish semantic/range safety. |
| Required field removed | Park as incompatible. | Missing information must not become a fabricated zero/default. |
| Unknown field rename | Park as incompatible. | Similar-looking names do not establish equivalence. |
| Number replaced by string or ambiguous coercion | Park unless an explicit source adapter defines and validates conversion. | Avoid guessing producer intent. |
| Malformed, deeply nested, invalid UTF-8, or oversized input | Preserve and classify safely without crashing or blocking unrelated work. | Required by Problem 3. |
| Sensor payload violates its basic contract | Quarantine; do not create a HazardEvent from it. | Schema compatibility does not make an invalid observation valid. |

New external fields are not automatically canonical top-level fields. Preserve source bytes, map supported fields explicitly, and follow the [canonical attributes policy](Canonical-and-Storage-Contract.md#1-preserve-the-existing-canonical-shape). Media filtering remains an allowlisted server-side projection; unknown fields cannot bypass it.

Keep canonical schema evolution separate from external writer compatibility. Existing Avro reader selection, registry framing, and compatibility settings must be reviewed rather than overwritten wholesale. Add deterministic PVMBG mock modes for rename, incompatible type, and required-field removal as required by M2; tests compare behavior with this declaration.

## 2. Failure Classification

| Class | Examples | Treatment |
| --- | --- | --- |
| Transient | Dependency unavailable, temporary storage/connectivity error, unknown schema ID during registry outage. | Durable scheduled retries with capped backoff, jitter, bounded concurrency; automatic recovery. |
| Incompatible/permanent for current adapter | Required data absent, unsupported type change, corrupt input that cannot become valid by waiting. | Park intact with reason and adapter context; no hot retry loop. |
| Ownership loss | Stale source/processor authority. | Refresh ownership and stop protected actions; retain legitimate accepted work for a successor. |
| Unknown commit/publish outcome | Timeout after a side effect may have committed. | Resolve by same operation identity; do not invent a new submission. |

Retry duration/attempt count does not by itself turn a transient error into a permanent one. Alert on backlog age and repeated failure; reclassify only with evidence. Retry scheduling releases workers between attempts and cannot monopolize resource pools. Apply source isolation and entity ordering while retries wait.

## 3. Failed-Input Record

The ingestion service owns its failure database and accesses it through its own credentials. Other services use APIs. Extend M1's binary-payload and failure-metadata pattern, but review old uniqueness/truncation behavior before reusing it.

Record at least:

- Stable operation/message identity and source/session context.
- Complete original bytes, byte count, integrity digest, and content/schema metadata.
- Safely extracted source/entity reference when available; no fabricated identity.
- First receipt time, last attempt time, attempt history/count, and correlation context.
- Reason/classification, retrying/parked state, adapter version, and source position.
- Replay request/job identity, current processing outcome, and recovery evidence.

A preview may be truncated for display, but the stored original must not be. A digest is an integrity check, not a substitute for bytes. Deduplicating only by payload digest can conflate distinct source operations with identical content; preserve operation identity and source position as the authority.

Do not acknowledge/delete the original broker delivery until the complete failure record is durably committed or the matching record is verified. A database outage leaves work pending with backoff. Failure-store unavailability must not block independent valid critical warnings merely because they share a global worker/transaction queue.

Use bounded streaming/spooling and parser limits to handle hostile size/depth safely. If a source response exceeds broker message limits, preserving it may require a recoverable chunk/object reference or streaming database path before acknowledgement; design and test that path. Do not claim arbitrary unbounded payload support or silently truncate. HTTP admission rejection before acceptance and preservation of failed acquired source input are distinct cases whose exact evaluator scope must be checked.

## 4. Inspection and `/unprocessed`

Only BNPB Internal Ops can inspect failed payloads or initiate replay. Enforce authorization in the backend and limit storage/broker credentials accordingly. Logs and general dashboards expose safe metadata and references, not complete failed payloads or credentials.

Provide `/internal/audit/unprocessed` through Aggregator using Appendix A's token, parameters, field names, and ordering/pagination contract where prescribed. Its statuses are `retrying` or `parked`; recovered messages no longer appear. An audit token's prescribed metadata access is not permission to expose an unrestricted original-payload download endpoint. Align internal audit authentication and Ops-only payload inspection explicitly.

Distinguish failed processing from normal durable accepted backlog. A received report waiting behind healthy asynchronous processing is not automatically schema-incompatible. Keep one authoritative failure/outcome view rather than independent contradictory DLQ counts and database states.

## 5. Ordered Replay

Internal Ops selects an explicit replay scope after an adapter fix: source, failure reason/version, identities, or an auditable set of message records. Capture the scope durably so retries do not accidentally expand it.

For each `source_ref_id`:

1. Preserve the original logical source order and operation identity.
2. Serialize replay and newer live operations that could change the same entity; queue later operations when an earlier required operation remains unresolved.
3. Permit unrelated identities to progress concurrently.
4. Re-run validation with the selected adapter, recording old/new versions without overwriting original bytes.
5. Apply canonical effects under the [canonical write rules](Canonical-and-Storage-Contract.md#7-read-and-write-outcomes).
6. Mark recovered only after the relevant durable completion is known.

Original timestamps and source positions remain original. Replay timestamps and job IDs are additional operational metadata. Do not make old data appear newly observed or generate new logical IDs to evade deduplication. A repeated replay command is itself idempotent.

An unparseable record may lack a reliable source reference. Preserve transport order/context and use a conservative scoped gate; exact ordering cannot be claimed from a guessed entity ID. This case needs a fixture and explicit adapter behavior before full compliance is claimed.

If replay fails again, retain the original and new attempt evidence. Transient outcomes resume automatically; still-incompatible outcomes remain parked for Ops. Rate-limit replay so it cannot starve live/critical processing. Incompatible-schema repair requires a deployment/operator action by design, while ordinary game-day outages must recover without intervention.

## 6. Evidence

- [ ] Compatible additions work; incompatible rename/type/removal park consistently.
- [ ] Malformed/deep/invalid-UTF-8/oversized inputs do not crash or restart components repeatedly.
- [ ] Stored-byte length/digest exactly matches original test input, including invalid bytes.
- [ ] Failure-store outage does not acknowledge away the only preserved input copy.
- [ ] Unauthorized payload inspection and replay are denied.
- [ ] Audit metadata uses prescribed shape and recovered rows disappear.
- [ ] Replay preserves same-entity order while unrelated identities remain active.
- [ ] Repeated replay and crash-after-commit create no duplicate canonical or notification effects.
- [ ] Transient dependency outages recover automatically with bounded traffic/concurrency.

## 7. Failure and Replay Interfaces v1

The ingestion-owned database stores original payload as binary data with `message_id` unique within source namespace. Schema: `message_id`, `source`, `source_namespace`, nullable `source_ref_id`, nullable original `source_position`, original `received_at`, `payload` bytes, `payload_sha256`, `content_type`, nullable `source_schema_id`, `adapter_version`, `reason`, `classification`, `status`, `attempts`, `last_attempt_at`, nullable `next_attempt_at`, correlation/trace fields, and nullable `replay_job_id`/`recovered_at`. Attempts are durable attempt records, not just a counter overwritten on every retry. The digest verifies complete original bytes on storage and retrieval.

`classification` is `transient` or `incompatible`; status is `retrying`, `parked`, or internal `recovered`. Ownership loss is an execution-control result, not a reason to discard accepted input. An unknown commit outcome remains retrying until operation reconciliation establishes completion. Initial transient retry uses the ingestion backoff policy. A repeated incompatible replay returns to parked while preserving previous attempt evidence. Failed-input records and deduplication history have no automatic expiry during the evaluation session.

Private Ops endpoints use existing authenticated BNPB Internal Ops identity checks:

| Endpoint | Request / response |
| --- | --- |
| `GET /internal/ingestion/failures` | Optional source/status, limit/cursor; safe metadata only. |
| `GET /internal/ingestion/failures/{message_id}/payload` | Complete `application/octet-stream` body plus digest; bounded streaming, no general dashboard access. Namespace is a required query parameter. |
| `POST /internal/ingestion/replays` | `{request_id, source, namespace, message_ids, adapter_version}`. Freeze the selected list in a durable job; return `202 {job_id,status:"pending"}` after job persistence. |
| `GET /internal/ingestion/replays/{job_id}` | Job status, selected/recovered/retrying/parked counts and safe per-message results. |

Replay request IDs are scoped to authenticated Ops identity. Same request/content returns the original job; changed scope under the same ID is a conflict. Replay 202 confirms job acceptance, not recovered canonical effects. The public Client API does not expose these endpoints to Media or Tim Lapangan. Audit credentials grant metadata access only.

Use Appendix A-shaped errors. Validation/authentication errors are 400/401/403; unknown authorized record/job is 404; replay ID conflict is 409; overload is 429 and unavailable persistence is 503. Private API 409 uses the same error envelope structure. Pagination order is `(received_at,message_id,source_namespace)` ascending; bind filters and continuation to the same authorized query. Payload access checks occur before lookup results can reveal existence.

`GET /internal/audit/unprocessed` is served through Aggregator under the prescribed audit bearer token. Return exactly the Appendix A item fields: `message_id`, `source`, `received_at`, `reason`, `retryable`, `attempts`, `status`. `retryable` is true exactly for `retrying`. Include `next_cursor` and common completeness metadata when units are unavailable. Defaults are 100 rows, maximum 1,000, source optional; use the same safe ascending metadata order. Recovered records disappear. Normal accepted work awaiting healthy asynchronous processing is not a failure record. Internal field-report failures remain in the Ops workflow; audit source filters stay within canonical source values unless evaluator instructions extend them.

Replay and live mutations share a durable per-entity sequence gate keyed by `(source, namespace, source_ref_id)`. Reserve order when accepted, not when a retry becomes runnable. A parked predecessor blocks later mutations for that identity; workers release capacity while waiting, and other identities continue. Records without a trustworthy identity use a source/namespace acquisition-order gate until the adapter can identify them; do not guess an entity or claim unrelated operations behind an unknown predecessor are provably independent. Successful identification transfers the pending operation into the entity gate without changing its original source order. Operator replay commands select work; they never bypass revision/fencing checks.

Source input larger than the 1 MiB broker-message limit is streamed into ingestion-owned binary payload storage with a digest before a small durable reference envelope is published. Ack/delete the acquired delivery only after the referenced object and its durable reference are committed. A referenced payload remains pinned until handoff/replay obligations finish. Use 64 KiB streaming chunks and depth-limited parsing (initial maximum depth 64), not an unbounded in-memory buffer. If disk quota is exhausted, stop acceptance/cursor advance and expose backpressure; never keep a truncated record and report preservation. HTTP requests rejected before 202 with `413 payload_too_large` have not been accepted and are distinct from acquired malformed source input. The supported input/storage quota must be stated in deployment configuration and tested with the supplied oversized case.
