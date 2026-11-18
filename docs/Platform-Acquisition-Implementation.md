# Source acquisition and durable HTTP admission

The coordinated binaries now record durable HTTP dispatch intents before
contacting a source. A newer lease cannot clear an old intent. This closes the
check/pause/send race that lease preflights alone left open. Finite successful
responses are saved before they are delivered to acquisition code, allowing a
successor to reuse the response after a failed database commit.

This is an in-process sender guard backed by each source's owning PostgreSQL
formation. It adds no gateway pods or database formation. The authoritative
formation must retain its existing replicated durability contract; isolated
single-copy tests do not establish that HA property.

## Ownership and database writes

Each process incarnation has a unique holder ID. The coordinator acquires and
renews a source lease, uses conservative monotonic deadlines, cancels work on
lease/readiness loss and drains work before releasing. The source scopes are
separate from public canonical delivery scopes:

| Owning database | Source scope | Work |
| --- | --- | --- |
| Canonical | `acquisition:PVMBG` | volcanic live/history polling and deferred mapping |
| Ingestion | `acquisition:BMKG` | raw live and archive acquisition |
| Ingestion | `acquisition:SATELIT` | satellite stream and session metadata |
| Ingestion | `acquisition:SENSOR_PANTAI` | coastal observations and station metadata |

The optional legacy BMKG poller uses its canonical acquisition scope when raw
BMKG is disabled. Never run both BMKG acquisition paths against the same feed.

Protected data transactions still take the authoritative lease SHARE lock and
check epoch, holder and database-clock expiry at COMMIT. A stale process can
finish saving its already-authorized HTTP receipt, using its unique dispatch
nonce, but cannot use that permission to start another request or commit input,
canonical data, cursors, status or failure effects.

## Dispatch protocol

`acquisition.Client` is installed on every coordinated source HTTP client.
Source code cannot dispatch through it without an active source context and a
request identity. The protocol is:

1. Validate the source lease under its ownership row lock and reserve a unique
   dispatch nonce in `acquisition_dispatch`. Commit and confirm the owning
   formation's durability contract **before** entering the real HTTP transport.
2. Keep the intent pending while the source request is unfinished. A successor
   cannot open another request for that lane, or open other lanes while an older
   epoch still has a pending request. No timeout or lease takeover reclaims it.
3. For finite responses, read the complete bounded body and save status, headers
   and bytes durably. A retry of the same logical request returns these bytes
   without contacting the source again. Only complete HTTP 200 responses are
   reused; fully received error responses can be retried with normal backoff.
4. For satellite SSE, retain the intent for the connection's lifetime. Close the
   underlying response body before recording it closed. A frozen or crashed
   process cannot authorize another stream simply by losing its lease. Stream
   reconnects resume from the existing persisted offset; replay remains normal
   and downstream offset/identity deduplication remains necessary.

A worker frozen before admission fails the authoritative check when it resumes.
A worker frozen after admission retains the durable intent, so its successor
waits for the receipt instead of independently issuing the same request.

The transport uses fresh connections to prevent net/http's transparent retries
on reused connections. It does not follow redirects, use environment proxies,
accept request bodies, or allow a source context to select another host/API
lane. Credentials influence a one-way request fingerprint; plaintext request
credentials are not written to the ledger.

## Request identities and cache bounds

Request generations follow committed work, not correlation UUIDs, lease epochs
or worker wall clocks:

| Path | Durable identity |
| --- | --- |
| Raw BMKG | namespace and next capture position, with API lane/archive cursor |
| Legacy PVMBG/BMKG | endpoint dispatch generation, advanced only on successful committed polling work, with archive cursor |
| Coastal observations | namespace and dispatch generation; successful empty pages also advance it |
| Satellite session metadata | authority database clock window of five seconds |
| Coastal station metadata | namespace and authority database clock window; only the source owner refreshes the shared snapshot |

Failed health/backoff attempts do not consume a successful cached response.
`last_attempt` records the current attempt, while `last_success` retains the
original saved response's snapshot boundary. Polling boundaries and metadata
windows use the database clock. Assessment history has a separate cache lane
from regular BMKG history so concurrent loops cannot replace each other's
unconsumed receipt.

The ledger keeps one response per fixed API lane, replacing consumed generations
rather than accumulating an archive. Finite bounds are 32 MiB per BMKG/PVMBG
response, additionally capped by the configured raw BMKG payload quota; 4 MiB
per coastal response; and 64 KiB for satellite metadata. SSE bodies are streamed
and are not buffered into this cache. The integrated profile has at most four
BMKG response lanes, two PVMBG lanes, two coastal lanes and one finite satellite
lane: about 200.1 MiB of logical receipt bytes at their maximum, before PostgreSQL
replication, WAL and transient old row versions. These bytes use the existing
storage formations. Reading a finite response also needs memory up to its bound
per concurrent request; the cache is not a hardware-capacity proof.

An incomplete or oversized body leaves its intent pending. It never silently
advances a cursor, discards the uncertainty or triggers an automatic refetch.

## Uncertain outcomes and availability

A crash after dispatch intent commit and before durable receipt completion is
ambiguous: the source may have received the request even when no reply survived.
The safe behavior is to stop automatic acquisition for that source. The ledger
survives process restarts and ownership changes. Other source scopes, existing
read traffic and committed outbox work can continue independently.

Do not delete pending rows, clear them after a TTL, restore an older database
snapshot or force a new namespace to bypass this protection. An operator must
first make the old sender unable to resume. That alone does not prove a finite
request was never received: restoring acquisition also needs a verified complete
receipt or receiver-side evidence/protocol establishing the original outcome.
Without such evidence the outcome remains unknown and the source stays blocked.
For streams, verified sender fencing and the durable offset must govern recovery;
a terminated connection cannot be inferred merely from pod deletion or lease
expiry. There is deliberately no generic force-unlock command.

Inspect the original primary's ledger using the read-only command:

```sql
SELECT scope,lane,dispatch_id,epoch,holder,state,streaming,started_at,completed_at,
       status,octet_length(body) AS receipt_bytes
FROM acquisition_dispatch ORDER BY scope,lane;
```

The monitoring bundle exposes admitted, reused and blocked dispatch counters,
a dashboard panel and the `AcquisitionDispatchBlocked` alert. A blocked source
is an incident, not evidence that acquisition recovered successfully.

Safety and an unconditional <=60-second takeover after an indefinitely frozen
or killed sender cannot both be promised by this client-only protocol when an
external feed offers no request-ID deduplication or fencing. Receiver-side
support or a physically fenced acquisition path is still necessary to meet
that stronger availability requirement. This implementation chooses duplicate
request safety and reports the unresolved outcome instead of silently weakening
it. It does not claim mathematically exactly-once execution inside an external
HTTP server, or eliminate intentional history/SSE replay requests.

## Deployment and validation

Run the existing offline maintenance workflow with the rebuilt role1 image.
Canonical migrations now include `0014_acquisition_dispatch.sql` and
`0015_poll_dispatch_progress.sql`. Ingestion applies the shared dispatch table
migration and its own `0005_dispatch_progress.sql`. Existing migration checksums
are unchanged. The release maintenance receipt binds the new schema/image
contract; old receipts must not be reused. Stop all old acquisition binaries
before migration and activation so an unguarded process cannot bypass admission.

Coordination remains opt-in for single-instance development via
`ACQUISITION_ENABLED=false`. The integrated replicated profile requires it;
raw ingestion always installs the guarded clients. Never use multiple unguarded
legacy pollers. No new release credentials or user-provided machine specs are
needed for this change.

The isolated proof runner uses temporary labelled PostgreSQL/toolchain containers
sharing a network-isolated loopback namespace, actual HTTP handlers and controlled
pause points. It publishes no host ports and contacts no live source:

```sh
python3 scripts/test-acquisition-dispatch.py
```

It checks freezes before/after durable admission, actual upstream request counts,
saved response reuse, stale database writes, process restart with a lost response,
stream handoff, missing/changed request identities, redirects/body bounds, lost
receipt acknowledgements, unconfirmed intent rejection, retry snapshot boundaries,
shared station reads and empty coastal pages. It requires all named proofs to
pass, rejects skips, records evidence and removes only its owned containers.

The four-VM freeze, permanent-node-loss, database-promotion, clock-skew, live-feed
and measured takeover acceptance runs remain deferred. A local protocol pass is
not a claim that those physical tests or the <=60-second SLA passed.
