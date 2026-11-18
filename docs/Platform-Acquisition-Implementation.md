# Source acquisition and automatic recovery

Source acquisition uses a database-backed lease, durable HTTP response reuse,
and automatic retry of unfinished **read-only GETs**. An uncertain request no
longer blocks its source indefinitely. A replacement resumes from persisted
input progress without an operator clearing a lock or choosing a new namespace.

The sender guard lives inside the acquisition services and uses their existing
PostgreSQL formations. It adds no gateway pods or database formation. The
replicated durability adapter remains mandatory in the integrated deployment.

## Ownership and effects

| Owning database | Acquisition scope | Work |
| --- | --- | --- |
| Canonical | `acquisition:PVMBG` | volcanic live/history polling and deferred mapping |
| Ingestion | `acquisition:BMKG` | raw live and archive acquisition |
| Ingestion | `acquisition:SATELIT` | satellite stream and session metadata |
| Ingestion | `acquisition:SENSOR_PANTAI` | coastal observations and station metadata |

A unique holder ID identifies each process incarnation. The default lease is
15 seconds, renewed every five seconds; the configurable lease range remains
12–30 seconds. Admission uses the authority's clock and conservative monotonic
local deadlines. Lease loss cancels work. Protected input/canonical transactions
still check the live epoch/holder at COMMIT, so a resumed obsolete worker cannot
commit data, cursors, status or failure effects.

The optional legacy BMKG path owns a canonical acquisition scope when raw BMKG
is disabled. Never run both acquisition paths against the same source. All old
unguarded binaries must be stopped during the offline schema/image cutover.

## Requests and unfinished work

1. The current live owner reserves a source/API-lane nonce and confirms durable
   storage before HTTP dispatch. A completed HTTP 200 receipt for the same
   logical request is returned from the database without another upstream call.
2. A new intent has a ten-second grace period. While an old epoch still has a
   request inside that period, another owner waits. Admission waits use a
   500-millisecond retry interval rather than growing exponential backoff.
3. The sender checks its local ownership and the authoritative nonce again
   immediately before entering the HTTP transport. A worker resumed after its
   admission became obsolete fails this check. Superseded nonces cannot overwrite
   a replacement's saved response or close its stream entry.
4. Transport/body failures mark the outcome interrupted, shortening the remaining
   grace period to at most 500 milliseconds. This is **not** a claim that the
   source never received the request. If that marker could not be saved, the
   original ten-second period still bounds waiting for an unestablished request.
5. The current owner can atomically supersede an eligible unfinished request and
   retry from its committed cursor/generation. Old pending lanes are retired
   source-wide after their grace periods, so an abandoned metadata request cannot
   hold the entire source forever. Source ownership is rechecked for every retry.
6. A successful finite response is saved before it is returned to decoding/input
   storage. A lost response-save acknowledgement is resolved by finding that saved
   receipt. Failure after fetching but before committing work does not consume
   its logical request generation.

Established satellite streams are recorded separately from unfinished connection
opens. A live owner's established stream is not reclaimed just because it is
older than ten seconds. A new owner can supersede an old owner's stream after
its grace period without requiring the dead process to close the old body.
Normal close releases its entry after closing the underlying body. Reconnection
uses the persisted offset and existing immutable receipts/deduplication; it does
not reset input progress. Satellite connection limits and Retry-After remain
upstream constraints, not a reason to spin or bypass admission.

Finite network reads are bounded to eight seconds even if a caller supplies no
shorter deadline. Existing BMKG/PVMBG deadlines are two seconds, coastal requests
use their configured timeout and satellite metadata uses five seconds. Dial,
TLS handshake and response-header waits are bounded as well. The transport uses
fresh HTTP/1 connections, does not follow redirects, ignores environment proxies,
and restricts methods/hosts/API lanes. It does not add hidden transport retries.

Coastal admission waits do not count as upstream circuit-breaker failures. Real
upstream failures still use the existing breaker and backoff. A newly elected
legacy poller ignores a predecessor's persisted retry delay until it makes
successful progress; one interrupted initial attempt cannot restore a long wait.

## Identity, retry semantics and limits

| Read | Logical identity |
| --- | --- |
| Raw BMKG | namespace and next capture position, API lane/archive cursor |
| Legacy PVMBG/BMKG | endpoint dispatch generation, API lane/archive cursor |
| Coastal observations | namespace and dispatch generation; successful empty pages advance it |
| Satellite metadata | authority database clock window of five seconds |
| Coastal stations | namespace/database-clock refresh window, with one shared station snapshot |

The generation advances only with committed work. `last_attempt` describes the
current attempt while `last_success` retains the original receipt's conservative
snapshot boundary. Assessment history has a separate lane from regular history
so concurrent readers cannot replace each other's unfinished receipts. Station
validators read the owner's shared database snapshot instead of opening their
own upstream connections.

Requests carry `X-Acquisition-Request-ID`, `X-Acquisition-Epoch` and
`X-Acquisition-Recovery`. The ID survives a lost-response recovery; the recovery
flag distinguishes an explicit recovery read. These headers aid reconciliation;
they do not make an external service implement deduplication or epoch fencing.
Request credentials are fingerprinted, not stored as plaintext in the ledger.
The owned BMKG/PVMBG request logs include a validated `acquisition` object with
`request_id`, `epoch` and `recovery`; these remain caller-reported observations,
not receiver-side authorization. Rebuild those mock images as well to capture the
new log fields. External satellite/coastal services are unchanged.

A source can receive an original GET and its recovery retry after the original
reply was lost. History/SSE replay is also expected. This implementation prevents
multiple normal pollers and duplicate persisted effects; it does **not** promise
exactly one physical HTTP arrival under arbitrary crashes, delayed packets or a
pause after the final validation. No client-only check makes the database decision
atomic with remote HTTP receipt. That stronger interpretation of P2.2 would need
receiver-side support or agreement about permitted recovery retries. Upstream
logs must distinguish recovery from ongoing traffic multiplied by replica count.

Automatic recovery depends on replayable source data: durable history/cursors
and satellite offsets must still be retained by the source. Retrying a read
cannot reconstruct a response that an upstream permanently discarded. Source
outages, exhausted connection limits or loss of database quorum can prevent the
60-second target; these conditions must be reported rather than called a pass.

The cache retains one finite response per fixed API lane: 32 MiB per BMKG/PVMBG
response (also capped by the raw BMKG payload quota), 4 MiB per coastal response,
and 64 KiB for satellite metadata. SSE bodies are streamed. The integrated
profile's logical finite receipt maximum is about 200.1 MiB before replication,
WAL and old row versions, using the existing formations. Per-request memory can
reach its body limit. Incomplete/oversized reads do not advance a cursor; they
remain failed reads subject to recovery and source backoff, not accepted data.

## Diagnosis and deployment

Use the existing inventory and maintenance receipt; no Audit token or approved
province/reference file is needed for this read-only inspection:

```sh
python3 scripts/platform-acquisition-status.py \
  --inventory /private/inventory.json --receipt /private/maintenance-receipt.json
```

The script queries captured original primaries and reports non-ready pending
work, grace deadlines, recovery eligibility, recovery counts and the previous
nonce. It prints metadata and receipt sizes, never body bytes or credentials.
Healthy established SSE connections remain allowed. A pending entry is recovery
state, not a permanent manual lock. Do not manually delete it or invent new
input identities to unblock it.

Source reference validation refuses to claim completion while automatic recovery
is unfinished, even if historical reference data was already applied. The
Prometheus dashboard includes recovery reads and current waiting state. The
`AcquisitionDispatchBlocked` alert requires a source's current admission wait to
persist for 60 seconds, and clears when the wait clears; historical counter
increments do not keep it firing. This diagnostic alert is separate from measured
fault-to-committed-progress acceptance.

Rebuild the role1 image and run the existing offline maintenance workflow before
activation. Canonical and ingestion migrations now include the shared additive
`0016_acquisition_recovery.sql`. Earlier migrations, including 0014/0015, retain
their original checksums. Maintenance receipts bind the new schema/image contract;
old receipts cannot authorize activation of the new bundle. No new Secrets or
machine-spec inputs are required.

Single-instance legacy development can still opt out with
`ACQUISITION_ENABLED=false`. The integrated profile requires coordination; raw
ingestion always installs the guarded clients. Multiple unguarded pollers are
unsupported.

## Evidence and remaining acceptance

```sh
python3 scripts/test-acquisition-dispatch.py
```

The isolated runner publishes no host ports, contacts no live feed and starts no
VMs. It uses PostgreSQL, actual HTTP/TLS handlers, controlled pauses, and a worker
subprocess that is actually killed after the upstream accepts a request. Three
replacement contenders use the normal 15-second lease. The measured interval
ends after a replacement commits a HazardEvent and protected progress. This
particular proof never edits lease timestamps, bypasses authority or manually
clears a request. Replaying the event must leave one stored HazardEvent.

Final automatic-recovery evidence:
`evidence/platform-acquisition-dispatch/78f2f678c154/result.json` records 22 passes,
zero failures/skips and no remaining cleanup. The killed-worker case committed
progress in 15,446 ms, with one stored HazardEvent and cursor 7. Its upstream saw
two HTTP requests: the original whose reply was lost and one recovery read with
the same logical ID. Three contenders did not produce three polling streams.
This is a measured local process recovery result, **not** four-node HA acceptance.
The final focused connector suites and 124 offline Python tests also passed;
`evidence/platform-source-validation/219fc048021f/result.json` records eight real
PostgreSQL observer checks, including visible automatic-recovery eligibility.
Both owned source middleware suites verify the new log fields and exclusion of
credential headers. Temporary containers from both isolated runners were cleaned.

The suite also covers before/after-admission freezes, stale writes, completed
response reuse after takeover, stale nonce completion/close rejection, stream
recovery without the old caller closing its body, actual satellite connector
resume, shared station reads, empty pages, missing/changed identities, body bounds,
redirect rejection, unconfirmed intent rejection and lost receipt acknowledgements.

Four-VM failover, permanent-node loss, database promotion, clock shifts, real
source request logs, live connection limits and the measured game-day criteria
remain pending on the actual formation. A local 60-second assertion cannot replace
those runs or establish unconditional recovery during an upstream/quorum outage.
