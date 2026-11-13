# Source acquisition development draft

**Implemented, unverified.** This change is development-first: tests/checks are
authored but unexecuted. No image build, migration, deployment, VM startup or
runtime experiment was run. Earlier coordination/bootstrap results do not cover
this change.

## Ownership and integration

New migration `0006_acquisition.sql` provisions `acquisition:BMKG` and
`acquisition:PVMBG` in the existing PostgreSQL authority table. Epochs persist
through handoffs and restarts. The canonical HTTP authority accepts only its
four canonical scopes; acquisition has a separate in-process authority and is
not exposed through the canonical ownership routes. Tokens cannot be exchanged
between acquisition and canonical delivery.

One acquisition owner controls all existing work for a source:

| Scope | Owned work |
| --- | --- |
| `acquisition:BMKG` | seismic/warning live loops, both history replay loops and assessment history requests |
| `acquisition:PVMBG` | volcanic live/history loops and deferred mapping retries |

Standbys keep serving reads and can recover attempts or deliver committed
outbox work without acquiring the source. They contend for the source lease
when ready. A new process generates a unique holder ID; it does not reuse a pod
name as authority. Acquisition retries use that same process holder. A successful
Acquire is always followed by Renew before work starts because an idempotent
Acquire can return an older expiry.

The coordinator renews at the returned interval (normally five seconds), uses
bounded two-second authority calls, and retries contention/outages after about
one second with jitter. Local admission uses request-start monotonic time and a
conservative TTL margin, rather than comparing the database expiry to the
application's wall clock. Renewal failure, local expiry, readiness loss or
failed authoritative preflight stop new work. Lease loss cancels all owned
loops and requests; local work drains before release or reacquisition.

Live and history clients perform an authoritative check before dispatch.
The first attempt after election resumes from persisted progress immediately,
ignoring only the predecessor's retry delay. Subsequent attempts retain normal
backoff. Successful history cursor and last-success boundaries are not reset.

## Database protection

The work context carries its lease generation through HTTP decoding, mapping,
observations, history cursor updates, poll status and failure retention.
`sourceTx` and the legacy `SaveDedup` path require the correct source token when
acquisition is enabled. `RecordFailure` now uses an admitted transaction too.
Context cancellation stripped by failure-retention cleanup cannot revive the
original cancelled generation.

Every protected transaction takes the authoritative lease SHARE lock and
registers the existing deferred epoch/holder/expiry check. The lease must still
be valid at COMMIT. Takeover waits for that protection, and server-side idle
transaction expiry releases a frozen application's abandoned data backend.
The earlier control-session cleanup still governs pending-attempt recovery.
Status/cursor advances, observations and resulting outbox work retain their
existing transaction boundaries. Lease checking does not infer a lost commit's
outcome; normal attempt recovery still checks the atomic marker and distributed
decisions. Durable queued publication uses its committed decision rather than
requiring the old acquisition lease to stay alive.

An HTTP preflight and remote source execution are separate operations. A process
can freeze between checking its lease and sending, and an upstream can finish a
request after its caller cancels. The mocks do not enforce fencing epochs. This
draft therefore does **not** prove physically exactly-once upstream requests
under every freeze or partition. It coordinates healthy pollers and fences stale
database effects. Stronger upstream exclusivity, the <=60-second takeover target,
clock shifts and permanent freezes need explicit integration evidence; stricter
remote-request fencing would require an upstream-aware protocol or intermediary.

## Configuration and handoff

Acquisition coordination remains opt-in for existing single-instance development:

```dotenv
ACQUISITION_ENABLED=false
ACQUISITION_LEASE_TTL=15s
```

For the replicated deployment, migrate offline through 0006, configure the
[replicated durability adapter](../deployment/storage/README.md), and enable
`ACQUISITION_ENABLED=true`. TTL must be 12..30 seconds in whole milliseconds.
There is no test-passed flag: the dependency is actual configured replication.
Missing scopes remove readiness, and unavailable authority/replication keeps
pollers from starting. Never enable multiple legacy pollers with acquisition
disabled. Do not mix old unguarded binaries with enabled writers during rollout;
stop writers/publishers for the offline migration and coordinate the cutover.

[Aggregator deployment](../deployment/aggregator/README.md) supplies the renderer,
three-replica placement, private Service, Secret/config references and shutdown
settings. Ingestion owns new connectors/raw acceptance; storage owns processor
state and audit semantics. Those integrations, SATELIT/SENSOR_PANTAI acquisition
and processor/checkpoint or audit ownership are not implemented by these two
existing-poller scopes.

## Deferred checks

The code includes coordinator cancellation/renewal/context tests, configuration
boundaries, real single-database contender/handoff and stale-progress checks,
one-shot bypass rejection, readiness/drain tests,
and placement/credential-reference checks. The isolated runner was extended to
include these without importing application env files:

```sh
# Later only: Docker-backed single-copy fencing proof, not replicated HA.
python3 scripts/test-coordination.py
python3 scripts/test-coordination.py --race
python3 -m unittest discover -s scripts -p aggregator_renderer_test.py
```

Further integrated evidence must include three replicas contending for both
sources; per-source holder/epoch and upstream request traces; live/history
progress before/after owner crash; expired owner's data/cursor/status/failure
writes rejected; request/renewal failures; one-shot bypass rejection; permanent
process/VM freezes and resumption; database failover and clock shifts. Measure
takeover from fault injection through successful protected progress using one
timing basis. Reconcile missing/duplicate effects and cursor continuity rather
than counting an election or health response as successful recovery.

No passing result or completed takeover SLA is claimed for this draft.
