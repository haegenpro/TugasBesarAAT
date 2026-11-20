# Read availability and source repair — 2026-11-20

The previous live deployment returned 503 for Audit events and BMKG checksums,
partial PVMBG/coastal checksums and empty partial Client API listings. SATELIT
had no canonical events. These failures occurred after authentication with the
configured temporary Audit credential. The official assistant-provided Audit
credential is still a separate later input; changing a token does not fix reads.

## Corrections

- Acquisition cloned a default TLS transport that could already advertise `h2`.
  Setting HTTP/1 protocols did not clear that inherited ALPN advertisement. The
  satellite logs showed binary HTTP/2 frames being parsed as an HTTP/1 response.
  Acquisition now advertises only `http/1.1`, disables HTTP/2 negotiation and
  retains one fresh connection per admitted request. The regression reproduces
  the failure with inherited ALPN before the correction and passes afterward.
- Live reads now retrieve visibility and physical coordinator identity together.
  They retain the shared publication lock, pending-attempt and generation checks,
  physical-primary identity comparison, and final live Patroni leader-lease check.
  The final check runs while the publication lock is still held. A lease loss
  or replacement postmaster invalidates the read. The canonical deadline remains
  two seconds.
- Citus catalog and shard placement are read in one coordinator statement.
  Inventory reuses that request's catalog generation and the final topology is
  checked again. One Patroni `/primary` response supplies member state and lease
  evidence instead of serial `/patroni` and `/primary` requests. Healthy replica
  state is accepted only from a consistent 503 replica response; a primary without
  the leader lock remains unavailable. Routed SQL receives 750 ms instead of an
  early 350 ms cutoff, within the unchanged overall deadline. Redundant checksum
  pings were removed; the actual query still establishes reachability.
- Prepared-transaction settlement checks are restricted to the current database
  on each participant. An unrelated database's prepared transaction cannot block
  this canonical cut. Prepared work in the canonical database still blocks it.
- Wildfire startup previously performed eleven serial queries, sharing a two-second
  budget with cold database connection setup. A live restart consumed 1.5 seconds
  establishing the connection and then exited as if configuration were invalid.
  Startup schema validation is batched and uses a separate 15-second read-only
  initialization budget. Normal canonical operations still have their two-second
  deadline. General schema validation is also batched. Batch SQL tracing records
  fingerprints and cumulative latency without SQL parameters or credentials.
- Runtime pod-to-node placement refreshes in the background with a ten-second
  freshness bound. Expired placement fails closed. Physical primary identity,
  Patroni leader lease, visibility generation and rows are still checked live.
  The replication observer has a bounded four-connection budget, including a
  separate reserved Field connection, instead of serializing general operations
  behind one observer connection.
- Ownership-only transactions prepare and confirm replication on the coordinator
  group where their rows live. The original coordinator identity and synchronous
  peer WAL proof are retained. Hazard and batch durability still verifies all
  participating groups.
- Tempo has two independently observed restart causes: probe-triggered shutdowns
  after two-second readiness/liveness timeouts, and confirmed `OOMKilled` exits
  at its 256 MiB limit during trace processing. The longer probes alone did not
  eliminate OOMs. The role1 SDK previously forced `AlwaysSample`, overriding
  OpenTelemetry sampling environment variables. It now honors the SDK's standard
  settings. The rendered role1 services sample 5% of new root traces and preserve
  the parent decision, including an explicitly sampled demonstration request.
  Logs and metrics are not sampled by this change. The memory limit is unchanged.
- Runtime binaries omit build paths and debug information. The shared image's
  reported size fell from 272,050,742 to 175,688,409 bytes. Component memory
  requests and limits are unchanged.

## Evidence checks that previously allowed false positives

`scripts/demo_checks_lib.py` now requires:

- Game-day checksums to be HTTP 200, correctly shaped, complete responses, with
  a nonempty event baseline before any fault. Recovery compares complete event
  identities, so new arrivals cannot hide loss of baseline records.
- Takeover to produce a completed successful response and advancing canonical
  processing within 60 seconds. A pending dispatch is insufficient. Upstream logs
  must also show a successful request from the new epoch.
- Complete Client API pagination to cover the initial Audit IDs, with all returned
  IDs present in a subsequent Audit traversal. Empty subsets and partial pages
  cannot pass the healthy coverage check.
- Every 202-accepted field report to reach `processed`, including first acceptance
  on a retry. Stable receipts must remain unique per idempotency key.

These changes have negative regressions for the previously observed false passes.
Identity retention tests need a baseline whose events are expected to remain
active; deliberate satellite retractions need the appropriate independent reference.

## Fresh deployment

The operator requested an application reset with **no VM data archives**. The
[application reset procedure](Platform-Application-Reset.md) preserves VMs, K3s,
node identities and Tester Agent, deletes only the assignment namespace and its
40 exact owned local PV contents, and keeps small private configuration records
on the controller. The completed reset records `data_archived: false`.

The private controller root is `~/aat-deploy/clean-20261120/`. Source
namespaces and credentials are preserved in `inputs/release.json`; database identities
and initialization receipts must be recaptured after reset. The repaired role1
image is:

```text
ghcr.io/danieldpw/aat/role1@sha256:3b3bb50f566f02b7267ddb6eda1358d8552025b5f3c8f2e1e354b7955d9a1b1d
```

Its tracked-source fingerprint is
`baddc70723b43dfa96b1d24b3e44b18f7cdeee717f03296ace9637aabab67b70`.
Image publication records remain under
`~/aat-deploy/repairs/read-recovery-20261120/images-v4/`.

## Validation status

Offline validation passed 195 Python tests and 370 Go tests before the sampling
change. The additional sampling regression passed with the platform package. The native Go run
skipped 182 tests needing additional integration inputs. The focused isolated
Citus run passed 26 tests and skipped one additional proof needing separate
inputs; its evidence is `evidence/platform-integration/8bac4b92964c/`. These
results establish component regressions, not four-node load/failover acceptance.

Fresh infrastructure bootstrap and initialization completed. The final v4
maintenance receipt passed and the desired bundle was rendered without blockers.
The apply command timed out awaiting Aggregator readiness while C was still
pulling its image. All three Aggregator pods subsequently ran, but readiness
remained variable. A later 59-workload snapshot still had Client API, Aggregator
and one NATS exporter unready. No controller was left scaled to zero by maintenance.

The current desired deployment is:

```text
~/aat-deploy/current-release.json
~/aat-deploy/clean-20261120/maintenance-v4/release-ready.json
~/aat-deploy/clean-20261120/full-release-v4/bundle.json
~/aat-deploy/clean-20261120/verification-v4/
```

The pointer explicitly marks readiness unverified. All four nodes remained Ready
with their original UIDs. Server-side diffs matched the desired resources except
for generation-only changes on NetworkPolicies; no spec or Secret changes were
reported. This does not establish functional health.

**Functional acceptance still fails.** The final authenticated observation returned
503 for Audit events, all four source checksums and both Media/Field hazard
listings. Logins returned 200. An earlier observation briefly returned complete
HTTP 200 for an empty Audit listing; this is insufficient evidence of source
progress. Raw capture count increased from 170 to 171 during the final window;
the initial canonical hazard count was zero and both consumer record counts
remained zero. The final canonical SQL observation itself was unavailable.

Tempo recorded another OOM termination during the sampling rollout. In the final
three-minute observation it recovered from backoff and its restart count changed
from 5 to 6, then remained at 6 for the remaining samples; two memory samples were
125 and 115 MiB. This is neither a memory safety guarantee nor proof the tracing
problem is fixed. Longer probes and sampling have not established acceptance.

Implementation changes stopped at the operator's strategy checkpoint. No load,
freeze, failover or destructive fault scenarios were run against this failing
baseline. NodePort remains deferred, as do M2 load/fault/exam/reference acceptance.
No VM data archives were created. Changes are preserved for review rather than
presented as a completed repair.

## Observability scope and strategy checkpoint

M2's Observabilitas section requires a metrics dashboard, one cross-service request
trace with step durations, and structured logs with correlation IDs. It does not
prescribe Tempo or collecting every request. The sampled profile retains tracing;
it is not permission to omit that demonstration. Parent-based sampling preserves
an explicitly sampled W3C `traceparent` throughout the participating services.

A bounded in-memory Jaeger deployment is an alternative for short demonstrations,
with a trace UI and no persistent trace disk. Its `max_traces` cap bounds retained
trace count, not bytes; it still needs sampling and a measured memory budget.
Traces disappear when its process restarts. No backend migration has been applied.
See [Jaeger memory storage](https://www.jaegertracing.io/docs/2.22/storage/memory/)
and [OpenTelemetry Go sampling](https://opentelemetry.io/docs/languages/go/sampling/).

The operator requested a stop and strategy reassessment if the final sampling
change does not establish a usable baseline. Further architecture changes,
resets and fault scenarios must not be used to obscure failing healthy reads.
The final bounded observation is recorded privately under
`~/aat-deploy/clean-20261120/verification-v4/`. A short stable observation is not a
sustained-load guarantee. Before redesign, separate host/VM memory pressure,
cross-node latency, and the number of sequential database/authority exchanges
within the unchanged two-second canonical deadline.
