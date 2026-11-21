# M2 acceptance evidence checklist

This is a runnable rehearsal guide and an evidence ledger, not a completed test
report. Real-VM tests await the team's hosts. Course-feed, province-reference and
historical catalog agreement await the supplied artifacts/credentials. Local
integration tests use synthetic data and cannot substitute for those checks.

Record revision/image tag, topology, source session, start/end UTC, clock status,
generator host, offered/achieved load, configuration and before/during/after
checksums for every run. Keep tokens and raw sensitive reports out of public
artifacts. Retain original acknowledgments and receipts when checking zero loss.

## Implemented local checks

| Check | Command / evidence boundary |
| --- | --- |
| Placement, isolation, memory classes, volume ownership/reset guards | `python3 -m unittest discover -s scripts/m2 -p 'test_*.py' -v` |
| Native service suites | `python3 scripts/test-services.py --race --jobs 2` |
| Citus commit fences, expired owners, dedup, Audit, retractions, visibility recovery, complete quarantine | `python3 scripts/m2/verify.py --suite canonical` |
| Field lost response/broker outage, original source offsets, durable wildfire region/reference fixture | `python3 scripts/m2/verify.py --suite ingestion` |
| Independent Auth/Dashboard/Notifier database schemas | `python3 scripts/m2/verify.py --suite services` |
| Bounded feed resume probe | `aggregator/cmd/satellite-probe`; practice token exercised as recorded below |
| Synthetic wildfire oracle | `aggregator/cmd/wildfire-reference replay`, fixture configuration and JSONL |

A skipped integration test is not a pass. Preserve the actual command output and
check that required database/broker settings were present. The normal unit suite
does not run destructive VM scenarios.

## Latest local Minikube rehearsal (2026-11-21)

Environment: two-node Docker Minikube profile `aat-m2`, Kubernetes v1.35.0,
image tag `m2-20261121-acceptance`. This is a local rehearsal, not the planned
two-host Ubuntu/K3s deployment. The private snapshots are under `.m2-local/`.

- `manage.py up` completed after fixing the passive Fluent Bit drain check and
  versioning the broker initializer Job by both source and image tag. All
  application, database and broker controllers reached Ready.
- `verify.py --suite all` passed after deployment and again after each recovery.
  The dedicated broker phase supplied the real NATS URL and passed the JetStream
  fetch, DLQ and warning-delivery checks. Generic package runs that lacked that
  variable reported those particular tests as skipped; those skips are not being
  counted as passes.
- Aggregators scaled 3 -> 5 -> 2 -> 3. All five were Ready with zero restarts,
  and the normal three replicas were restored. This checked readiness and
  placement only; no request load was applied during the scale sequence.
- With B stopped, the guardian promoted canonical group 2 on A and selected the
  A-only write policy. Audit returned a complete result with no unavailable
  units. Isolated Citus dedup/fencing writes, ingestion tests, and real NATS
  broker tests passed during the outage. Restarting B restored readiness and the
  post-recovery integration suite passed.
- The first B rejoin had a kube-proxy `too many open files` exit and B-side
  networking stalled. A clean Minikube profile restart restored it. A second
  B stop/start did not reproduce the file-descriptor error; Calico and
  kube-proxy restarted and all workloads became Ready without another profile
  restart. The transient Minikube issue has no code-level fix or physical-host
  reproduction yet.
- In a post-recovery snapshot while mock ingestion was still active, Docker node
  containers used about 3.48/6 GiB on A and 1.19/2.15 GiB on B. Per-pod working
  sets were about 222 MiB for Grafana
  (256 MiB limit), 49 MiB Prometheus, 41 MiB Loki, 17 MiB Jaeger, and 6 MiB per
  Fluent Bit pod. The Grafana figure leaves little margin. Minikube's Metrics API
  was unavailable, so these values came from the node/container and kubelet
  cgroup snapshots, not `kubectl top`.

The local PC had no `k6` binary, and the separate generator PC was not connected
to this run. Therefore the 10,000-VU/120-second run, 1-vs-3 throughput target,
request-continuity/rolling test, and all performance thresholds remain pending.
The outage was a Minikube node stop/start, not a physical host power loss, and
the <=60-second takeover target was not timed. Synthetic input continued during
the run, so Audit counts/checksums changed; these snapshots do not establish a
strict before/after zero-loss comparison of acknowledged source records.

## Live satellite practice trial (2026-11-21)

The owner configured the token privately and approved reuse of the 38-province
[reference table](Province-Reference.md). It was checksum checked and installed
with writers stopped. Credentials are absent from public evidence.

The first 32-record probe exercised detections/retractions but missed updates.
A live acquisition trial then exposed `{offset,key,changes}` partial updates,
which the original normalizer incorrectly parked as incomplete detections. The
fixed normalizer preserves the patch and the original bytes; the processor
merges supported changes into the retained detection before validation. The
fixture oracle preserves the original operation and fingerprint for replay.
Satellite/wildfire race tests and ingestion integration regressions passed.

The expanded probe passed three 1,024-record legs (initial, overlapping resume,
next-offset resume), covering 2,850 detection, 164 update, and 58 retraction
deliveries. Replayed offsets retained matching payload/operation hashes. The
session advertised speed 60. These are replayed deliveries, not counts of unique
events stored by the application.

Release `m2-20261121-satellite1` was deployed. Acquisition was paused while the
retained 1,961-record trial was drained and 76 parser failures were selected
through the authenticated Ops replay endpoint. Of those, 75 resolved. Two records
remained parked because their source updates followed a retraction of the target
(offsets 859 and 1480). Their raw bytes remain available. Whether those updates
should leave the target inactive or reactivate it needs course-contract/reference
confirmation; this run does not claim complete source-semantic agreement.

Normal acquisition resumed from the stored position. One sample showed received
5139, prepared 2493, applied 2465, with virtual time/watermark advancing and no
remaining incomplete-detection parser failures. Processing lag grew. The source
already had more than 2.45 million historical offsets when the trial began, and
this trial started at 0. No 60x catch-up, complete-history replay, p95 processing
latency, or official reference comparison has passed. Evidence is private under
`.m2-local/satellite-probe-live-*` and `.m2-local/evidence-satellite-*`.

The demo cluster is stopped after evidence capture to bound disk use. Data and
cursors persist; starting the same profile resumes the token-enabled deployment.

## Retraction rule and deployed Ops replay (2026-11-21)

The owner approved treating updates as attribute changes that preserve an inactive
detection's retraction. A later detection event may explicitly restore it.
Both the fixture oracle and durable worker implement this rule. Race tests and
replicated-ingestion regressions passed, including patch/full updates after
retraction, a replacement worker after lease expiry, repeated delivery, and
preserving inactive state during an older retained-input replay. This supersedes
the earlier local active-target assumption; agreement with the assistants'
reference remains a separate gate.

The PVMBG mock now accepts JSON `mode` values `v1`, `compatible`, and `breaking`
on `POST /admin/schema-version`. Breaking mode renames `alert_level` to `level`,
changes the eruption count to text, and omits ash height. The original empty-body
M1 toggle remains available. See [the video walkthrough](Replay-Demo.md).

Release `m2-20261121-replay1` was deployed with acquisition disabled for the bounded
rehearsal. The actual Client API Ops flow passed: Internal Ops downloaded a
541-byte retained incompatible PVMBG payload with verified SHA-256; Media/Field
inspection and replay attempts returned 403; reader version 2 was activated;
the replay request selected one input; an immediate duplicate request selected
zero; the selected failure resolved; and exactly one canonical event existed for
the demo source report. The mock returned to v1. The reader was left on version
2, which also accepts v1. Canonical fencing/replay integration tests also passed.
Private evidence is `.m2-local/evidence-pvmbg-ops-replay-002/result.json`.

Older retryable HTTP-503 records were preserved as separate operational history;
the check did not erase them or claim the whole failures list was empty. Their
resolution/status presentation needs review. Browser assets and the UI's API
endpoints were exercised; an actual browser-click/video rehearsal is not recorded.

The two old satellite inputs were requested through the Client API after the fix,
but still await replay behind the retained raw-stream backlog. Completion of those
two live replays is not claimed. New satellite acquisition remains disabled in
the rehearsal deployment to bound storage use; the normal private configuration
still contains the token. Use normal `manage.py up` to restore that configuration.

### Remaining acceptance gates

| Area | Still unconfirmed |
| --- | --- |
| Real deployment | Ubuntu/K3s on separate physical hosts; WireGuard/LAN behavior and restart; actual 4 CPU/8 GB/32 GB fit |
| P1 recovery | Empty-B replica rebuild, timed failover, full acknowledged-input zero-loss comparison, final replica/failback evidence with the official catalog |
| P2 ownership | SIGSTOP/unfreeze, clock skew, <=60-second takeover and upstream overlap evidence during actual failure/scaling |
| P2 load | 10,000 VUs for >=120 seconds with 3 -> 5 -> 2, <1% unexpected errors, 3-vs-1 >=2x throughput, M1 latency target, and rolling requests without failures |
| P3 full rehearsal | Browser recording, broader live malformed-input/oversize cases, retry-history state cleanup, Field-pod crash after 202 with acknowledged-report reconciliation |
| P4 satellite | The two queued historical replays; full clean interrupted/uninterrupted comparison; official reference/province agreement; 60x catch-up <=60 seconds and processing p95 |
| P5 coastal | Live independent endpoint failures, stale reads, critical delivery with canonical storage blocked, complete receipt reconciliation and <=5-second p95 |
| P5 priority | Media overload while Field login and report submission continue through reserved capacity |
| Observability/resources | Browser metrics/logs/traces incident walkthrough and resource caps under sustained load, rollout and recovery |
| Reset | Full destructive application/observability reset on the cluster; only guards/filesystem tests and dry runs have been exercised |
| External artifacts/AI | Official catalog/Audit/evaluation artifacts when supplied; optional OpenRouter integration with actual credentials and model |

## P1: partitioning and recovery

- Import the authoritative earthquake catalog through the canonical service owner;
  do not insert into another service's private database. Record catalog count and
  checksum before failure. The importer/contract must match the supplied catalog.
- Check eight canonical shards span worker groups 1 and 2, each with a second
  physical copy. Verify primaries write on both physical nodes when healthy.
- Record `/internal/audit/checksum`, representative `/internal/audit/location`
  results, Patroni roles/timelines and NATS stream/consumer replica state.
- Run independent reads/writes while B powers off. Measure failure detection and
  takeover; retain every acknowledged write. Check partial/unavailable responses
  explicitly rather than accepting an empty result as complete.
- Restart B and verify replication/catch-up, both shard copies and restoration of
  worker 2's B primary. Compare acknowledged-input results and checksum.
- Rehearse the separate empty-B test only on the dedicated B agent VM:

```bash
sudo python3 deployment/m2/rebuild-b.py --hostname ACTUAL_B_HOSTNAME
sudo python3 deployment/m2/rebuild-b.py --hostname ACTUAL_B_HOSTNAME --execute
```

This script checks the hostname/agent role, stops B and its K3s containers, clears
only the six declared B application replica directories and restarts the agent.
It preserves B's K3s identity. It has not yet been tested on the real VMs. Keep A
running and observe rejoin from A; do not invoke the full application reset or
schema initialization to conceal lost data. A failure requires separate recovery
and is outside the accepted two-node availability promise.

## P2: leases, scaling and request continuity

- Compare upstream logs while Aggregator replicas scale 3 -> 5 -> 2. Permit retries
  after an ambiguous response while checking there is no simultaneous ordinary
  source owner and no duplicate final HazardEvent.
- Stop/freeze the current owner and measure takeover <=60 seconds. Inspect lease
  scope, pod/process incarnation and epoch. Unfreeze it and check stale commits
  are rejected. Database-expiry integration tests alone do not prove SIGSTOP or
  physical clock-skew behavior.
- Compare 1 versus 3 Aggregators on the same warmed dataset and generator; require
  at least 2x throughput, preserving the same operation mix and latency boundary.
- Run 10,000 VUs for >=120 seconds from the separate PC. Report both offered and
  achieved rates, timeouts, 429s and 5xxs. Keep unexpected errors below 1% according
  to the evaluator's boundary; never exclude overload responses to manufacture a pass.
- Roll Aggregator pods while requests continue; require no failed requests and
  graceful drain. Use a new image tag and Kubernetes rolling deployment. `up`
  intentionally stops writers for offline migrations and is not this demonstration.

## P3: incompatible payload and ordered replay

- Exercise compatible writer extras, malformed bytes and a breaking PVMBG schema.
  Check the M1 reader projection and whole original payload preservation.
- Open Client API `/ops` as Internal Ops. Inspect failure reason/status, download
  original bytes and compare SHA-256. Metadata-only Audit must not leak raw data.
- Activate the explicit approved shared reader/adapter version, select retained
  messages and replay. Verify entity ordering and stable canonical identity.
- Crash after canonical commit but before source/transport acknowledgment; retry
  the same identity and compare final state. Include oversized and truncated
  transport responses; a retained prefix must never be described as complete.

## P4: satellite timing and state

- Probe actual SSE IDs, session/reset behavior, inclusive/exclusive resume,
  reconnect overlap, update/retract payloads and virtual-clock behavior.
- Install the authoritative province artifact and record SHA-256. Confirm WGS84
  distance, spherical center, ties and fire identity agree with the course reference.
- Exercise low-confidence exclusion, singleton counts, transitive bridges,
  1 km/360-minute boundaries, late corrections, split/merge and immediate
  predecessor restoration. Inspect stable hazard UUIDs and retraction output.
- Run at 60x and demonstrate catch-up <=60 seconds against the declared source
  clock. Record received/prepared/applied original offsets separately, oldest
  pending age and durable-receipt-to-canonical p95. Never relabel output ordinals
  as original feed offsets.
- Kill/restart the processor during a correction and compare final state with an
  uninterrupted run. Check bounded working memory/backpressure under a dense region.

## P5: coastal failures and priority

- Exercise observations and station-metadata endpoint failures independently;
  confirm breaker opening, bounded half-open probes and the shared request limit.
- Verify stored hazard reads remain fast while the source fails, with visible
  stale/unavailable status. Successful repeats of old observations do not imply
  fresh measurements. Check virtual source age separately from wall-clock receipt age.
- Overload Media traffic while Field requests continue through live introspection
  and reserved admission/DB capacity. Include login and report submission, not just
  reads. Client-supplied role headers must not bypass authentication.
- Block canonical storage while valid critical warnings continue through their
  separate durable lane. Verify expected identities were persisted first and every
  eligible warning reaches the recipient; measure p95 <=5 seconds. Rejected invalid
  critical inputs and missing receipts must remain visible in reconciliation.
- Open Grafana metrics/logs/traces during the incident and explain it from measured
  evidence. Confirm all application replicas are scraped, correlation IDs match
  across handoffs, and telemetry stays within resource caps without repeated restarts.

## Resource and demo lifetime checks

Use cgroup/container working sets and host disk use during load, rollback,
rebuild and warm startup. Record peak memory per role, node totals, OOM/restarts,
WAL/message growth, filesystem free space and catch-up headroom. Test full-queue
backpressure without deleting pending work. Configured requests, PV sizes and
retention settings do not establish physical fit.

Save evidence before stopping. Ordinary stop/start preserves state. Explicit
application/observability resets are only for controlled demo cleanup outside
measured recovery windows.
