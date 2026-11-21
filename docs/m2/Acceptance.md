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

## Sustained-load and physical-shard follow-up (2026-11-21)

This follow-up supersedes the earlier statement that no local k6 run was
available. The dedicated real-NATS integration phase and all other integration
suites passed on `m2-20261121-local-acceptance2`. The physical P1 fixture placed
256 colocated events in all eight shards, verified all 16 physical copies and
primary roles on both nodes, and checked global pagination and analytics/Audit
counts. This is a synthetic distribution check; official catalog skew and an
empty-B recovery ledger still require their own evidence.

The local 10,000-VU run completed its configured 135-second hold with source
acquisition/processors active (satellite disabled), performing 3 -> 5 -> 2 and an
actual Aggregator image update. Of 718005 operations, 655022 were controlled 429s,
62732 succeeded and 267 were unexpected. Unexpected errors were 0.0372% of all
operations or 0.4238% when controlled rejections are also excluded from the
denominator. There were no transport failures. HTTP-200 hazard reads averaged
158.4/s, with p95 about 2.1 seconds. Those bodies were not retained, so complete
versus explicit partial pages cannot be reconstructed from that capture. Later
runs use body checks and separate complete/partial counters. Sampled VUs spanned 134.5 seconds at target, but
one missed observation breaks the continuous sampled interval. This does not
prove failure-free rolling requests or P2 ownership criteria during the run.

Controlled rejection was about 91%; M2 specifies no maximum percentage for it.
This is overload evidence, not proof of useful 10,000-user operating capacity.
Field outcomes and successful throughput must be assessed separately. A fixed
153691-row comparison measured 367 vs 453 successful reads/s with 1 vs 3 Aggregators:
1.2345x, below the required 2x. That requirement remains unmet.

Larger HTTP pools did not solve the issue: the subsequent full-load experiment
had 1.4315% unexpected errors after excluding controlled rejections, 407 transport
failures, and only 69.6 successful reads/s. It is a failed experiment. Holding Auth
admission slots until downstream completion also failed a same-data comparison:
no meaningful throughput gain and substantially more Field rejection. Both
changes were rejected and the original defaults/slot lifetime restored.

Kubelet resource samples showed no pod OOM or restart changes during either
full-load run. Grafana approached 241 MiB of its 256 MiB limit. The local nodes use
6 GiB (A) and about 2.15 GiB (B), with no Docker CPU quota; the generator was capped
at 2 CPU/4 GiB and frequently reached its CPU ceiling. These are not the planned
real-VM resource-fit results. Private evidence resides under
`.m2-local/load-10000-scaling-001`, `load-10000-capacity-001`,
`throughput-comparison.json`, and `admission-comparison.json`.

Use the [local load runner](Load-Testing.md) to reproduce measurements. The
remaining-gates table below still applies, with the attempted load/scaling checks
recorded here rather than counted as fully accepted.

The repeated fixed-data comparison with body checks confirmed all pages were
complete: Citus served 448.6 -> 532.2 pages/s with one -> three Aggregators
(1.1866x). An optional worker-read prototype served 468.7 -> 756.3 pages/s
(1.6137x), about 42% more with three instances, with no partial/invalid responses
or unexpected errors. This prototype remains experimental and below the 2x
criterion; the normal path is not changed by these results. Its real Citus tests
also compare both strategies' full pagination and verify that losing one worker
connection returns the surviving group explicitly marked partial.

### Capacity findings at reassessment

The latest measured throughput using the normal coordinator read path and
default limits is **448.6 complete hazard pages/s with one Aggregator and
532.2 with three**, a **1.1866x** gain. The best comparable scaling result,
**1.7596x**, requires experimental worker-read code and configuration changes;
it is not the throughput of the accepted implementation. No measured pair
establishes the required 2x gain.

| Read path and configuration | One Aggregator: reads/s | Three Aggregators: reads/s | Gain | Response validation |
| --- | ---: | ---: | ---: | --- |
| Normal Citus path, two Client APIs, default limits | 448.6 | 532.2 | 1.1866x | Complete pages checked |
| Normal Citus path, four Client APIs, per-client limit 64 | 575.8 | 648.4 | 1.1261x | HTTP 200 only |
| Experimental worker reads, two Client APIs, original comparison | 468.7 | 756.3 | 1.6137x | Complete pages checked |
| Experimental worker reads, two Client APIs, tuned limits | 465.9 | 819.7 | 1.7596x | Complete pages checked |
| Experimental worker reads, four Client APIs, tuned limits | 556.8 | 891.7 | 1.6015x | Complete pages checked |

These are dedicated **200-VU** read benchmarks: 10-second ramp, 60-second hold,
15-second ramp-down and 0.01-second think time, with report submission disabled.
Rates cover the whole measured run. Acquisition/processors were paused to keep
each pair's dataset fixed, then restored. Each one-versus-three pair held the
other settings and dataset constant; datasets differ between experiments, so
rows are not a controlled comparison of every setting. All listed pairs had
zero unexpected errors; body-checked pairs had zero partial or invalid pages.
These results do not replace the mixed 10,000-VU acceptance run.

The first and third rows used the same image and 174099-row dataset, switching
the experimental read strategy off/on. The configuration-only row used 170030
rows; the tuned two-API and four-API worker comparisons used 180044 and 182827
rows respectively. Worker reads query the Citus shard-holding PostgreSQL
servers directly and merge pages in the Aggregator, while still consulting the
coordinator for publication/topology metadata. This remains an uncommitted,
disabled prototype. Its later connection-budget checks and partial-page tests
do not establish full fault/recovery acceptance.

Private evidence: `read-strategy-comparison.json`,
`throughput-front-tier-client64-comparison.json`,
`worker-tuned-front-comparison.json` and `worker-api4-tuned-comparison.json` under
`.m2-local/`. Raw captures and private configuration are not committed.

A completed diagnostic profiling run showed substantial syscall, allocation/GC
and JSON work in both Aggregator and Client API, plus logging contention in mutex
profiles. These identify investigation candidates, not a single proven
bottleneck or an accepted optimization. Temporary profiling source overlays and
deployment images were removed/restored; no further optimization was adopted.
Private evidence: `profiles-capacity-001/`.

### Ownership and startup follow-up

A runtime PVMBG polling-owner freeze check passed on the normal deployment. The
process was stopped for 25.84 seconds; a different holder committed under a newer
lease within 25.72 seconds. The same container resumed, no duplicate canonical
source identities were found, and upstream intervals showed peak concurrency one
on both `/volcanic-reports` and `/history`. A stale in-flight commit attempt was
not observed; explicit late-commit rejection remains integration-test evidence,
not a claim about this runtime sample. Evidence: `freeze-pvmbg-evidence.json`.
Physical clock-skew testing remains a separate gate.

The registry pool configuration was corrected and verified: all 240 burst reads
returned 200 and the named PostgreSQL datasource held four sessions rather than
twenty. This fixes a connection-budget discrepancy, without claiming a P2 scaling
pass. See [configuration details](Configuration.md).

A fresh startup reproduced `too many open files` on A's kube-proxy and a fsnotify
watcher failure in controller-manager, with the shared host's inotify instance
limit at 128. Temporarily increasing it to 1024 and restarting the failed A-side
services restored cluster networking. This identifies a concrete cause for this
startup incident; it does not prove the earlier B incident had the same cause.
The temporary host change was restored to 128 when the test cluster stopped.
Evidence: `acceptance-startup-incident.json`.

### Stopped checkpoint

Testing stopped at the owner's request for reassessment. Before shutdown, the
deployment was restored to three Aggregators and two Client APIs on
`m2-20261121-capacity1`, with normal defaults and no experimental overrides.
Both Minikube nodes are stopped, no load generators remain, and volumes/data
were retained. The worker-read prototype and other unfinished code changes
remain outside this findings commit. No further testing is implied by recording
these results. Private evidence: `reassessment-checkpoint.json`.

### VPN-disconnected throughput rerun (2026-11-21)

At the owner's request, the normal coordinator read path was measured again
after they confirmed disconnecting the unwanted VPN. The generator's destination
was `192.168.49.3` over the local Docker bridge; host policy rules used only the
standard local/main/default tables. A `wg0` interface remained present, but the
measured route did not traverse it. No code changes, new images or worker-read
experiments were introduced for this rerun.

The same dedicated 200-VU protocol above used two Client APIs, default limits and
`m2-20261121-capacity1`. Acquisition/processors were paused for a fixed 191131
active rows, unchanged before and after both measurements.

| Aggregators | Complete hazard pages/s | Media hazard-read p95 | Field hazard-read p95 |
| --- | ---: | ---: | ---: |
| One | 433.3 | 184.0 ms | 203.7 ms |
| Three | 510.4 | 200.9 ms | 173.2 ms |

The gain was **1.1780x**, close to the earlier default-path **1.1866x** and still
below 2x. Both runs completed with zero unexpected errors, transport failures,
partial pages or invalid responses. Controlled rejections were 86.86% and 80.27%
of all operations respectively; Field unsuccessful outcomes were 0.0315% and
0.1673%. These are read benchmarks with report submission disabled, not another
10,000-VU mixed-workload acceptance run. The configured 60-second hold completed;
REST sampling observed 56.1 and 51.1 seconds continuously at target VUs.

This rerun reproduces the scaling shortfall without the unwanted VPN. It gives
no evidence that the VPN caused that shortfall, but cannot reconstruct the old
VPN's routing or isolate its historical effect: the earlier run had fewer rows
and host conditions can vary. Deployment defaults/replicas were restored after
measurement, both cluster nodes were stopped with volumes retained, and the
temporary inotify limit was restored to 128. Private evidence:
`.m2-local/throughput-vpn-recheck-20261121T105422Z/comparison.json` and the two
per-run `run.json`, `summary.json` and `generator.log` captures.

Connection-level balancing with reused HTTP connections remains a possible
contributor, not a confirmed cause. The earlier tuned four-API worker-read run
showed all three Aggregators busy at about 1.27-1.36 CPU cores during the hold,
which does not suggest one idle replica in that experiment. CPU is not a request
distribution measurement, and that configuration differs from the default-path
rerun. Per-pod request counts/in-flight requests and latency at both Client API
and Aggregator are needed before attributing the scaling gap to uneven routing.

### Per-pod balancing diagnosis (2026-11-21)

The owner subsequently authorized a measured diagnosis using existing per-pod
telemetry. All three Aggregators received meaningful hazard traffic, with shares
of 35.98%, 37.51% and 26.51% in the sampled window. A fixed 192452-row pair measured
405.7 -> 513.1 complete pages/s, or 1.2649x; unexpected errors, transport failures,
partial pages and invalid responses were all zero. Instrumentation overhead
means this pair is not a controlled comparison against earlier runs.

Scaling relieved Aggregator pool waits, while shared database query latency and
coordinator CPU rose. The local nodes plus generator consumed about 15.2 logical
CPU equivalents during the three-pod hold. The evidence favors shared work and
host contention over a broken balancer, without identifying one exclusive cause.
See [capacity diagnosis](Capacity-Diagnosis.md) for measurements, limitations,
specification requirements versus optional design choices, and proposed next
changes. No application optimization was adopted. Normal deployment settings
were restored, both nodes stopped, data retained and host inotify restored to 128.

### Read-cost implementation trial (2026-11-21)

The owner approved reduced database round trips, owner-side Media summary
projection and evaluation of worker reads. A stale Secret was found to override
the configured database pool (actual eight versus intended six); environment
precedence was fixed and all candidates used the verified six-plus-three budget.
On the same 196859 fixed active rows, the eight 200-VU runs measured:

| Implementation | One Aggregator: complete pages/s | Three Aggregators: complete pages/s | Gain |
| --- | ---: | ---: | ---: |
| Previous images, corrected configuration | 370.8 | 415.7 | 1.1210x |
| Reduced transaction-startup round trips | 356.2 | 504.6 | 1.4165x |
| Above plus owner summaries | 471.5 | 567.7 | 1.2040x |
| Above plus worker reads | 511.5 | 846.7 | 1.6553x |

Worker reads were beneficial, but **same-version scaling still misses 2x**.
Every measured run had zero unexpected/transport errors and zero invalid/partial
responses. The dataset stayed fixed, and live scope/raw-field checks passed.
Focused race suites, 19 infrastructure tests and 20 real-Citus fixture passes
covered both strategies. See [implementation findings](Capacity-Diagnosis.md)
for read semantics, connection reservations, response-size reduction and limits.

Seven failed initial warm-up probes after readiness remain recorded separately;
they are not hidden in the measured success totals. Startup/rolling continuity
remains unconfirmed. This was not a new 10000-VU mixed-workload acceptance run.
Coordinator reads remain the default; worker reads are selectable in operator
configuration. The harness restored the previous images/settings, stopped both
nodes, retained data and restored host inotify to 128.

### Mixed 10000-VU read-strategy comparison (2026-11-21)

Both strategies used the same readcost1 images, default admission/HTTP settings,
verified 6+3 database budget, two Client APIs, active BMKG/PVMBG/coastal acquisition
and processors, and the same 30-second ramp / 135-second hold / 15-second down
profile with 3 -> 5 -> 2 scaling. Satellite stayed disabled; no image rollout was
requested. Data grew during each sequential run.

| Outcome | Coordinator | Workers |
| --- | ---: | ---: |
| Complete hazard pages/s | 178.5 | 156.2 |
| Explicit partial pages | 2285 | 3225 |
| Controlled rejections / all operations | 88.32% | 89.38% |
| Unexpected-error rate, controlled outcomes excluded | 0.0072% | 0.0196% |
| Successful Field hazard-read p95 | 2.293 s | 2.540 s |
| Unsuccessful Field operations, including controlled rejection | 68.46% | 73.10% |
| Accepted Field reports | 5229 | 5048 |

Workers were 12.5% lower in complete-read throughput in this comparison. Both
passed the runner's unexpected-error threshold, but these results do not
establish Field overload protection or complete normal responses. There were no
invalid responses, sampled OOM kills or restart increments. The colocated
generator was near its two-core limit in both runs.

Both configured holds completed and replica counts converged to 3, 5 and 2.
Target-VU observation spans were 126.1 seconds continuously for coordinator and
123.8 seconds first-to-last for workers; a worker observation gap left only 82.1
seconds continuously sampled. This is not a full acceptance pass or evidence
for same-version 2x scaling or failure-free image rollout. See the
[full comparison and limitations](Capacity-Diagnosis.md). Coordinator remains
the default. Original deployment settings were restored, both nodes stopped,
data retained and host inotify restored to 128.

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

## Earlier retraction rule and deployed Ops replay (2026-11-21)

The earlier rule below is superseded by the reactivation decision recorded next.
The owner initially approved treating updates as attribute changes that preserve an inactive
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

The two old satellite inputs were requested through the Client API after the fix
and both completed. Offsets 859 and 1480 now have state `applied`; their targets
remain inactive with updated confidence/FRP. Private evidence is
`.m2-local/satellite-inactive-replay-resolved.json`. New satellite acquisition
remains disabled in the rehearsal deployment to bound storage use; the normal
private configuration still contains the token. Use normal `manage.py up` to
restore that configuration.

## Reactivation decision and verified replay (2026-11-21)

The owner approved a valid later update reactivating its retained detection.
Partial changes merge into the retained attributes. Source offsets order these
changes: an older detection, update, or retraction cannot undo newer state or
attributes. Unknown targets remain parked. Low-confidence detections remain
excluded from grouping even when active, as M2 requires. This supersedes both
the earlier active-target rejection and the subsequent keep-inactive rule.

The oracle and durable worker implement this behavior. Race tests for wildfire
and satellite processing and the seven-test ingestion integration suite passed.
Coverage includes partial/full updates, replacement workers after lease expiry,
older replay, idempotent delivery, and preserving one fire identity when a fully
retracted fire returns. The worker now retains the immediate predecessor fire
before clearing a detection's current membership.

Release `m2-20261121-reactivation1` was deployed and reached application readiness.
With acquisition disabled, the offline migration selected only the two affected
retained updates. Internal Ops downloaded their original bytes, checked SHA-256
against the live probe, and requested their replay. Both reached `applied` with
active targets. Offset 859 has confidence `l` and no fire membership; offset 1480
has confidence `n` and belongs to a fire. A read-only migration postcheck found
zero remaining candidates. No source bytes, offsets, or storage were reset.
Private evidence is `.m2-local/satellite-reactivation-resolved.json` and
`.m2-local/reactivation-postcheck.json`.

This bounded correction does not establish full-history reference agreement or
the 60x catch-up SLO. Acquisition remains disabled in the rehearsal configuration;
the normal private configuration retains the token for the next normal startup.

### Remaining acceptance gates

| Area | Still unconfirmed |
| --- | --- |
| Real deployment | Ubuntu/K3s on separate physical hosts; WireGuard/LAN behavior and restart; actual 4 CPU/8 GB/32 GB fit |
| P1 recovery | Empty-B replica rebuild, timed failover, full acknowledged-input zero-loss comparison, final replica/failback evidence with the official catalog |
| P2 ownership | Runtime freeze/takeover observed; stale in-flight resume, physical clock skew and upstream evidence during scaling still need their full checks |
| P2 load | 10,000 VUs for >=120 seconds with 3 -> 5 -> 2, <1% unexpected errors, 3-vs-1 >=2x throughput, M1 latency target, and rolling requests without failures |
| P3 full rehearsal | Browser recording, broader live malformed-input/oversize cases, retry-history state cleanup, Field-pod crash after 202 with acknowledged-report reconciliation |
| P4 satellite | Full clean interrupted/uninterrupted comparison; official reference/province agreement; 60x catch-up <=60 seconds and processing p95 |
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
