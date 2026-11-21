# Local capacity diagnosis

## Finding

The measured load balancer sends meaningful work to all three Aggregators.
There is moderate imbalance, but the evidence does not support a missing or idle
replica as the main explanation for the failed 2x throughput criterion. Shared
database work, frontend work and the colocated host's CPU pressure are stronger
explanations. The measurements do not uniquely establish one exclusive bottleneck.

The initial diagnosis did not adopt application changes. The subsequent
implementation trial below records the changes and selectable worker-read path.

## Method and result — 2026-11-21

One and then three Aggregators used `m2-20261121-capacity1`, two Client APIs and
the existing default configuration. Each run used 200 VUs, a 10-second ramp,
60-second hold, 15-second ramp-down, 0.01-second think time and no report
submission. Source acquisition/processors were paused for a fixed 192452 active
rows; the count was unchanged throughout the comparison. This is a read-path
diagnostic, not the mixed 10000-VU acceptance test.

| Measurement | One Aggregator | Three Aggregators |
| --- | ---: | ---: |
| Complete hazard pages/s, whole run | 405.7 | 513.1 |
| Unexpected errors / transport failures | 0 / 0 | 0 / 0 |
| Partial / invalid hazard responses | 0 / 0 | 0 / 0 |
| All unsuccessful operations, including controlled rejection | 87.17% | 79.96% |
| Coordinator CPU, sampled hold mean | 2.42 cores | 2.96 cores |
| Combined Client API CPU, sampled hold mean | 2.10 cores | 2.05 cores |
| Combined Aggregator CPU, sampled hold mean | 1.17 cores | 1.72 cores |
| Node A + node B + generator CPU, Docker sampled hold mean | 14.1 cores | 15.2 cores |
| Generator CPU, Docker sampled hold mean; limit 2 cores | 1.85 cores | 1.72 cores |

The resulting gain is **1.2649x**, below 2x. Diagnostic sampling adds overhead,
so this pair must not be presented as a controlled comparison against earlier
uninstrumented runs. It still reproduces the scaling shortfall.

The host has 16 logical CPUs on 10 physical cores; CPU figures are logical CPU
equivalents, not proof of usable physical-core headroom. Docker node limits
remain 6 GiB on A and about 2.15 GiB on B, without hard CPU quotas. These are not
the planned separate 4 CPU / 8 GB VM conditions. Moving the generator off-host
will remove one source of contention, but is not itself proof of a 2x pass.

Existing Prometheus metrics supplied per-pod request counts and latency over a
45-second window ending about 65 seconds after runner startup. Those counts are
scrape-based increases, not exact request totals. Node summaries and owned
coordinator connection activity were sampled during the load. Bounded per-pod
log tails were read after each run, rather than continuously during measurement.
Pool-wait counters count acquisitions finding the pool empty, not wait duration.
In-flight HTTP requests are not directly exposed: duration-sum rates provide an
average concurrency estimate, not an instantaneous in-flight gauge.

## Load distribution

During the three-Aggregator sampled window, successful hazard-read shares were:

| Aggregator pod suffix | Share | Sampled hold mean CPU |
| --- | ---: | ---: |
| `pkjjc` | 35.98% | 0.65 cores |
| `j4sbg` | 37.51% | 0.62 cores |
| `sbjlb` | 26.51% | 0.45 cores |

Both normal and Field routes received traffic on all three pods. Client API
successful hazard reads were approximately 52.6% / 47.4% between its two pods.
Connection reuse can cause uneven Service distribution; this sample does not
show gross starvation. Restarting Client API before each measurement also
re-established its upstream connections after the Aggregator replica change.
No extra proxy or replacement balancing policy is justified by this result.

## Where time and work go

The single Aggregator recorded about 23652 empty-pool acquisitions in the
45-second scrape window. With three Aggregators, two pods recorded zero and the
third about 1.5 after scrape extrapolation. Scaling therefore relieved a real
local pool constraint.

The same canonical page query's bounded log-tail p95 was 21 ms with one
Aggregator and 42, 42 and 43 ms on the three pods. Pool-acquisition p95 changed
from 35 ms to 25 / 0 / 0 ms. These are overlapping operation timings from bounded
tails, not additive end-to-end percentiles or exactly matched sampling windows.
Together with coordinator CPU rising to about 3 cores and both worker primaries
using about 1.5 cores each, they support shared database/host contention after
local pool pressure subsides. Activity samples also saw transactions waiting for
the application between statements; they did not identify advisory-lock waits
as the dominant sampled wait event. The shared read lock does not serialize all
readers against one another.

In the three-pod scrape window, about 48215 requests were rejected by the
per-client gate, after successful authentication. About 64959 were rejected by
the frontend authentication-capacity gate. Auth's own capacity-rejection counter
did not increase. There were roughly three successful Auth introspections for
each successful public hazard read in that window. This is consistent with
unnecessary authentication work on requests later rejected by admission; it does
not mean that authorization can be skipped.

Previously saved CPU profiles, captured under the tuned worker-read experiment,
also identify Client API JSON decoding as substantial work: `Decoder.Decode`
appears in 38.1% of sampled Client API CPU stacks. Synchronous structured logging
appears prominently in mutex profiles. These older profiles identify candidates,
not CPU percentages measured for this default-path diagnostic; their cumulative
stack percentages must not be added together or treated as wall-time fractions.

## Specification requirements versus implementation choices

| Current choice | Required behavior | What could change without discarding the requirement |
| --- | --- | --- |
| Full raw records travel from Aggregator to Client API before Media projection | M1 P3 requires Media to receive only summary fields and rejects explicit raw access | Let the owner produce an authenticated summary projection; keep scope checks, schema validation and Field raw access |
| Every protected request makes a synchronous Auth call before the per-client gate | M1 P3 requires trust separation, scopes, expiry, refresh and rejection of the old token after refresh | Review admission ordering and authentication implementation; any alternative must preserve immediate old-token rejection, rather than blindly caching valid decisions |
| A page uses BEGIN, shared publication lock, unpublished-state check, distributed page SELECT, ROLLBACK and a separate source-status SELECT | M2 P1 requires correct global ordering, explicit partial results and declared consistency; P2 requires fenced acquisition | Reduce round trips or batch compatible work while preserving the publication/fencing guarantees; deleting the barrier without a replacement is not an accepted change |
| Every SQL operation and pool acquisition writes a synchronous success log | M2 Observability requires metrics, a timed cross-service trace and structured correlated logs | Keep useful request/error logs and traces; consider sampling successful SQL detail and retaining aggregate timings, rather than logging every driver operation |
| Per-client limit 16, HTTP pool 8 and separate Field capacity reservations | M1 requires bounded overload handling; M2 P5 requires Field service during Media overload | Exact values and budget arrangement are choices, but raising them indiscriminately already failed earlier tests; retain bounded memory/connections and verify Field protection |
| Two demo client identities shared by every VU | Useful for stressing per-client isolation, but not a specified number of distinct clients | Describe this workload honestly; changing identities alone is not a demonstrated capacity fix and must not replace the original acceptance workload |

Citus itself is the selected implementation, not a mandated product. Global
sharded reads and replicas are required; a coordinator on every normal read,
eight shards and this exact transaction protocol are design choices. The earlier
direct-worker prototype improved throughput but still failed 2x and adds recovery
and connection-budget obligations. This diagnosis does not justify adopting it
or switching databases before trying smaller reductions in per-request work.

The shared noncanonical PostgreSQL process is not identified as the primary
bottleneck by these measurements. Splitting it into additional database
processes would consume memory without addressing the measured canonical and
frontend costs.

## Recommended next changes to evaluate

1. Reduce canonical read round trips while keeping the same publication barrier
   semantics, including error/partial-result behavior.
2. Avoid fetching and decoding raw fields for Media summary requests. Keep
   authorization at the owner and gateway and retain strict contract checks.
3. Reduce successful SQL/pool-acquisition log volume while preserving correlated
   request/error logs, trace steps and measurable query/pool timings.
4. Review admission-before-expensive-authentication options with explicit
   refresh/revocation tests. Do not adopt a token-validity cache by assumption.

Evaluate changes individually against a fixed-data baseline, retain the original
public workload, and repeat final capacity evidence from the separate generator
machine. No implementation change in this list has yet been accepted or applied.

## Evidence and cleanup

Private captures are under
`.m2-local/balancing-diagnosis-20261121T110539Z/`: `report.json` and the two
per-run `run.json`, `summary.json` and `generator.log` files. Query fingerprints
identify the same committed page query in every bounded tail. Raw logs,
credentials and private configuration are not included in this document.

The deployment was restored to three Aggregators, two Client APIs, normal source
settings and default performance limits. Both Minikube nodes were then stopped,
volumes/data retained, and the temporary inotify limit restored to 128.

## Subsequent implementation and fixed-data comparison

Before comparing implementations, runtime inspection found a configuration
precedence bug: stale public tuning in `aggregator-runtime` overrode the
ConfigMap. The actual general database pool was eight connections, although the
configuration declared six. This qualifies the earlier descriptions of
"default" runs: those historical pairs used the actual eight-connection pool.
The Secret now loads first and the ConfigMap second, preserving credentials and
making public tuning authoritative. Running processes were verified at six
general plus three Field connections. This fix is committed as `f069494`.

The new comparison used that corrected budget for **every** candidate, the same
196859 active rows throughout, two Client APIs and the same 200-VU protocol.
There were no per-client, HTTP-concurrency or Field-reservation increases. Images
were built from an isolated source copy so unrelated drafts did not enter them.

| Implementation | One Aggregator: complete pages/s | Three Aggregators: complete pages/s | Same-version gain |
| --- | ---: | ---: | ---: |
| Previous application images, corrected configuration | 370.8 | 415.7 | 1.1210x |
| Transaction startup and shared lock in one call | 356.2 | 504.6 | 1.4165x |
| Above plus owner-side summary projection | 471.5 | 567.7 | 1.2040x |
| Above plus direct worker reads, two general connections per worker | 511.5 | 846.7 | 1.6553x |

Worker reads increased the three-instance result by **49.1%** relative to the
optimized coordinator path. The 846.7 versus 415.7 comparison mixes application
versions; it is an implementation improvement, **not** evidence for P2's 2x
scaling requirement. The valid same-version worker-read result is 1.6553x,
which still fails that requirement.

Existing per-pod counters showed three-pod shares of approximately
39/20/41% for the baseline, 36/34/31% for transaction startup, 38/23/39% for
summaries and 30/36/34% for worker reads. All replicas served traffic. Different
connection distributions and shared-host conditions limit attribution of each
throughput change to code alone; this is one sequential comparison, not a
statistical estimate. The startup-only change was mixed for one versus three
instances. Owner projection deterministically reduced the sampled owner response
from about 32.1 KB to 21.0 KB, roughly 35%, for the same 50 records.

The retained implementation changes are:

- Transaction startup and shared publication-lock acquisition are two SQL
  statements sent in one call. The publication-state check remains a **later
  statement**, so its READ COMMITTED snapshot is acquired after any lock wait.
  A regression test holds the writer lock, commits a new unpublished state, and
  checks that the waiting reader returns an empty explicitly partial page.
- The public Client API requests the internal summary view only after its
  existing scope checks. The owner projects list rows and preserves source
  freshness, revision metadata, ordering, cursor and partial markers. Field/raw
  list reads and detail reads keep their previous contracts. The summary decoder
  validates required fields, enums and duplicate keys; older owner pods returning
  raw lists remain compatible without exposing raw fields to Media.
- Worker reads consult coordinator publication/topology state, query current
  shard primaries in parallel and merge bounded sorted group pages. Missing
  groups or recovery-mode connections are explicitly partial and suppress the
  continuation cursor. Worker connections are reserved from the existing
  per-member budgets, including the separate Field pool. This changes reads,
  not ownership, writes, storage replication or the number of database groups.

Worker reads are now a supported selectable implementation rather than an
uncommitted prototype. `database.aggregator_read_strategy` defaults to
`coordinator`; select `workers` to use the measured worker profile, with
`database.aggregator_worker_read_connections=2`. The deployment and verification
helpers understand both strategies. See [configuration](Configuration.md).

Validation passed: focused Aggregator race checks, the Client API race suite,
19 infrastructure tests and ten real-Citus M2 fixtures in each read strategy
(20 passes). Citus checks include physical primary/replica distribution, full
pagination through both strategies, an injected unreachable worker group,
publication recovery and the new summary/lock regressions. Live probes before
each measured run verified equal Media/Field ordering, no raw fields in Media,
403 for explicit Media raw access and retained raw fields for Field.

All eight measured runs had zero unexpected errors, transport failures, invalid
responses and partial pages; every run's configured hold completed and the
dataset stayed fixed. Controlled rejection remained substantial. Worker mode's
three-instance Field unsuccessful rate was about 0.271%, and successful
Media/Field hazard-read p95 was about 144/135 ms. These read-only results do not
establish full Field report priority under the mixed 10000-VU workload.

Startup is a separate unresolved observation. An initial attempt aborted before
any measured load when a startup probe timed out. The completed comparison
recorded seven first-attempt warm-up failures after readiness, including
timeouts and 503s, before bounded warm-up retries succeeded. No k6 failures were
retried or excluded. These captures do not identify the startup cause or prove
failure-free rolling requests; warming the service is not a fix for that gate.

Private evidence: `.m2-local/readcost-comparison-20261121T113848Z/` contains the
comparison, per-run captures and historical balance evidence. The earlier
aborted attempt is retained separately under
`readcost-comparison-20261121T113301Z/`. Citus logs are
`.m2-local/readcost-citus-integration-002.log`. A first new-fixture attempt
correctly rejected its accidental owner change; the fixture was corrected to
reuse one source owner before the successful run.

Candidate images `aat/aggregator:m2-20261121-readcost1` and
`aat/client-api:m2-20261121-readcost1` were built and loaded on both nodes. After
measurement, the harness restored the previous deployment images, three
Aggregators, two Client APIs and source settings; it then stopped both nodes,
retained data and restored inotify to 128. New code and configuration do not
constitute a permanent promotion of the candidate images or a full M2 pass.

## Mixed 10000-VU coordinator versus worker comparison

At the owner's request, both read strategies were subsequently measured using
the **same** `readcost1` Aggregator and Client API images, summary optimization,
two Client APIs, general/Field database budget 6/3 and unchanged admission/HTTP
limits. Worker mode reserved two general worker connections and one Field worker
connection from those budgets. Both used the pinned generator image with
2 CPU / 4 GiB limits, a 30-second ramp, 135-second hold, 15-second ramp-down,
one-second think time, 10% Field VUs and a report every ten Field iterations.
Both requested and observed **3 -> 5 -> 2** Aggregators; no image rollout occurred
during either measured run.

BMKG/PVMBG polling, coastal acquisition and the three processors remained active;
satellite acquisition stayed disabled in the bounded profile. These were
sequential runs with evolving data, not the earlier fixed-data comparison:
coordinator active rows grew 199074 -> 199975, and worker rows 200097 -> 200825.

| Outcome | Coordinator | Workers |
| --- | ---: | ---: |
| Complete hazard pages/s, whole run | **178.5** | **156.2** |
| Complete pages | 32392 | 28360 |
| Explicit partial pages | 2285 | 3225 |
| Partial share of HTTP-successful hazard reads | 6.59% | 10.21% |
| Invalid hazard responses | 0 | 0 |
| Controlled 429s / all operations | 88.32% | 89.38% |
| Unexpected errors, excluding controlled rejections from denominator | 5 / 0.0072% | 13 / 0.0196% |
| Transport failures, included in unexpected errors | 2 | 0 |
| Successful Media hazard-read p95 | 2.428 s | 2.535 s |
| Successful Field hazard-read p95 | 2.293 s | 2.540 s |
| Unsuccessful Field operations, including controlled rejection | 68.46% | 73.10% |
| Accepted Field reports | 5229 | 5048 |
| Successful Field report-submission p95 | 2.421 s | 2.457 s |

Successful read latencies include explicitly partial 200 responses; the capture
does not provide complete-page-only latency percentiles.

Workers delivered **12.5% less complete-read throughput** in this pair, despite
the earlier fixed-data 200-VU improvement. The coordinator's unexpected outcomes
were three 401s and two transport failures; worker mode had three 401s and ten
503s. Neither run had invalid hazard bodies. Explicitly partial answers are
reported separately rather than counted as complete useful throughput.

The unsuccessful Field rate covers login, refresh, reads and report submissions;
it is not a Field-read-only percentage. For more detail, coordinator Field read
outcomes included 10029 successes and 25568 controlled rejections; workers had
8363 successes and 26003 controlled rejections. Field report attempts included
5229 accepted / 13376 rejected versus 5048 accepted / 17279 rejected. Neither
result establishes Field protection during overload merely because the overall
unexpected-error threshold passed.

Existing cached counters over a 45-second window ending 115 seconds after each
runner started showed about 111410 versus 117201 frontend authentication-capacity
rejections and 21845 versus 35102 per-client rejections. Aggregator empty-pool
acquisition increments were about 1044 versus 3319; Auth increments about
17775 versus 18608. These are scrape-based counts, not measured wait durations.
They support investigating authentication/admission and reduced coordinator/Field
pool capacity under mixed activity, rather than assuming faster worker queries
must improve the whole request path. They do not prove a sole bottleneck.

The generator averaged about 1.969 / 1.957 CPU cores while 10000 VUs were observed,
near its two-core cap. Node A+B CPU averages were about 11.15 / 9.60 cores.
Neither run showed sampled restart increments or OOM kills; neither host-memory
guard triggered. Grafana reached about 255.6 MiB of its 256 MiB limit. The shared
host, saturated generator, differing connection distributions and evolving
source state limit causal attribution from this single sequential pair.

Both runners exited zero and completed their configured stages. Coordinator VU
sampling continuously observed the target for 126.1 seconds. Worker first-to-last
target observations spanned 123.8 seconds, but an observation gap reduced the
longest continuous sampled span to 82.1 seconds. The worker capture therefore
does not independently demonstrate an uninterrupted sampled 120-second interval.
No one-versus-three or image-rollout acceptance claim follows from these runs.
A failed initial coordinator warm-up probe is retained separately from k6 results.

Keep coordinator reads as the default for this mixed-load setup. Worker reads
remain selectable and beneficial in the earlier fixed-data comparison; this
10k result does not support making them the universal default. No application
code or tuning changed between the measured strategies.

Private evidence is `.m2-local/read-strategy-10000-20261121T120643Z/`, including
`comparison.json`, each strategy's `run.json`, `summary.json`, `generator.log`,
resource snapshots and `cached-counters.json`. The harness restored original
images, replicas and strategy overrides, stopped both nodes, retained data and
restored the temporary host inotify limit to 128.

Post-run cleanup exposed a harness bug: the full-load processor-check loop
reused the generator's `name` variable. Docker had launched the correct k6
container, but final inspection/removal and a hypothetical low-memory stop used
the last processor name. Neither memory guard triggered in these captures, so
the workload/results are unaffected. The loop now uses a separate variable;
a regression test forces low memory at 10000 VUs and verifies stopping,
inspecting and removing the actual generator. All 20 infrastructure tests pass.
Both retained generator states were then inspected (exit 0, not OOM-killed) and
only those stopped containers were removed. The original captures retain this
as a separate `post_run_cleanup` entry rather than replacing missing metadata.
