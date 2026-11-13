# Platform acceptance and evidence plan

Owner: Platform, Coordination & Reliability. Component owners contribute
instrumentation, fixtures and assertions. This matrix records required work;
unexecuted scenarios are not passed results.

Executed checks for the first handoff are recorded in the
[local validation report](Platform-Coordination-Validation.md), including a real
PostgreSQL race run. All later deployment/HA scenarios below remain pending.

## Evidence rules

Record run ID, code revision, image identities, configuration without secrets,
actual topology, fault start/end, workload, outcomes and resource usage. Retain
logs with correlation IDs and matching traces/metrics. Distinguish complete,
partial, retryable, controlled rejection and unknown outcomes. Keep the same
identity/timing origin through retries. Do not derive cross-host latency by
subtracting clocks deliberately skewed by a test; record timing basis and error.

Reuse the existing Prometheus/Grafana/Loki/Alloy/Tempo stack and k6 runner. Add
Kubernetes discovery and node/service/replica labels. Load generator saturation
must be measured separately from application limits, especially on one laptop.
Pin tested versions and keep raw results in ignored `evidence/` with sanitized
report-ready summaries. Preserve failure counterexamples as failures.

## Acceptance matrix

| Requirement | Proof / measurements | Partners | Status |
| --- | --- | --- | --- |
| Canonical ownership acquisition/takeover | one winner, increasing epoch, scope policy, renewal/release, stale write rejection | Storage | Implemented local test runner; see its per-run results |
| Transaction-bound fencing | takeover waits for protected transaction; busy expired transaction fails COMMIT; idle client backend terminated | Storage | Implemented local test runner; no VM/HA claim |
| Lease protocol boundary | dedicated auth, strict JSON, bounded body/deadline, no client-selected expiry/TTL | Storage + Ingestion | Implemented tests |
| Additive migration | fresh/M1 upgrade and repeat application; existing checksums unchanged; restricted runtime grants | Storage | Local PostgreSQL regression suite in runner |
| >=3 Aggregators across nodes | placement, healthy-instance entry, per-instance throughput/error/latency | Storage + Platform | Pending |
| Single acquisition and <=60s takeover | upstream request logs, owner epochs, contiguous progress and unique effects | Ingestion + Storage | Pending |
| Worker/authority clock shifts | +45s/-30s, no conflicting successful owners; trace uncertainty | All owners | Pending |
| VM freeze and resume | stale instance cannot write until authority refreshed; bounded cleanup | All owners | Pending |
| Acknowledged-write durability | primary/disk loss, all surviving required WAL/decision state, checksum reconciliation | Storage + Platform | Pending |
| Canonical progress after node loss | safe promotion/routing and commits on two copies before rebuilding | Storage + Platform | Pending |
| Replacement node | join empty local disk, online rebuilding, restored three-copy placement; no full dataset on one node | Storage + Platform | Pending |
| Snapshot ownership/expiry | capture crash/freeze, bounded writer pause, stale cleanup cannot release a successor gate or publish expired capture | Storage | Pending |
| Partial read/analytics | bounded responses, truthful missing units and checksum scope during storage loss | Storage | Pending |
| Service storage isolation | forbidden direct database/network connections fail; required HA and Tester Agent still work | All owners | Pending |
| Broker/202/recipient durability | crash immediately around acknowledgements, accepted identity reconciliation, disk loss, queue capacity | Ingestion | Pending |
| M2 sustained load | >=10,000 VUs, >=120s, replicas 3->5->2; unexpected errors <1%; polling uniqueness preserved | All owners | Pending |
| Horizontal throughput | identical workload/data/environment, successful throughput at 3 replicas >=2x at 1 | Storage + Platform | Pending |
| Version rollout | requests and in-flight work survive replacement during load | All owners | Pending |
| Satellite burst | 60x playback, catch-up <=60s after burst, entry-to-canonical p95 and lag | Ingestion + Storage | Pending |
| Critical coastal delivery | zero distinct required critical operations missing; p95 <=5s; recipient inbox reconciliation | Ingestion | Pending |
| Overload priority/isolation | Tim Lapangan continues while Media is rejected; faulty sensor does not consume all source capacity | Ingestion + API owners | Pending |
| Combined game day | Tester Agent + observed metrics/traces/logs; automatic recovery without intervention during recovery window | All owners | Pending |

## Signals and reliability accounting

Collect per-service request latency/error/throughput; acquisition/renewal/expiry
outcomes and owner changes; stale write rejection; database role, replication lag
and two-copy availability; pending/unknown transactions; queue/backlog/received,
prepared and applied progress; replay outcomes; audit gate/capture duration and
expiry; critical warning receipt age; CPU/RAM/disk/OOM and load generator health.
Each metric has an owner, bounded labels and explicit success/failure meaning.

The [coastal contract](Coastal-Delivery-and-Validation.md) defines the starting
SLI as zero missing critical operations and >=95% within five seconds over the
declared game-day window. Track good/late/failed/pending outcomes and the common
denominator. The timely-delivery budget is 5% of required distinct operations;
zero-loss reconciliation is separate. Confirm the evaluator's timing convention
and preserve original timestamps across replay. Include tail/max and backlog,
not just p95. Produce the required post-mortem with impact, detection, recovery
and improvements.

## First executable proof

```sh
python3 scripts/test-coordination.py
```

This creates only its own loopback PostgreSQL container, runs actual canonical
HTTP/store integration with the real ownership authority and **test-only
single-copy durability**, captures runtime/image identity and results, and
removes its labelled container/data. Evidence is written to
`evidence/platform-ownership/<run-id>/`. `--race` enables the Go race detector.

The test simulates a stalled client database transaction; it does not suspend a
VM, shift a host clock, or establish replicated failover. The production
canonical write path stays unavailable without a verified durability adapter.
Full deployment/load/Tester Agent evidence remains a later milestone.
