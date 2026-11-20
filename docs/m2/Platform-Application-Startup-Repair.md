# Application startup repair — 2026-11-20

This records the earlier deployment. Its database identities and bundles were
superseded by the later application reset. See
[read and source repair](Platform-Read-and-Source-Repair.md) for the current work.
Do not reapply the historical bundles below to the reset application.

The first full release applied its services but exceeded the Aggregator's
progress deadline. Live diagnosis found the following startup and recovery
configuration problems.

| Problem | Evidence | Correction |
| --- | --- | --- |
| Missing Citus runtime worker authentication | Coordinator errors reported `fe_sendauth: no password supplied` for `aggregator_app`; only `postgres` had a `pg_dist_authinfo` entry | A bounded administrator Job installs existing Secret credentials on all five captured canonical primary groups, then verifies worker fan-out as the runtime role |
| Aggregator could not check Patroni authority | `/ready` reported `store: false`; outgoing port 8008 was allowed but canonical PostgreSQL ingress omitted Aggregator | Permit Aggregator ingress to canonical Patroni port 8008, retaining the existing application restrictions |
| Consumers attempted to secure `/tmp` itself | Both consumers exited with `chmod /tmp: operation not permitted` | Configure owner-created socket directories `/tmp/dashboard-admin/` and `/tmp/notifier-admin/` |
| Alloy could not access its state directory | Its image eventually pulled, then exited with `mkdir /var/lib/alloy/data: permission denied` | Mount its existing 128 MiB temporary state volume at `/tmp/alloy-data` and use that path |
| Broker recovery was repeatedly interrupted | Broker B exited normally after probe-triggered SIGTERM while a JetStream consumer was catching up | Use server-focused readiness and JetStream-enabled liveness, five-second probe timeouts and a longer startup window; update members one at a time |
| Tempo exceeded its 256 MiB limit | Repeated `OOMKilled` exits occurred while completing trace blocks and compacting older blocks | Limit concurrent flushes, block and Parquet buffers; set a 128 MiB Go memory target while keeping the container limit at 256 MiB |
| Registry recovery was interrupted | Events recorded liveness and startup probe failures followed by container termination, with exit code 0 | Allow Java/database warm-up a longer startup window and tolerate short liveness stalls; readiness still gates traffic |
| Multi-step ingestion/coastal health checks outlasted their probes | Direct checks returned HTTP 200 after 2.13 and 2.43 seconds, exceeding Kubernetes's two-second timeout | Observe those two handlers for up to five seconds, every ten seconds; keep application operation deadlines and all health conditions unchanged |
| Processing updates lacked space for an extra replica | Scheduler events reported two nodes with insufficient memory and two excluded by topology spread constraints | Use `maxSurge: 0`, `maxUnavailable: 1` for ingestion/coastal updates; retain three/two replicas respectively after rollout |

The NATS health endpoints and liveness/readiness timings follow the
[official chart's probe settings](https://github.com/nats-io/k8s/blob/main/helm/charts/nats/values.yaml).
Tempo's memory settings use its
[2.9.0 ingester configuration](https://github.com/grafana/tempo/blob/v2.9.0/modules/ingester/config.go)
and [storage block configuration](https://github.com/grafana/tempo/blob/v2.9.0/tempodb/encoding/common/config.go).
The Go memory target is a soft runtime target; the container limit remains
the enforced ceiling. These changes require observation under sustained load.

The Citus Job checks formation, group, primary status and the original
PostgreSQL system ID before writing authentication metadata. Passwords come
from existing Secret references and are passed through libpq/SQL parameter
binding, not printed or embedded in manifests. The Job verifies all four worker
connections as `aggregator_app`. An unchanged reviewed release reuses its
completed Job. Credential rotation is a separate administrator operation.

The repair uses the existing images, database identities, source namespaces and
offsets, and the same 40 local-PV definitions. No database was reset. Memory
requests are 19,456 MiB including the additional one-time 128 MiB authentication
Job; individual role requests and limits retain the M2 values.

## Historical artifacts

```text
~/aat-deploy/completed-runtime-fix/release.json
~/aat-deploy/full-release-stable/bundle.json
~/aat-deploy/repairs/application-startup-93e3e4c9f3/
```

These paths are retained as historical inputs. The earlier `full-release/` bundle
contains the original socket paths and lacks the authentication step and
Patroni rule. Intermediate repair renderings are retained for inspection.
The existing demo volume-preparation scripts remain applicable.

## Validation and remaining runtime work

161 offline tests pass. Tempo's exact pinned image also accepts the rendered
configuration with `-config.verify=true`. The live authentication Job configured all five groups
and verified runtime worker access. Aggregator and Client API readiness return
HTTP 200, and Field login/logout return HTTP 200/204.

After the final processing rollouts completed, a live generation/readiness
check passed for all 59 workloads listed in the bundle. All 76 active
application-namespace pods were Ready in the same observation. The private
`bundle-readiness.json` records workload identities and hashes of the reviewed
stage artifacts. These are readiness observations, not source/load/failure
acceptance results.

All three brokers passed their full JetStream `/healthz` check after sequential
updates. Tempo completed trace flushes and compaction at the existing limit;
one measurement showed 72 MiB, but a 180 MiB Go target still allowed OOM kills
during simultaneous compaction and flushing. The final rendering lowers that
target to 128 MiB. Its replacement pod had no restarts during the final short
observation, with one memory sample at 101 MiB. This is not a sustained-load
memory guarantee.
Registry completed its repaired rollout. During ingestion/coastal updates one
replica can be unavailable; those updates require no extra replica's memory.

Audit rejects missing and wrong tokens with HTTP 401. The configured shared
Satellite/Audit value passes authentication, but the checksum request currently
returns HTTP 503; authenticated hazard reads also returned 503. These are
data-read availability failures, not successful end-to-end data checks.

The shared value is a temporary local-test credential, not a verified official
Audit credential. M2's Demonstrasi section specifies that assistants distribute
the special Audit API credential after the hard deadline. `AUDIT_TOKEN` and
`SATELLITE_FEED_TOKEN` are independent Secret keys. Replace only the Audit input
when it arrives, update the prepared release Secret and restart Aggregator
replicas to load it. No image rebuild or database reset is needed. Preserve the
updated release inputs so a later apply cannot restore the temporary value.
The current 503 occurs after authentication and remains a separate data-read
availability problem.

An additional temporary diagnostic ran the actual Store methods inside an
Aggregator pod. All four visibility samples found a pending canonical attempt;
the authority/visibility checks took about 0.46–1.13 seconds. A raw distributed
read succeeded in 1.49 seconds in one sample and 2.41 seconds in another, while
the validated reads still failed. Earlier checks found no outstanding
PostgreSQL prepared transactions. Kubernetes metadata tests also observed
responses exceeding the client's 600 ms timeout.

These observations establish that authentication and some physical data reads
work, while the complete consistency-checked read remains unavailable. They
identify pending-write contention and cumulative metadata/authority latency as
areas for further investigation; they do not establish one sole cause or
justify bypassing consistency checks or the existing two-second deadline.
Full source, latency, load and failure acceptance remain separate from
deployment readiness.
