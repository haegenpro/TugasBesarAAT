# Platform observability handoff

Status: development implementation; images, configuration startup, connectivity,
signal accuracy, overhead and retention have **not been validated**. No VMs need
to be started to review these sources.

## Prepare later

Build the existing `monitoring/Dockerfile` targets and the new
`platform-metrics/Dockerfile`, then record actual repository digests in a private
copy of `deployment/monitoring/images.example.json`. Supply the vendor Tempo and
node-exporter images by digest too. The Kubernetes renderer overrides the
Compose-specific monitoring launcher with each vendor executable.

```sh
python3 scripts/platform-monitoring.py --storage-plan /private/storage/plan.json \
  --images /private/monitoring-images.json --output /private/monitoring
```

This only renders resources and a local-volume requirements file. Provision
those node-bound paths with the reported UID, and provide a `grafana-runtime`
Secret with `GF_SECURITY_ADMIN_USER` and `GF_SECURITY_ADMIN_PASSWORD` before
deployment. No credentials belong in ConfigMaps or committed image inventories.
Use port forwarding to access Grafana; there is no public monitoring ingress.

Prometheus discovers annotated application pods within the assignment namespace.
Only the annotated container port is scraped, avoiding duplicate request totals.
Aggregator annotations are built in. Other Go services declare `metrics_port`
in their service catalog entry. Add `OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4318`
to their public environment; Aggregator already sets it. Owner-contributed
services must use the same request correlation and W3C trace context contract.

Alloy tails namespace pod logs through the Kubernetes API, with namespace,
service, pod and node labels. Prometheus, Alloy and platform-metrics share a
namespace Role allowing discovery, log reads and metrics-server pod reads.
It grants no Secrets, pod exec, writes, cluster-wide discovery or Tester Agent
changes. API log collection adds kubelet/API overhead: measure it under load.

## Signals and interpretation

| Signal | Meaning and remaining work |
| --- | --- |
| `app_http_requests_total`, duration histograms | Per-instance rates, status and latency; count controlled 429 separately from transport/5xx errors |
| `app_acquisition_{bmkg,pvmbg}_active` | Local process believes it owns acquisition; compare authoritative epochs and upstream logs for uniqueness |
| `app_acquisition_*_renew_failures_total` | Renewal failure observed by this process; freezes require external observation |
| `app_acquisition_dispatch_{admitted,reused,blocked}_total` | Real dispatch decisions, cached response reuse and rejected sends; use the acquisition status script for durable pending metadata |
| `app_stale_owner_rejections_total` | Authority rejected stale work; does not count every deferred SQL commit rejection |
| `app_outbox_pending` | Committed outbox depth; owners add event age, source lag and delivery reconciliation |
| Patroni `/metrics`, etcd `:2381/metrics` | Member role, WAL positions and DCS health; float WAL metrics are diagnostic, never a durability acknowledgement |
| NATS exporters | One exporter per original broker member; stream placement and recipient durability still require runtime evidence |
| `platform_pod_cpu_cores`, `platform_pod_memory_bytes` | metrics-server measurements, joined with pod/node/role identity; not a memory limit or proof of unused capacity |
| `platform_metrics_collection_success`, snapshot age | Distinguish stale cached samples from current observations; missing data never means zero |
| Node exporter | Read-only host CPU/RAM/disk observations on four VM nodes; hostNetwork exposure/CNI behavior needs validation |

The generated Grafana dashboard shows availability, request rate, backlog and
resource usage. Prometheus rules detect scrape failure, stale collection,
unexpected application errors and sustained backlog. Thresholds are starting
operational values, not acceptance criteria. Alertmanager routing and final
recipient SLA/burn-rate alerts need the owners' complete event denominator.

The integrated profile adds discovery annotations to Aggregator, ingestion and
coastal pods, three distinct broker-member exporters, satellite offset lag,
wildfire received/prepared/applied progress and correction age, coastal source
state, and API admission rejections. Controlled 429s do not dilute the unexpected
HTTP error-rate denominator.

Notifier can enable `WARNING_SLO_METRICS_ENABLED=true` (set by the service catalog).
One independent bounded collector per process refreshes the acquired-warning
cohort every 30 seconds with a ten-second deadline. It reuses the owner's
expectation/receipt reconciliation and exports expected, pending, overdue,
coverage, clock anomalies, p95 completeness and budget use. Null ratios remain
NaN, never zero; authority failure sets collection_success=0. Delivery continues
independently. Replicas expose the same cohort, so dashboard counts use max,
not sums across replicas. The initial observation window is the preceding hour.

Budget alerts require current successful collection, complete coverage and no
clock anomaly. This is still the `durably_acquired_critical_observations` scope:
compare against the original sensor/reference identity set before claiming zero
source loss. Verify database-clock synchronization and bound collection overhead
on the target hardware. The latest-receipt duration panel is explicitly separate
from cohort p95; it cannot substitute for the five-second acceptance proof.

Focused checks passed for discovery, three-member broker coverage, coverage-aware
budget alerts, the notifier's unknown/null metric semantics and the observed live
satellite payload/explicit User-Agent. Full dashboard startup, CNI enforcement,
runtime overhead and four-node delivery/failure evidence remain deferred.

## Capacity and reliability limits

This profile uses single local copies of Prometheus, Loki, Tempo and Grafana with
24-hour metric/log/trace retention where configured. It is not HA monitoring.
An external store or replicated monitoring profile is required to preserve all
evidence through monitoring-node disk loss. Export evidence before destructive
experiments and retain collector gaps in the report.

Do not run the full storage, application and monitoring profiles on the current
7 GiB VM allocation. Use the later capacity report to choose components for
development, then resize or move acceptance to larger hardware. All load and
fault evidence remains deferred until both teammates finish their integrations.

Configuration references: [Prometheus Kubernetes discovery](https://prometheus.io/docs/prometheus/latest/configuration/configuration/),
[Alloy Kubernetes log source](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.kubernetes/),
[Patroni metrics](https://patroni.readthedocs.io/en/latest/rest_api.html).

## Durable source dispatch incidents

The integrated dashboard includes dispatch admission/reuse/block rates, and
`AcquisitionDispatchBlocked` alerts on sustained blocked sends. Admission counters
are diagnostic; reconcile the source's actual request log for uniqueness proof.
The [acquisition protocol and runbook](Platform-Acquisition-Implementation.md)
provides read-only inspection of original captured primaries. It exposes no
credentials or cached response bodies and never clears an intent.

Source acceptance now requires dispatch metadata from both owning databases. A
pending old epoch, expired holder or finite request older than ten seconds fails
acceptance even when the reference dataset was previously applied. A healthy
current SSE connection remains allowed. Ten seconds is an observation threshold,
not a safe expiry or permission to retry. Physical-node fencing, upstream outcome
reconciliation and the <=60-second takeover target remain separate acceptance
work; no force-unlock shortcut is provided.
