# M2 deployment and operation

The reviewed architecture is in the local, Git-ignored `M2-Design.md`. The M1/M2
specifications are also ignored. This runbook and the executable infrastructure
are committed. Run operator commands from the repository root.

The development topology is two Minikube nodes. The intended final topology is
two Ubuntu VMs on different physical hosts, each with 4 processors, 8 GB RAM and
32 GB disk. The real VMs are not available yet. Rendering manifests or passing
local tests does not establish physical-host, network or official acceptance.

## Prepare a development cluster

Requirements: Linux amd64/arm64, Python 3.12+, Docker Engine, and Docker socket
access. Native service tests use the Go version declared by each module.

```bash
python3 scripts/m2/install_tools.py
python3 scripts/m2/manage.py init
python3 scripts/m2/minikube.py start
python3 scripts/m2/manage.py plan
```

The installer pins kubectl v1.35.0 and Minikube v1.39.0 with checksum checks.
Tools, kubeconfig, cluster state, manifests and secrets stay in `.m2-local/`.
The Minikube helper starts A first, adds B separately and records the actual node
names in the private configuration. Do not assume the worker suffix is `m02`.
The helper does not change the host-global kubeconfig or another cluster.

Inspect `.m2-local/config.json`. Credentials are generated once in
`.m2-local/secrets.json` with mode 0600. Preserve both files across restarts.
Fill external source/model credentials only in private configuration. Generated
manifests contain Secrets; never attach them to public reports.

```bash
python3 scripts/m2/images.py build
python3 scripts/m2/images.py load-minikube
python3 scripts/m2/manage.py up
python3 scripts/m2/manage.py status
```

`up` validates both nodes, compiles the manifests, stops writers, starts the
storage foundation, waits for its readiness, initializes owner roles, migrates
schemas offline, applies all enabled workloads and waits for readiness. A failure
leaves data in place and stops at the failed phase. Correct that failure and rerun
`up`. This operation interrupts application traffic; use it for installation or
offline schema upgrades, not the P2 rolling-update demonstration.

Images use `IfNotPresent`. Give each changed build a new `image_tag` before
building/loading, so `up` deploys that exact version on every node. Reusing a tag
can leave already-running pods on the previous image. A successful image build is
not evidence that Kubernetes is running it.

## Disk and memory

Check `df -h /` and `docker system df` before builds. Image operations require
8 GiB free by default, with an explicit `--min-free-gib` option. This preflight is
a starting check, not a reservation or a guarantee that an archive fits. Export
archives and Minikube image copies need additional disk. Prefer building/exporting
on the separate PC when the development device is nearly full.

`docker builder prune --force` removes unused build cache and makes future builds
slower. It preserves images and volumes. Do not run a blanket volume prune:
unused database volumes can still contain wanted data. The M2 application reset
below is a different operation from Docker cache cleanup.

PostgreSQL has ten instances: coordinator and two Citus worker groups, ingestion,
and shared service storage, with one member of each group on each node. NATS has
two members on A and one on B. Services have independent databases and roles even
where they share a PostgreSQL formation. Patroni uses Kubernetes DCS.

Each PostgreSQL and broker volume initially declares 2 GiB. Source histories each
have 1 GiB; Prometheus/Loki/Grafana each have 1 GiB on A. These are demo starting
sizes. A hostPath capacity declaration does not enforce a filesystem quota.
Monitor actual disk use, WAL, message queues, quarantine and rebuild headroom.
Each broker has a 1 GiB aggregate message ceiling below its 2 GiB volume size;
full durable queues backpressure publishers instead of evicting pending work.

`plan` reports fixed-node and scheduler-placed memory separately. Requests/caps
and an idle startup do not prove fit under load. Docker Minikube may report host
RAM as node allocatable despite Docker memory caps. Measure container/node usage,
restarts and OOM events during load, rollouts and recovery. Keep room for K3s,
the OS, init containers and temporary replicas.

## Browser access and identities

```bash
python3 scripts/m2/access.py client-api
python3 scripts/m2/access.py grafana
```

Run these in separate terminals. They listen on loopback and close with Ctrl-C.
Client API serves `/analytics` and `/ops`; use the Internal Ops identity. Grafana
provides metrics, Loki log search and Jaeger trace search. Its username is `admin`.
Passwords are in the private secrets file (`internal`, `grafana`); do not put them
in command arguments or screenshots. Demo client IDs are `media-demo`,
`field-demo`, and `internal-demo`, with their corresponding private secrets.

For the controlled incompatible-source and retained-input replay video, follow
[Replay-Demo.md](Replay-Demo.md). `scripts/m2/mock_schema.py` selects the required
PVMBG breaking mode without putting admin credentials in command arguments.

The Client API also has a NodePort for direct access from the VPN/load machine.
Discover its assigned port with the selected context's `get service client-api`.
Use direct NodePort access for load tests: a kubectl port-forward can become a
measurement bottleneck. Keep browser/admin services on the VPN or loopback.

Prometheus discovers each annotated process separately, preserving per-replica
counters. Logs are collected by Fluent Bit on A/B. Tracing is parent-aware with
5% sampling and bounded queues. Jaeger holds at most 1000 traces in memory, so
trace evidence must be captured before a restart. Retention is deliberately short
for a demo; this is not long-term production telemetry storage.

## Source credentials and province reference

Satellite acquisition stays disabled while `external.satellite_token` is empty.
Use a namespace that identifies the actual source session/history. Do not reuse
an old namespace after a feed reset or change initial offsets against retained
state. The example 1800-second lateness remains a development assumption until
course-feed evidence establishes a suitable value.

The production wildfire processor refuses to invent province coordinates. The
owner-approved practice table and its checksum are documented in
[Province-Reference.md](Province-Reference.md). For another authoritative
reference, normalize it to a JSON array containing exactly
`id`, `name`, `latitude`, `longitude`, and record the approved file's SHA-256.
The synthetic fixtures under `docs/m2/fixtures` are only for tests.

```bash
python3 scripts/m2/manage.py stop
python3 scripts/m2/manage.py deploy --foundation
python3 scripts/m2/manage.py ready --timeout 600
```

For first installation, use `up` first (with the satellite token blank), then
`stop` and `deploy --foundation`. `ready` checks all existing controllers, including
those deliberately stopped. Install the verified reference while writers are off:

```bash
python3 scripts/m2/reference.py --file /private/provinces.json --sha256 APPROVED_SHA256
python3 scripts/m2/reference.py --file /private/provinces.json --sha256 APPROVED_SHA256 --execute
python3 scripts/m2/manage.py up
```

Reference changes are refused after observations have been processed in that
namespace, except an identical reinstall. The Audit token is independently supplied
later through `external.audit_token`; the generated development token is not an
official credential. Optional AI requires `features.ai=true`, an explicit model
and an OpenRouter key. AI runs on B and may be unavailable during B failure.

The read-only feed probe records bounded source/resume evidence without logging
the token. Run from `aggregator/` with a private token file and a new output path:

```bash
go run ./cmd/satellite-probe --namespace SOURCE_SESSION --token-file /private/satellite-token \
  --from 0 --records 32 --duration 45s --output ../.m2-local/probe-001
```

Satellite detections and valid updates reactivate a retained detection at their
source offset. Retractions make it inactive; older replays cannot override a
newer lifecycle change or its attributes. Low-confidence detections remain
excluded from grouping, even while active. Updates without a retained base are
parked. Restoration of the same logical fire preserves its event identity.

For data processed by the earlier keep-inactive update rule, stop writers and
bring up only the foundation as above, then review the narrow migration:

```bash
python3 scripts/m2/requeue_lifecycle_updates.py
python3 scripts/m2/requeue_lifecycle_updates.py --execute
python3 scripts/m2/manage.py up
```

The default is read-only. Execution requires stopped writers and marks only
already-applied satellite updates whose latest retained target is inactive for
review. It preserves original input bytes, IDs and source offsets. After starting
the corrected workers, select those parked inputs in Internal Ops and replay them.
This migration does not clear history or replace a full reference comparison.

## Normal shutdown and restart

```bash
python3 scripts/m2/manage.py stop
python3 scripts/m2/manage.py start
python3 scripts/m2/manage.py ready
```

Stop records replica counts; start restores them without clearing data. Keep the
same configuration/context and credentials. `up` instead restores the manifest's
configured counts, performs schema setup and waits for readiness. It can be used
after a failed first installation. Neither command wipes storage.

## Manual demo reset

Reset is explicit and destructive. Never use it to hide lost data during a
failure/recovery assessment. Plan first:

```bash
python3 scripts/m2/reset.py
python3 scripts/m2/reset.py --scope observability
```

The plan lists exact node/path/volume mappings from the rendered configuration
and checks them against live PV ownership, claim and node affinity. The default
changes nothing. To erase the reviewed application plan:

```bash
python3 scripts/m2/reset.py --execute
python3 scripts/m2/manage.py up
```

Reset stops all owned controllers and waits for pods to exit. Bounded node-pinned
helpers remove only selected owned directories. The application scope includes
all database/broker copies, mock histories and sensor state, keeping source cursors
and derived stores coherent. It clears only application-owned Patroni initialization
records and broker initializer jobs. It preserves credentials, Kubernetes, VPN,
repository files and observability volumes. Reinstall the province reference after
application reset. `start` refuses to run writers against uninitialized empty stores.

Observability is separate:

```bash
python3 scripts/m2/reset.py --scope observability --execute
python3 scripts/m2/manage.py start
```

Both nodes must be Ready for a full reset. Partial-node data-loss recovery is not
this reset procedure. Do not delete PV/PVC objects just to clear their contents:
Retain volumes and explicit node mappings are intentional.

## Move to the real Ubuntu VMs

Use [the VM handoff checklist](VM-Deployment.md) for the network, private
configuration, K3s bootstrap, image transfer and acceptance sequence. All commands
accept `--config .m2-local/k3s.json` for the real deployment. Never rely on the
operator's current kubectl context; the helpers always select the configured one.

## Verification and remaining acceptance

```bash
python3 -m unittest discover -s scripts/m2 -p 'test_*.py' -v
python3 scripts/test-services.py --jobs 2
python3 scripts/m2/verify.py --suite all
bash -n deployment/m2/bootstrap-node.sh
```

Database integration suites use temporary isolated schemas and owned loopback
port-forwards. They are not destructive node-failure tests. See
[Acceptance.md](Acceptance.md) for the evidence that must be collected separately:
physical-host recovery, actual source/reference agreement, clock/freeze behavior,
60x catch-up, five-second warnings, sustained load/scaling and resource fit.

## Import the course catalog

Normalize the supplied catalog into JSONL with one canonical BMKG `SEISMIC` event
per line. Keep its original identity in `source_ref_id`; include `severity`,
`area_name`, coordinates, RFC3339 `occurred_at` and canonical `attributes`. Do not
invent a catalog checksum or assume the source file's format before it is supplied.
For example, a synthetic normalized row is:

```json
{"source":"BMKG","source_ref_id":"fixture-event-1","hazard_type":"SEISMIC","severity":"WASPADA","area_name":"Fixture Region","latitude":-6.2,"longitude":106.8,"occurred_at":"2026-11-12T00:00:00Z","attributes":{"magnitude":5.5,"depth_km":10}}
```

```bash
python3 scripts/m2/catalog.py --input /private/catalog.jsonl --sha256 VERIFIED_SHA256
python3 scripts/m2/catalog.py --input /private/catalog.jsonl --sha256 VERIFIED_SHA256 --execute
```

The importer streams bounded batches through the authenticated canonical HTTP
owner. It never writes another service's database. Interrupted/ambiguous batches
can be retried with the same identities. Check the final Audit count/checksum and
physical shard distribution; the local synthetic import test is not agreement
with the official dataset.

## Prepare the separate load PC

Install k6 there and transfer `scripts/m2/load.js` plus only the restricted client
credential file, not the entire deployment secret set:

```bash
python3 scripts/m2/load_credentials.py
```

Keep that output private. Set `M2_CREDENTIALS_FILE` to its absolute path on the
load PC, and target the private Client API NodePort. Example (on the load PC):

```bash
ulimit -n 65535
M2_CREDENTIALS_FILE=/private/load-credentials.json \
M2_BASE_URL=http://NODE_PRIVATE_IP:CLIENT_NODEPORT \
M2_VUS=10000 M2_HOLD=120s M2_RUN_ID=demo-001 \
M2_SUMMARY_FILE=load-summary-demo-001.json k6 run scripts/m2/load.js
```

The default mix is 90% Media and 10% Field, with a 60-second ramp, 120-second hold,
15-second ramp-down and one-second think time. Each VU owns its token/refresh
rotation. Defaults exercise reads; set `M2_REPORT_EVERY=10` to add a Field report
every ten Field iterations. A unique `M2_RUN_ID` is required when writing reports.
An ambiguous report retries the exact key/body. Run a small smoke test first,
then inspect generator CPU/RAM/sockets so its limits do not masquerade as server
throughput. Do not generate a per-request JSON output file on a nearly full disk;
the compact summary retains aggregate and role/operation-tagged metrics.

`m2_unsuccessful` includes every non-success response. `m2_unexpected_errors`
exempts only explicit 429 responses carrying `Retry-After`, matching M2's controlled
rejection allowance; admission rejections and transport failures remain separately
reported. Preserve offered VUs, achieved request rate and both outcomes. Do not
present mostly rejected traffic as useful application throughput.

During the steady hold, use the operator PC to perform the required temporary
scale changes, allowing readiness between changes:

```bash
python3 scripts/m2/manage.py scale --aggregators 5
python3 scripts/m2/manage.py ready
python3 scripts/m2/manage.py scale --aggregators 2
python3 scripts/m2/manage.py ready
python3 scripts/m2/manage.py scale --aggregators 3
```

Three is the normal count; 1 and 3 are also supported for comparable throughput
runs. Hold the dataset, generator, operation mix and think time constant. Rolling
updates use Kubernetes' deployment rollout with a new loaded image tag; do not use
`up` during that measured test because it performs offline schema setup.

Capture small before/during/after status evidence in distinct new private directories:

```bash
python3 scripts/m2/evidence.py --output .m2-local/evidence-before
```

This saves revision, actual placement/readiness/restarts, cgroup working sets and
an Audit checksum. Missing evidence is recorded as unavailable. It does not save
Secret manifests or replace the load report, source logs or actual host measurements.
See [Configuration.md](Configuration.md) for the full configuration reference.
