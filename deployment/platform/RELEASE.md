# Integrated release preparation

Current host onboarding follows the
[manual VirtualBox/Wi-Fi guide](../../docs/m2/Platform-VirtualBox-WiFi-Setup.md),
with one Ubuntu VM on each of four physical machines and no VPS cluster node.
For these manually installed guests, use [existing-cluster inventory v3](../../docs/m2/Platform-Existing-Cluster-Deployment.md).
It uses an existing admin kubeconfig and operator-run scripts for root-filesystem
volumes. Initial release/apply and maintenance have been exercised on the four-VM
cluster; full source/load/failure acceptance and automated VM replacement remain
pending. See the [startup repair record](../../docs/m2/Platform-Application-Startup-Repair.md)
for observed readiness and unresolved data-read failures. V1/v2 retain their
separate wired provider/SSH/ownership workflows.

The target is four physical hosts, each with one VM. Actual addresses, provider,
RAM and disk capacity remain operator inputs. Do not use the former single-host
7 GiB bootstrap allocation as a full release capacity claim.

The [input-driven workflow](../../docs/m2/Platform-Input-Driven-Workflow.md) now
automates image build/publish/digest capture, external input completion, cluster
and storage preparation, stopped-writer maintenance, activation and measured
source/combined game-day validation. Declared machine allocation may remain an
assumption until execution; no hardware questionnaire is needed to write a plan.

`scripts/platform-release.py` assembles the canonical Citus formation, independent
ingestion PostgreSQL formation, service catalog, owner processing, isolation and
monitoring and four owned HA service databases into one dependency-ordered bundle. It does not build images, start
VMs, rotate credentials, migrate databases or apply resources.

Use the [private configuration generator](../../docs/m2/Platform-Private-Configuration.md)
to prepare consistent unused credentials and pending inputs, or copy
`release.example.json` into a private directory and provide its referenced
JSON files. `images.json` combines the service/monitoring digest maps with actual
`storage` and `role1` image digests. Build the role1 image using
`deployment/role1/Dockerfile` with repository-root context. Never use a made-up
digest or a synthetic province table for an accepted release.

Use `deployment/aggregator/config.integrated.example.json` and
`deployment/role1/config.integrated.example.json` for integrated feature settings.
The role1 input must explicitly enable analytics and contain the R3 source
configuration. `wildfire_config` and `province_reference` reference UTF-8 files;
their exact table hash and satellite namespace/start must match. The configuration
generator copies the bundled [38-row province reference](../../docs/m2/Province-Reference.md)
by default; manual preparation must copy it alongside the integrated wildfire
config (as `wildfire.json`). Use the actual
feed session and course reference inputs. Audit sessions can be enabled in the
same input; live Audit API is enabled independently by the owner renderer.

The private satellite session check confirmed that `from=0` yielded detection IDs
0, 1 and 2 on the practice feed. The session reported 60x speed. This is a bounded
interface observation, not a burst/recovery/zero-loss acceptance result. The
credential is deliberately absent from public examples. A generated private
Secrets bundle contains credentials and must remain outside version control.

```sh
python3 scripts/platform-release.py --inventory /private/four-hosts.json \
  --config /private/release.json --output /private/releases/RUN
python3 scripts/platform-deploy.py plan --inventory /private/four-hosts.json \
  --bundle /private/releases/RUN/bundle.json
```

For first deployment, prepare storage independently before full activation:

```sh
python3 scripts/platform-release.py --phase infrastructure \
  --inventory /private/four-hosts.json --config /private/release.json \
  --output /private/releases/BOOTSTRAP
```

This phase needs only the actual storage image digest from the image map, plus
supplied or already prepared storage Secrets. It renders all three formations,
namespace and network isolation, including ingestion replication/admin flows.
It does not read the province table, require other application images,
or start sources, consumers or application servers. It still enforces declared
storage memory budgets and Secret keys; live apply still checks all four Ready
nodes, physical-host prerequisites and allocatable capacity. Add any operator
disk/prerequisite blockers to `infrastructure_unresolved` in the private config.
`ready_for_apply` concerns this storage phase only; `ready_for_activation` remains
false and application prerequisites remain in `activation_pending`.

On the real nodes, explicitly prepare disks using `platform-storage.py
prepare-disks --inventory ... --plan ...` for each of `canonical-plan.json`,
`ingestion-plan.json` and `service-db-plan.json` (the shared formation hosting
the auth, registry, dashboard and notifier databases). Then review/apply the
infrastructure bundle with `platform-deploy.py`. It waits for all storage
workloads. Use the existing administrator initialization and capture commands in
[owned databases](../../docs/m2/Platform-Owned-Service-Databases.md) and
[storage deployment](../storage/README.md). Bootstrap the Registry using its
actual pinned application image before restricting its runtime role. Run
[canonical/ingestion migrations and BMKG cutover](../role1/README.md) with
writers/publishers stopped. Preserve the original identity captures for recovery.

After those operations and the pending external inputs are complete, prepare a
**new full release** with captured canonical/ingestion durability files and
recorded initialization/migration evidence. `platform-maintenance.py` captures all
original formations and executes/verifies owner initialization, migrations and
BMKG cutover. Its `release-ready.json` includes a receipt bound to current
tools/migration hashes, images, inventory, namespaces and original identities;
the full renderer resolves matching initialization blockers automatically.
Changed or incomplete receipts are refused. `platform-inputs.py` fills supplied
external inputs without regenerating internal credentials. Prepare application/monitoring local paths from the full
release requirement files before its apply. Do not apply processing merely to
discover whether its schema is ready. No bootstrap/apply step has been run yet.

The optional `secrets` input supplies a private Secret-only List in a first stage,
before storage. Missing or empty required keys block review even for supplied
Secrets. The bundle handles the three database formations separately, records their
Secret references and avoids duplicate Aggregator/Namespace resources. Service
databases are rendered automatically when `owner_databases` is null; an explicit
artifact path remains available for reviewed alternatives. See
[owned service databases](../../docs/m2/Platform-Owned-Service-Databases.md) for
per-owner credentials and isolation, administrator initialization and their
2.1 GiB memory cost (formerly 8.5 GiB as four formations).
Missing captured PostgreSQL identities, service schema initialization, migration/
cutover, or capacity remain visible blockers. Use verified maintenance receipts
and completed external inputs rather than manually removing initialization gates.
Review exact artifact hashes in
the plan; application apply uses the validated bytes rather than rereading files.

The broker catalog now reserves five 1 GiB raw/critical streams plus three
256 MiB legacy streams, 5.75 GiB per broker. Its 8 GiB file-store cap requires a
10 GiB local volume, leaving 2 GiB declared filesystem headroom. Every broker
needs that capacity because all streams use three copies. These are declared
reservations, not measured use; physical disk growth and retained WAL/database
data need separate accounting. Existing PVCs/data are never implicitly resized.
Keep raw/critical retention and acknowledgement rules unchanged.
Prepare the APP/system account Secrets using the
[broker recovery wiring](../../docs/m2/Platform-Broker-Recovery.md); applications
and the separate broker-init Secret receive only the APP URL. Same-name disk
replacement retains broker membership and rebuilds original stream/consumer
copies instead of permanently removing the returning server.

Focused offline regression checks are authored in `bundle_integration_test.py`.
The full four-node, migration, failover and sustained-load validation remains a
later execution phase; rendered files alone do not establish acceptance.
