# Integrated release preparation

The target is four physical hosts, each with one VM. Actual addresses, provider,
RAM and disk capacity remain operator inputs. Do not use the former single-host
7 GiB bootstrap allocation as a full release capacity claim.

`scripts/platform-release.py` assembles the canonical Citus formation, independent
ingestion PostgreSQL formation, service catalog, owner processing, isolation and
monitoring and four owned HA service databases into one dependency-ordered bundle. It does not build images, start
VMs, rotate credentials, migrate databases or apply resources.

Use the [private configuration generator](../../docs/Platform-Private-Configuration.md)
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
their exact table hash and satellite namespace/start must match. Use the actual
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
supplied or already prepared storage Secrets. It renders all six formations,
namespace and network isolation, including ingestion replication/admin flows.
It does not read the missing province table, require other application images,
or start sources, consumers or application servers. It still enforces declared
storage memory budgets and Secret keys; live apply still checks all four Ready
nodes, physical-host prerequisites and allocatable capacity. Add any operator
disk/prerequisite blockers to `infrastructure_unresolved` in the private config.
`ready_for_apply` concerns this storage phase only; `ready_for_activation` remains
false and application prerequisites remain in `activation_pending`.

On the real nodes, explicitly prepare disks using `platform-storage.py
prepare-disks --inventory ... --plan ...` for each of `canonical-plan.json`,
`ingestion-plan.json`, `auth-db-plan.json`, `registry-db-plan.json`,
`dashboard-db-plan.json` and `notifier-db-plan.json`. Then review/apply the
infrastructure bundle with `platform-deploy.py`. It waits for all storage
workloads. Use the existing administrator initialization and capture commands in
[owned databases](../../docs/Platform-Owned-Service-Databases.md) and
[storage deployment](../storage/README.md). Bootstrap the Registry using its
actual pinned application image before restricting its runtime role. Run
[canonical/ingestion migrations and BMKG cutover](../role1/README.md) with
writers/publishers stopped. Preserve the original identity captures for recovery.

After those operations and the pending external inputs are complete, prepare a
**new full release** with captured canonical/ingestion durability files and
recorded initialization/migration evidence. Resolve only the corresponding
explicit blockers. Prepare application/monitoring local paths from the full
release requirement files before its apply. Do not apply processing merely to
discover whether its schema is ready. No bootstrap/apply step has been run yet.

The optional `secrets` input supplies a private Secret-only List in a first stage,
before storage. Missing or empty required keys block review even for supplied
Secrets. The bundle handles all six database formations separately, records their
Secret references and avoids duplicate Aggregator/Namespace resources. Service
databases are rendered automatically when `owner_databases` is null; an explicit
artifact path remains available for reviewed alternatives. See
[owned service databases](../../docs/Platform-Owned-Service-Databases.md) for
credentials, administrator initialization and their 8.5 GiB memory cost.
Missing captured PostgreSQL identities, service schema initialization, migration/
cutover, or capacity remain visible blockers. Complete those prerequisites before
removing their corresponding unresolved entries. Review exact artifact hashes in
the plan; application apply uses the validated bytes rather than rereading files.

The broker catalog now reserves five 1 GiB raw/critical streams plus three
256 MiB legacy streams, 5.75 GiB per broker. Its 8 GiB file-store cap requires a
10 GiB local volume, leaving 2 GiB declared filesystem headroom. Every broker
needs that capacity because all streams use three copies. These are declared
reservations, not measured use; physical disk growth and retained WAL/database
data need separate accounting. Existing PVCs/data are never implicitly resized.
Keep raw/critical retention and acknowledgement rules unchanged.
Prepare the APP/system account Secrets using the
[broker recovery wiring](../../docs/Platform-Broker-Recovery.md); applications
and the separate broker-init Secret receive only the APP URL. Same-name disk
replacement retains broker membership and rebuilds original stream/consumer
copies instead of permanently removing the returning server.

Focused offline regression checks are authored in `bundle_integration_test.py`.
The full four-node, migration, failover and sustained-load validation remains a
later execution phase; rendered files alone do not establish acceptance.
