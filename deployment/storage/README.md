# Replicated storage development draft

**Implemented, unverified.** No image build, renderer execution, migration,
deployment or HA check has been run for this draft. The VMs and API proxy remain
stopped. Development can continue before validation.

| Mode | Physical database groups | Purpose |
| --- | --- | --- |
| `postgres` | One group, three copies on A/B/C | Small replication/routing/adapter component integration; does not satisfy final sharding |
| `citus` | Coordinator A/B/C; G1 B/C/D, G2 A/C/D, G3 A/B/D, G4 A/B/C | Full planned placement: fifteen PostgreSQL members; each VM misses one worker group |

Each group uses Patroni 4.1.5 quorum synchronization with one synchronous peer,
strict mode, timeline checks and rewind. Three separate etcd members use mutual
TLS on A/B/C. Two HAProxy routers check Patroni `/primary` and close sessions
when a backend is marked down. Headless discovery publishes unready members so
replicas can join without becoming eligible primary endpoints. Image/dependency
pins reuse M1 sources; earlier M1 results do not validate this new deployment.

Data uses local PVs with node affinity, explicit PVC bindings and Retain.
`prepare-disks` checks the mounted guest data filesystem and platform ownership,
creates owned directories, and refuses unmarked existing storage. It does not
recursively change existing database files or delete data. PVC capacities are
scheduling declarations, not filesystem quotas; WAL/dataset growth still needs
measurement. PostgreSQL paths use image UID/GID 999; etcd paths use 1000. The
derived image/runtime identity and these permissions need checking later.

Store members request 512 MiB and limit 1 GiB each. Etcd/router pods request
128 MiB and limit 256 MiB. Full Citus requests **8,320 MiB** before Kubernetes or
other services; the current **7,168 MiB** VM allocation cannot schedule it.
Rendering is allowed now; use component mode or change capacity before later
deployment. Both modes use the same formation name: do not switch a live
formation's mode or apply a smaller render to it. Retain disks and reconcile
mode/topology changes explicitly.

## Commands prepared for later integration

The commands below are for later execution. No command starts VMs or applies
resources implicitly. Build/push the derived image when ready and record its
actual registry digest; the renderer rejects mutable tags. The adjacent
`Dockerfile.dockerignore` limits the build context to required sources.

```sh
docker build -f deployment/storage/Dockerfile -t YOUR_REGISTRY/aat-patroni:development .
# Push and record YOUR_REGISTRY/aat-patroni@sha256:ACTUAL_DIGEST before rendering.
python3 scripts/platform-storage.py render --mode postgres \
  --image YOUR_REGISTRY/aat-patroni@sha256:ACTUAL_DIGEST --output evidence/storage-postgres
python3 scripts/platform-storage.py create-secrets \
  --plan evidence/storage-postgres/plan.json --output evidence/storage-postgres/private
```

Rendering writes Kubernetes `resources.json`, `plan.json` and
`durability.template.json`. The template has unusable system-ID placeholders
until real bootstrap identities are recorded. Secret generation creates fresh
database/replication/Patroni credentials, a private etcd CA, 90-day peer/client
certificates, `secrets.json` and `application.env`. The CA key is never mounted.
It refuses nonempty destinations and repository paths outside ignored
`evidence/`; it does not rotate an existing formation's credentials. Likewise,
editing `bootstrap.dcs` does not change initialized Patroni configuration.

When the platform is deliberately started in the later phase:

```sh
python3 scripts/platform-storage.py prepare-disks --plan evidence/storage-postgres/plan.json
/var/tmp/aat-m2/cache/k3s kubectl --kubeconfig /var/tmp/aat-m2/secrets/kubeconfig \
  apply -f evidence/storage-postgres/private/secrets.json
/var/tmp/aat-m2/cache/k3s kubectl --kubeconfig /var/tmp/aat-m2/secrets/kubeconfig \
  apply -f evidence/storage-postgres/resources.json
# Wait for the three physical members of every group to join.
python3 scripts/platform-storage.py capture-config --plan evidence/storage-postgres/plan.json \
  --output evidence/storage-postgres/durability.json
```

The existing platform bootstrap must already have created the application
namespace before applying secrets. Capture checks all three members against the
plan and requires a shared PostgreSQL system identifier within each group,
distinct between groups. Retain this config with the formation's persistent
state; do not recapture a different cluster identity to make a retry pass.

With all application writers/publishers stopped, run the normal administrator
migration chain with target `postgres` or `citus` and runtime role
`aggregator_app`. New migration 0005 journals the restricted replication probe
on the coordinator; bootstrap installs that same probe on every physical group.
Only administrators can change its formation/group metadata. Runtime receives
function EXECUTE, not pg_monitor/superuser privileges. Earlier migration files
are unchanged. For full Citus, inspect automatic worker registration and
inter-node/runtime permissions before migration. The existing Citus migration
still performs distribution and removal of retained coordinator heap data.

Mount the captured config at `/etc/aat/durability.json` and inject
`application.env` privately into Aggregator deployment. It supplies
`DATABASE_URL`, the relevant `STORAGE_GROUP_n_URL` secrets and
`CANONICAL_DURABILITY_CONFIG`. Enable the existing canonical/ownership settings
separately. This draft uses SCRAM on the private pod network for PostgreSQL and
HTTP for Patroni; DCS uses mutual TLS. Service network isolation and further
transport hardening remain later platform work.

## Adapter and checks

The adapter checks formation/group/system/member identity, synchronous settings
and exact configured Citus groups. After observing committed records it captures
WAL **insert** positions on every primary, then waits for local and synchronous
peer **flush** through those positions. Sampling a flush position alone could
confirm a prefix older than the commit. Changed primaries, synchronous sets,
missing groups or uncertainty return retryable unavailable.

Registration, mutations, authority operations and terminal resolution force
synchronous commit locally. Resolution still needs session/row exclusion,
settled prepared decisions and the atomic commit marker. Endpoint changes are
allowed only within the original configured groups; adding/removing groups
remains explicit. Outbox publication confirms data/decision prefixes while
holding its row lock. Confirmation anchors evidence to the actual reader's
backend identity, rather than accepting an unrelated primary's WAL positions.

This depends on the Patroni/DCS promotion and fencing protocol; it does not
independently prove safe promotion, permanent-frozen-primary fencing or physical
disk survival. Multi-entity Citus success retains the existing separate protocol
flag. Snapshot/audit failover, empty-disk replacement, permanent application
freezes, clock shifts and full load acceptance remain separate milestones.

These checks are **written, not executed**:

```sh
python3 -m unittest discover -s scripts -p storage_renderer_test.py
go -C aggregator test ./internal/store -run '^TestReplication'
python3 scripts/check-storage-ha.py observe --plan evidence/storage-postgres/plan.json \
  --config evidence/storage-postgres/durability.json
```

`observe` checks actual pod placement/image identities, primary-service routing
using the restricted runtime credential, physical identity and flushed quorum
WAL prefixes. It creates no canonical fixtures and injects no fault.

Fault scenarios also require a shared healthy canonical API entry, running
Aggregators using this adapter, `CANONICAL_SERVICE_TOKEN` in the runner
environment, and no competing acquisition for `canonical:BMKG`. They create
identified NORMAL-severity fixtures, potentially published to test consumers.
Use the integration formation, not live data.

```sh
python3 scripts/check-storage-ha.py single-vm --plan evidence/storage-postgres/plan.json \
  --config evidence/storage-postgres/durability.json --api-url http://YOUR_HEALTHY_API_ENTRY
python3 scripts/check-storage-ha.py two-vm --plan evidence/storage-postgres/plan.json \
  --config evidence/storage-postgres/durability.json --api-url http://YOUR_HEALTHY_API_ENTRY
```

`single-vm` pauses the coordinator primary's owned VM, checks survivor WAL and
routing, replays an acknowledged batch unchanged, reconciles its original
identity/revision, and commits another fixture before restoring the VM.
`two-vm` pauses two coordinator-copy VMs, requires no successful writes and at
least one controlled 503; transport timeouts alone are inconclusive. This is a
combined VM/DCS/copy outage, not an isolated replication failure. After restore,
the original unknown batch is retried with unchanged logical identities.

Both resume paused VMs in `finally`, reconcile acknowledged fixtures after
restore, and retain success/failure events, revision/dirty paths, runtime image
identities and config hashes in ignored `evidence/platform-storage/<run-id>/`.
Fixture rows remain for review. Ordinary failures/interruptions trigger cleanup;
a killed runner or host can still require manual VM resume. No passing result
exists until a scenario is executed.

References: [Patroni Citus groups and primary maintenance](https://patroni.readthedocs.io/en/latest/citus.html),
[quorum replication](https://patroni.readthedocs.io/en/latest/replication_modes.html),
[bootstrap configuration](https://patroni.readthedocs.io/en/latest/yaml_configuration.html),
[PostgreSQL WAL functions](https://www.postgresql.org/docs/16/functions-admin.html).
