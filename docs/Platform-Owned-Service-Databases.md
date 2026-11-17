# Owned service databases

The release now renders the four application databases instead of expecting an
externally supplied manifest. These use ordinary PostgreSQL, with one primary
and two replicas on different VMs. Only the canonical hazard store uses Citus.

| Formation suffix | Database | Runtime role | Owning service |
| --- | --- | --- | --- |
| auth-db | auth | auth_app | auth-service |
| registry-db | registry | registry_app | schema-registry |
| dashboard-db | dashboard | dashboard_app | consumer-dashboard |
| notifier-db | notifier | notifier_app | consumer-notifier |

Each formation includes three PostgreSQL instances, a three-member TLS etcd
cluster for Patroni, and two primary routers. Application Services preserve the
names `auth-db`, `registry-db`, `dashboard-db` and `notifier-db`. PostgreSQL
placement rotates the omitted VM across these four formations. Role, credentials,
volume paths and NetworkPolicies are separate for each owner. Patroni uses strict
synchronous replication; target-host failover validation remains required.

Together these four formations request **8,704 MiB (8.5 GiB)** of RAM, before
guest/host overhead, other databases and applications. They add twelve PostgreSQL
processes inside the existing four VMs. They do not require additional VMs.
The integrated release has thirty PostgreSQL copies overall: fifteen canonical,
three ingestion, and these twelve. The four-host example's 4 GiB per VM is a
placeholder and does not fit the full release. Use actual measured capacity;
do not reduce the assignment's per-role budgets to make the report pass.

## Preparation and first activation

`platform-release.py` writes these resources into the `owner-databases` stage,
plus `database-requirements.json` and four `*-db-plan.json` files. A standalone
render is also available:

```sh
python3 scripts/platform-databases.py render --inventory /private/four-hosts.json \
  --image registry.example/patroni@sha256:ACTUAL_DIGEST --output /private/databases
```

For each plan, use the existing `platform-storage.py create-secrets` and
`prepare-disks` commands. Keep generated credentials under a private path, or
the ignored `evidence/` directory. Generated `application.env` contains that
owner's connection string; add it to the corresponding runtime Secret along
with the service's other required settings. Registry additionally receives
`APICURIO_DATASOURCE_USERNAME` and `APICURIO_DATASOURCE_PASSWORD` from its file;
its JDBC URL stays public in the catalog. Never mix administrator credentials
into a runtime Secret. The bundle reports required Secret key names, and live
preflight inspects key names without printing values.

Render and review first, then prepare/apply the storage stages using the existing
storage workflow. Do not activate applications until initialization is complete.
Initialization runs **against a live database while the owning applications are
stopped**; it does not mean executing SQL without a database.

```sh
# Registry alone: bootstrap vendor DDL with the exact application image first.
python3 scripts/platform-databases.py bootstrap-registry --writers-stopped \
  --inventory /private/four-hosts.json --plan /private/releases/RUN/registry-db-plan.json \
  --image registry.example/registry@sha256:ACTUAL_DIGEST
# Run initialize for auth-db, registry-db, dashboard-db and notifier-db.
python3 scripts/platform-databases.py initialize --writers-stopped \
  --inventory /private/four-hosts.json --plan /private/releases/RUN/auth-db-plan.json
```

The registry bootstrap starts a temporary administrator pod, waits for its actual
readiness endpoint and removes only that run's pod. Administrator initialization
then grants DML to the runtime role. Normal Registry startup has
`APICURIO_SQL_INIT=false`; application credentials cannot create schema objects.
Auth/dashboard/notifier use their checked-in schema files, an advisory lock and
a checksum journal. Repeated initialization is safe; changed checksums and
untracked existing schemas require explicit migration/adoption. This is first
deployment initialization, not a general schema upgrade framework.

Capture original system IDs for all four formations using
`platform-storage.py capture-config`. Include every plan/config pair in the
replacement configuration; the updated replacement example shows all six
formations. Empty-disk recovery then applies the same surviving-copy and WAL
checks to these databases. The release monitors Patroni and etcd for all six.

## Focused verification

Offline checks cover three-node placement, Service routing, six-formation release
assembly, capacity blockers and all-volume recovery planning. The isolated
`scripts/test-owned-databases.py` check uses temporary labelled containers and
cached images to verify schema initialization/repetition, checksum and legacy
schema refusal, restricted runtime privileges, actual Apicurio bootstrap and
readiness with startup DDL disabled. These checks establish neither replicated
failover nor four-host capacity. No physical deployment is performed by them.
