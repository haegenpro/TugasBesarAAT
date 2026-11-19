# Owned service databases

The release renders the four application databases itself instead of expecting an
externally supplied manifest. They share **one** replicated PostgreSQL formation,
`service-db`: one primary and two replicas on different VMs, with one Patroni etcd
cluster and one pair of primary routers. Only the canonical hazard store uses Citus.

| Owner Service name | Database | Runtime role | Owning service | Connection limit |
| --- | --- | --- | --- | ---: |
| auth-db | auth | auth_app | auth-service | 64 |
| registry-db | registry | registry_app | schema-registry | 24 |
| dashboard-db | dashboard | dashboard_app | consumer-dashboard | 64 |
| notifier-db | notifier | notifier_app | consumer-notifier | 64 |

## Ownership enforcement

Assignment M1 point 2 and M2 Platform point 8 require every store to have exactly
one owning service, enforced by the platform so that other components technically
cannot connect to it. Each owner keeps a separate database, and the platform
enforces the boundary in layers:

1. **Network.** NetworkPolicies admit only the four owning services (and their
   stopped-writer `*-db-init` initializers) to the formation's routers and
   PostgreSQL members. The Aggregator, Client API, ingestion, processors and all
   other components cannot open a connection to it.
2. **Authentication.** The Patroni image renders `pg_hba.conf` from the declared
   owners. It contains exactly one `local`/`host` rule per *(database, role)* pair,
   administrator and replication rules, and ends with `host all all all reject`.
   An owner naming another owner's database is refused before its password is
   checked (`pg_hba.conf rejects connection`).
3. **Privileges.** `CONNECT` is revoked from `PUBLIC` on every database, including
   `postgres` and `template1`, and granted only to the owning role. Owner roles are
   `NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT`, hold no
   role memberships and get DML only on their own schema.
4. **Credentials.** Every owner has its own password key in the formation Secret;
   its runtime Secret receives only its own `DATABASE_URL` (Registry: its own
   `APICURIO_DATASOURCE_*`). No runtime Secret contains administrator credentials.
5. **Capacity.** Per-role `CONNECTION LIMIT`s equal what each owner could open on a
   dedicated instance (replicas plus one rollout surge, times its pool), so one
   owner cannot exhaust the shared server. The sum (216) stays below
   `max_connections` (224), leaving superuser-reserved slots for administration.

PostgreSQL has no cross-database queries; `dblink`/`postgres_fdw` must not be
installed. Maintenance verifies every layer on **every copy** and records it in the
initialization receipt (see below); the full release refuses a receipt without it.

Trade-off: the four owners share one primary's CPU, memory (one 512Mi request /
1Gi limit storage instance), WAL and failover. A failover briefly affects all four
at once, and a heavy query from one owner can slow the others. Measure pod memory,
active sessions and disk latency during the load test.

Application Services keep the names `auth-db`, `registry-db`, `dashboard-db` and
`notifier-db`; all four route to the shared primary. PostgreSQL copies are placed
on A, B and D (as is the ingestion formation), keeping them off C, the smaller VM
that also runs a K3s server. Patroni uses strict synchronous replication;
target-host failover validation remains required.

## Memory

The shared formation requests **2,176 MiB** (3 PostgreSQL x 512 + 3 etcd x 128 +
routers 256), instead of **8,704 MiB** for four separate formations. The
integrated release now has three formations and 21 PostgreSQL copies: fifteen
canonical, three ingestion and three shared service copies. The full release
requests **19,328 MiB (18.9 GiB)**, down from 25,856 MiB. With the measured
allocatable memory of the current cluster (two 8 GB and two 4 GB VMs) and a
512 MiB per-node reserve, the offline plan has no memory blocker; C's pinned
requests are 3,072 MiB of 3,403 MiB. A K3s server process itself uses roughly
0.75 GiB outside pods, which the per-node reserve does not fully cover, so C
remains the tightest node. Use actual measured capacity; do not reduce the
assignment's per-role budgets to make the report pass.

## Preparation and first activation

`platform-release.py` writes these resources into the `owner-databases` stage,
plus `database-requirements.json` and `service-db-plan.json`. A standalone render
is also available:

```sh
python3 scripts/platform-databases.py render --inventory /private/four-hosts.json \
  --image registry.example/patroni@sha256:ACTUAL_DIGEST --output /private/databases
```

Use the existing `platform-storage.py create-secrets --plan service-db-plan.json`
and `prepare-disks` commands. Keep generated credentials under a private path, or
the ignored `evidence/` directory. `create-secrets` writes one file per owner
(`auth-db.env`, `registry-db.env`, `dashboard-db.env`, `notifier-db.env`); add each
to the corresponding runtime Secret along with the service's other settings. The
Registry JDBC URL stays public in the catalog. Never mix administrator credentials
into a runtime Secret. The bundle reports required Secret key names, and live
preflight inspects key names without printing values.

Render and review first, then prepare/apply the storage stages using the existing
storage workflow. Do not activate applications until initialization is complete.
Initialization runs **against a live database while the owning applications are
stopped**; it does not mean executing SQL without a database.

```sh
# Registry alone: bootstrap vendor DDL with the exact application image first.
python3 scripts/platform-databases.py bootstrap-registry --writers-stopped \
  --inventory /private/four-hosts.json --plan /private/releases/RUN/service-db-plan.json \
  --image registry.example/registry@sha256:ACTUAL_DIGEST
# Then initialize auth-db, dashboard-db and notifier-db in turn.
python3 scripts/platform-databases.py initialize --writers-stopped --database auth-db \
  --inventory /private/four-hosts.json --plan /private/releases/RUN/service-db-plan.json
# Check owner separation on every copy (read-only).
python3 scripts/platform-databases.py verify-isolation \
  --inventory /private/four-hosts.json --plan /private/releases/RUN/service-db-plan.json
```

The registry bootstrap starts a temporary administrator pod, waits for its actual
readiness endpoint and removes only that run's pod. Administrator initialization
then grants DML to the runtime role. Normal Registry startup has
`APICURIO_SQL_INIT=false`; application credentials cannot create schema objects.
Auth/dashboard/notifier use their checked-in schema files, an advisory lock and
a checksum journal, each in its own database. Repeated initialization is safe;
changed checksums and untracked existing schemas require explicit
migration/adoption. This is first deployment initialization, not a general schema
upgrade framework.

The maintenance workflow (`platform-maintenance.py run`) performs the four
initializations, then `verify-service-db-isolation`, then the canonical/ingestion
migrations and BMKG cutover. Capture original system IDs for the three formations
using `platform-storage.py capture-config`, and include every plan/config pair in
the replacement configuration (see `deployment/platform/replacement.example.json`).
Empty-disk recovery applies the same surviving-copy and WAL checks to the shared
formation. The release monitors Patroni and etcd for all three.

## Focused verification

Offline checks cover placement, Service routing, three-formation release assembly,
capacity on the measured cluster, all-volume recovery planning, the rendered
`pg_hba.conf` against the isolation checker, and refusal of every broken
isolation layer. The isolated `scripts/test-owned-databases.py` check runs one
PostgreSQL server configured like the shared formation and tries every role
against every database over the network: only the four own-database logins may
succeed. It also verifies schema initialization/repetition, checksum and legacy
schema refusal, restricted runtime privileges, and actual Apicurio bootstrap and
readiness with startup DDL disabled. It needs cached `postgres:16-alpine3.24` and
`aat-m1-prep-check-schema-registry:latest` images. These checks establish neither
replicated failover nor four-host capacity. No physical deployment is performed
by them.
