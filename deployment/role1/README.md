# M3/M4 owner deployment

This is prepared code, not a deployed or accepted cluster. Preserve the four-node,
three-copy placement agreements. The full system does not fit the recorded 7 GiB
VM allocation. Nothing here starts VMs, changes Docker settings, runs migrations
or applies manifests implicitly.

The root-context `Dockerfile` builds Aggregator, raw-ingestion and the offline
migration/cutover tools with Go 1.27.1. Build/push and supply actual immutable image
digests. `Dockerfile.dockerignore` excludes local credentials and evidence.

Ingestion owns a **separate** PostgreSQL formation, credentials and migration
journal. Reuse the platform storage workflow with `--formation-suffix ingestion
--mode postgres`; never point it at canonical volumes or databases. The extra
formation retains the same platform ownership labels and resource classes.
Capture its actual bootstrap system IDs before creating its durability ConfigMap.
Generated ingestion application secrets use `INGESTION_DATABASE_URL` and
`INGESTION_GROUP_0_URL`, not canonical `DATABASE_URL`.

```sh
# Linux platform host, after deliberate bootstrap/capacity preparation:
python3 scripts/platform-storage.py render --formation-suffix ingestion --mode postgres \
  --image REGISTRY/patroni@sha256:ACTUAL_DIGEST --output evidence/ingestion-storage
# Follow platform-storage create-secrets, prepare-disks and capture-config;
# these are explicit separate operations, with their normal ownership checks.

# Stop ALL writers/publishers and settle distributed recovery before migrations.
# Supply admin URLs privately; runtime startup never performs DDL.
go -C aggregator run ./cmd/migrate --offline --dir ../store/migrations \
  --target citus --runtime-role aggregator_app
go -C aggregator run ./cmd/raw-ingestion migrate --offline .. aggregator_app
# MIGRATION_DATABASE_URL + captured canonical config for this command:
go -C aggregator run ./cmd/input-cutover --offline BMKG YOUR_STABLE_NAMESPACE

python3 scripts/role1-integration.py \
  --inventory deployment/platform/inventory.example.json \
  --aggregator-config deployment/aggregator/config.example.json \
  --config /private/role1-config.json \
  --aggregator-image REGISTRY/role1@sha256:ACTUAL_DIGEST \
  --ingestion-image REGISTRY/role1@sha256:ACTUAL_DIGEST \
  --output evidence/role1-owner-resources.json
```

The example source namespaces and 16 MiB retained-payload quota are explicitly
synthetic development inputs. Configure actual stable namespaces and starts;
never infer a satellite offset. Payload storage retains immutable chunks for the
evaluation session and rejects capacity exhaustion. Captures larger than 16 MiB
remain stored/pending rather than being loaded into the decoder's memory; this
initial adapter does not claim unlimited input capacity.

Prepare the runtime Secret from `runtime.env.example`; create the separate
ingestion storage Secret and captured durability ConfigMap. No credential belongs
in the public configuration. Rendering references these objects but never creates
fake credentials or system IDs. Add owner artifacts and ingestion storage to the
existing platform deployment bundle, avoiding duplicate Aggregator resources.

The renderer enables M3/M4 deliberately. Before applying it, stop legacy BMKG
live/history writers on **all** replicas, drain old publications, run the offline
cutover and retain its database state. PVMBG stays on its existing path. A failed
cutover remains offline: rerun with the identical namespace/identities; do not
delete ownership epochs, migration journals or pending work. The cutover preserves
UUIDs, timestamps, revisions, notices, cursors and outboxes, imports observations
and journals progress. Do not switch back to an old binary after ordered writes.

Aggregator keeps its 256Mi/512Mi class; ingestion is an event consumer at
128Mi/256Mi. Ingestion storage uses 512Mi/1Gi per copy. Shared broker capacity and
volume declarations are unchanged. Review actual private broker configuration,
stream reservations and free disk before provisioning additional streams. A 1GB
file-store ceiling cannot reserve two simultaneous 1GiB raw streams; this is a
configuration gate to check, not a reason to resize Docker or shared volumes
automatically. Staged local tests may use different prepared settings.

The audit service account only gets namespace-scoped Pod get/list. Its rotating
token is explicitly projected while automatic token mounting remains disabled.
Policies add exact ingestion, storage, Patroni and Kubernetes API flows without
giving ingestion canonical credentials or audit access to Secrets/nodes. Actual
CNI connectivity, image startup, primary promotion and resource fit remain checks
to execute on the prepared platform.
