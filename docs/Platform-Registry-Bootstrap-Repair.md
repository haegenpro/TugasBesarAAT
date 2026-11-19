# Registry bootstrap repair — 2026-11-19

The first stopped-writer maintenance attempt completed auth schema
initialization, then timed out waiting for the Registry initializer.

The initial image pull took 2m36s of the four-minute readiness window. A cached
reproduction on VM B then terminated immediately with exit 127 and:

```text
Fatal glibc error: CPU does not support x86-64-v3
```

Inspection of the actual image found a UBI 10 Java 21 base. VM A exposed AVX2;
B/C/D did not. All four guests exposed the CPU features needed by UBI 9.

## Changes

- `registry/Dockerfile` copies the same pinned Apicurio 3.3.3 Java application
  onto a pinned UBI 9 Java 21 runtime; no VM CPU settings are changed.
- The image bounds the Java heap at 64 MiB, uses Serial GC, and bounds metaspace,
  code cache, direct memory and thread stacks. Requests remain 128 MiB and the
  container limit remains 256 MiB.
- Registry initialization polls pod status and reports terminal failure
  immediately. Its bounded readiness window is ten minutes to accommodate cold
  pulls. The pod deadline is eleven minutes.
- Maintenance saves private pod state, events and startup logs under
  `registry-bootstrap/` before deleting the temporary initializer.

Validation: 154 offline tests pass. The rebuilt image passed a real temporary
PostgreSQL initialization, 21 schema registrations and 20 reads at the 256 MiB
container limit; the final memory sample was approximately 212 MiB. Test
containers and their isolated network were removed. This is a bounded
functional check, not a full load result.

## Private deployment artifacts

```text
~/aat-deploy/images-registry-cpu-20261119/images.json
~/aat-deploy/release-inputs-registry-fix/release.json
~/aat-deploy/infrastructure-verified/bundle.json
~/aat-deploy/maintenance-routing-fix/
```

Only the Registry image digest changed in the image map. Existing credentials,
certificates, storage image, database identities and source declarations were
retained. The infrastructure bundle is unchanged and does not need another
apply for this image change.

The failed original maintenance receipt remains in `maintenance-initial/`.
The cached CPU failure evidence is retained in
`repairs/registry-startup-d1b7e44b32/`. Do not delete receipts or reset databases
to retry initialization. Successful owner schema initialization is journaled
and can be checked again during the new stopped-writer maintenance run.

## Migration connectivity follow-up

The repaired Registry initialized successfully on VM B. All four service
schemas initialized, and owner isolation passed on every shared copy. The
canonical migration then failed on its first connection, before executing DDL.

Live Service endpoints and NetworkPolicies were present and matched the
migration's labels. Temporary probes using those labels on all four nodes
could reach zero of ten canonical targets immediately after startup, then all
ten after five seconds and again ten seconds later. This included direct
PostgreSQL pod addresses and router addresses, as well as all five primary
Services. The probes and their pods were removed; private results remain in:

```text
~/aat-deploy/repairs/canonical-routing-c36a637a70/
~/aat-deploy/repairs/canonical-routing-7df5f43998/
```

Each maintenance Job now has an init container that retries an authenticated,
read-only PostgreSQL primary check for at most 90 seconds before its migration
container starts. Both containers have the same 128/256 MiB request/limit;
they run sequentially, so Kubernetes reserves the maximum, not their sum.
The existing owner NetworkPolicies continue to apply. Only connection readiness
is retried by the gate; it does not replay migration statements.

The controller also polls Job completion and terminal failure with an original
UID check, retaining logs when it stops. The intermediate failed canonical run
is preserved in `maintenance-registry-fix/`; the new stopped-writer retry uses
`maintenance-routing-fix/` and the same Registry-fixed release configuration.

## Live maintenance result

`maintenance-routing-fix/receipt.json` now has `passed: true`, and the controller
reported `Maintenance verified`. All four service initializations, owner
isolation on all three copies, canonical migrations, ingestion migrations and
BMKG cutover completed. Actual schema journals matched the source checksums,
and all seven original PostgreSQL group identities were checked again after
maintenance.

The successful canonical Job's log records an initial connection retry, an
authenticated primary check and `migration complete: citus`. The startup gate
therefore addressed the observed live race rather than merely adding a delay.

Continue input completion and application activation from:

```text
~/aat-deploy/maintenance-routing-fix/release-ready.json
```

The failed intermediate canonical Job is archived with its pod state and logs
under `maintenance-registry-fix/` before removal. Its receipt remains failed;
the new receipt is the successful initialization authority. Application
activation and full HA/load acceptance follow separately.
