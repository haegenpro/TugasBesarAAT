# Application reset on the existing four-VM cluster

This is an explicit destructive reset of the assignment application. It preserves
the four VMs, K3s, Kubernetes node identities, system components and Tester Agent.
It deletes the assignment namespace and its exact owned local PV objects, deletes
the old volume contents, and recreates empty directories with their declared owners.
**No application-data archive is kept on the VMs.** Small configuration snapshots,
including Secrets, remain in the private controller output directory with mode 0600.

Do this only when a fresh assignment dataset is intended. Deleting the namespace
alone leaves local PV contents intact and does not produce a clean database.
New databases need new physical identity captures and a new initialization receipt.

The reset tool creates temporary maintenance Jobs with access only to
`/var/lib/aat`. Before deleting anything it checks all four guest ownership markers,
the captured node UIDs, volume paths and owners, and at least 2 GiB free space per
guest. It rejects system namespaces, foreign PVs and changed node/namespace/PV
identities. It requires cluster-admin access and an immutable storage image containing
`/opt/patroni/bin/python`; it does not use SSH or modify Tester Agent.

## Prepare before resetting

Keep a private directory for this rebuild, with the reviewed image map, source
namespaces, credentials, runtime settings and egress rules. Use the existing
[deployment guide](Platform-Existing-Cluster-Deployment.md) to prepare release inputs.
Use a new output directory for every rendering and maintenance run.

For the 2026-11-20 repair, the controller directory is
`~/aat-deploy/clean-20261120/`. Its `inputs/release.json` is the fresh input descriptor.
The inventory remains `~/aat-deploy/inventory-existing.json`. Credentials and source
namespaces are preserved; previous maintenance receipts and database identities
are excluded from these fresh inputs. NodePort is deferred.

```bash
INV="$HOME/aat-deploy/inventory-existing.json"
FRESH="$HOME/aat-deploy/clean-20261120"

python3 scripts/platform-release.py --inventory "$INV" \
  --config "$FRESH/inputs/release.json" --phase infrastructure \
  --output "$FRESH/infrastructure"

python3 scripts/platform-release.py --inventory "$INV" \
  --config "$FRESH/inputs/release.json" --output "$FRESH/activation-plan"

python3 scripts/platform-existing.py volumes --inventory "$INV" \
  --bundle "$FRESH/activation-plan/bundle.json" --allow-disk-overcommit \
  --output "$FRESH/volume-plan"
```

The activation plan remains blocked until initialization and identity capture.
It is used here to review the complete volume and workload layout, not to activate
applications. The infrastructure plan must have no blockers. The demo disk policy
keeps the existing PV capacities and requires 2 GiB actual free space.

## Review and execute the reset

Set `STORAGE_IMAGE` to the immutable `storage` reference in the reviewed image map.
The plan command reads the cluster without modifying it. Review its namespace,
four node UIDs and complete local-volume list before running `apply`.

```bash
python3 scripts/platform-application-reset.py plan --inventory "$INV" \
  --volumes "$FRESH/volume-plan/plan.json" --image "$STORAGE_IMAGE" \
  --output "$FRESH/reset-plan.json"

python3 scripts/platform-application-reset.py apply --inventory "$INV" \
  --plan "$FRESH/reset-plan.json" --delete-data \
  --output "$FRESH/reset"
```

Require `reset/result.json` to report `passed: true` and `data_archived: false`.
A stopped or failed reset must be inspected before retrying. Keep the output;
do not remove namespace/PV finalizers or rerun migrations blindly.

## Bootstrap and activate from the fresh inputs

```bash
python3 scripts/platform-deploy.py apply --inventory "$INV" \
  --bundle "$FRESH/infrastructure/bundle.json"

python3 scripts/platform-maintenance.py run --inventory "$INV" \
  --config "$FRESH/inputs/release.json" --infrastructure "$FRESH/infrastructure" \
  --writers-stopped --output "$FRESH/maintenance"

python3 scripts/platform-release.py --inventory "$INV" \
  --config "$FRESH/maintenance/release-ready.json" --output "$FRESH/full-release"

python3 scripts/platform-deploy.py apply --inventory "$INV" \
  --bundle "$FRESH/full-release/bundle.json"
```

The maintenance step captures the fresh PostgreSQL system IDs, initializes the
owner databases and pinned Registry, applies the canonical/ingestion migrations,
and verifies BMKG cutover. The final rendering binds those receipts to the exact
images and inputs. Keep using that final rendering for later applies; an older
bundle can restore obsolete images or configuration.

Verify authenticated hazard listings, all Audit source checksums, event pagination
and locations. Healthy reads must be complete. Then compare acquisition, canonical
processing and consumer frontiers over time. Readiness alone does not establish
source progress or M2 load/failure acceptance.
