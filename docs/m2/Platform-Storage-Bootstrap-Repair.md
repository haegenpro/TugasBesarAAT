# Four-host storage bootstrap repair — 2026-11-19

The first application-infrastructure apply created all five original stages but
timed out during final workload readiness. Applying manifests did not establish
that every database had bootstrapped or was replicating. Diagnosis on the actual
A/B/C/D cluster identified the following defects.

| Defect | Observed effect | Source correction |
|---|---|---|
| Missing loopback physical replication rule | Patroni's local `IDENTIFY_SYSTEM` connection failed; PostgreSQL's `host all all` does not cover physical replication | Allow `replicator` on `127.0.0.1/32` in both single-owner and shared-owner HBA layouts |
| PostgreSQL and coordination started together | Concurrent pulls, initialization and base backups coincided with coordination delays; canonical worker group 4 never completed its first bootstrap | Wait for each formation's etcd triplet, then initialize and wait for each PostgreSQL triplet in order; install coordination NetworkPolicies before waiting |
| Kubernetes fsGroup changed existing file modes | PostgreSQL rejected PGDATA mode `0770` and its copied TLS key mode `0660` after remount | Use `OnRootMismatch`; on startup restore owned, regular PGDATA to `0700` and its existing regular `server.key` to `0600`; reject symlinks and unexpected owners |
| Patroni sets `cluster_name` to formation scope | SQL evidence could not identify the individual physical member; maintenance rejected it | Write the validated pod name outside replicated PGDATA and read it through the restricted replication probe; trim its newline explicitly |
| Readiness timeout did not identify the wait target | The original output only showed the previously applied stages | Print each workload name before waiting; allow etcd probes five seconds |

These changes retain the three copies per PostgreSQL group, synchronous
replication settings, existing credential values and prescribed pod memory
requests/limits. The infrastructure requests **12,672 MiB**; this repair does not
downscale it.

## Recovery of the incomplete empty group

Canonical worker group 4 was exceptional: its first primary had initialized a
PostgreSQL directory but had not created the application database or completed
Patroni bootstrap. No application writers had been deployed. A stopped copy of
that directory was inspected using an isolated temporary PostgreSQL instance:
it contained zero user databases and zero public tables.

Only after those checks, all three members of that group were stopped, the
failed original files were preserved, and a compare-and-swap cleared that
group's incomplete `initialize` key while requiring the absence of a leader,
configuration and status key. A corrected member initialized the empty group;
the other two copied it. This was a recovery of a verified empty initial
bootstrap, not a procedure for resetting an existing database.

Private proof is under:

```text
~/aat-deploy/repairs/empty-worker-bootstrap-46d115dbad/
```

VM B retains the failed original files at:

```text
/var/lib/aat/local-pv/aat-fourhost-storage/aat-fourhost-storage-g4-b/pgdata.failed-bootstrap-46d115dbad
```

Do not delete these directories as part of a retry. Other original PostgreSQL
groups were not reinitialized. Recovery retries and restarts are separate from
an application-level exactly-once upstream request claim.

## Artifacts for this repaired deployment

The final publication is `~/aat-deploy/images-keyfix-20261119/images.json`.
Storage and role1 contain the matching replication probe SQL; the other image
digests are retained. Existing passwords, certificates and source declarations
were copied without regeneration into:

```text
~/aat-deploy/release-inputs-verified/release.json
~/aat-deploy/infrastructure-verified/bundle.json
```

Use those paths for this deployment instead of the older `release-inputs/` and
`infrastructure/` artifacts. Intermediate repair directories are retained for
inspection. Do not reapply their older image digests.

Offline validation: **145 tests pass**, including replication HBA selection,
permission repair without data changes, member identity, sequential bootstrap
dependencies and the existing-cluster adapter. An isolated PostgreSQL check also
verified the physical replication connection and exact SQL member identity,
including the marker file's trailing newline.

Live verification completed on the four-node cluster:

- Serialized rollout passed for all **30 StatefulSets**. All seven PostgreSQL
  groups retained the identities captured after empty-group recovery, with two
  streaming replicas after each PostgreSQL restart.
- Reapplying `infrastructure-verified/bundle.json` passed all **15 stages** and
  every final workload readiness wait.
- Final checks found **4 Ready nodes, 36 Ready infrastructure pods, 30 Bound
  PVCs**, and seven groups with one primary and two streaming replicas. No node
  reported MemoryPressure or DiskPressure.
- The replication probe SQL checksum inside both published images matches the
  repository's migration file.

Private live receipts:

```text
~/aat-deploy/repairs/rolling-bootstrap-fix-954aef0c4e/result.json
~/aat-deploy/repairs/final-readiness-cbee4fc455/result.json
```

Schema initialization and application activation were not run during this
repair. This is infrastructure readiness evidence, not full HA/load acceptance.

After the repaired infrastructure apply and replication verification succeed,
the next step is stopped-writer initialization. The prepared source declarations
are BMKG `bnpb-bmkg-v1`, offset `1`, and Satellite
`bnpb-satellite-practice-v1`, offset `0`. Review them before the first maintenance
receipt, because they become part of the captured source identity.

```bash
export AAT_DEPLOY_DIR="$HOME/aat-deploy"

python3 scripts/platform-maintenance.py run \
  --inventory "$AAT_DEPLOY_DIR/inventory-existing.json" \
  --config "$AAT_DEPLOY_DIR/release-inputs-verified/release.json" \
  --infrastructure "$AAT_DEPLOY_DIR/infrastructure-verified" \
  --writers-stopped \
  --output "$AAT_DEPLOY_DIR/maintenance-initial"
```

The output directory must be new. Require a successful receipt before completing
external activation inputs against its `release-ready.json`.

Database infrastructure readiness does not complete application deployment.
Stopped-writer schema initialization, source input completion, full activation,
load/scaling measurements and physical fault acceptance remain separate steps
in [the existing-cluster guide](Platform-Existing-Cluster-Deployment.md).
