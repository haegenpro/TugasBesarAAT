# Integrated empty-disk node replacement

The new provider-aware workflow covers one VM on each of four physical machines.
It plans all local volumes before mutation and reuses the canonical owner's
original-copy, WAL-prefix, DCS-learner and timeline checks for each declared
PostgreSQL formation. Canonical and ingestion configurations are mandatory;
other service-owned formations must also be included once deployed.

```sh
python3 scripts/platform-node-replacement.py plan \
  --inventory /private/four-hosts.json --config /private/replacement.json \
  --node aat-fourhost-b
# Later, after the plan is ready and the failed VM is fenced shut off:
python3 scripts/platform-node-replacement.py replace-node \
  --inventory /private/four-hosts.json --config /private/replacement.json \
  --node aat-fourhost-b
```

Copy `deployment/platform/replacement.example.json` next to the reviewed release
bundle and actual captured durability configurations. Replace claim names with
the actual cluster identity. Every original local claim on the target needs an
explicit recovery strategy; an unknown live claim or hostPath stops execution.

| Data | Recovery path |
| --- | --- |
| Canonical, ingestion and all four service PostgreSQL formations | Verify two surviving original copies, replace lost DCS member as learner, rebuild original empty member, check system identity/replayed WAL/primary timeline |
| Broker | Verify two current original copies for every declared stream, peer-remove the fenced server, rejoin with empty storage, verify all three stream peers current and no sequence regression |
| Single-copy source/sensor state | Require an owner-produced consistent backup tar, its exact claim identity and SHA-256; refuse missing, oversized or unsafe restores |
| Monitoring local data | Explicit `observability` policy recreates it and records an evidence gap; it does not pretend retained logs/traces survived |

For a source restore, add `{ "kind":"restore", "archive":"backups/CLAIM.tar",
"sha256":"ACTUAL_SHA256" }` under that exact claim. Archives are bounded
uncompressed tar (64 MiB and 10,000 entries), limited to regular files/directories.
Absolute/traversal paths, links and duplicate entries are rejected before mutation.
The owning service must provide a consistent capture and validate its logical
contents. A live, changing SQLite file must not simply be copied as a backup.
If no safe backup or replica exists, recovery remains blocked: the platform does
not recreate lost source state as if it were preserved data.

## Execution sequence and retained state

All other VMs must be running and every affected database group must retain two
flushed original copies. The controller holds only the already-lost member's
workload controllers at zero while restoring its paths. Surviving replicas and
application workloads continue; the supplied Tester Agent is never modified.
Original replica counts and every phase are recorded.

The host helper verifies its ownership marker, VM UUID, attached disk paths and
shut-off state. It retains old disks in a run-specific quarantine and recreates
only that VM's root/data storage. VirtualBox detaches and unregisters old media
without `--delete`; libvirt retains its definition and original paths. The host
operation is journalled and repeatable with the same run ID, never starts the VM,
and refuses concurrent or changed incomplete operations. A killed helper may
leave its lock directory; inspect the original journal before removing a stale
lock. Old disks must never be booted alongside the replacement.

The controller removes the fenced Kubernetes node, starts the empty VM, joins it
through a surviving server with a CA-pinned token, prepares original local paths,
restores required artifacts, and resumes only the held controllers. It promotes
caught-up DCS learners, verifies fresh PostgreSQL receiver identity/timeline and
recorded WAL prefixes, checks broker peers, and waits for workloads to recover.
Broker recovery requires the rebuilt broker image containing `recover-node` and
its private `NATS_URL`; missing tools/credentials stop before disk replacement.

Failures keep journals, quarantine and uncertain workloads for explicit repair;
the top-level controller is not yet a fully resumable phase engine. It never
silently rolls back data or boots old storage. Do not declare automatic recovery
under interrupted replacement until those phase-fault scenarios are exercised.

## Verification status

Focused offline checks passed for formation/volume coverage, refusal of unbacked
source data, retained disk/idempotent provider operations, and unsafe archive
refusal. The broker helper's original-quorum/current-replica checks passed in its
Go unit test. No real VM was replaced and no disk-loss acceptance test was run.
The broker's empty-disk catch-up path and both hypervisors require target-host
execution. After recovery, independently reconcile accepted operation identities,
Audit API checksum, replica placement and continued write/read availability.
Healthy pods and nonregressing stream sequence numbers alone are not zero-loss
evidence.

Broker protocol reference: [JetStream durable recovery considerations](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/README.md).
