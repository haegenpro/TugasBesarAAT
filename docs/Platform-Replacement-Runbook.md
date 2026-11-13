# Empty-node replacement development draft

Implemented, unexecuted. No VM has been stopped, started, replaced or rebuilt
for this change. Recovery is not part of normal bootstrap and never deletes old
disks. `plan` is offline; `replace-node` is an explicitly mutating future command.

```sh
python3 scripts/platform-recovery.py plan --storage-plan evidence/storage-postgres/plan.json --node aat-m2-d
# Later only, after the failed owned VM is fenced shut off:
python3 scripts/platform-recovery.py replace-node --storage-plan evidence/storage-postgres/plan.json \
  --config evidence/storage-postgres/durability.json --node aat-m2-d
```

The target must match the inventory, libvirt ownership UUID and exact original
disk paths, and already be shut off. It is never declared fenced merely because
the Kubernetes API stopped hearing from it. Unknown application PVC/hostPath
workloads block replacement until their owners supply recovery. This draft
handles the platform bootstrap and canonical formation; broker/source/recipient
or other service data need their own validated recovery before full-node use.

For each affected PostgreSQL group, both original survivors must be reachable,
have the captured system identity, expose one primary and a synchronous peer
flushed through a captured primary insert position. No group is relocated onto
all survivors. The replacement retains the original worker-group absence and
node-local placement. The recorded prefix is evidence of catch-up to that point,
not proof of every concurrent acknowledgement without workload reconciliation.

On a lost A/B/C node, canonical DCS membership is removed only after fencing and
survivor checks; the replacement is admitted as a learner with the existing
cluster membership, then promoted only after catching up. The owned workload's
startup arguments are reconciled to `initial-cluster-state=existing`. Avoid
reapplying an older `new` bootstrap manifest over this repaired membership.
The fenced Kubernetes node is deleted so K3s removes old node/server membership.
Only then are its old root/data disk files renamed into the run's quarantine
directory, empty disks created, and a fresh guest joined through a surviving
server. Future bootstrap remembers that surviving seed. Only this platform's
private SSH known-host entry is rotated for the explicitly replaced guest.

The tool recreates only original owned local paths and waits for PostgreSQL
replica identity/replay and four-node health. It preserves the original config,
IDs, quarantine and phase evidence under the platform state directory. Host
memory/disk checks run before mutation. All pinned artifacts remain pinned.

An interrupted replacement is not rolled back by booting the old disks: doing so
could resurrect a removed DCS/server member or stale database primary. Keep the
old VM fenced, retain quarantine and inspect the exact completed phases. This
first executor deliberately stops on uncertainty; phase-by-phase repair/resume
and full application-owner recovery must be exercised before operational use.
The metadata record is not a distributed backup, and a one-laptop host loss can
still remove all four VMs.

Deferred scenarios: every node including original server A; failure during each
membership/disk/join phase; DCS learner failure; missing second copy; identity
mismatch; unknown persistent workloads; insufficient host capacity; concurrent
canonical writes with identity/checksum reconciliation; restoration of original
placement without a full dataset on one node. No passing recovery result exists.

Membership references: [K3s member controller](https://github.com/k3s-io/k3s/blob/main/pkg/etcd/member_controller.go),
[etcd runtime reconfiguration](https://etcd.io/docs/v3.5/op-guide/runtime-configuration/).
