# Application deployment to the existing four VMs

This path uses the manually installed K3s cluster, inventory **version 3**, and
one root disk per VM. The controller is physical Mint. Initial deployment and
maintenance are supported by this path. The first infrastructure deployment on
the physical cluster exposed the storage issues described in
[the bootstrap repair record](Platform-Storage-Bootstrap-Repair.md); follow that
record's configuration paths for the repaired November 19 deployment.
The subsequent [Registry bootstrap repair](Platform-Registry-Bootstrap-Repair.md)
supersedes the Registry image and maintenance configuration paths in that record.
For the current full release, use the bundle paths in the
[application startup repair](Platform-Application-Startup-Repair.md), which also
records the remaining data-read availability failures separately from readiness.
Automated VM replacement, shutdown, resizing and physical fault injection still
belong to the other providers; v3 never invokes them. Manage the VMs manually.

The last reported addresses are A `192.168.100.27`, B `.28`, C `.30`, D `.29`.
If DHCP or a VPN migration changed them, use the current reachable addresses.
The inventory captures actual Kubernetes InternalIP values; regenerate an unused
inventory if those are different. A deployed inventory/maintenance receipt is
bound to its original identities and must be explicitly reconciled after changes.

## 1. Retain the successful image map on Mint

Use the terminal where publication succeeded. Publish storage and role1 from the
current source: storage requires the loopback replication, remount permission and
member identity fixes, and role1 must contain the matching replication probe SQL.
An image from commit `0b98d99` alone predates those bootstrap fixes.

```bash
export AAT_DEPLOY_DIR="$HOME/aat-deploy"
mkdir -p "$AAT_DEPLOY_DIR/bin"
chmod 700 "$AAT_DEPLOY_DIR" "$AAT_DEPLOY_DIR/bin"

python3 - <<'PY'
import json, os
from pathlib import Path
root = Path(os.environ['AAT_DEPLOY_DIR'])
assert os.environ.get('AAT_IMAGE_RUN'), 'Set AAT_IMAGE_RUN to the successful publication directory'
run = Path(os.environ['AAT_IMAGE_RUN'])
assert json.loads((run / 'result.json').read_text())['passed'] is True
target = root / 'images-current.json'
if target.exists():
    assert target.read_bytes() == (run / 'images.json').read_bytes(), 'Preserve the existing map and select a new filename for a different release'
else:
    target.write_bytes((run / 'images.json').read_bytes())
target.chmod(0o600)
print('Image map:', target)
print('Storage image:', json.loads(target.read_text())['storage'])
PY
```

If this is a new terminal, set `AAT_IMAGE_RUN` to the actual successful output
directory before running this block. Do not generate a timestamp for an old run.
Verify a pull of the printed storage digest inside every VM with
`sudo k3s crictl pull 'ACTUAL_REFERENCE'`. Existing registry credentials remain in
each VM's `/etc/rancher/k3s/registries.yaml`.

## 2. Export an administrative kubeconfig from B

Inside **B**:

```bash
sudo install -m 600 -o aat -g aat \
  /etc/rancher/k3s/k3s.yaml /home/aat/aat-admin-kubeconfig.yaml
```

On **Mint**:

```bash
scp -o PubkeyAuthentication=no -o PreferredAuthentications=password \
  aat@192.168.100.28:/home/aat/aat-admin-kubeconfig.yaml \
  "$AAT_DEPLOY_DIR/kubeconfig.yaml"
scp -o PubkeyAuthentication=no -o PreferredAuthentications=password \
  aat@192.168.100.28:/usr/local/bin/k3s \
  "$AAT_DEPLOY_DIR/bin/k3s"
```

These options prevent the SSH agent from exhausting the server's authentication
attempts with unrelated keys. Enter the VM user `aat`'s password. Require both
transfers to succeed before running:

```bash
chmod 600 "$AAT_DEPLOY_DIR/kubeconfig.yaml"
chmod 700 "$AAT_DEPLOY_DIR/bin/k3s"

python3 - <<'PY'
import os
from pathlib import Path
path = Path(os.environ['AAT_DEPLOY_DIR']) / 'kubeconfig.yaml'
text = path.read_text()
old = 'server: https://127.0.0.1:6443'
new = 'server: https://192.168.100.28:6443'
assert text.count(old) == 1 or text.count(new) == 1, 'Inspect the server address locally'
path.write_text(text.replace(old, new))
path.chmod(0o600)
PY

export AAT_KUBECONFIG="$AAT_DEPLOY_DIR/kubeconfig.yaml"
"$AAT_DEPLOY_DIR/bin/k3s" kubectl --kubeconfig "$AAT_KUBECONFIG" \
  get nodes -o wide
```

The copied binary runs only `kubectl` on Mint; do not run its `server` or `agent`
commands. This uses the already installed K3s version. Keep the kubeconfig private:
it grants cluster administrator access. After verifying the copy, remove the
temporary export on B with `rm /home/aat/aat-admin-kubeconfig.yaml`.

The API address here is B. A/B/C still need two etcd voters available. If B later
fails, select another healthy server in the kubeconfig; this is not an automatic
API load balancer. TLS verification stays enabled. Do not use `--insecure-skip-tls-verify`.

## 3. Capture and check the existing inventory

Run on **Mint in the repository root**:

```bash
"$AAT_DEPLOY_DIR/bin/k3s" kubectl --kubeconfig "$AAT_KUBECONFIG" \
  get nodes -o json > "$AAT_DEPLOY_DIR/nodes.json"

python3 scripts/platform-existing.py inventory \
  --nodes-file "$AAT_DEPLOY_DIR/nodes.json" \
  --kubeconfig "$AAT_KUBECONFIG" \
  --k3s-binary "$AAT_DEPLOY_DIR/bin/k3s" \
  --state-dir "$AAT_DEPLOY_DIR/state/aat-fourhost" \
  --output "$AAT_DEPLOY_DIR/inventory-existing.json"

python3 scripts/platform-existing.py check \
  --inventory "$AAT_DEPLOY_DIR/inventory-existing.json"
```

Require `ready: true`. Capture requires all four nodes Ready, the original
cluster/physical-host labels and A/B/C server roles with D as agent. Memory comes
from actual allocatable memory. Live operations reject changed node UIDs,
addresses or reduced capacity. This inventory supports the existing default
`10.42.0.0/16` pod and `10.43.0.0/16` service networks; inspect/review any different
network before use. No fake MAC addresses, physical-provider inventory or cloud-init
ownership files are needed.

## 4. Generate private credentials and render infrastructure

Create this directory **once**; preserve it across the remaining phases:

```bash
python3 scripts/platform-configure.py \
  --inventory "$AAT_DEPLOY_DIR/inventory-existing.json" \
  --images-file "$AAT_DEPLOY_DIR/images-current.json" \
  --output "$AAT_DEPLOY_DIR/release-inputs"

python3 scripts/platform-release.py \
  --inventory "$AAT_DEPLOY_DIR/inventory-existing.json" \
  --config "$AAT_DEPLOY_DIR/release-inputs/release.json" \
  --phase infrastructure \
  --output "$AAT_DEPLOY_DIR/infrastructure"

python3 scripts/platform-deploy.py plan \
  --inventory "$AAT_DEPLOY_DIR/inventory-existing.json" \
  --bundle "$AAT_DEPLOY_DIR/infrastructure/bundle.json"
```

Require `ready_for_apply: true` and review the workloads. The database-only
phase requests 12,672 MiB and contains 21 PostgreSQL copies, nine application
etcd pods and six routers. It starts no source or application. Satellite/Audit
tokens and egress configuration can remain pending here. Full activation retains
those gates; do not replace missing credentials with dummy values.

Private configuration needs Python 3 and `openssl` on Mint. If `openssl` is
missing, install it using Mint's package manager. An existing output directory
is refused to prevent implicit credential rotation. Reuse it rather than rerun
generation. Subsequent rendered artifact directories must also be new.

## 5. Prepare the database directories inside each VM

On **Mint**:

```bash
python3 scripts/platform-existing.py volumes \
  --inventory "$AAT_DEPLOY_DIR/inventory-existing.json" \
  --bundle "$AAT_DEPLOY_DIR/infrastructure/bundle.json" \
  --output "$AAT_DEPLOY_DIR/infrastructure-volumes"

cat "$AAT_DEPLOY_DIR/infrastructure-volumes/plan.json"
```

Review the paths/capacities. These scripts contain no credentials. Transfer them
to the corresponding VMs:

```bash
scp -o PubkeyAuthentication=no -o PreferredAuthentications=password \
  "$AAT_DEPLOY_DIR/infrastructure-volumes/aat-fourhost-a.py" \
  aat@192.168.100.27:aat-prepare-infrastructure.py
scp -o PubkeyAuthentication=no -o PreferredAuthentications=password \
  "$AAT_DEPLOY_DIR/infrastructure-volumes/aat-fourhost-b.py" \
  aat@192.168.100.28:aat-prepare-infrastructure.py
scp -o PubkeyAuthentication=no -o PreferredAuthentications=password \
  "$AAT_DEPLOY_DIR/infrastructure-volumes/aat-fourhost-c.py" \
  aat@192.168.100.30:aat-prepare-infrastructure.py
scp -o PubkeyAuthentication=no -o PreferredAuthentications=password \
  "$AAT_DEPLOY_DIR/infrastructure-volumes/aat-fourhost-d.py" \
  aat@192.168.100.29:aat-prepare-infrastructure.py
```

Inside **each VM**, run:

```bash
sudo python3 "$HOME/aat-prepare-infrastructure.py"
```

Require `prepared: true` on all four. The scripts check the guest's captured IP,
root-owned storage ancestors, ownership markers and free space, then prepare only
the rendered directories with their actual container UIDs. They refuse unmarked
existing data, conflicting owners and symlinks. A fresh guest storage marker is
created only when the local-PV base is empty. Existing matching directories are
preserved. They do not create a mount or change VM/K3s settings.

Free-space checks budget declared PV capacities plus 2 GiB; they are a preflight,
not filesystem quotas/reservations. Repeating the script after much data has
accumulated may refuse the free-space check. Also check free space on each
physical host containing a dynamic VDI; guest free space cannot prove host space.
Do not run the old `prepare-disks`/`prepare-volumes` commands for inventory v3.

### Short-lived assignment disk policy

The operator selected the existing disks and volume capacities with routine
cleanup for this assignment. A has a 32 GiB disk and B a 25 GiB disk; their root
partitions already fill the disks. The strict full-volume script requires
26.5 GiB free on A and 19.5 GiB on B, while the observed free space was about
19.75 GiB and 11.53 GiB. This is a conservative capacity preflight failure,
not an allocation of that many bytes at startup.

An explicit option exports scripts that require **2 GiB actual free space** on
each guest without requiring full backing for every declared volume capacity:

```bash
python3 scripts/platform-existing.py volumes \
  --inventory "$AAT_DEPLOY_DIR/inventory-existing.json" \
  --bundle "$AAT_DEPLOY_DIR/full-release/bundle.json" \
  --allow-disk-overcommit \
  --output "$AAT_DEPLOY_DIR/full-volumes-demo"
```

These scripts have already been generated for the current deployment. Copy
from `full-volumes-demo/` to each corresponding guest as
`~/aat-prepare-full.py`, replacing the previously copied strict script, and run
`sudo python3 "$HOME/aat-prepare-full.py"`. Require `prepared: true`; its output
also identifies the `free-space-only` capacity check. Existing storage ownership,
node identity, path and symlink checks remain enforced. The volume definitions,
replication, memory resources, images and maintenance receipt are unchanged.

Check `df -h / /var/lib/aat` before and after assignment runs. The 2 GiB floor
is checked at preparation time; it is not a continuous disk reservation. Local
PV sizes do not impose filesystem quotas, so actual data growth can still fill
the root filesystem. For routine cleanup, start with archived OS logs and
package-download caches, for example:

```bash
sudo journalctl --disk-usage
sudo journalctl --rotate
sudo journalctl --vacuum-size=200M
sudo apt-get clean
df -h / /var/lib/aat
```

Keep logs needed for an active investigation before vacuuming. Database, broker,
K3s and local-PV directories are persistent cluster state, not cache cleanup
targets. Resetting assignment data needs a separate stopped-writer procedure;
replacing databases also invalidates their captured maintenance identities.

## 6. Apply the database infrastructure from Mint

After all four guest scripts succeed:

```bash
python3 scripts/platform-deploy.py apply \
  --inventory "$AAT_DEPLOY_DIR/inventory-existing.json" \
  --bundle "$AAT_DEPLOY_DIR/infrastructure/bundle.json"

"$AAT_DEPLOY_DIR/bin/k3s" kubectl --kubeconfig "$AAT_KUBECONFIG" \
  -n sistem-informasi-kebencanaan-bnpb get pods -o wide
```

Apply checks the live identities, readiness and memory, then uses reviewed
artifact bytes and waits for the workloads. This changes the application
namespace and its owned local PVs; no VMs are provisioned. Coordination triplets
become ready before PostgreSQL starts, then each PostgreSQL group becomes ready
before the next group starts. It uses B's API,
while Kubernetes schedules the database pods across all four VMs.

If it stops, keep the output and inspect the failing pod's events/logs. Do not
regenerate credentials, delete PVCs, uninstall K3s or apply a different image map
as a blind retry. Initializers will verify original database identities before
activation.

## 7. Initialize the stopped applications' databases

Keep every application/source stopped for this first deployment.

Review the proposed BMKG/Satellite namespaces and starting offsets in
`release-inputs/role1.json` before this step. They must match the intended fresh
source session and remain stable after maintenance. If different values are
needed, supply them before capturing this receipt, not during activation.

```bash
python3 scripts/platform-maintenance.py run \
  --inventory "$AAT_DEPLOY_DIR/inventory-existing.json" \
  --config "$AAT_DEPLOY_DIR/release-inputs/release.json" \
  --infrastructure "$AAT_DEPLOY_DIR/infrastructure" \
  --writers-stopped \
  --output "$AAT_DEPLOY_DIR/maintenance"
```

Require its final verification message and `passed: true` in the receipt. This
captures all three formations, initializes four service schemas including the
actual Registry image, verifies owner isolation on every copy, runs canonical and
ingestion migrations and BMKG cutover, and writes `maintenance/release-ready.json`.
It uses Kubernetes API calls, not SSH/sudo automation. Failed maintenance retains
its receipt/logs; inspect before selecting a new run output.

## 8. Complete activation inputs, prepare full volumes, activate

For the November 19 deployment, input completion and full rendering have now
been prepared at `~/aat-deploy/completed/`, `full-release/` and `full-volumes/`.
Both `AUDIT_TOKEN` and `SATELLITE_FEED_TOKEN` currently use the group token at
the operator's explicit request for local testing. This does not establish the
official Audit credential. M2's Demonstrasi section says assistants distribute
the special Audit API credential after the hard deadline. Treat that credential
as an independent input and replace the temporary `AUDIT_TOKEN` when it arrives.
The Satellite session request returned HTTP 200 for group `sekredagosuites`;
that verifies Satellite access, not the official Audit credential.
The full plan reports no blockers, requests 19,328 MiB, and contains 40 local
PVs (including the existing database PVs). Application activation is pending
execution of the four full-volume scripts on their corresponding VMs.
Audit endpoint authentication still requires verification after activation.

The code already accepts separate `audit_token_file` and
`satellite_token_file` inputs, wired to separate Secret keys. Later Audit token
replacement requires updating the existing private release inputs, rendering a
new bundle, applying the updated Secret and restarting the Aggregator replicas
because they read the token from environment variables at startup. It needs no
image rebuild, database initialization, source-offset change or Satellite token
change. Keep unrelated credentials and the Audit cursor-signing key intact.
Use the updated bundle for subsequent applies; an older bundle contains the
temporary token and can restore it. Verify missing/wrong tokens return 401 and
the supplied token passes authentication. Follow separate assistant instructions
for Satellite exam credentials and session activation.

The external egress file allows ingestion HTTPS only to the feed's IPv4
addresses resolved during preparation (`104.21.84.171/32` and
`172.67.195.83/32`). Recheck the feed's resolved addresses if access later fails
after a DNS change. The captured source namespaces and offsets are preserved.

Use [external input completion](Platform-Input-Driven-Workflow.md) and
`platform-inputs.py` **against `maintenance/release-ready.json`**, writing a new
completed configuration directory. Supply actual Satellite/Audit token files,
bounded satellite egress and chosen source namespaces/offsets. Preserve the
captured source identity; changing it after maintenance invalidates its receipt
and requires a new stopped-writer maintenance run. Internal credentials must be
preserved. Never erase pending entries merely to make a plan pass.

When the completed configuration reports no pending inputs:

```bash
python3 scripts/platform-release.py \
  --inventory "$AAT_DEPLOY_DIR/inventory-existing.json" \
  --config "$AAT_DEPLOY_DIR/completed/release.json" \
  --output "$AAT_DEPLOY_DIR/full-release"

python3 scripts/platform-deploy.py plan \
  --inventory "$AAT_DEPLOY_DIR/inventory-existing.json" \
  --bundle "$AAT_DEPLOY_DIR/full-release/bundle.json"

python3 scripts/platform-existing.py volumes \
  --inventory "$AAT_DEPLOY_DIR/inventory-existing.json" \
  --bundle "$AAT_DEPLOY_DIR/full-release/bundle.json" \
  --output "$AAT_DEPLOY_DIR/full-volumes"
```

Require a full plan with no blockers. Review/copy the four new volume scripts
as in section 5, using a new remote filename such as `aat-prepare-full.py`, and
run them under sudo in their respective VMs. Existing database directories and
markers must match; additional application/monitoring directories are created.
Then activate the reviewed bundle:

```bash
python3 scripts/platform-deploy.py apply \
  --inventory "$AAT_DEPLOY_DIR/inventory-existing.json" \
  --bundle "$AAT_DEPLOY_DIR/full-release/bundle.json"
```

For initial access, run this on Mint and leave it running:

```bash
"$AAT_DEPLOY_DIR/bin/k3s" kubectl --kubeconfig "$AAT_KUBECONFIG" \
  -n sistem-informasi-kebencanaan-bnpb port-forward svc/client-api 18080:8080
```

In a second Mint terminal, use `curl -i http://127.0.0.1:18080/ready`.
A reviewed Client API NodePort/NetworkPolicy can provide direct LAN access later.
Runtime pod memory, rolling updates, 3→5→2 scaling, warning latency,
node loss, source reconciliation and physical-network acceptance still require
measurement. The 18.875 GiB request estimate is not proof of those results.

## 9. Redistribute existing single-copy volumes

Add `local_placement` to a reviewed copy of the private release descriptor.
Keep its existing file references, image pins and initialization receipt. Use
the `services` map for source workloads and the `monitoring` map for stateful
observability workloads. Each entry names a node from the captured inventory.
A move off the original node requires a new `volume_suffix`, which creates a
new PV, PVC and guest path while retaining the original volume.

The following example shifts 320 MiB of requested memory and three local
volumes from A to B. It leaves BMKG, Sensor Pantai, Prometheus and Tempo on A.
Confirm B's live requested memory, CPU and free disk, including rollback copies
and rollout headroom, before selecting this mapping. Use a different unused
suffix for subsequent moves. Keep the inventory order and the original
PostgreSQL and broker placement.

```json
{
  "local_placement": {
    "services": {
      "pvmbg-mock": {"node": "aat-fourhost-b", "volume_suffix": "move1"}
    },
    "monitoring": {
      "loki": {"node": "aat-fourhost-b", "volume_suffix": "move1"},
      "grafana": {"node": "aat-fourhost-b", "volume_suffix": "move1"}
    }
  }
}
```

Render to a new directory with `platform-release.py` and review the bundle
using `platform-deploy.py plan`. Generate the guest scripts with
`platform-existing.py volumes`. Compare their `plan.json` with the live PV/PVC
bindings, node names, disk paths, sizes and owners before preparing the target.
The full apply command refuses to change an existing StatefulSet's claim or
node pin. Perform each move as a targeted cutover.
Use the rendered artifacts only for the selected workloads when the read fix
is running as a targeted image rollout. Keep the exact deployed digest in the
rollout record until a full-upgrade contract can bind it.

1. Record the live workload, PV/PVC and source frontier or retained-history
   baseline. Prepare the reviewed destination directory on the target VM.
2. Scale the selected writer to zero and verify that no pod mounts the old
   claim. Capture its state while stopped. For a source volume, the guest-side
   `deployment/platform/capture_source.py` supports a bounded SQLite backup and
   emits archive bytes with their SHA-256, integrity result and history frontier.
   Its inputs are the reviewed `cluster_id`, `path`, `claim` and `uid`.
3. Restore onto the prepared target and verify the archive digest, ownership
   and application state before starting the writer. For a source archive,
   `deployment/platform/restore_source.py` accepts reviewed `path`, `uid` and
   `sha256` metadata and the archive on stdin. For Loki and Grafana, use a
   stopped-writer backup that includes their data and SQLite state where
   applicable. Retain the source PV/PVC and the checkpoint.
4. Apply only the reviewed target PV and PVC, check their binding, then apply
   the matching StatefulSet with the original replica count. Verify source
   sequence continuity or stored observability history through its Service.
   Save the updated descriptor, exact artifacts and pre/post evidence. Fence the
   moved writer and reconcile changes since cutover before reversing a move.

These placements reduce correlated outages. Each moved service still depends
on its own host and retained state.

## 10. Prepare redundant validation access

On the Linux controller, select its actual private IPv4 address as seen from
the guest LAN. Optionally supply the private IPv4 address of a separate load
generator for direct access. Render to a fresh evidence directory and review
the NodePort Services, NetworkPolicies and HAProxy config before starting.

```bash
python3 scripts/platform-access.py render \
  --inventory "$AAT_DEPLOY_DIR/inventory-existing.json" \
  --controller-address "$AAT_CONTROLLER_IPV4" \
  --output "$AAT_DEPLOY_DIR/validation-access"

python3 scripts/platform-access.py start \
  --inventory "$AAT_DEPLOY_DIR/inventory-existing.json" \
  --output "$AAT_DEPLOY_DIR/validation-access"
```

The version 3 Services preserve the external source IP with
`externalTrafficPolicy: Local`. Only nodes with a local endpoint forward that
traffic. The loopback proxy checks `/ready` on each node and uses the configured
controller address for outgoing connections. The generated ingress rules allow
the declared external callers as `/32` entries. On the actual K3s cluster,
check traffic from an allowed and an unlisted LAN caller because NetworkPolicy
enforcement and node traffic exceptions depend on the networking setup. Keep
the NodePorts on the private network.

Verify authenticated requests through `127.0.0.1:18080` and
`127.0.0.1:18081` on the controller. Observe which node endpoints are ready,
then verify continuity during the selected VM fault while the controller stays
up. These commands set up the entry point. The successful request and fault
evidence comes from the live run.
