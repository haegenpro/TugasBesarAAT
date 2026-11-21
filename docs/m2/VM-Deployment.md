# Two-host Ubuntu VM handoff

Status: prepared for deployment; not yet exercised on the team's real VMs.
Record actual values in this table before the physical demo. Do not substitute
Minikube container names for physical hosts.

| Item | A | B |
| --- | --- | --- |
| Physical host name / owner | TO FILL | TO FILL |
| Physical host CPU / RAM | TO FILL | TO FILL |
| Ubuntu VM name / release | TO FILL | TO FILL |
| VM allocation | 4 processors / 8 GB RAM / 32 GB disk | 4 processors / 8 GB RAM / 32 GB disk |
| Kubernetes role | K3s server and worker | K3s agent |
| Kubernetes node name | node-a (example) | node-b (example) |
| WireGuard address | 10.77.0.2 (example) | 10.77.0.3 (example) |

A owns the embedded K3s datastore and remains required. Mandatory destructive node
scenarios target B. The separate PC/VPS may generate load and collect evidence;
it does not become a third application node.

## 1. Establish the private network

1. Install WireGuard, curl, OpenSSL, Python 3 and ping on both Ubuntu VMs. Give
   the VMs stable names. Enable clock synchronization on each.
2. Generate distinct WireGuard keys on the hub and each peer. Keep private keys
   and filled configuration files mode 0600. Use the templates in
   `deployment/m2/wireguard-{hub,node}.conf.example`.
3. The example hub endpoint is `103.16.132.242:51820`. Enable IPv4 forwarding on
   that VPS and allow peer-to-peer forwarding over `wg0` in its existing
   firewall. Only the hub's WireGuard UDP port must be publicly reachable.
   Peer A/B use `PersistentKeepalive=25` and initiate outbound connections.
4. Enable `wg-quick@wg0`. Add the operator/load PC as a separate peer if needed;
   use its own key and address, not A or B's identity.
5. Check routing, round-trip latency and packet loss from A to B and B to A.
   Reboot each VM once and confirm the tunnel returns automatically. The hub
   must preserve each peer's `/32` AllowedIPs mapping. Do not NAT traffic
   between these peers into a different node identity.

Example subnets are VPN `10.77.0.0/24`, pods `10.42.0.0/16`, services
`10.43.0.0/16`. Avoid overlap with home LANs and other VPNs. On a LAN fallback,
use that interface/address consistently in the node configuration and kubeconfig.

Run the read-only preflight on A (reverse addresses for B):

```bash
python3 deployment/m2/preflight-node.py --interface wg0 \
  --address 10.77.0.2 --peer 10.77.0.3
```

Retain its JSON output as evidence. It checks CPU/RAM, disk, address, MTU, ping,
clock synchronization and storage-root type. It does not test sustained bandwidth
or prove five-second warning latency. A 47 ms ping alone does not decide whether
the tunnel meets the application SLO.

Permit required traffic only between the intended nodes/operator:

| Traffic | Purpose |
| --- | --- |
| A TCP 6443 | K3s API, agent join and operator kubectl |
| A/B UDP 8472 | Flannel VXLAN over the selected private interface |
| A/B TCP 10250 | Kubelet metrics/exec paths used by the control plane |
| Client API assigned NodePort | External load/client PC over VPN/LAN |

Do not expose VXLAN or the Kubernetes API to the public Internet merely to
connect the home VMs. Confirm the existing host firewall also permits forwarding
for pod CIDRs. Keep the official Tester Agent's access unobstructed.

## 2. Install K3s once

Copy `deployment/m2/node.env.example` privately to A and B. Set the same explicit
K3s release on both. Set A/B's distinct node names, addresses and interface. B's
`M2_SERVER_URL` points to A. Run on A:

```bash
set -a
. ./node.env
set +a
sudo -E bash deployment/m2/bootstrap-node.sh a
```

The script downloads the K3s installer, pins the release, creates the private join
token if absent and installs the selected node role. It refuses to overwrite the
opposite existing role or use a symlink as application storage. It never wipes
Kubernetes or application data. Rerunning requires the same role, name and network
settings; it is not an automatic cluster-conversion tool.

Securely copy A's `/etc/aat-m2/k3s-token` to that same mode-0600 path on B. Load B's
own environment and run:

```bash
sudo -E bash deployment/m2/bootstrap-node.sh b
```

From A, confirm both nodes are Ready with `sudo k3s kubectl get nodes -o wide`.
Check node addresses are the intended private addresses and that cross-node pods
can reach each other. Keep the join token, VPN keys and A's kubeconfig private.

## 3. Prepare the operator machine and images

Copy A's `/etc/rancher/k3s/k3s.yaml` securely to a private kubeconfig on the
operator PC. Replace its `server` address with `https://A-PRIVATE-IP:6443` and
rename its context to `aat-k3s`. Use that kubeconfig through `KUBECONFIG`.
Do not disable TLS verification. The bootstrap includes A's private IP as a TLS SAN.

```bash
python3 scripts/m2/manage.py --config .m2-local/k3s.json init \
  --profile k3s --context aat-k3s --node-a node-a --node-b node-b
python3 scripts/m2/manage.py --config .m2-local/k3s.json plan
```

Use the actual discovered node names. This configuration uses the K3s pod/service
CIDRs. Review `storage_root`, image prefix/tag, source namespace and all optional
features before applying. Keep `.m2-local/secrets.json` securely backed up. The
operator scripts currently use that one credential set for their selected cluster;
keep separate repository working directories/private state if independent
credentials are needed for simultaneous local and real deployments.

Build on the separate PC if the development device lacks disk. Its image
architecture must match the target VMs. Set a unique image tag for the revision,
then build and export **with runtime images included**:

```bash
python3 scripts/m2/images.py --config .m2-local/k3s.json build
python3 scripts/m2/images.py --config .m2-local/k3s.json export \
  --include-runtime --output .m2-local/images.tar
```

The runtime option includes the pinned sensor, Grafana, Prometheus, Loki, Jaeger
and Fluent Bit images. The exporter pulls missing third-party images but refuses
to substitute for an unbuilt application image. Keep the generated checksum with
the archive and verify it before transfer/import. Allow space for the archive plus
unpacked images on the importing host. A registry is an alternative when a large
archive is impractical.

Import the identical archive into each VM's K3s containerd (after checking SHA-256
on the machine holding the archive):

```bash
ssh node-a 'sudo k3s ctr -n k8s.io images import -' < .m2-local/images.tar
ssh node-b 'sudo k3s ctr -n k8s.io images import -' < .m2-local/images.tar
```

With a registry, configure pull access on both VMs and use its prefix in the private
configuration. Do not assume Docker images on the operator PC are visible inside
K3s containerd.

## 4. Start and verify the application

```bash
python3 scripts/m2/manage.py --config .m2-local/k3s.json up
python3 scripts/m2/manage.py --config .m2-local/k3s.json status
python3 scripts/m2/access.py --config .m2-local/k3s.json client-api
python3 scripts/m2/access.py --config .m2-local/k3s.json grafana
```

Check Internal Ops analytics and recovery pages, dashboards, searchable structured
logs and sampled traces. Install the authoritative province file before enabling
satellite processing; see [README.md](README.md). Later-supplied Audit and source
credentials require a private configuration update and controlled workload rollout.
Never include credentials in the topology report or evidence screenshots.

## 5. Failure/recovery rehearsal

Use [Acceptance.md](Acceptance.md) to record exact boundaries and expected
outcomes. Start with a reversible B power-off/restart, then rehearse the separate
empty-B-storage scenario on disposable test data. Do not wipe A or run the full
application reset during an assessed recovery window.

The expected mechanism is A-side ownership takeover, B's PG base backups and NATS
replica catch-up on rejoin, then restoration of worker 2's primary on B. Check
Patroni timelines/roles, Citus shard copies, broker stream/consumer replicas,
canonical checksums, receipts and original source frontiers. Node Ready alone is
insufficient. The automatic guardian policy and empty-B rejoin still require
measured destructive acceptance before they may be claimed as proven.

A failure is outside the two-node availability promise. Restart A without deleting
its embedded K3s datastore, restore network access and inspect every formation.
Preserve the private credential set while retaining database data. Never run a
K3s uninstall script as a way to restart the application.

## 6. Day-of-demo checklist

- Record physical-host identity, VM allocation, image revision/tag and source session.
- Confirm free disk, synchronized clocks, all intended replicas, PG sync state and
  JetStream quorum; check restarts/OOM events and resource headroom.
- Run the generator from the separate PC, with enough sockets/CPU/RAM for 10,000 VUs.
- Confirm supplied Audit credentials, authoritative catalog/reference and source keys.
- Open browser analytics, Grafana logs/metrics/traces and Tester Agent logs.
- Preserve before/during/after evidence and acknowledged-input receipts.
- Use ordinary stop/start between sessions. Use the explicit reset only outside
  the measured assessment window, after saving required evidence.
