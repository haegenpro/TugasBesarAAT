# Manual K3s bootstrap for the four Wi-Fi VMs

Continue after the [VirtualBox/Ubuntu setup](Platform-VirtualBox-WiFi-Setup.md).
Run every command below **inside Ubuntu**, either through its console or SSH.
Windows and Mint are only the physical hosts; installation is identical inside
their guests. Use one existing disk per VM. The VPS stays outside this cluster.

The current guests use **regular Ubuntu Server 24.04**, not the minimized
selection. The package command below ensures the tools are present; packages
already installed do not need reinstalling. No desktop or Guest Additions are
required for this K3s setup.

This guide installs the Kubernetes infrastructure. It does not deploy databases
or applications, adopt these guests into the existing provider, establish a
redundant API endpoint, or constitute an M2 acceptance result. Instructions are
prepared for fresh guests; actual four-host execution remains to be verified.

## 1. Check guest addresses and peer connectivity

On each VM:

```bash
hostname
ip -br -4 addr
ip route
```

Record the IPv4 address on the guest Ethernet interface, normally `enp0s3`.
When a prompt asks for an address, enter just the address, without the `/24`
(or other prefix length) shown by `ip`, for example `192.168.1.31`.
Ignore loopback `127.0.0.1` and addresses on unrelated physical-host/VPN/bridge
interfaces. Keep guest addresses stable through router reservations or the
static configuration described in the VM guide.

| VM | K3s role | Physical-host label |
| --- | --- | --- |
| `aat-fourhost-a` | Server and workloads; initializes embedded etcd | `host-a` |
| `aat-fourhost-b` | Server and workloads; joins A | `host-b` |
| `aat-fourhost-c` | Server and workloads; joins A | `host-c` |
| `aat-fourhost-d` | Agent/workloads; joins A | `host-d` |

From every guest, test each other guest's actual address:

```bash
ping -c 3 OTHER_GUEST_IPV4
```

Replace `OTHER_GUEST_IPV4`; it is not a literal hostname. If ICMP is filtered,
also test a known listening service such as SSH with
`nc -vz -w 3 OTHER_GUEST_IPV4 22`. An SSH success verifies TCP connectivity, not
the later pod network. Resolve unexplained connectivity failures before joining
nodes. Do not manually change an address merely to make its prefix resemble a
different machine's address.

The planned pod CIDR `10.42.0.0/16` and service CIDR `10.43.0.0/16` must not
overlap the actual LAN/VPN. If they conflict, choose nonoverlapping ranges
before initialization and use the same values on all three servers.

## 2. Prepare all four guests

In the terminal you will use to install K3s, set these two values by answering
the prompts. Use the guest interface from step 1, not the host's `wlo1`.

```bash
read -rp "This Ubuntu VM's IPv4 address: " AAT_NODE_IP
read -rp "Guest interface [enp0s3]: " AAT_NODE_IFACE
AAT_NODE_IFACE=${AAT_NODE_IFACE:-enp0s3}
```

These shell variables last only in that terminal; enter them again if you
reconnect or reboot before running the installation command.

Install the small host tools and prepare the same kernel networking settings
used by the existing repository bootstrap:

```bash
sudo apt update
sudo apt install -y curl ca-certificates iproute2 iputils-ping netcat-openbsd nano
sudo modprobe overlay
sudo modprobe br_netfilter
printf '%s\n' overlay br_netfilter | sudo tee /etc/modules-load.d/aat-k3s.conf >/dev/null
printf '%s\n' 'net.ipv4.ip_forward=1' 'net.bridge.bridge-nf-call-iptables=1' | sudo tee /etc/sysctl.d/90-aat-k3s.conf >/dev/null
sudo sysctl --system
sudo mkdir -p /var/lib/aat/local-pv
```

For a consistent no-swap guest configuration, check `swapon --show`. If active,
run `sudo swapoff -a` and comment only the guest's swap entry in `/etc/fstab`
using `sudoedit /etc/fstab`. Check `swapon --show` again after any reboot. Do not
change the physical Mint host's swap. This is our selected configuration rather
than an assertion that all Kubernetes versions prohibit swap; see
[Kubernetes swap management](https://kubernetes.io/docs/concepts/cluster-administration/swap-memory-management/).

### Firewall connections

Check `sudo ufw status verbose`. A fresh Ubuntu guest commonly has UFW inactive;
do not enable or disable firewalls as an incidental installation step. If any
host, guest or network firewall is active, permit these flows between the four
trusted guest addresses:

| Destination | Source | Port |
| --- | --- | --- |
| A/B/C | All four guests | TCP 6443 |
| A/B/C | Other servers A/B/C | TCP 2379 and 2380 |
| All guests | Other guests | UDP 8472 for default Flannel VXLAN |
| All guests | Other guests as needed for kubelet/metrics | TCP 10250 |

Guest forwarding rules must also permit the selected pod/service network
traffic. Keep SSH available if using it for administration. Follow
[K3s network requirements](https://docs.k3s.io/installation/requirements) for
the relevant firewall, including UFW pod/service allowances; keep cluster ports
restricted to the trusted nodes/admin network.

### Download the pinned installer on every guest

The version and installer hash below match
[the repository inventory](../../deployment/platform/inventory.fourhost.example.json).
Run on A, B, C and D:

```bash
curl -fL 'https://raw.githubusercontent.com/k3s-io/k3s/v1.37.2%2Bk3s1/install.sh' -o /tmp/aat-k3s-install.sh
printf '%s\n' 'ed01f89fd977bf20ac1516bbebf8370bf3ddbaa55dac8aba610956a4c78cc00b  /tmp/aat-k3s-install.sh' | sha256sum -c -
```

Continue only when the checksum reports `OK`. These manual commands pin/check
the installer and select the exact K3s version; the installer downloads and
verifies the release binary using upstream checksums. Unlike the repository's
automated provider, this path does not separately compare that binary to the
inventory's binary hash. Do not silently switch to a different version if a
download fails.

## 3. Initialize A

Run **on A only**, after setting its IP/interface variables above:

```bash
sudo env INSTALL_K3S_VERSION='v1.37.2+k3s1' \
  sh /tmp/aat-k3s-install.sh server \
  --cluster-init \
  --node-name aat-fourhost-a \
  --node-ip "$AAT_NODE_IP" \
  --flannel-iface "$AAT_NODE_IFACE" \
  --node-label platform.aat/cluster=aat-fourhost \
  --node-label platform.aat/physical-host=host-a \
  --cluster-cidr 10.42.0.0/16 \
  --service-cidr 10.43.0.0/16 \
  --secrets-encryption \
  --disable traefik \
  --disable servicelb \
  --write-kubeconfig-mode 0600

sudo k3s kubectl wait --for=condition=Ready node/aat-fourhost-a --timeout=300s
sudo k3s kubectl get nodes -o wide
```

The server also runs workloads. `--cluster-init` selects embedded etcd, which
will have three members after B/C join. The server networking and Secrets
encryption settings must match across A/B/C. These settings follow the
repository bootstrap and [K3s HA instructions](https://docs.k3s.io/datastore/ha-embedded).

Retrieve A's CA-pinned join token:

```bash
sudo cat /var/lib/rancher/k3s/server/token
```

Use the complete token for B/C/D. It grants cluster access; transfer it privately
between your machines, not into chat, tracked configuration or a public report.

## 4. Join B and C as servers

On **B**, set its identity and provide A's guest address and token:

```bash
AAT_NODE_NAME=aat-fourhost-b
AAT_PHYSICAL_HOST=host-b
read -rp "A's Ubuntu guest IPv4 address: " AAT_A_IP
read -rsp "Paste A's complete join token: " AAT_JOIN_TOKEN
printf '\n'
```

Run the following on B, with B's own `AAT_NODE_IP` and `AAT_NODE_IFACE` still set:

```bash
sudo env INSTALL_K3S_VERSION='v1.37.2+k3s1' K3S_TOKEN="$AAT_JOIN_TOKEN" \
  sh /tmp/aat-k3s-install.sh server \
  --server "https://$AAT_A_IP:6443" \
  --node-name "$AAT_NODE_NAME" \
  --node-ip "$AAT_NODE_IP" \
  --flannel-iface "$AAT_NODE_IFACE" \
  --node-label platform.aat/cluster=aat-fourhost \
  --node-label "platform.aat/physical-host=$AAT_PHYSICAL_HOST" \
  --cluster-cidr 10.42.0.0/16 \
  --service-cidr 10.43.0.0/16 \
  --secrets-encryption \
  --disable traefik \
  --disable servicelb \
  --write-kubeconfig-mode 0600

unset AAT_JOIN_TOKEN
sudo k3s kubectl wait --for=condition=Ready "node/$AAT_NODE_NAME" --timeout=300s
```

On **C**, repeat the identity/prompt and install blocks, with these two values
instead:

```bash
AAT_NODE_NAME=aat-fourhost-c
AAT_PHYSICAL_HOST=host-c
```

Use C's own IP/interface and the same A address/token. Join B, confirm it becomes
Ready, then join C. Do not use `--cluster-init` on B or C; that would initialize
another cluster rather than joining A.

## 5. Join D as the agent

On **D**, with D's own IP/interface variables set:

```bash
read -rp "A's Ubuntu guest IPv4 address: " AAT_A_IP
read -rsp "Paste A's complete join token: " AAT_JOIN_TOKEN
printf '\n'

sudo env INSTALL_K3S_VERSION='v1.37.2+k3s1' K3S_TOKEN="$AAT_JOIN_TOKEN" \
  sh /tmp/aat-k3s-install.sh agent \
  --server "https://$AAT_A_IP:6443" \
  --node-name aat-fourhost-d \
  --node-ip "$AAT_NODE_IP" \
  --flannel-iface "$AAT_NODE_IFACE" \
  --node-label platform.aat/cluster=aat-fourhost \
  --node-label platform.aat/physical-host=host-d

unset AAT_JOIN_TOKEN
```

D runs workloads without an etcd/control-plane vote. Use `kubectl` on A/B/C for
administration; an agent does not receive the server's administrator kubeconfig.
SSH is not the node-to-node communication channel.

## 6. Verify all four nodes from A

```bash
sudo k3s kubectl get nodes -o wide
sudo k3s kubectl wait --for=condition=Ready nodes --all --timeout=300s
sudo k3s kubectl get nodes --show-labels
sudo k3s kubectl get pods -A -o wide
sudo k3s secrets-encrypt status
```

Check that **all four expected names** appear and are Ready. A successful
`wait --all` alone cannot detect a node that never registered. A/B/C should
show server/control-plane/etcd roles; D may show `<none>` in the roles column,
which is normal for an agent. Check the internal IPs and per-host labels.
CoreDNS, metrics-server and the local-path provisioner should settle into
healthy states; an early Pending/ContainerCreating state can occur during
initial image downloads.

If a server fails, inspect that guest with
`sudo journalctl -u k3s -n 100 --no-pager`. On D use
`sudo journalctl -u k3s-agent -n 100 --no-pager`. For a registered NotReady node,
use `sudo k3s kubectl describe node NODE_NAME` on a healthy server. Diagnose the
reported networking/token/configuration error rather than uninstalling or
reinitializing existing cluster state.

## 7. Install the unchanged assignment Tester Agent

On A, create the application's required namespace before applying the Tester
manifest, which contains Role/RoleBinding objects in that namespace:

```bash
sudo k3s kubectl create namespace sistem-informasi-kebencanaan-bnpb --dry-run=client -o yaml | sudo k3s kubectl apply -f -
sudo k3s kubectl label namespace sistem-informasi-kebencanaan-bnpb platform.aat/cluster=aat-fourhost --overwrite
curl -fL 'https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml' -o /tmp/aat-tester-agent.yaml
printf '%s\n' '964c086918cc31d5e887958b70d0606070b1e0c514c0017231ca651d0efa0f39  /tmp/aat-tester-agent.yaml' | sha256sum -c -
```

Only after the checksum reports `OK`, apply the exact manifest:

```bash
sudo k3s kubectl apply -f /tmp/aat-tester-agent.yaml
sudo k3s kubectl rollout status daemonset/tester-agent -n tester-agent --timeout=300s
sudo k3s kubectl get daemonset tester-agent -n tester-agent
sudo k3s kubectl get pods -n tester-agent -o wide
```

Expect desired/current/ready counts of four and one Tester pod on each node.
The pinned manifest/image are unchanged. List the available scenarios without
executing a fault:

```bash
sudo k3s kubectl exec -n tester-agent daemonset/tester-agent -- /app list
```

The Tester DaemonSet uses `hostNetwork: true`. Its readiness verifies deployment
on the hosts, not connectivity between ordinary pods through Flannel. Cross-node
pod IP, Service and DNS checks remain a separate checkpoint before applications.

## User-reported bootstrap checkpoint, 18 November 2026

The operator supplied output showing A/B/C Ready with control-plane/etcd roles,
D Ready as an agent, the expected distinct physical-host labels, and all nodes
on `v1.37.2+k3s1`. CoreDNS, local-path-provisioner and metrics-server were Running;
the gateway API CRD Helm job was Completed. Secrets encryption was Enabled,
with all server hashes matching. This is operator-reported basic bootstrap
evidence, not an independently executed physical-network, failover or application
acceptance test. The earlier C/B naming collision was no longer present in the
reported node names and physical-host labels; node IP mapping was not included
in that final output.

## Next checkpoint

Continue with [manual networking and administration without A](Platform-Manual-Network-and-Failover.md).
It covers using B/C directly, spreading CoreDNS, an ordinary pod network matrix,
and a later controlled A-pause control-plane check.

Four Ready nodes complete the basic manual K3s bootstrap. After the Tester
installation above, verify cross-node pod IP, Service and DNS traffic and prepare
the application release inputs. The existing
network-probe and provider commands depend on an inventory/provider integration
that is not yet in place for these manual guests.

The current management approach is `sudo k3s kubectl` locally on a server.
Joining through A is enough for initial bootstrap; do not claim a redundant
external API entry point. During later A-failure tests, administer from B/C or
provide a separately verified frontend. Application storage, migrations,
single-disk/provider integration, image publishing and real HA/load evidence
remain separate deployment work.

Reference: [K3s installation/configuration](https://docs.k3s.io/installation/configuration).

For the application phase, continue with [GHCR preparation and deployment order](Platform-Manual-GHCR-and-Application-Deployment.md).
Image preparation can run now; the guide identifies the existing-cluster and
single-disk deployment integration that remains before live application apply.
