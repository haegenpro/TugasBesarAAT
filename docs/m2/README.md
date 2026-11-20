# M2 implementation and operation

The reviewed architecture is kept locally in M2-Design.md. The assignment
specifications and that design file are intentionally Git-ignored. This guide
and the executable infrastructure are committed.

Development uses two Minikube nodes. The final deployment uses K3s on two Ubuntu
VMs on different physical hosts. Container nodes are not the final demo topology.

## Local prerequisites

- Linux amd64/arm64, Python 3.12+, Docker Engine, and access to the Docker socket.
- Go 1.27.1 for native application tests; otherwise build in the supplied images.
- Sufficient free disk for Kubernetes and application images. Per-node memory
  declarations are reservations/caps, not proof that workloads fit under load.

Install project-local, checksum-verified tools and prepare the isolated cluster:

```bash
python3 scripts/m2/install_tools.py
python3 scripts/m2/minikube.py start
python3 scripts/m2/manage.py init
```

Tools, kubeconfig, Minikube state, generated manifests, and credentials are under
the ignored .m2-local directory. These commands do not modify the host-global
kubeconfig or other clusters. The default node names are aat-m2 and aat-m2-m02.
The helper starts the first node and adds its worker separately; do not combine
Minikube's multi-node creation with its shared static-IP option.

Edit .m2-local/config.json for explicit node/context/image settings. Secrets are
generated once into .m2-local/secrets.json with mode 0600. Existing credentials
are never regenerated implicitly. Actual source/model credentials go into the
private configuration; never paste a generated manifest into a public issue.

```bash
python3 scripts/m2/manage.py plan
python3 scripts/m2/manage.py render
python3 scripts/m2/images.py build --component storage --component broker
python3 scripts/m2/images.py load-minikube --component storage --component broker
python3 scripts/m2/manage.py deploy
python3 scripts/m2/manage.py status
```

Implementation status: the first increment renders the storage/broker
foundation. Application workloads, schema bootstrap, recovery, and observability
are integrated in subsequent increments. A successful apply does not establish
application readiness or completion of P1–P5.

## Storage and isolation

Canonical Citus has a coordinator and two worker groups, each with one instance
on A and one on B. Ingestion has its own two-instance PostgreSQL formation.
Auth/Registry/Dashboard/Notifier own separate databases and roles in another
two-instance formation. Patroni uses the Kubernetes API rather than separate
etcd servers. PostgreSQL leader labels use database-role, leaving role=store
available for the official tester.

Each PostgreSQL and broker instance has its own node-pinned Retain volume under
/var/lib/aat-m2. The initial 2 GiB PostgreSQL volumes are development starting
values. The three broker volumes are 2 GiB each, with a 1 GiB per-broker message
ceiling and fsync-before-ack configured. Real use, metadata, WAL, catch-up, and
filesystem headroom must be measured. Capacity declarations alone do not enforce
host filesystem quotas.

The renderer denies incoming application traffic by default and permits the
appropriate owners to their storage groups. HBA rules also enforce each shared
database/role pair. The official tester namespace remains permitted. Never apply
a policy that obstructs the privileged official Tester Agent.

## Ordinary stop/start

```bash
python3 scripts/m2/manage.py stop
python3 scripts/m2/manage.py start
```

Stop records the original replica counts and scales managed application
controllers down. Start restores those counts. Neither operation clears data or
credentials. Keep the same configuration and secret files while data is retained.
Reset is a separate explicit operation; it must never be used to hide a failed
recovery during the assessed window.

## Preparing the Ubuntu VMs

Prepare each VM with 4 processors, 8 GB RAM and 32 GB disk. Record the physical
host specifications separately for the topology declaration. Install curl,
OpenSSL and WireGuard. Use independent VPN keys on A, B and the VPS hub.

Templates are in deployment/m2/wireguard-hub.conf.example and
deployment/m2/wireguard-node.conf.example. Install each filled configuration as
/etc/wireguard/wg0.conf with mode 0600. Enable IPv4 forwarding on the hub and
permit forwarding between its WireGuard peers. Enable wg-quick@wg0 on all peers.
The VPS needs its public UDP VPN port reachable; the home hosts do not require
inbound port forwarding. Use LAN instead if measured VPN performance fails.

Validate peer-to-peer ping, MTU and reboot recovery before installing K3s. The
example VPN subnet is 10.77.0.0/24, K3s pods use 10.42.0.0/16 and services use
10.43.0.0/16. Change them together if they overlap existing networks.

Copy deployment/m2/node.env.example privately to each VM. Set its node name and
VPN address. On A:

```bash
set -a
. ./node.env
set +a
sudo -E bash deployment/m2/bootstrap-node.sh a
```

Copy A's /etc/aat-m2/k3s-token securely to the same mode-0600 path on B. Use B's
own node.env and run bootstrap-node.sh b. The token is not printed by the script.
K3s release is explicit in node.env; use the same release on both nodes.

Export A's private kubeconfig to the operator machine and replace its server
address with A's reachable VPN address. Set a distinct context. Initialize an M2
configuration for that context and the real node names:

```bash
python3 scripts/m2/manage.py --config .m2-local/k3s.json init \
  --profile k3s --context aat-k3s --node-a node-a --node-b node-b
```

Set pod/service CIDRs to the K3s values in this private configuration. Review the
plan before deploying. The default Minikube CIDRs are not the K3s defaults.

Images can be exported without a registry:

```bash
python3 scripts/m2/images.py export --output .m2-local/images.tar
ssh node-a 'sudo k3s ctr -n k8s.io images import -' < .m2-local/images.tar
ssh node-b 'sudo k3s ctr -n k8s.io images import -' < .m2-local/images.tar
```

Alternatively configure an image registry and credentials. Application start
does not provision VMs, open home-router ports, or erase existing disks.

## Validation

```bash
python3 -m unittest discover -s scripts/m2 -p 'test_*.py' -v
python3 scripts/test-services.py --jobs 2
bash -n deployment/m2/bootstrap-node.sh
```

The infrastructure tests check ownership/placement/resource invariants and
destructive path guards. Native HTTP tests need loopback socket access.
Physical network interruption, kernel separation, clock/freeze behavior and
official exam outcomes remain real-VM acceptance work, not implied by rendering
or unit tests.
