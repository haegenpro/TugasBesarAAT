# Four-node platform bootstrap

This workflow provisions four independent QEMU/KVM guests and a k3s cluster:
three control-plane/server nodes that also run workloads, plus one agent node.
It installs the assignment's unchanged pinned Tester Agent. It does not yet
deploy the Citus/Patroni formation or the application stack.

## Prerequisites and review

Linux x86-64 with KVM, system libvirt access, Docker access, Python 3.11+,
`qemu-img`, `genisoimage`, `ssh`, `ssh-keygen`, `openssl` and `ip`. The controller uses
existing libvirt/Docker permissions; it does not install host packages, change
home-directory permissions, or require passwordless sudo. Guest administration
uses only the generated key for the four owned guests.

Review [inventory.example.json](inventory.example.json), copy it to a local
inventory if needed, and edit node CPU/RAM/disks, addresses and physical-host
declarations. Cloud image, k3s binary/installer and Tester manifest use SHA-256
pins; the API proxy uses an image digest. Guest provisioning does not upgrade
host packages or execute an unpinned installer.

The example uses `/var/tmp/aat-m2` for VM state so system QEMU can traverse the
directory without opening Daniel's private home. Configure a durable user-owned
location reachable by libvirt for longer-lived deployments; `/var/tmp` is
subject to host cleanup policies. Keep the directory while the cluster has
data, including the base image backing each root overlay. Secrets/SSH keys and
kubeconfig are under `state_dir/secrets` with restrictive permissions. Never
commit or share that directory.

The light example assigns 2 GiB to each of three servers and 1 GiB to the agent,
7 GiB total, and sparse 12 GiB root + 8 GiB
data disks per node. This is a **bootstrap configuration**, not proof the whole
application/database/load workload fits. Sparse maxima total 80 GiB and are not
reserved physical disk; monitor actual host free space. The doctor requires
8 GiB free for initial setup and reports configured memory and disk maxima.
Existing laptop applications and VM demand also need room. The doctor also
enforces a 4 GiB host reserve and at least 1 GiB of available-memory headroom
after accounting for VM memory not already resident. The earlier 3 GiB-per-VM
profile fails this laptop's memory budget. Passing the light bootstrap check
does not permit deploying the full 7.5 GiB canonical database reservation here.

Read-only host preflight:

```sh
python3 scripts/platform-cluster.py doctor
```

Render XML/cloud-init/API-proxy review artifacts without changing VM/network
state, using an ed25519 **public** key:

```sh
python3 scripts/platform-cluster.py render --output evidence/platform-preview --public-key-file /path/to/key.pub
```

## One-command bootstrap

```sh
python3 scripts/platform-cluster.py up
```

For a customized inventory, pass `--inventory <file>` before the command.

The operation:

1. Checks prerequisites, existing resource ownership and libvirt subnet overlap.
2. Generates an independent SSH identity and verifies pinned downloaded artifacts.
3. Defines only its named, metadata/UUID-marked NAT network and four KVM guests.
4. Creates independent root/data qcow2 images; existing disks are preserved.
   Cloud-init formats only a new guest data disk and mounts it at `/var/lib/aat`.
5. Installs checksum-pinned k3s using private config files over SSH and starts
   the first etcd member. SSH ignores personal configuration/agents and uses only
   this deployment's generated identity.
6. Issues a 30-day readiness-only client certificate through Kubernetes CSR/RBAC,
   then starts a labelled, digest-pinned HAProxy container. Its fixed host
   endpoint balances server APIs and authenticates `/readyz` checks with that
   restricted identity and CA verification. It has no Kubernetes admin key.
   TLS client traffic passes through; each server includes the endpoint in SANs.
7. Retrieves a CA-pinned join token and joins other nodes directly to the first
   server's private address. K3s discovers all server endpoints after joining and
   maintains its own client-side load balancer. Host API access uses HAProxy;
   guest registration does not require opening a new host firewall port. K3s
   enables Secrets encryption and keeps its default network-policy controller.
   No default-deny policy blocks Tester.
8. Verifies four Ready nodes with distinct kernel boot IDs, creates the required
   application namespace, and applies the exact pinned Tester Agent manifest.

The API proxy is host-side cluster infrastructure, sharing the same physical
failure domain as the four local VMs. It survives loss of an individual VM;
this is not a claim of surviving loss of the laptop. A future multi-host/cloud
provider needs its own reachable fixed endpoint/network setup while preserving
the application contracts. This initial provider implements local libvirt only.
Its health certificate is renewed by `up`/`bootstrap` when less than a day
remains; the proxy reloads when certificate/config content changes. Run this
maintenance before the 30-day validity expires. Bootstrap currently expects the
first server to be reachable initially; automated replacement/bootstrap with
that server permanently missing remains future recovery work. Runtime continuity
with the first server paused is separately tested.

No host-wide kubeconfig/context is changed. Use the downloaded client explicitly:

```sh
/var/tmp/aat-m2/cache/k3s kubectl --kubeconfig /var/tmp/aat-m2/secrets/kubeconfig get nodes -o wide
```

## Inspect, resume and stop

```sh
python3 scripts/platform-cluster.py status
python3 scripts/platform-cluster.py verify
python3 scripts/platform-cluster.py verify-failover
python3 scripts/platform-cluster.py bootstrap
python3 scripts/platform-cluster.py install-tester
python3 scripts/platform-cluster.py stop
```

`verify-failover` briefly pauses the owned first server VM, checks API recovery,
agent lease renewal and a Kubernetes control-plane write, and resumes the VM in
cleanup. It records `api-failover.json` and verifies all nodes again. This is an
infrastructure test, not proof of canonical ownership or replicated storage.

`bootstrap` resumes k3s setup on already provisioned guests; `install-tester`
reapplies the same supplied manifest. A state marker and guest markers prevent
adopting unrelated resources or silently replacing an existing configuration.
If initial installation fails, inspect the affected guest's `cloud-init status`
and `journalctl -u k3s` / `k3s-agent`. Owned failed service starts can be retried.

`stop` requests graceful shutdown and stops only the labelled API proxy. It
preserves disks, VM definitions, private state and the dedicated network; no
delete/reset command is provided. Check status until shutdown completes. Do not
delete state while VMs are running. Inventory changes after provisioning require
explicit reconciliation; the controller refuses silent reprovisioning with a
different fingerprint. For a CPU/RAM-only adjustment, save the exact previous
inventory and stop the cluster **before** editing the desired resources. Then run:

```sh
python3 scripts/platform-cluster.py resize --previous-inventory /path/to/previous-inventory.json
python3 scripts/platform-cluster.py up
```

Resize requires all four VMs shut off and preserves disk paths, guest identity,
networking, versions and cluster settings. Other topology changes require
explicit reconciliation. Back up/rebalance application data before changing node
identity or storage. Resource-location changes do not redefine fencing or
canonical write contracts.

Successful bootstrap evidence is `state_dir/evidence/nodes.json` and
`bootstrap.json`, plus the Tester Agent and failover JSON when verified. This
proves only cluster startup/version/kernel identities and the recorded API test;
HA database durability, upstream ownership, permanent freeze recovery,
application load and combined-failure acceptance remain separate tests.

## Local validation

```sh
python3 -m unittest discover -s scripts/platform_lib -t scripts -p '*_test.py' -v
```

Tests cover independent VM/disks, reserved addresses, ownership refusal,
bootstrap/join flags, fixed-endpoint routing, deterministic credential-free
rendering and invalid topology rejection. Actual VM bootstrap is an additional
integration gate; generated XML/configuration alone is not proof of deployment.

Sources: [Ubuntu pinned image/checksums](https://cloud-images.ubuntu.com/releases/noble/release-20261017/),
[k3s release](https://github.com/k3s-io/k3s/releases/tag/v1.37.2+k3s1),
[HA k3s](https://docs.k3s.io/datastore/ha-embedded),
[k3s agent discovery](https://docs.k3s.io/architecture),
[Kubernetes client certificates](https://kubernetes.io/docs/reference/access-authn-authz/certificate-signing-requests/),
[libvirt networks](https://libvirt.org/formatnetwork.html),
[cloud-init](https://docs.cloud-init.io/en/latest/reference/examples.html).
