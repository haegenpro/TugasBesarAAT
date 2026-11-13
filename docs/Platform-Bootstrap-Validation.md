# Lightweight four-node bootstrap validation

Executed on 13 November 2026 on Daniel's laptop. The ownership/fencing milestone
is committed as `61807f4`; this bootstrap is subsequent implementation work.
Neither M1 nor M2 specification Markdown is included in that commit.

## Deployed configuration

Four QEMU guests use **KVM hardware acceleration**, managed through system
libvirt. Each guest has its own kernel, independently writable root/data disks
and kernel boot ID. They share one physical laptop; this does not demonstrate
physical-host-loss tolerance.

| Node | Kubernetes role | Configured RAM | Private address |
| --- | --- | --- | --- |
| `aat-m2-a` | control plane / etcd / worker | 2 GiB | `192.168.127.11` |
| `aat-m2-b` | control plane / etcd / worker | 2 GiB | `192.168.127.12` |
| `aat-m2-c` | control plane / etcd / worker | 2 GiB | `192.168.127.13` |
| `aat-m2-d` | agent / worker | 1 GiB | `192.168.127.14` |

Total configured guest RAM: **7 GiB**. Ubuntu runtime reports 24.04.5 LTS,
kernel `6.8.0-147-generic`; k3s is `v1.37.2+k3s1`, with containerd
`2.3.7-k3s1`. Versions/artifact checksums are pinned in the
[inventory](../deployment/platform/inventory.example.json).

The initial 3 GiB-per-VM profile was interrupted following the user's memory
concern. The first 2 GiB-per-VM revision narrowly failed the new headroom check.
The accepted lighter profile keeps three 2 GiB control-plane servers and a
1 GiB agent. Stopped VMs were resized without replacing their disks or identities.
The preflight now enforces both a 4 GiB host reserve and 1 GiB startup headroom.

## Checks completed

- Ten Python validation tests pass, including disk/identity isolation, ownership
  refusal, RAM checks, resize restrictions and minimal health-check RBAC.
- All five generated libvirt XML documents pass `virt-xml-validate`; actual
  definitions and CPU/RAM resize pass libvirt's own schema validation.
- Four nodes are Ready with four distinct kernel boot IDs and no reported
  MemoryPressure, DiskPressure or PIDPressure.
- CoreDNS, local-path-provisioner and metrics-server rollouts are available.
- The application namespace is created; the unchanged, checksum-pinned Tester
  Agent manifest runs **4/4 Ready** on the four nodes.
- The fixed host API endpoint is `https://192.168.127.1:7443` and validates with
  the generated kubeconfig. Proxy TLS health checks use the server CA and a
  dedicated client identity whose explicit RBAC grant is only `GET /readyz`.
  Actual requests with this certificate returned **200** for `/readyz` and
  **403** for `/api/v1/nodes`; the proxy receives no admin key.
- Rerunning `up` on the existing light cluster succeeds without recreating disks
  or changing node identities. Personal SSH configuration/agents are excluded;
  only the generated VM identity is used.
- Python compilation and `git diff --check` pass.

Initial registration uses the first server's private VM address; k3s then
discovers all control-plane endpoints and maintains its own client load balancer.
The host API proxy is for host clients. Guest-to-host proxy traffic was blocked
on this host; no host firewall modification was needed for VM-to-VM registration.
See [k3s discovery](https://docs.k3s.io/architecture).

## Actual VM pause/recovery experiment

`python3 scripts/platform-cluster.py verify-failover`:

1. Verified the four-node baseline and core deployments.
2. Paused the owned first server VM (`aat-m2-a`) through libvirt.
3. Observed the fixed API endpoint available through surviving servers after
   **9.546 seconds** in this run.
4. Observed the agent's node-lease resource version advance while the first
   server remained paused, demonstrating continuing agent/control-plane traffic.
5. Created and deleted a Kubernetes ConfigMap through the fixed endpoint while
   that server was paused, demonstrating a control-plane write.
6. Resumed the VM and revalidated its actual API readiness, all four nodes and
   core deployments. Complete experiment: **44.771 seconds**.

The experiment passed and the first server was restored. This is **Kubernetes
control-plane continuity**, not canonical Citus durability, application ownership
fencing, upstream acquisition takeover or 10,000-VU evidence. It is one measured
run, not a worst-case availability bound.

## Resource snapshot and limits

After all nodes and Tester Agent were running, the host reported roughly
**3.2 GiB available** and **133 MiB swap used** out of 2 GiB. At that snapshot,
`kubectl top nodes` reported working memory of 1014, 588, 651 and 364 MiB for
A/B/C/D respectively. These are guest workload metrics, distinct from host RAM
used by QEMU, guest caches/kernel and other laptop applications. No application
load test was run.

This establishes that the **light bootstrap** runs on this device. It does not
establish capacity for the selected full deployment: canonical worker/coordinator
database requests alone total **7.5 GiB**, exceeding the profile's entire guest
RAM before Kubernetes/OS and other services. Application storage/HA deployment
must use a larger/shared host or a jointly revised architecture. No full Citus
formation, application database migration or application load was deployed here.

## Evidence and operation

Local private state is `/var/tmp/aat-m2`; keep it while the cluster has data.
Raw non-secret bootstrap evidence is under its `evidence/` directory:

- `bootstrap.json` and `nodes.json`.
- `tester-agent.json`.
- `api-failover.json`.

Keys, tokens and kubeconfig are under its protected `secrets/` directory and are
not committed. `/var/tmp` is subject to host cleanup policies; configure a more
durable user-owned, libvirt-accessible directory for longer-lived use.

```sh
python3 scripts/platform-cluster.py status
python3 scripts/platform-cluster.py verify
python3 scripts/platform-cluster.py stop
```

`stop` gracefully shuts down the owned guests and stops the API proxy while
preserving disks and state. See the [operator guide](../deployment/platform/README.md)
for startup, resource changes, certificate maintenance and remaining limitations.
