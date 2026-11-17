# Four physical hosts, one VM each

The deployment target is four separate physical machines. Each runs exactly one
Linux VM. A/B/C are k3s servers and workers; D is a k3s agent/worker. The existing
three-copy canonical placement remains unchanged. Windows hosts use VirtualBox;
Linux hosts use QEMU/KVM through libvirt. The controller runs on a Linux host.

`deployment/platform/inventory.fourhost.example.json` is a review template with
two Windows and two Linux hosts. The actual mix is configurable; it is not a
claim about the team's devices. Set each host's provider and OS-appropriate state
directory/bridge, each VM's distinct LAN IP/MAC, and actual resource allocation.
The validator requires exactly one VM on each declared host and rejects duplicate
physical host identities. Legacy two-host inventories remain supported.

Only the controller uses local transport. All other physical hosts, including
remote Linux hosts, use the fixed SSH `aat-platform-host` helper. Install that
wrapper using an absolute Python/repository path; send requests over JSON stdin.
The wrapper accepts no arbitrary command argument. Keep the helper code revision
aligned during platform changes. Application releases use container images and
do not require Git pulls on every VM.

Prepare and verify SSH host keys before use; keys and known_hosts paths are
controller-local private inputs. Remote Linux accounts need their existing
libvirt/KVM permissions. Windows helper setup, VirtualBox bridged Ethernet and
Linux Ethernet bridge preparation follow the existing multi-host runbook.
No script reconfigures a physical NIC carrying the control connection.

```sh
python3 scripts/platform-cluster.py --inventory /private/four-hosts.json doctor
# Later, after measured capacity/network prerequisites pass:
python3 scripts/platform-cluster.py --inventory /private/four-hosts.json up
python3 scripts/platform-cluster.py --inventory /private/four-hosts.json verify
```

The example's 4 GiB RAM / 12 GiB root / 32 GiB data per VM is a placeholder,
not an approved fit. Each host doctor measures available RAM and free disk and
reserves host OS/startup space plus worst-case disk/base-conversion growth. A
machine with only 30 GiB free may fail this profile. The application bundle has a
separate workload/recovery capacity check; four 8 GiB hosts do not automatically
prove the full stack fits. Requests/limits and replica acknowledgements are not
reduced to make a plan appear feasible.

Node VM pause/resume and host collectors dispatch through each node's provider.
Network evidence requires all declared machine identities, not just the original
two-host pair. The cable recorder selects a non-controller host and records that
host's identity. Physical fault execution and measured recovery remain deferred.

The management API proxy still runs on the controller physical host. Losing that
host can remove this administrative entry point even when surviving Kubernetes
servers retain quorum. Do not claim controller-host-loss API continuity without
a separately verified redundant entry point. Multi-provider disk-loss replacement
is a separate recovery integration; the legacy destructive libvirt path remains
blocked for v2 inventories until it can recover all owned persistent workloads.
