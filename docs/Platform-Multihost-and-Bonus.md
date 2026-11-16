# Two-host bootstrap and physical-network bonus

Implemented against baseline `feee7de5dd356f9dc02d377f7a46bcefdb40f92a`. This is a fresh-formation provider and evidence implementation. It is not a completed two-host deployment or an award of bonus points.

## Contract and boundaries

The [assignment](IF4031%20-%20Tugas%20Besar%20-%20M2.md) grants five points for nodes on different physical hosts, with both host displays, real-network latency/data-lag measurements and an actual cable/Wi-Fi disconnection. Code and report are both required. Only this bonus is targeted; AI and automatic scaling remain deferred. Existing mandatory scaling/load criteria remain separate work.

Keep one Kubernetes cluster, four independent guest kernels, the exact assignment namespace/role labels and the unmodified Tester Agent. Canonical storage remains four nonempty worker groups with three copies and two-copy acknowledgement. All application/storage protocols, migrations and pinned images remain unchanged.

| Host | Provider | Guest placement | Bootstrap guest RAM |
|---|---|---|---:|
| `linux` | Linux/KVM, system libvirt; controller runs here | A/B, both K3s server/workers | 4 GiB |
| `windows` | Windows VirtualBox 7.0.x; Windows-local helper over SSH | C server/worker, D agent/worker | 3 GiB |

Three control-plane votes are split 2+1 between physical machines. This does not tolerate loss of either physical host: losing Linux loses control-plane quorum. A 2+2 VM network partition also leaves some database groups with insufficient copies on each side; current global durability checks may stop all canonical writes. Truthful unavailability, fenced owners and retained pending obligations are expected. Never weaken acknowledgements to make the demonstration appear available.

## Delivered implementation

- `scripts/platform_lib/multihost_inventory.py`: inventory v2 with per-host provider/transport/state/capacity, explicit bridged LAN, static guest addresses and a Linux LAN API endpoint. Version-1 NAT inventory validation is unchanged.
- `host_agent.py` / `platform-host.py`: fixed JSON-stdin protocol, allowlisted lifecycle/transfer/collector operations, owned state and VM UUIDs, immutable transfer receipts, partial-provisioning intents, independent disks and conservative capacity checks. There are no VM deletion, package-installation, physical-NIC mutation or database-promotion operations.
- `multihost.py`: Linux controller dispatches libvirt locally and VirtualBox through the Windows account's SSH helper. Pinned qcow2 is converted to VDI, transferred through SFTP, hashed again remotely and cloned into independent guest root/data disks. Guest K3s bootstrap, restricted API health identity, HAProxy and Tester installation reuse the existing implementation.
- Pause/resume operations are provider-aware in the API-continuity, storage-HA and existing game-day runners. Raw `virsh` is refused in v2. Multi-host disk-loss replacement and v2 resize are deliberately blocked before mutation; existing v1 workflows remain available.
- `pod_probe.py`: four temporary, unprivileged, fixed-node pods check every peer, cluster DNS and a Service. They use a caller-supplied immutable Python3 image and the assignment's `other` resource class. Cleanup checks exact run/cluster ownership before deleting only probe resources.
- `platform-network.py` / `network_evidence.py`: bounded host-local observers, fixed physical-fault cues, resumable evidence download, complete terminal digests and independent owner reconciliation. Network-only success remains `blocked` for bonus acceptance.

## Prerequisites and inventory

Use `deployment/platform/inventory.multihost.example.json` as a template, not as working LAN configuration. Give the formation a new `aat-...` identity and a new state directory on each machine. Do not point it at the old `aat-m2` disks/state. Keep actual credentials, management keys, known-hosts files and private probe configuration outside tracked files.

Before running `up`:

1. Connect both hosts to the same wired switch/router. Reserve all host/guest IPs and MACs outside conflicting DHCP allocations. The example uses `192.168.50.0/24`; replace it with the actual nonoverlapping LAN. Retain a distinct pod/service network.
2. On Linux, prepare an existing Ethernet bridge (example `br0`) with the host LAN address and its physical Ethernet member. Use the host's network manager locally; the tool does not enslave or reconfigure the NIC carrying management traffic. Install KVM/libvirt, `qemu-img`, `genisoimage`, OpenSSH client/SFTP, Python 3.11+, OpenSSL and Docker. The controller account needs the existing libvirt/KVM/Docker permissions.
3. On Windows, install/use VirtualBox's bridged-network driver and select the **wired adapter description exactly**, matching `VBoxManage list bridgedifs`. Set `hosts.windows.bridge` accordingly; never attach a second NAT/Wi-Fi NIC to a guest. Install Python 3.11+ and provision an OpenSSH server/SFTP subsystem explicitly. The SSH account must be the account owning the VirtualBox VMs.
4. Make this same repository revision available on Windows. Install a fixed `aat-platform-host.cmd` wrapper in the SSH account's PATH; use actual absolute paths:

   ```bat
   @echo off
   "C:\Path\To\Python\python.exe" "C:\Path\To\Repo\scripts\platform-host.py"
   exit /b %errorlevel%
   ```

   No `%*`, caller command text or JSON argument expansion belongs in the wrapper. JSON goes through stdin. Authenticate management SSH with a dedicated key and verify the Windows host-key fingerprint out of band before adding it to the configured `known_hosts`. Strict host-key checking is required; personal SSH configuration/agents are not used.
5. Keep the application namespace and Tester permissions unobstructed. Restrict TCP 6443 to the node/admin network; TCP 2379/2380 between servers; UDP 8472 between nodes; required TCP 10250 access; TCP 22 for management; and the configured TCP 7443 API frontend. Guest/host/switch firewalls must permit these flows. Do not expose VXLAN publicly or disable firewalls globally. Pod-owned PostgreSQL, Patroni, broker and storage-DCS flows remain governed by their existing policies.
6. Populate host state paths, management key paths and real addresses in a private inventory. Windows VM state must be on a local drive, not a UNC share. The helper refuses symlinks/reparse paths. Linux guest disk storage must not use NFS/CIFS/tmpfs. Clock synchronization should remain enabled; the observer deliberately reports unknown one-way latency when a clock error bound is unavailable.

The doctor checks **each host independently**: measured RAM/available RAM, a 4 GiB host reserve, 1 GiB startup headroom, virtualization tools, Ethernet carrier and projected disk growth. Disk projection includes configured root/data maxima, base/conversion workspace and a 16 GiB host reserve; existing allocated owned files are credited. The stated 16 GB/256 GB nominal capacity is not substituted for measurements. Pod requests and limits are never reduced.

The 7 GiB guest bootstrap does not fit the approximately 18 GiB full pod request estimate. Full application deployment requires a separate sized inventory, owner database artifacts and successful live/offline bundle capacity assessment including recovery/surge headroom. Multi-host bundle reports now include fixed requests per physical host; live application apply also requires both host doctors to pass.

## Commands

Run the controller on Linux. Commands below use a private edited inventory and `rtk` as required in this workspace; it can be omitted on an operator machine without RTK.

```sh
rtk proxy python3 scripts/platform-cluster.py --inventory /private/twohost.json doctor
rtk proxy python3 scripts/platform-cluster.py --inventory /private/twohost.json render \
  --output evidence/twohost-preview --public-key-file /private/preview.pub

# Explicit infrastructure action, after both host checks and LAN setup pass:
rtk proxy python3 scripts/platform-cluster.py --inventory /private/twohost.json up
rtk proxy python3 scripts/platform-cluster.py --inventory /private/twohost.json status
rtk proxy python3 scripts/platform-cluster.py --inventory /private/twohost.json verify
rtk proxy python3 scripts/platform-cluster.py --inventory /private/twohost.json verify-network \
  --probe-image YOUR_REGISTRY/python@sha256:ACTUAL_IMMUTABLE_DIGEST
rtk proxy python3 scripts/platform-cluster.py --inventory /private/twohost.json verify-failover
rtk proxy python3 scripts/platform-cluster.py --inventory /private/twohost.json stop
```

`verify-network` requires an image with `python3` and its standard library; a mutable tag is refused. Its pods have a 180-second lifetime. API/guest keys are scoped to this formation. The frontend readiness certificate has the existing renewal behaviour and no admin privileges. `stop` requests graceful shutdown without removing disks. Inspect status until guests stop; a paused guest must first be explicitly resumed. An unavailable host leaves a nonzero, partially stopped result rather than deleting or replacing its resources.

If provisioning is interrupted, retry with the **same** inventory/state after inspecting the failure. Uploaded chunks reconcile their original offsets; SFTP artifacts must match their final digest. A verified completed artifact is reused without uploading another copy, including reconciliation of a lost receipt. Partial VM intent permits only the stopped, matching UUID and owned disk paths to be completed. An interrupted image conversion is retained and requires explicit inspection, not automatic replacement. Missing original guest keys, changed inventory, different disk paths or unmarked existing resources fail closed.

## Physical cable test and measurements

```sh
rtk proxy python3 scripts/platform-network.py plan --inventory /private/twohost.json
rtk proxy python3 scripts/platform-network.py run --inventory /private/twohost.json \
  --output evidence/network-RUN --application-probes /private/probes.json

# Recover downloads under the original run identity if the controller was interrupted:
rtk proxy python3 scripts/platform-network.py fetch --inventory /private/twohost.json \
  --output evidence/network-RUN

rtk proxy python3 scripts/platform-network.py reconcile --inventory /private/twohost.json \
  --output evidence/network-RUN --application-evidence /private/owner-evidence.json \
  --demo-attestation /private/demo-attestation.json
```

`run` requires current four-node verification and a passed cross-node pod/DNS/Service probe for the same inventory. It starts an observer on each host, then announces **60 seconds baseline → remove Windows Ethernet cable for 30 seconds → reconnect and observe for 180 seconds**. Physical removal/reconnection is the operator's only planned action. Do not restart pods, repair databases, manually replay work or change leases during the observation window. The tool itself never disconnects a NIC. Both hosts retain logs independently of management connectivity; observers reserve time for a bounded final iteration and self-terminate.

Record both physical displays showing the host/VM mapping, resource allocation and distinct guest kernels, as well as the actual cable action. VM software's “disconnect cable” button, iptables loss or a stopped API proxy does not substitute for this physical demonstration. A machine-ID hash alone cannot establish physical separation.

The recorder reports cross-host TCP-connect RTT p50/p95/p99 and unavailable samples. This includes connection setup and is neither one-way latency nor proof that an API accepting TCP is healthy. HTTP probes have an absolute two-second budget (including slow headers/body), a 1 MiB body bound, no redirects and no proxy fallback. Logs omit URL queries, tokens and complete response bodies. Host logs are bounded to 8 MiB, fetches to 1 MiB; terminal byte counts/digests are checked. Interrupted downloads resume at their retained byte offset under the original run identity; mismatched final digests fail without replacing the evidence.

Optional private `probes.json` maps host IDs to up to eight probes. URLs must use a literal trusted LAN IP. Only `/metrics` and the canonical entity GET are supported. Example shape (replace credentials/identity; do not commit the actual file):

```json
{"linux":[
  {"name":"bmkg-progress","kind":"metrics","url":"http://192.168.50.31:8080/metrics"},
  {"name":"known-effect","kind":"entity","url":"http://192.168.50.31:8080/internal/canonical/entities?source=BMKG&source_ref_id=YOUR_FIXTURE_ID","token":"PRIVATE_SERVICE_TOKEN","operation_id":"ORIGINAL_OPERATION_ID","expected_revision":1}
],"windows":[]}
```

For an application run, launch the existing authorized owner fixture publisher as the prepared workload; this new recorder performs no source/canonical writes. Retain exact source/operation identities. Selected received/prepared/applied metrics are **process telemetry**, not durable receipt authority. The owner evidence uses the existing recovery-checker schema (independent acknowledged operations/entities/outcomes, stable complete checksum and unresolved-operation list), plus `network_run_id` and latency markers:

```json
{"host":"linux","machine_sha256":"FROM_COLLECTOR_START","clock_domain":"FROM_COLLECTOR_START","source":"BMKG","operation_id":"ORIGINAL_OPERATION_ID","ack_monotonic_ns":123456789}
```

Markers must be recorded on the same physical observer OS/monotonic clock domain after its collector starts. The domain is returned in `run.json` and the host log header. Matching entity observations bind source pair, canonical UUID and revision to the acknowledged owner record. The reported acknowledgement-to-observation delay includes polling/network/processing delay; it is not the coastal delivery SLO. Historical/superseded revisions need their own observation and cannot be fabricated from a later current row. Never subtract two unsynchronized host clocks.

The private demo attestation contains exactly `run_id`, sorted `physical_hosts`, `reviewer`, `both_displays_artifact`, and `cable_disconnect_artifact`, referring to retained reviewed video/screenshots. Reconciliation returns zero only when network checks, independent application reconciliation, latency coverage and physical-demo attestation are supplied. Network-only recording exits 2 (`blocked`), failed assertions exit 1, and unresolved operations stay blocked. A passed evidence check is not a claim that the instructor awarded points.

## Validation on 15 November 2026

Raw local evidence: `evidence/platform-multihost/20261115t145610/` (ignored). No physical host was provisioned, no VM was started and no cable fault was executed during implementation.

| Target | Result | Evidence / limitation |
|---|---|---|
| Existing v1 inventory/render/identity/resize tests | Passed | Included in complete Python suite; v1 NAT workflow preserved |
| v2 validation/static addressing/bridged rendering | Passed | Deterministic preview, fresh identity, SATA/virtio distinction, LAN/path/provider rejection |
| Provider lifecycle and resource ownership | Passed locally | Fake-command libvirt create/retry and VirtualBox start/pause/resume/stop; unowned/drifted disks refused |
| Transfer and protocol safety | Passed locally | Duplicate chunk, conflicting bytes, corruption, path escape, completed-upload reuse/lost receipt, fixed JSON SSH command, bounded peer output |
| Collector and evidence checks | Passed locally | Detached process configuration, retry identity, no-cut/missing-terminal failure, wrong scope/clock/UUID and partial checksums refused |
| HTTP deadlines and credential redaction | Passed locally | Loopback test including deliberately trickled headers; no service credentials/body leaked |
| Whole Python suite / compilation | Passed | Final suite: 62 tests, zero failures (`post-review-tests.log`); compileall exit zero. Initial 59-test evidence remains historical |
| Interrupted collection / cancelled pod probe | Passed locally | Original-offset download and final digest checks; cancellation records failure and cleans only owned resources, refusing a changed ownership label |
| CLI preview / network plan | Passed | Synthetic public key only; no host changes. An initial preview attempt lacked its supplied public-key file; corrected using an explicit synthetic fixture |
| Windows read-only prerequisites | Blocked | Actual VirtualBox 7.0.18r162988 detected. This machine reported 7,916 MiB total RAM, approximately 635 MiB available, and 39.43 GiB free on the configured drive. Cold Windows guest allocation is 3,072 MiB plus host/headroom reserves; conservative projected disk requirement is 80 GiB. The example's Ethernet description is intentionally unset. Measurements are historical samples, not current guarantees |
| Initial Windows link observer attempt | Failed, replaced | Launching PowerShell exceeded its bound. Final observer uses documented IP Helper `GetIfTable`/`MIB_IFROW`, read-only; actual interface-table enumeration and final doctor executed successfully and reported blockers |
| Actual Linux/KVM + Windows VM bootstrap, peer pod matrix, VM pause/recovery | Blocked | Actual edited inventory, reachable Linux controller, host bridges/SSH and capacity are absent here |
| Real cable cut, full application reconciliation and bonus demonstration | Blocked | Requires actual deployment and reviewed physical evidence; synthetic tests do not establish these |
| AI/autoscaling bonus, full load/game day and disk replacement | Skipped | Outside this pass; mandatory acceptance remains tracked separately |

## Operator handoff

First populate the real per-host inventory and host prerequisites. Run both capacity checks, then bootstrap and verify cross-host pod networking. Prepare the owner application workload only on adequately sized infrastructure. Execute/record the physical cable scenario last, retain failures and logs, reconcile original identities, and link measured results and limitations in the assignment report.

References: [K3s network requirements](https://docs.k3s.io/installation/requirements), [K3s Flannel options](https://docs.k3s.io/networking/basic-network-options), [VirtualBox 7.0 command reference](https://docs.oracle.com/en/virtualization/virtualbox/7.0/user/vboxmanage.html), [libvirt existing host bridges](https://libvirt.org/formatnetwork.html#using-an-existing-host-bridge), [Windows interface state](https://learn.microsoft.com/en-us/windows/win32/api/ifmib/ns-ifmib-mib_ifrow), [GetIfTable](https://learn.microsoft.com/en-us/windows/win32/api/iphlpapi/nf-iphlpapi-getiftable).
