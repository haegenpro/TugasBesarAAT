# Ubuntu VMs in VirtualBox over Wi-Fi

This is the current manual setup: four physical machines on the same Wi-Fi
network, one Ubuntu Server VM on each, using VirtualBox on Linux Mint and
Windows. A/B/C will run K3s servers and workloads; D will run an agent and
workloads. The publicly accessible VPS is outside this initial cluster.
The current guest installation uses regular Ubuntu Server, not minimized.

The first checkpoint is four bootable Ubuntu VMs with stable, mutually reachable
addresses. For this short-lived cluster, each VM has **one virtual disk** shared
by Ubuntu, K3s and application data. This guide does not install the application
stack or claim that the four-host deployment has been tested.

## Current plan versus existing automation

| Item | Current manual setup | Existing repository provider |
| --- | --- | --- |
| Linux virtualization | VirtualBox | QEMU/KVM through libvirt |
| Windows virtualization | VirtualBox 7.2 | VirtualBox 7.0.x version check |
| Physical connection | Wi-Fi with bridged guests; verify actual connectivity | Existing wired Ethernet bridges/adapters |
| VM creation | GUI and Ubuntu ISO installer | Owned cloud-image disks, cloud-init and generated identities |
| VM disks | One VDI; application directories on the root filesystem | Separate root/data disks and a data mount at `/var/lib/aat` |
| Administration | VM console; SSH recommended for convenience | Dedicated SSH identities and fixed remote host helpers |
| Lifecycle/replacement | Manual for these VMs | Provider ownership checks and lifecycle/replacement journals |

Do not run `platform-cluster.py up` or the workflow's `cluster` phase against
these manually created VMs. The provider does not support adopting them. Later
storage/apply/recovery commands also depend on inventory, ownership, SSH and
provider checks; they are not automatically compatible just because K3s works.
Provider adaptation or a reviewed manual deployment procedure is still needed.
The [four-host overview](Platform-Four-Physical-Hosts.md) preserves the automated
path separately.

SSH is **not required by K3s**. K3s nodes communicate over Kubernetes, etcd and
container-network ports, not SSH. Console-only installation/administration is
possible. SSH makes remote commands and file transfer easier; the repository's
existing remote-management automation does require it.

## 1. Names and VM resources

Create exactly one VM on each physical machine:

| Physical machine | VirtualBox name and Ubuntu hostname | Later K3s role |
| --- | --- | --- |
| A | `aat-fourhost-a` | Server/control plane, etcd and workloads |
| B | `aat-fourhost-b` | Server/control plane, etcd and workloads |
| C | `aat-fourhost-c` | Server/control plane, etcd and workloads |
| D | `aat-fourhost-d` | Agent/workloads |

These names are operator conventions, not assignment requirements. Three
servers provide a three-vote embedded-etcd quorum. All four can run application
pods; do not create four VMs on the Mint laptop.

**Starting at 4 GiB is optional, not an Ubuntu or K3s installation step.** It is
a smaller allocation for checking installation/networking on a busy host. If
the machine can spare the intended application allocation now, create the VM
with 8 GiB/4 vCPUs, or more RAM as capacity permits, from the outset. There is
no benefit to deliberately starting smaller and resizing immediately afterward.

| Setting, per VM | Ubuntu/network/K3s preparation | Full application planning |
| --- | --- | --- |
| RAM | 4,096 MiB | Start planning at 8,192 MiB; 12,288 MiB gives more headroom where the host can spare it |
| Virtual processors | 2 | 4 where host capacity permits |
| Single disk | 64 GiB, dynamically allocated VDI | Shared by OS, container images, logs and application data; verify actual growth/headroom |
| OS | Ubuntu Server 24.04 LTS, AMD64 | Same |

The preparation allocation is **not capacity for the full M2 stack**. The
current full profile requests approximately 18.9 GiB RAM cluster-wide before
guest OS/K3s overhead. Four 4 GiB VMs cannot schedule it. Two 8 GiB plus two
4 GiB VMs pass the offline memory plan but are not a verified fit: placement, CPU, rollout surge and
node-loss recovery also need checking. Increase RAM/vCPUs with the VM fully
powered off; reinstalling Ubuntu is unnecessary.

At the 18 November 2026 setup check, this Mint machine reported approximately
15.2 GiB total RAM, 7.6 GiB available, 16 logical CPUs and 41 GiB free on the
repository's filesystem. **4 GiB/2 vCPUs** was the cautious suggestion for that
snapshot: 8 GiB was more than the available RAM at the time. Close unnecessary
programs and reassess before assigning more. This is a momentary measurement,
not the other machines' specifications. Check the actual VM storage filesystem.

The 64 GiB is a starting estimate, not an assignment minimum or a measured
requirement. A dynamic disk grows as data is written; it does not immediately
reserve 64 GiB. The current 41 GiB host free space does not cover its eventual
maximum. The ISO, snapshots and VM overhead also consume physical disk. Check
actual guest and host free space before full data/load runs. Snapshots are
unnecessary for initial setup.

If a 32 GiB VM is already installed, keep it for Ubuntu/network/K3s preparation.
Check space before full deployment; increasing a VDI later also requires
expanding the guest partition/filesystem, not reinstalling Ubuntu.

## 2. Download and install VirtualBox

### On Linux Mint 22.x

This Mint 22.3 machine reports Ubuntu base `noble`, architecture `x86_64`.
Choose **Ubuntu 24.04/Noble AMD64 `.deb`** from
[Oracle's downloads](https://www.oracle.com/virtualization/technologies/vm/downloads/virtualbox-downloads.html).
The checked version is 7.2.22:

[Download VirtualBox 7.2.22 for Noble AMD64](https://download.virtualbox.org/virtualbox/7.2.22/virtualbox-7.2_7.2.22-175245~Ubuntu~noble_amd64.deb).

Run on the **physical Mint host**:

```bash
cd ~/Downloads
sudo apt update
sudo apt install build-essential dkms "linux-headers-$(uname -r)"
sudo apt install ./virtualbox-7.2_7.2.22-175245~Ubuntu~noble_amd64.deb
VBoxManage --version
```

Use the actual downloaded filename if it changes. Open VirtualBox from Mint's
application menu as your normal user.

**Secure Boot is enabled on this Mint host.** If installation requests a
temporary Machine Owner Key (MOK) password, record it and reboot the physical
machine. On the MOK screen choose **Enroll MOK → Continue → Yes → enter that
password → reboot**. Some hosts already trust their signing key and show no
prompt. The guest EFI setting does not control physical-host Secure Boot.

If VM startup reports a kernel-driver or rejected-key error, inspect on Mint:

```bash
mokutil --sb-state
modinfo -F signer vboxdrv
sudo modprobe vboxdrv
sudo journalctl -k -b -n 60 --no-pager
```

For a module build failure, verify matching kernel headers and run
`sudo /sbin/vboxconfig` if present. Untrusted/unsigned modules need the host's
signing and MOK enrollment procedure; rebuilding alone does not authorize them.
See [Oracle installation](https://docs.oracle.com/en/virtualization/virtualbox/7.2/user/installation.html)
and [Ubuntu Secure Boot](https://documentation.ubuntu.com/security/security-features/platform-protections/secure-boot/).

### On Windows

For Windows 10/11 on an Intel/AMD machine, download
[VirtualBox 7.2.22 for Windows](https://download.virtualbox.org/virtualbox/7.2.22/VirtualBox-7.2.22-175245-Win.exe)
from [Oracle's downloads](https://www.oracle.com/virtualization/technologies/vm/downloads/virtualbox-downloads.html).
Use the same Ubuntu AMD64 ISO linked below. The guest OS is Ubuntu even though
the physical host is Windows.

1. Open **Task Manager → Performance → CPU** and check **Virtualization**. If
   disabled, enable Intel VT-x/Virtualization Technology or AMD SVM in the
   machine's BIOS/UEFI settings, then return to Windows.
2. Run the `.exe` installer and approve its administrator prompt. Keep the
   networking/bridged-network driver selected. Python API integration is
   unnecessary for this GUI procedure; deselect it if it requests Python.
3. Finish installation and reboot if requested. Networking may briefly
   disconnect while its driver is installed.
4. Open VirtualBox from Start. Create one VM with the machine's assigned name,
   for example `aat-fourhost-b`. Store it on a local drive with adequate free
   space, such as `D:\VirtualBox VMs`, rather than a synced/OneDrive directory.
5. Follow sections 4–6 below for VM creation and Ubuntu installation. Select
   **8,192 MB/4 vCPUs** directly if Windows and its other programs can spare
   those resources; 4,096 MB/2 vCPUs is just the smaller preparation option.
6. In **Settings → Network → Adapter 1**, choose **Bridged Adapter** and the
   physical Wi-Fi adapter's actual description. Avoid WSL, Hyper-V/vEthernet,
   host-only and VPN adapter entries. Leave Cable Connected enabled.
7. Install Ubuntu in the VM using the shared installer choices below. All
   subsequent `sudo`, disk, update and Linux network commands run **inside
   Ubuntu**, not in Windows PowerShell. Use PowerShell only for host commands
   such as ISO checksum verification and optional `ssh aat@GUEST_IP`.

If needed, inspect the installed version in Windows PowerShell:

```powershell
& "C:\Program Files\Oracle\VirtualBox\VBoxManage.exe" --version
```

The Windows host does not need an SSH server for this manual process. Set
Windows power/lid behavior so the physical machine does not sleep while its
cluster VM is running. The Ubuntu screens and later data-directory setup are
the same on Mint and Windows.

## 3. Download the Ubuntu installer

[Ubuntu Server 24.04.5 LTS AMD64 ISO](https://releases.ubuntu.com/24.04/ubuntu-24.04.5-live-server-amd64.iso).

Use the same ISO on all four machines. Attach it directly; no extraction or USB
stick is needed. This interactive installer differs from the cloud image used
by the automated provider.

For an integrity check, download [SHA256SUMS](https://releases.ubuntu.com/24.04/SHA256SUMS)
to the same folder. On Mint:

```bash
cd ~/Downloads
sha256sum --ignore-missing -c SHA256SUMS
```

The downloaded server ISO's line should report `OK`. On Windows PowerShell:

```powershell
Get-FileHash "$env:USERPROFILE\Downloads\ubuntu-24.04.5-live-server-amd64.iso" -Algorithm SHA256
```

Compare the value against the entry for the exact same filename in `SHA256SUMS`.

## 4. Create the VM

Open VirtualBox → **New**. For A use:

| Wizard field | Selection |
| --- | --- |
| Name | `aat-fourhost-a` |
| Folder | Durable local storage with enough free space, outside this Git repository |
| ISO Image | Downloaded Ubuntu Server ISO |
| OS/type | Linux |
| Distribution/version | Ubuntu, 64-bit |
| Unattended installation | Disable **Install OS Using Unattended Installation**, or enable **Skip Unattended Installation**, depending on displayed wording |
| Base memory | `8192 MB` if the host can spare it; `4096 MB` for the smaller preparation option |
| Processors | `4` where host capacity permits; `2` for the smaller preparation option |
| Use EFI | Off for this consistent manual x86 setup; retain that boot mode afterward |
| Hard disk | One new VDI, 64 GiB, dynamically allocated; **Pre-allocate Full Size** off |

This is the VM's only hard disk; no second data disk is needed. On other
machines use the B/C/D name. See the [VirtualBox creation reference](https://docs.oracle.com/en/virtualization/virtualbox/7.2/user/create-vm.html)
if the wizard layout differs.

## 5. Review settings before first boot

With the VM powered off, select **Settings**:

| Page | Selection |
| --- | --- |
| System → Motherboard | Hard disk/optical boot enabled, floppy disabled, I/O APIC enabled |
| System → Processor | Chosen vCPU count; execution cap 100% |
| System → Acceleration | Leave defaults; nested VT-x/AMD-V is unnecessary for K3s containers |
| Display | VMSVGA, 16–32 MB video RAM, 3D acceleration off |
| Storage | Root VDI on SATA; Ubuntu ISO in optical drive |
| Network → Adapter 1 | Enabled, **Bridged Adapter** |
| Adapter 1 → Name | The physical host's actual Wi-Fi adapter |
| Adapter Type | Intel PRO/1000 MT Desktop if a choice is needed |
| Advanced network | Cable Connected on; Promiscuous Mode Deny; keep unique generated MAC |
| Network → Adapters 2–4 | Disabled for this initial single-interface setup |

On Mint select the wireless interface, often `wlp...`; on Windows select the
actual Intel/Realtek/other Wi-Fi adapter. Avoid VPN, Docker and host-only
adapters. Inside Ubuntu, the virtual Ethernet interface is commonly `enp0s3`:
the VM does not need your physical Wi-Fi password.

Wi-Fi bridging must work with the actual adapter/access point. A bridged guest
should get its own LAN IP. Default VirtualBox NAT often gives every VM
`10.0.2.15`; those separate NATs do not connect nodes on different physical
hosts. See [VirtualBox networking](https://docs.oracle.com/en/virtualization/virtualbox/7.2/user/networkingdetails.html).

## 6. Install Ubuntu from the ISO

Click **Start** and choose **Try or Install Ubuntu Server** if shown. The
installer uses a text interface: arrows/Tab to navigate, Space to toggle,
Enter to accept. **Right Ctrl** normally releases a captured keyboard/mouse.

| Installer screen | Project selection |
| --- | --- |
| Language/keyboard | Your preference and matching keyboard |
| Installer update | Continue without updating for this walkthrough |
| Installation type | Ubuntu Server, regular selection used by the current setup |
| Network | DHCP initially; note the IPv4 address/interface |
| Proxy/mirror | No proxy unless required; a working default mirror |
| Storage | Entire **64 GiB virtual disk**; deselect LVM for simplicity |
| Profile | Your name; hostname `aat-fourhost-a`; username `aat`; private password |
| Ubuntu Pro | Skip for now |
| SSH | Recommended: Install OpenSSH server; allow password login initially |
| Featured server snaps | Leave unselected; install K3s later |

Use B/C/D hostnames on their machines; passwords need not match. The storage
confirmation applies to the new virtual disk, not the physical OS. SSH remains
optional for K3s; omit it for console-only administration. These are project
choices alongside the [Ubuntu installer guide](https://ubuntu.com/server/docs/tutorial/basic-installation/).

The regular Server selection is a convenience for interactive administration,
not a K3s or assignment requirement. Minimized installs fewer utilities; add the
commands used in this guide before its inspection/network steps:

```bash
sudo apt update
sudo apt install -y curl ca-certificates iproute2 iputils-ping netcat-openbsd nano
```

Both choices use the same VM hardware settings. Package minimization does not
reduce the application's Kubernetes memory requests. If regular Server is
already installed, there is no need to reinstall merely to select minimized.

When finished select **Reboot Now**. If prompted to remove the installation
medium, use **Devices → Optical Drives → Remove disk from virtual drive**, then
press Enter. If the installer starts again, power off, detach the ISO in
Storage settings and restart.

## 7. First login and updates

Log in as `aat`; password typing displays no characters. A shell prompt without
a desktop is normal for Ubuntu Server. Run **inside the guest**:

```bash
hostnamectl
cat /etc/os-release
ip -br -4 addr
ip route
free -h
df -h /
```

Confirm the hostname, Ubuntu 24.04, RAM and root disk. Note the actual interface
name rather than assuming `eth0`. Update this fresh VM:

```bash
sudo apt update
sudo apt upgrade -y
sudo apt install -y curl ca-certificates openssh-server netcat-openbsd
sudo systemctl enable --now ssh
sudo timedatectl set-timezone Asia/Jakarta
sudo timedatectl set-ntp true
sudo reboot
```

For console-only use, omit `openssh-server` and the SSH service command. K3s
supplies its own container runtime; a guest Docker installation is unnecessary.

## 8. Prepare application data directories on the same disk

Use the same disk for Ubuntu, K3s and application data. M2 requires `role=store`
data on the node's local disk through local PV or hostPath; a persistent root
filesystem directory meets that locality requirement. A second disk is not
required. For this short-lived cluster we choose the simpler single-disk layout.

Inside each Ubuntu VM:

```bash
sudo mkdir -p /var/lib/aat/local-pv
findmnt --target /var/lib/aat/local-pv
df -h / /var/lib/aat
```

The mount target should be `/`: the application directory is on the root
filesystem. There is no extra partitioning, formatting or `/etc/fstab` entry.
Later deployment creates each persistent volume's subdirectory and appropriate
ownership. Each guest has its own VDI and data directories. Do not share a
writable VDI across nodes or use `chmod -R 777` for volume permissions.

Both OS and data share the available space, so check `df -h` during load/demo
runs. Local PV/hostPath data outlives pod restarts but is lost if its node disk
is erased; the application replication/recovery protocols remain necessary.

The existing automation still assumes separate root/data disks:
`platform-storage.py prepare-disks` checks `findmnt --mountpoint /var/lib/aat`.
That check, provider disk definitions and disk-replacement assumptions need
adaptation before those tools can manage this layout. Creating these
directories does not complete that integration, which is already pending for
the manual VirtualBox/Wi-Fi provider.

## 9. Keep guest addresses stable

Record each VM's IPv4 address, interface and MAC:

```bash
ip -br -4 addr
ip -br link
ip route
```

Prefer router DHCP reservations for each VM; reboot and verify address
stability. Physical host addresses must differ from guest addresses. Wi-Fi
bridging can affect DHCP client identification; use a static guest address if
reservations do not work reliably.

These are **examples only**; use the actual LAN prefix, gateway and DNS:

| Guest | Example IPv4 |
| --- | --- |
| A | `192.168.50.31/24` |
| B | `192.168.50.32/24` |
| C | `192.168.50.33/24` |
| D | `192.168.50.34/24` |
| Gateway/DNS | `192.168.50.1` |

Exclude static addresses from DHCP allocation and ensure they are unused.
Planned K3s pod `10.42.0.0/16` and service `10.43.0.0/16` ranges must not overlap
the LAN or VPN.

For a static address, use the **VM console**, list `/etc/netplan/` and `sudoedit`
the existing installer YAML. Replace its interface stanza rather than adding a
conflicting file. Example for A **only if this is the real network**:

```yaml
network:
  version: 2
  ethernets:
    enp0s3:
      dhcp4: false
      addresses:
        - 192.168.50.31/24
      routes:
        - to: default
          via: 192.168.50.1
      nameservers:
        addresses:
          - 192.168.50.1
```

Replace `enp0s3` with the observed interface. Use `sudo netplan try`, verify
connectivity, and accept if correct. The console remains available if changing
the network interrupts SSH.

## 10. Test peer connectivity and optional SSH

From **every guest**, using actual addresses:

```bash
ping -c 3 192.168.50.31
ping -c 3 192.168.50.32
ping -c 3 192.168.50.33
ping -c 3 192.168.50.34
getent ahostsv4 releases.ubuntu.com
curl -I --max-time 15 https://releases.ubuntu.com/24.04/
```

Test all directions, not only host-to-own-guest. Ping may be filtered; when SSH
is installed, also test a listening service on the peer:

```bash
nc -vz -w 3 192.168.50.32 22
ssh aat@192.168.50.32
```

On Mint use a terminal; on Windows use PowerShell with its OpenSSH client.
Connect to the **guest IP**, not the physical Windows/Mint host. Physical-host
SSH servers are unnecessary for this manual path. Use `exit` to return.

### Copy/paste commands between host and guest

For Ubuntu Server's text console, use an SSH session in the physical host's
terminal to paste commands. VirtualBox's Shared Clipboard is a GUI/Guest
Additions feature; selecting Bidirectional alone does not add clipboard paste
to the plain Linux console. See [Guest Additions clipboard support](https://docs.oracle.com/en/virtualization/virtualbox/7.2/user/guestadditions.html).

Keep the VM running, connect with `ssh aat@GUEST_IP`, and paste into that local
terminal: **Ctrl+Shift+V** on Mint or Windows Terminal. Select terminal output
and use **Ctrl+Shift+C** to copy. Commands entered in the SSH session execute
inside Ubuntu; Guest Additions or a guest desktop are unnecessary for this.
An SSH session is a new shell, so re-enter any K3s IP/interface variables that
were previously set in the VirtualBox console.

Compare the first SSH connection's fingerprint with this in the VM console:

```bash
sudo ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

For an SSH failure check in the guest:

```bash
systemctl status ssh --no-pager
sudo ss -lntp 'sport = :22'
sudo ufw status verbose
```

If UFW is active, allow TCP 22 from the actual trusted management addresses,
for example `sudo ufw allow from 192.168.50.0/24 to any port 22 proto tcp` only
if that is your trusted LAN. Do not disable all firewalls to mask the problem.

For Internet-without-peer-connectivity, check AP/client isolation, guest Wi-Fi
restrictions, the bridge selection and host/guest firewalls. If the adapter or
access point cannot bridge peer traffic, the direct-LAN route is not ready.
Use a suitable network or a VPN **inside the Ubuntu guests** with K3s configured
for that interface. A host-only VPN does not automatically connect NATed guests.
Do not proceed with direct-LAN cluster addresses until they work.

## 11. Operate the VMs and record the checkpoint

Keep a private record of physical host, guest name, IP/interface/MAC,
RAM/vCPUs and VDI location. This can be a local note; the
existing provider inventory does not support these manual guests yet. Keep
passwords and later tokens/keys outside tracked examples.

Keep physical computers on AC power and prevent sleep/lid-close suspension
during cluster operation. Use `sudo poweroff` for normal shutdown and
VirtualBox **Start** to restart. Paused/saved state is not powered off when
changing RAM/CPU. Use **Machine → Pause** only for later deliberate freeze
experiments after baseline checks. Do not revert VM snapshots after database,
broker or etcd formation: that can rewind durable state. A physical Wi-Fi
disconnect is a separate fault from a VM pause.

VM preparation is complete when all four nodes:

- Boot Ubuntu from the root VDI, with the installer ISO detached.
- Have unique hostnames and guest network identities.
- Keep their intended addresses after reboot.
- Reach peer guests, resolve DNS and download over HTTPS.
- Have recorded allocations and sufficient physical-host free space.
- Support console administration or the optional SSH login.

Before application deployment, also verify the data directories exist on the
guest disk and enough free space remains for images, volumes and logs. The
existing provider/storage checks still need the integration described above.

## Next: K3s and deployment integration

Continue with the [manual K3s bootstrap guide](Platform-Manual-K3s-Setup.md).
It covers peer connectivity, the pinned installer, A initialization, B/C server
joins, D agent join and verification. All commands run inside Ubuntu on either
host OS.

The later K3s install should match the repository pin `v1.37.2+k3s1`: A/B/C
servers, D agent; server Secrets encryption; network-policy support retained;
Traefik and ServiceLB disabled as in the existing bootstrap. Use each guest's
**actual interface name**, not the cloud image's hard-coded `eth0`. Add the
project node/physical-host labels consistently in a compatible deployment plan.

K3s needs traffic independently of SSH: TCP 6443 from nodes to servers, TCP
2379/2380 between servers, UDP 8472 between nodes for default Flannel VXLAN, and
required TCP 10250 access between nodes. Firewalls must also permit the selected
pod/service routing. Follow [K3s network requirements](https://docs.k3s.io/installation/requirements)
and [HA server setup](https://docs.k3s.io/datastore/ha-embedded).

Verify cross-node pod/Service/DNS networking after four nodes become Ready,
before storage, the unchanged assignment Tester Agent, migrations and app
activation. SSH connectivity and Ready nodes alone do not prove the pod network,
application HA or M2 acceptance. A fixed/redundant API entry point remains a
deployment task. The VPS is optional for later public access/load generation;
it is not needed as a fifth cluster node.
