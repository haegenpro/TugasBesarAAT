"""Allowlisted local provider operations. SSH passes JSON stdin, never shell code.

No VM/disk deletion, package installation, NIC reconfiguration or guest promotion
is implemented here. The helper must run as the intended VM-owning account.
"""
import base64
import ctypes
import getpass
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import threading
import time
import uuid
import xml.etree.ElementTree as ET

from .inventory import domain_xml, fingerprint, owned, require, resource_uuid, validate
from .multihost_inventory import capacity, host_nodes

MAX_REQUEST = 128 * 1024
MAX_RESPONSE = 2 * 1024 * 1024
OPERATIONS = {"doctor", "status", "init", "upload", "upload-status", "seal-upload", "create", "start", "stop", "pause", "resume", "replace-empty", "collector-start", "collector-fetch"}


def execute(args, *, timeout=60, check=True, input=None):
    result = subprocess.run([str(a) for a in args], input=input, capture_output=True, timeout=timeout)
    if check and result.returncode:
        # Do not return command output (which may include credentials or host data).
        raise RuntimeError(f"provider command {Path(str(args[0])).name} failed with exit {result.returncode}")
    return result


def bounded_execute(args, data, timeout=300):
    """Bound SSH output before allocation, including a malicious/broken peer."""
    process = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = [bytearray(), bytearray()]
    overflow = threading.Event()
    def read(stream, index):
        while True:
            chunk = stream.read(65536)
            if not chunk:
                break
            if len(output[index]) + len(chunk) > MAX_RESPONSE:
                overflow.set()
                process.kill()
                break
            output[index].extend(chunk)
        stream.close()
    def write():
        try:
            process.stdin.write(data)
            process.stdin.close()
        except (BrokenPipeError, OSError):
            pass
    workers = [threading.Thread(target=read, args=(process.stdout, 0), daemon=True),
               threading.Thread(target=read, args=(process.stderr, 1), daemon=True), threading.Thread(target=write, daemon=True)]
    for worker in workers:
        worker.start()
    try:
        process.wait(timeout=timeout)
    except BaseException:
        process.kill()
        process.wait(timeout=5)
        raise
    finally:
        for worker in workers:
            worker.join(timeout=5)
    require(not overflow.is_set() and not any(w.is_alive() for w in workers), "SSH output/reader quota exceeded")
    return subprocess.CompletedProcess(args, process.returncode, bytes(output[0]), bytes(output[1]))


def digest(path):
    with Path(path).open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def machine_values():
    if os.name == "nt":
        class Memory(ctypes.Structure):
            _fields_ = [("length", ctypes.c_ulong), ("load", ctypes.c_ulong)] + [(n, ctypes.c_ulonglong) for n in
                ("total", "available", "page_total", "page_available", "virtual_total", "virtual_available", "extended")]
        m = Memory()
        m.length = ctypes.sizeof(m)
        require(ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m)), "cannot read Windows memory")
        import winreg
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Cryptography") as key:
            identity = winreg.QueryValueEx(key, "MachineGuid")[0]
        return m.total // 2**20, m.available // 2**20, hashlib.sha256(identity.encode()).hexdigest()
    info = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    identity = Path("/etc/machine-id").read_bytes().strip()
    return int(info["MemTotal"].split()[0]) // 1024, int(info["MemAvailable"].split()[0]) // 1024, hashlib.sha256(identity).hexdigest()


def windows_links():
    """Read IP Helper carrier state without repeatedly launching PowerShell.

    MIB_IFROW/GetIfTable are documented by Microsoft (ifmib.h/iphlpapi.h).
    ctypes structures calculate the table padding; do not assume a row offset.
    """
    require(os.name == "nt", "Windows interface observer requires Windows")
    dword = ctypes.c_uint32
    class Row(ctypes.Structure):
        _fields_ = [("name", ctypes.c_wchar * 256)] + [(n, dword) for n in ("index", "type", "mtu", "speed", "mac_length")] + [
            ("mac", ctypes.c_ubyte * 8)] + [(n, dword) for n in ("admin", "oper", "last_change", "in_octets", "in_unicast", "in_nonunicast", "in_discards", "in_errors", "in_unknown", "out_octets", "out_unicast", "out_nonunicast", "out_discards", "out_errors", "out_queue", "description_length")] + [("description", ctypes.c_ubyte * 256)]
    class Table(ctypes.Structure):
        _fields_ = [("count", dword), ("rows", Row * 1)]
    size = dword()
    api = ctypes.windll.iphlpapi.GetIfTable
    require(api(None, ctypes.byref(size), False) in (0, 122) and 0 < size.value <= 1 << 20, "interface table size unavailable")
    data = ctypes.create_string_buffer(size.value)
    require(api(data, ctypes.byref(size), False) == 0, "interface table unavailable")
    count = dword.from_buffer(data).value
    require(count <= 1024 and Table.rows.offset + count * ctypes.sizeof(Row) <= len(data), "invalid interface table bounds")
    result = []
    for i in range(count):
        row = Row.from_buffer(data, Table.rows.offset + i * ctypes.sizeof(Row))
        require(row.description_length <= 256, "invalid interface description size")
        if row.type == 6:  # Ethernet; do not use Wi-Fi as the data-plane interface.
            description = bytes(row.description[:row.description_length]).decode("mbcs", errors="replace").rstrip("\x00")
            result.append({"interface": description, "index": row.index, "up": row.admin == 1 and row.oper == 5,
                           "oper_state": row.oper, "speed_bps": row.speed})
    return result


def pairs(raw):
    result = {}
    for line in raw.decode("utf-8", errors="replace").splitlines():
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        try:
            result[k] = json.loads(v)
        except ValueError:
            result[k] = v
    return result


class HostAgent:
    def __init__(self, cfg, host_id, runner=execute, root=None):
        validate(cfg)
        require(cfg["version"] == 2, "host helper accepts inventory v2 only")
        self.cfg, self.run = cfg, runner
        self.host = next(h for h in cfg["hosts"] if h["id"] == host_id)
        self.nodes = host_nodes(cfg, self.host)
        self.root = Path(root if root is not None else self.host["state_dir"])
        self.owner = {"cluster_id": cfg["cluster_id"], "host_id": host_id, "inventory_sha256": fingerprint(cfg)}
        self.vbox = shutil.which("VBoxManage") or str(Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "Oracle/VirtualBox/VBoxManage.exe")

    def path(self, relative):
        require(isinstance(relative, str) and re.fullmatch(r"[A-Za-z0-9_./-]{1,240}", relative), "invalid owned relative path")
        p = self.root / relative
        require(not Path(relative).is_absolute() and ".." not in Path(relative).parts, "path escapes host state")
        for q in [self.root, *self.root.parents, p, *p.parents]:
            require(not q.is_symlink() and not (hasattr(q, "is_junction") and q.is_junction()), "symlink/reparse state paths refused")
        require(p.resolve().is_relative_to(self.root.resolve()), "resolved path escapes host state")
        return p

    def check_owner(self):
        marker = self.path("host-owner.json")
        require(marker.exists() and json.loads(marker.read_text()) == self.owner, "host state owner/fingerprint mismatch")

    def init(self):
        self.path("host-owner.json")
        if self.root.exists():
            require(not list(self.root.iterdir()) or (self.root / "host-owner.json").is_file(), "refusing unmarked nonempty host state")
        self.root.mkdir(parents=True, exist_ok=True)
        if not (self.root / "host-owner.json").exists():
            if os.name == "nt":
                raw = self.run(["whoami", "/user", "/fo", "csv", "/nh"]).stdout.decode()
                sid = re.search(r"S-1-5-[0-9-]+", raw)
                require(sid, "cannot establish Windows owner SID")
                self.run(["icacls", self.root, "/inheritance:r", "/grant:r", f"*{sid.group()}:(OI)(CI)F", "*S-1-5-18:(OI)(CI)F"])
            else:
                self.root.chmod(0o755)  # libvirt needs traversal; secrets are separate.
            self._json("host-owner.json", self.owner)
        self.check_owner()
        for d in ("artifacts", "disks", "vms", "intents", "collectors"):
            self.path(d).mkdir(exist_ok=True)
        self.path("collectors").chmod(0o700)
        return self.owner

    def _json(self, name, value):
        p = self.path(name)
        p.parent.mkdir(parents=True, exist_ok=True)
        temporary = p.with_suffix(p.suffix + ".new")
        with temporary.open("w", encoding="utf-8") as f:
            f.write(json.dumps(value, sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())
        temporary.chmod(0o600)
        temporary.replace(p)
        if os.name != "nt":
            descriptor = os.open(p.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def _node(self, name):
        return next(n for n in self.nodes if n["name"] == name)

    def _vbox(self, *args, **kwargs):
        return self.run([self.vbox, *args], **kwargs)

    def _virsh(self, *args, **kwargs):
        return self.run(["virsh", "--connect", "qemu:///system", *args], **kwargs)

    def status(self, name, partial=False):
        node = self._node(name)
        vmid = resource_uuid(self.cfg, name)
        if self.host["provider"] == "libvirt":
            names = self._virsh("list", "--all", "--name").stdout.decode().split()
            if name not in names:
                return {"node": name, "state": "absent"}
            xml = self._virsh("dumpxml", name).stdout.decode()
            self.assert_libvirt(node, xml)
            state = self._virsh("domstate", name).stdout.decode().strip()
        else:
            names = self._vbox("list", "vms").stdout.decode("utf-8", errors="replace")
            if not re.search(r'^"' + re.escape(name) + r'"\s+\{', names, re.M):
                return {"node": name, "state": "absent"}
            v = pairs(self._vbox("showvminfo", name, "--machinereadable").stdout)
            require(v.get("UUID") == vmid, "unowned VirtualBox UUID")
            require(Path(v["CfgFile"]).resolve() == self.path(f"vms/{name}/{name}.vbox").resolve(), "VirtualBox configuration outside owned state")
            mark = self._vbox("getextradata", name, "aat.owner").stdout.decode().strip()
            if mark != "Value: " + fingerprint(self.cfg):
                intent = self.path(f"intents/{name}.json")
                require(partial and v.get("VMState") == "poweroff" and intent.exists() and
                        json.loads(intent.read_text()) == {**self.owner, "node": name, "uuid": vmid}, "unmarked VirtualBox VM refused")
            attached = {str(Path(val).resolve()) for k, val in v.items() if k in ("SATA-0-0", "SATA-1-0") and val != "none"}
            expected = {str(self.path(f"disks/{name}-{k}.vdi").resolve()) for k in ("root", "data")}
            require(attached <= expected if partial else attached == expected, "VirtualBox disks differ from owned paths")
            controllers = {val for k, val in v.items() if re.fullmatch(r"storagecontrollername\d+", k)}
            require(controllers <= {"SATA"} if partial else controllers == {"SATA"}, "unexpected VirtualBox storage controllers")
            media = {k: str(Path(val).resolve()) for k, val in v.items() if re.fullmatch(r"SATA-\d+-\d+", k) and val not in ("none", "emptydrive")}
            expected_media = {"SATA-0-0": str(self.path(f"disks/{name}-root.vdi").resolve()),
                              "SATA-1-0": str(self.path(f"disks/{name}-data.vdi").resolve()),
                              "SATA-2-0": str(self.path("artifacts/" + name + "-seed.iso").resolve())}
            require(all(k in expected_media and expected_media[k] == val for k, val in media.items()) and
                    (partial or media == expected_media), "unexpected VirtualBox attached media")
            if not partial:
                require(v.get("memory") == node["memory_mib"] and v.get("cpus") == node["vcpus"] and
                        str(v.get("macaddress1", "")).lower() == node["mac"].replace(":", "") and
                        v.get("nic1") == "bridged" and v.get("bridgeadapter1") == self.host["bridge"] and
                        all(v.get("nic"+str(i)) in (None, "none") for i in range(2,9)), "VirtualBox hardware/network drift")
            state = {"poweroff": "shut off", "running": "running", "paused": "paused"}.get(v.get("VMState"), v.get("VMState"))
        return {"node": name, "uuid": vmid, "state": state}

    def assert_libvirt(self, node, xml):
        root = ET.fromstring(xml)
        require(owned(xml, self.cfg) and root.findtext("uuid") == resource_uuid(self.cfg, node["name"]), "unowned libvirt VM")
        actual = {d.find("source").get("file") for d in root.findall("./devices/disk") if d.get("device") == "disk"}
        expected = {str(self.path(f"disks/{node['name']}-{k}.qcow2")) for k in ("root", "data")}
        nic = root.find("./devices/interface/source")
        require(actual == expected and nic is not None and nic.get("bridge") == self.host["bridge"] and
                root.findtext("memory") == str(node["memory_mib"]) and root.findtext("vcpu") == str(node["vcpus"]) and
                root.find("./devices/interface/mac").get("address") == node["mac"] and len(root.findall("./devices/interface")) == 1 and
                root.find("./devices/disk[@device='cdrom']/source").get("file") == str(self.path(f"seeds/{node['name']}/seed.iso")), "libvirt disk/network/hardware drift")

    def link(self):
        if self.host["provider"] == "libvirt":
            bridge = Path("/sys/class/net") / self.host["bridge"]
            interface = Path("/sys/class/net") / self.host["ethernet"]
            linked = (bridge / "bridge").is_dir() and (interface / "master").resolve() == bridge.resolve()
            up = linked and (interface / "carrier").read_text().strip() == "1"
            return {"interface": self.host["ethernet"], "bridge": self.host["bridge"], "up": up}
        entries = [v for v in windows_links() if v["interface"] == self.host["bridge"]]
        require(len(entries) <= 1, "ambiguous Windows Ethernet adapter")
        return entries[0] if entries else {"interface": self.host["bridge"], "up": False, "reason": "configured Ethernet adapter not found"}

    def doctor(self):
        expected = "Linux" if self.host["provider"] == "libvirt" else "Windows"
        require(platform.system() == expected, "helper dispatched to wrong OS")
        total, available, machine = machine_values()
        p = self.root
        while not p.exists():
            p = p.parent
        tools = ("virsh", "qemu-img") if expected == "Linux" else (self.vbox,)
        missing = [v for v in tools if not (Path(v).is_file() or shutil.which(v))]
        states = [self.status(n["name"]) for n in self.nodes] if not missing else []
        running = [s["node"] for s in states if s["state"] in ("running", "paused")]
        allocated = 0
        for directory in ("artifacts", "disks", "vms", "seeds"):
            folder = self.path(directory)
            files = list(folder.rglob("*")) if folder.exists() else []
            require(len(files) <= 10000, "owned directory inventory quota")
            for file in files:
                self.path(str(file.relative_to(self.root)).replace("\\", "/"))
                if file.is_file():
                    info = file.stat()
                    allocated += getattr(info, "st_blocks", 0) * 512 if hasattr(info, "st_blocks") else info.st_size
        budget = capacity(self.cfg, self.host, total, available, shutil.disk_usage(p).free / 2**30, running, allocated / 2**30)
        link = self.link()
        if expected == "Linux":
            usable = os.access("/dev/kvm", os.R_OK | os.W_OK)
            version = self.run(["virsh", "--version"]).stdout.decode().strip() if not missing else None
            addresses = json.loads(self.run(["ip", "-j", "address", "show", "dev", self.host["bridge"]]).stdout)
            usable = usable and any(a.get("local") == self.host["address"] for v in addresses for a in v.get("addr_info", []))
            filesystem = self.run(["findmnt", "-n", "-o", "FSTYPE", "--target", p]).stdout.decode().strip()
            usable = usable and filesystem not in ("nfs", "nfs4", "cifs", "tmpfs", "ramfs", "fuse.sshfs")
        else:
            usable = not missing
            version = self._vbox("--version").stdout.decode().strip() if not missing else None
            usable = usable and bool(re.fullmatch(r"7\.0\.\d+r\d+", version or ""))
            if usable:
                hostinfo = self._vbox("list", "hostinfo").stdout.decode("utf-8", errors="replace")
                usable = bool(re.search(r"Processor supports HW virtualization:\s+yes", hostinfo, re.I))
                adapters = self._vbox("list", "bridgedifs").stdout.decode("utf-8", errors="replace")
                usable = usable and any(re.search(r"^Name:\s*" + re.escape(self.host["bridge"]) + r"\s*$", block, re.M) for block in adapters.split("\n\n"))
                usable = usable and ctypes.windll.kernel32.GetDriveTypeW(str(self.root.anchor)) == 3
        return {"host": self.host["id"], "hostname": platform.node(), "os": expected, "machine_sha256": machine,
                "provider_version": version, "missing": missing, "link": link, "capacity": budget, "vms": states,
                "ready": not missing and usable and link["up"] and budget["memory_ok"] and budget["disk_ok"],
                "scope": "bootstrap only; no full application/load fit claim"}

    def upload(self, args):
        self.check_owner()
        name = args["name"]
        allowed = {"cloud.qcow2", "base.vdi"} | {n["name"] + "-seed.iso" for n in self.nodes}
        require(name in allowed and re.fullmatch(r"[0-9a-f]{64}", args["sha256"]), "unapproved artifact")
        if name == "cloud.qcow2":
            require(args["sha256"] == self.cfg["cloud_image"]["sha256"], "cloud artifact pin differs")
        target, temporary = self.path("artifacts/" + name), self.path("artifacts/" + name + ".partial")
        if target.exists():
            require(digest(target) == args["sha256"], "existing artifact hash differs")
            return {"complete": True, "bytes": target.stat().st_size}
        data = base64.b64decode(args["data"], validate=True)
        require(len(data) <= 65536 and type(args["offset"]) is int and args["offset"] >= 0, "transfer chunk quota")
        current = temporary.stat().st_size if temporary.exists() else 0
        # Reconcile a lost chunk reply without appending it twice.
        if args["offset"] < current:
            with temporary.open("rb") as f:
                f.seek(args["offset"])
                require(f.read(len(data)) == data, "conflicting transfer retry")
            require(args["offset"] + len(data) <= current, "overlapping transfer chunk")
        else:
            require(args["offset"] == current, "transfer offset gap")
            require(current + len(data) <= max(n["root_gib"] for n in self.nodes) * 2**30, "artifact total quota")
            with temporary.open("ab") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
        complete = bool(args.get("final"))
        if complete:
            require(digest(temporary) == args["sha256"], "transferred artifact hash mismatch")
            temporary.replace(target)
            self._json("artifacts/" + name + ".receipt.json", {"sha256": args["sha256"], "bytes": target.stat().st_size})
        return {"complete": complete, "bytes": target.stat().st_size if complete else temporary.stat().st_size}

    def artifact(self, name):
        p = self.path("artifacts/" + name)
        receipt = json.loads(self.path("artifacts/" + name + ".receipt.json").read_text())
        require(digest(p) == receipt["sha256"] and p.stat().st_size == receipt["bytes"], "artifact corrupted after transfer")
        return p

    def seal_upload(self, args):
        """Verify an SFTP-transferred immutable artifact without a per-chunk SSH process."""
        self.check_owner()
        name = args["name"]
        require(name in ({"cloud.qcow2", "base.vdi"} | {n["name"] + "-seed.iso" for n in self.nodes}), "unapproved artifact")
        require(re.fullmatch(r"[0-9a-f]{64}", args["sha256"]), "invalid artifact digest")
        if name == "cloud.qcow2":
            require(args["sha256"] == self.cfg["cloud_image"]["sha256"], "base pin mismatch")
        target, partial = self.path("artifacts/" + name), self.path("artifacts/" + name + ".partial")
        selected = target if target.exists() else partial
        require(selected.stat().st_size <= max(n["root_gib"] for n in self.nodes) * 2**30, "artifact total quota")
        require(digest(selected) == args["sha256"], "SFTP artifact digest mismatch")
        if selected == partial:
            partial.replace(target)
        self._json("artifacts/" + name + ".receipt.json", {"sha256": args["sha256"], "bytes": target.stat().st_size})
        return {"complete": True, "bytes": target.stat().st_size}

    def upload_status(self, args):
        """Reconcile a completed transfer before sending its bytes again."""
        self.check_owner()
        name = args["name"]
        require(name in ({"cloud.qcow2", "base.vdi"} | {n["name"] + "-seed.iso" for n in self.nodes}), "unapproved artifact")
        require(re.fullmatch(r"[0-9a-f]{64}", args["sha256"]), "invalid artifact digest")
        if name == "cloud.qcow2":
            require(args["sha256"] == self.cfg["cloud_image"]["sha256"], "base pin mismatch")
        self.path("artifacts/" + name + ".partial")  # Check before SFTP can follow a stale link.
        if not self.path("artifacts/" + name).exists():
            return {"complete": False, "bytes": 0}
        # Also restores a missing receipt after a lost rename/receipt reply.
        return self.seal_upload(args)

    def create(self, name):
        self.check_owner()
        node = self._node(name)
        intent = self.path(f"intents/{name}.json")
        initial = self.status(name, partial=intent.exists())
        if initial["state"] != "absent" and self.path(f"intents/{name}.complete.json").exists():
            require(json.loads(self.path(f"intents/{name}.complete.json").read_text()) == {**self.owner, "node": name, "uuid": resource_uuid(self.cfg, name)}, "completed intent differs")
            return self.status(name)
        require(initial["state"] in ("absent", "shut off"), "partial provisioning requires a stopped VM")
        wanted = {**self.owner, "node": name, "uuid": resource_uuid(self.cfg, name)}
        if intent.exists():
            require(json.loads(intent.read_text()) == wanted, "node intent mismatch")
        else:
            require(initial["state"] == "absent", "refusing to adopt existing VM")
            require(not any(self.path(f"disks/{name}-{k}.{ext}").exists() for k in ("root", "data") for ext in ("vdi", "qcow2")), "unmarked existing node disks")
            self._json(f"intents/{name}.json", wanted)
        seed = self.artifact(name + "-seed.iso")
        if self.host["provider"] == "libvirt":
            base = self.artifact("cloud.qcow2")
            require(digest(base) == self.cfg["cloud_image"]["sha256"], "base image pin mismatch")
            for kind in ("root", "data"):
                p = self.path(f"disks/{name}-{kind}.qcow2")
                if not p.exists():
                    args = ["qemu-img", "create", "-f", "qcow2"]
                    if kind == "root":
                        args += ["-F", "qcow2", "-b", base]
                    self.run([*args, p, f"{node[kind + '_gib']}G"])
                info = json.loads(self.run(["qemu-img", "info", "--output=json", p]).stdout)
                require(info["format"] == "qcow2" and info["virtual-size"] == node[kind + "_gib"] * 2**30, "disk format/size differs")
                require((info.get("full-backing-filename") == str(base)) if kind == "root" else not info.get("backing-filename"), "disk backing differs")
            # Rendered XML expects the same owned seed path on both providers.
            dest = self.path(f"seeds/{name}/seed.iso")
            dest.parent.mkdir(parents=True, exist_ok=True)
            if dest.exists():
                require(digest(dest) == digest(seed), "seed differs")
            else:
                shutil.copyfile(seed, dest)
            xml = domain_xml(self.cfg, node)
            self.assert_libvirt(node, xml)
            if initial["state"] == "absent":
                definition = self.path(f"vms/{name}.xml")
                definition.write_text(xml)
                self._virsh("define", definition, "--validate")
        else:
            base = self.artifact("base.vdi")
            for kind in ("root", "data"):
                p = self.path(f"disks/{name}-{kind}.vdi")
                if not p.exists():
                    if kind == "root":
                        self._vbox("clonemedium", "disk", base, p, "--format", "VDI", timeout=240)
                    else:
                        self._vbox("createmedium", "disk", "--filename", p, "--size", str(node["data_gib"] * 1024), "--format", "VDI", timeout=240)
                self._vbox("modifymedium", "disk", p, "--resize", str(node[kind + "_gib"] * 1024))
            if initial["state"] == "absent":
                self._vbox("createvm", "--name", name, "--uuid", wanted["uuid"], "--ostype", "Ubuntu_64", "--basefolder", self.path("vms"), "--register")
            self._vbox("setextradata", name, "aat.owner", fingerprint(self.cfg))
            self._vbox("modifyvm", name, "--memory", str(node["memory_mib"]), "--cpus", str(node["vcpus"]),
                       "--nic1", "bridged", "--bridgeadapter1", self.host["bridge"], "--macaddress1", node["mac"].replace(":", ""), "--nictype1", "virtio", "--cableconnected1", "on",
                       *[x for i in range(2,9) for x in ("--nic"+str(i),"none")])
            v = pairs(self._vbox("showvminfo", name, "--machinereadable").stdout)
            if "storagecontrollername0" not in v:
                self._vbox("storagectl", name, "--name", "SATA", "--add", "sata", "--controller", "IntelAhci", "--portcount", "3")
            else:
                require(v["storagecontrollername0"] == "SATA", "unexpected storage controller")
            for port, medium in enumerate([self.path(f"disks/{name}-root.vdi"), self.path(f"disks/{name}-data.vdi"), seed]):
                self._vbox("storageattach", name, "--storagectl", "SATA", "--port", str(port), "--device", "0", "--type", "dvddrive" if port == 2 else "hdd", "--medium", medium)
        result = self.status(name)
        self._json(f"intents/{name}.complete.json", wanted)
        return result

    def action(self, name, action):
        self.check_owner()
        state = self.status(name)["state"]
        allowed = {"start": ("shut off", "running"), "stop": ("running", "shut off"), "pause": ("running", "paused"), "resume": ("paused", "running")}
        require(state in allowed[action], "VM state is incompatible with action")
        done = {"start": "running", "stop": "shut off", "pause": "paused", "resume": "running"}[action]
        if state != done:
            if self.host["provider"] == "libvirt":
                self._virsh({"start": "start", "stop": "shutdown", "pause": "suspend", "resume": "resume"}[action], name)
            elif action == "start":
                self._vbox("startvm", name, "--type", "headless")
            else:
                self._vbox("controlvm", name, {"stop": "acpipowerbutton", "pause": "pause", "resume": "resume"}[action])
        return self.status(name)

    def replace_empty(self,name,run_id):
        """Retain old disks and recreate only this fenced owned VM's storage.

        Application/DCS quorum and fencing are checked by the controller first.
        This helper independently refuses a running VM, foreign paths, a second
        concurrent replacement or a changed run identity. It never starts a VM.
        """
        self.check_owner();self._node(name)
        require(isinstance(run_id,str)and re.fullmatch(r"[0-9a-f]{32}",run_id),"replacement run identity required")
        lock=self.path("intents/"+name+".replacement.lock")
        try:lock.mkdir()
        except FileExistsError as exc:raise ValueError("another host replacement operation is active; inspect retained lock before retry")from exc
        try:
            journal_path="intents/"+name+".replacement.json"
            expected={**self.owner,"node":name,"uuid":resource_uuid(self.cfg,name),"run_id":run_id}
            path=self.path(journal_path)
            if path.exists():
                record=json.loads(path.read_text())
                if record["identity"]!=expected:
                    require(record["phase"]=="complete" and self.status(name)["state"]=="shut off","resume the original incomplete replacement run")
                    prior=self.path(record["quarantine"])/"replacement.complete.json"
                    require(not prior.exists(),"previous replacement journal already archived")
                    path.rename(prior)
                    record={"identity":expected,"phase":"prepared","retained":[],"quarantine":"quarantine/"+run_id+"/"+name}
                    self._json(journal_path,record)
                if record["phase"]=="complete":return {"state":self.status(name),"quarantine":record["quarantine"],"run_id":run_id}
            else:
                require(self.status(name)["state"]=="shut off","replacement VM must be fenced shut off")
                record={"identity":expected,"phase":"prepared","retained":[],"quarantine":"quarantine/"+run_id+"/"+name}
                self._json(journal_path,record)
            require(self.status(name,partial=True)["state"]=="shut off","fence lost during replacement")
            quarantine=self.path(record["quarantine"]);quarantine.mkdir(parents=True,exist_ok=True)
            completed=self.path("intents/"+name+".complete.json")
            if completed.exists():
                require(json.loads(completed.read_text())=={k:v for k,v in expected.items()if k!="run_id"},"completed provisioning ownership differs")
                completed.rename(quarantine/"provisioning.complete.json")
            ext="qcow2"if self.host["provider"]=="libvirt"else"vdi"
            for port,kind in enumerate(("root","data")):
                if kind in record["retained"]:continue
                disk=self.path(f"disks/{name}-{kind}.{ext}");destination=quarantine/disk.name
                if self.host["provider"]=="virtualbox"and not destination.exists():
                    self._vbox("storageattach",name,"--storagectl","SATA","--port",str(port),"--device","0","--type","hdd","--medium","none")
                    # A lost disk may remain registered as inaccessible. Closing
                    # its registry entry never uses --delete and is retryable.
                    medium=self._vbox("showmediuminfo","disk",disk,check=False)
                    if medium.returncode==0:self._vbox("closemedium","disk",disk)
                if destination.exists():
                    require(not disk.exists(),"ambiguous replacement disk state")
                elif disk.exists():
                    require(disk.is_file(),"original disk is not a regular owned file")
                    disk.rename(destination)
                record["retained"].append(kind);record["phase"]="disks-retained";self._json(journal_path,record)
            self.create(name)
            require(self.status(name)["state"]=="shut off","fresh replacement must remain stopped")
            record["phase"]="complete";self._json(journal_path,record)
            return {"state":self.status(name),"quarantine":str(quarantine),"run_id":run_id}
        finally:lock.rmdir()

    def dispatch(self, op, args):
        require(op in OPERATIONS, "operation not allowlisted")
        require(platform.system() == ("Linux" if self.host["provider"] == "libvirt" else "Windows"), "helper dispatched to wrong OS")
        if op == "doctor":
            return self.doctor()
        if op == "status":
            return [self.status(n["name"]) for n in self.nodes]
        if op == "init":
            return self.init()
        if op == "upload":
            return self.upload(args)
        if op == "upload-status":
            return self.upload_status(args)
        if op == "seal-upload":
            return self.seal_upload(args)
        if op == "create":
            return self.create(args["node"])
        if op == "replace-empty":
            require(set(args)=={"node","run_id"},"replacement accepts only owned node/run identities")
            return self.replace_empty(args["node"],args["run_id"])
        if op.startswith("collector-"):
            from .network_evidence import collector_operation
            self.check_owner()
            return collector_operation(self, op, args)
        return self.action(args["node"], op)


def serve(data):
    require(len(data) <= MAX_REQUEST, "request exceeds 128 KiB")
    request = json.loads(data)
    require(set(request) == {"version", "inventory", "host", "operation", "args"} and request["version"] == 1, "invalid host protocol")
    result = HostAgent(request["inventory"], request["host"]).dispatch(request["operation"], request["args"])
    response = json.dumps({"version": 1, "result": result}).encode()
    require(len(response) <= MAX_RESPONSE, "host response exceeds quota")
    return response
