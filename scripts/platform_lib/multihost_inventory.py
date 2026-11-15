"""Version-two inventory: existing Ethernet LAN, two explicit provider hosts."""
import copy
import ipaddress
from pathlib import PurePosixPath, PureWindowsPath
import re
from urllib.parse import urlsplit

from .inventory import require


def validate(cfg):
    from .inventory import validate as validate_local
    require(cfg.get("version") == 2, "multi-host inventory version must be 2")
    hosts = cfg.get("hosts", [])
    require(not any(c in cfg["state_dir"] for c in '\r\n\x00"'), "invalid controller path")
    require(len(hosts) == 2 and {h.get("provider") for h in hosts} == {"libvirt", "virtualbox"},
            "one Linux/libvirt and one Windows/VirtualBox host required")
    require(len({h["id"] for h in hosts}) == 2 and len({h["address"] for h in hosts}) == 2, "duplicate host identity/address")
    net = cfg["network"]
    require(net.get("mode") == "bridged", "v2 supports existing bridged Ethernet LAN only")
    lan = ipaddress.IPv4Network(net["cidr"])
    require(net.get("dns") and all(ipaddress.ip_address(a).version == 4 for a in net["dns"]), "explicit IPv4 DNS required")
    for h in hosts:
        require(re.fullmatch(r"[a-z][a-z0-9-]{0,30}", h["id"]), "invalid host id")
        address = ipaddress.IPv4Address(h["address"])
        require(address in lan and address not in (lan.network_address, lan.broadcast_address, ipaddress.ip_address(net["gateway"])), "invalid host LAN address")
        require(isinstance(h["bridge"], str) and 1 <= len(h["bridge"]) <= 200 and not any(c in h["bridge"] for c in "\r\n\x00"), "explicit Ethernet bridge/adapter required")
        state = PureWindowsPath(h["state_dir"]) if h["provider"] == "virtualbox" else PurePosixPath(h["state_dir"])
        require(not any(c in h["state_dir"] for c in '\r\n\x00"'), "invalid host path")
        if h["provider"] == "virtualbox":
            require(re.fullmatch(r"[A-Za-z]:", state.drive), "Windows VM storage must use a local drive, not UNC")
        require(state.is_absolute() and ".." not in state.parts and cfg["cluster_id"] in state.parts and len(state.parts) >= 3,
                "host state must be an absolute cluster-specific directory")
        require(h.get("reserve_mib", 0) >= 4096 and h.get("startup_headroom_mib", 0) >= 1024, "preserve host reserve/startup headroom")
        if h["provider"] == "libvirt":
            require(h["id"] == cfg["controller_host"] and h.get("transport") == "local", "Linux controller must own the local libvirt provider")
            require(re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,14}", h["bridge"]) and re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,14}", h["ethernet"]), "invalid Linux interface")
        else:
            require(h.get("transport") == "ssh", "Windows requires SSH helper transport")
            require(re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,40}", h["ssh_user"]), "invalid management SSH user")
            for k in ("ssh_key_file", "known_hosts"):
                p = PurePosixPath(h[k])
                require(p.is_absolute() and ".." not in p.parts and not any(c in h[k] for c in '\r\n\x00"'), "controller SSH paths must be absolute")
    controller = next(h for h in hosts if h["id"] == cfg["controller_host"])
    api = urlsplit(cfg["api_endpoint"])
    require(api.hostname == controller["address"] and api.port == 7443, "API must bind the Linux controller LAN address on 7443")
    linux = next(h for h in hosts if h["provider"] == "libvirt")
    windows = next(h for h in hosts if h["provider"] == "virtualbox")
    require([n["physical_host"] for n in cfg["nodes"]] == [linux["id"], linux["id"], windows["id"], windows["id"]], "place A/B on Linux and C/D on Windows")
    require(not ({h["address"] for h in hosts} & {n["address"] for n in cfg["nodes"]}), "host/guest addresses overlap")
    # Reuse v1 common/pin validation without weakening its NAT-only rules.
    common = copy.deepcopy(cfg)
    common.update(version=1, libvirt_uri="qemu:///system", api_endpoint=f"https://{net['gateway']}:7443")
    common["network"].update(name=cfg["cluster_id"], bridge="aatpreview0")
    validate_local(common)
    return cfg


def host_nodes(cfg, host):
    return [n for n in cfg["nodes"] if n["physical_host"] == host["id"]]


def capacity(cfg, host, total_mib, available_mib, free_gib, running=(), allocated_gib=0):
    nodes = host_nodes(cfg, host)
    allocation = sum(n["memory_mib"] for n in nodes)
    additional = sum(n["memory_mib"] for n in nodes if n["name"] not in running)
    # Includes maxima, immutable base/conversion workspace and 16 GiB host disk reserve.
    disk = max(0, sum(n["root_gib"] + n["data_gib"] for n in nodes) + max(n["root_gib"] for n in nodes) * 2 - allocated_gib) + 16
    return {"host_total_mib": total_mib, "host_available_mib": available_mib, "guest_allocation_mib": allocation,
            "additional_guest_mib": additional, "reserve_mib": host["reserve_mib"], "free_disk_gib": free_gib,
            "projected_disk_gib": disk, "memory_ok": allocation + host["reserve_mib"] <= total_mib and
            additional + host["startup_headroom_mib"] <= available_mib, "disk_ok": free_gib >= disk}
