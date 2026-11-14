"""Validate topology and render artifacts without changing the host."""
import hashlib
import copy
import ipaddress
import json
from pathlib import Path, PurePosixPath
import re
from urllib.parse import urlparse
import uuid
import xml.etree.ElementTree as ET

OWNER_NS = "urn:aat:platform:1"
ET.register_namespace("aat", OWNER_NS)
NAMESPACE = "sistem-informasi-kebencanaan-bnpb"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def https_url(value):
    url = urlparse(value)
    return url.scheme == "https" and bool(url.hostname) and not url.username and not url.password and not url.fragment


def validate(cfg):
    require(cfg.get("version") == 1, "inventory version must be 1")
    cluster = cfg.get("cluster_id", "")
    require(re.fullmatch(r"aat-[a-z0-9](?:[a-z0-9-]{0,22}[a-z0-9])?", cluster), "cluster_id must be a short aat- DNS label")
    require(cfg.get("namespace") == NAMESPACE, "assignment namespace must be preserved")
    require(cfg.get("libvirt_uri") == "qemu:///system", "VM provider requires qemu:///system")
    # This is a Linux/libvirt host path even when validating a plan on Windows.
    state = PurePosixPath(cfg.get("state_dir", ""))
    require(state.is_absolute() and ".." not in state.parts and state.name == cluster and len(state.parts) >= 4,
            "state_dir must be an absolute cluster-specific directory")
    require(re.fullmatch(r"[a-z][a-z0-9_-]{0,30}", cfg.get("ssh_user", "")), "invalid ssh_user")
    for field, minimum in (("host_reserve_mib", 2048), ("startup_headroom_mib", 512)):
        value = cfg.get(field, 4096 if field == "host_reserve_mib" else 1024)
        require(type(value) is int and value >= minimum, f"invalid {field}")
    network = cfg["network"]
    require(re.fullmatch(r"aat-[a-z0-9](?:[a-z0-9-]{0,22}[a-z0-9])?", network["name"]), "network needs an aat- DNS label")
    require(re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_-]{0,14}", network["bridge"]), "invalid bridge name")
    subnet = ipaddress.IPv4Network(network["cidr"])
    gateway = ipaddress.IPv4Address(network["gateway"])
    require(subnet.is_private and subnet.prefixlen >= 16, "network must be a bounded private IPv4 subnet")
    require(gateway in subnet and gateway not in (subnet.network_address, subnet.broadcast_address), "invalid gateway")
    pods, services = ipaddress.IPv4Network(cfg["pod_cidr"]), ipaddress.IPv4Network(cfg["service_cidr"])
    require(pods.is_private and services.is_private and not pods.overlaps(services)
            and not pods.overlaps(subnet) and not services.overlaps(subnet), "node/pod/service networks must be distinct private networks")
    api = urlparse(cfg["api_endpoint"])
    require(https_url(cfg["api_endpoint"]) and api.path in ("", "/") and not api.query
            and api.hostname == str(gateway) and api.port and 1024 < api.port < 65536,
            "libvirt API endpoint must be https://<network gateway>:<unprivileged port>")
    nodes = cfg["nodes"]
    require(len(nodes) == 4 and sum(n["role"] == "server" for n in nodes) == 3
            and sum(n["role"] == "agent" for n in nodes) == 1, "need three servers and one agent")
    require(nodes[0]["role"] == "server", "first node must bootstrap a server")
    for key in ("name", "address", "mac"):
        require(len({n[key] for n in nodes}) == 4, f"duplicate node {key}")
    for node in nodes:
        require(re.fullmatch(re.escape(cluster) + r"-[a-z0-9](?:[a-z0-9-]{0,18}[a-z0-9])?", node["name"]), "node name must be a cluster-owned DNS label")
        address = ipaddress.IPv4Address(node["address"])
        require(address in subnet and address not in (gateway, subnet.network_address, subnet.broadcast_address), "invalid node address")
        require(re.fullmatch(r"52:54:00:(?:[0-9a-f]{2}:){2}[0-9a-f]{2}", node["mac"]), "MAC must be a lowercase QEMU MAC")
        for field, lower in (("vcpus", 1), ("memory_mib", 2048 if node["role"] == "server" else 1024), ("root_gib", 8), ("data_gib", 4)):
            require(type(node[field]) is int and node[field] >= lower, f"invalid {field} for {node['name']}")
        require(isinstance(node["physical_host"], str) and re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?", node["physical_host"]),
                "physical_host must be a valid Kubernetes label value")
    for url_key, hash_key, section in (("url", "sha256", "cloud_image"), ("binary_url", "binary_sha256", "k3s"),
                                       ("installer_url", "installer_sha256", "k3s"), ("manifest_url", "manifest_sha256", "tester")):
        require(https_url(cfg[section][url_key]), "artifact URLs must use HTTPS without credentials")
        require(re.fullmatch(r"[0-9a-f]{64}", cfg[section][hash_key]), "artifact needs a SHA-256 pin")
    require(re.fullmatch(r"v\d+\.\d+\.\d+\+k3s\d+", cfg["k3s"]["version"]), "k3s version must be pinned")
    for field in (cfg["api_proxy_image"], cfg["tester"]["image"]):
        require(re.fullmatch(r"[a-z0-9./_-]+@sha256:[0-9a-f]{64}", field), "container images must use immutable digests")
    return cfg


def load(path):
    return validate(json.loads(Path(path).read_text()))


def fingerprint(cfg):
    return hashlib.sha256(json.dumps(cfg, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def validate_resize(previous, current):
    def immutable(cfg):
        result = copy.deepcopy(cfg)
        result.pop("host_reserve_mib", None)
        result.pop("startup_headroom_mib", None)
        for node in result["nodes"]:
            node.pop("memory_mib", None)
            node.pop("vcpus", None)
        return result
    require(immutable(previous) == immutable(current), "resize may change only CPU/RAM and host headroom; disks, identities and topology must stay unchanged")


def memory_budget(cfg, total_mib, available_mib, running_rss_mib=0):
    allocation = sum(n["memory_mib"] for n in cfg["nodes"])
    reserve = cfg.get("host_reserve_mib", 4096)
    headroom = cfg.get("startup_headroom_mib", 1024)
    additional = max(0, allocation - running_rss_mib)
    return {"host_memory_mib": total_mib, "host_available_mib": available_mib,
            "host_reserve_mib": reserve, "startup_headroom_mib": headroom,
            "vm_memory_mib": allocation, "additional_vm_memory_mib": additional,
            "memory_budget_ok": allocation + reserve <= total_mib and additional + headroom <= available_mib}


def resource_uuid(cfg, name):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"urn:aat:platform:{cfg['cluster_id']}:{name}"))


def metadata(element, cfg):
    ET.SubElement(ET.SubElement(element, "metadata"), f"{{{OWNER_NS}}}owner", {"cluster": cfg["cluster_id"]})


def owned(xml, cfg):
    try:
        marker = ET.fromstring(xml).find(f"./metadata/{{{OWNER_NS}}}owner")
        return marker is not None and marker.get("cluster") == cfg["cluster_id"]
    except ET.ParseError:
        return False


def xml_string(element):
    ET.indent(element)
    return ET.tostring(element, encoding="unicode") + "\n"


def network_xml(cfg):
    net = cfg["network"]
    root = ET.Element("network")
    ET.SubElement(root, "name").text = net["name"]
    ET.SubElement(root, "uuid").text = resource_uuid(cfg, "network")
    metadata(root, cfg)
    ET.SubElement(root, "forward", {"mode": "nat"})
    ET.SubElement(root, "bridge", {"name": net["bridge"], "stp": "on", "delay": "0"})
    ET.SubElement(root, "domain", {"name": cfg["cluster_id"] + ".internal", "localOnly": "yes"})
    subnet = ipaddress.IPv4Network(net["cidr"])
    ip = ET.SubElement(root, "ip", {"address": net["gateway"], "netmask": str(subnet.netmask)})
    dhcp = ET.SubElement(ip, "dhcp")
    for node in cfg["nodes"]:
        ET.SubElement(dhcp, "host", {"mac": node["mac"], "name": node["name"], "ip": node["address"]})
    return xml_string(root)


def domain_xml(cfg, node):
    state = Path(cfg["state_dir"])
    root = ET.Element("domain", {"type": "kvm"})
    ET.SubElement(root, "name").text = node["name"]
    ET.SubElement(root, "uuid").text = resource_uuid(cfg, node["name"])
    metadata(root, cfg)
    ET.SubElement(root, "memory", {"unit": "MiB"}).text = str(node["memory_mib"])
    ET.SubElement(root, "vcpu").text = str(node["vcpus"])
    os = ET.SubElement(root, "os")
    ET.SubElement(os, "type", {"arch": "x86_64", "machine": "q35"}).text = "hvm"
    ET.SubElement(os, "boot", {"dev": "hd"})
    features = ET.SubElement(root, "features")
    ET.SubElement(features, "acpi")
    ET.SubElement(features, "apic")
    ET.SubElement(root, "cpu", {"mode": "host-passthrough", "check": "none"})
    ET.SubElement(root, "on_poweroff").text = "destroy"
    ET.SubElement(root, "on_reboot").text = "restart"
    ET.SubElement(root, "on_crash").text = "destroy"
    devices = ET.SubElement(root, "devices")
    for name, target in (("root", "vda"), ("data", "vdb")):
        disk = ET.SubElement(devices, "disk", {"type": "file", "device": "disk"})
        ET.SubElement(disk, "driver", {"name": "qemu", "type": "qcow2", "discard": "unmap"})
        ET.SubElement(disk, "source", {"file": str(state / "disks" / f"{node['name']}-{name}.qcow2")})
        ET.SubElement(disk, "target", {"dev": target, "bus": "virtio"})
    seed = ET.SubElement(devices, "disk", {"type": "file", "device": "cdrom"})
    ET.SubElement(seed, "driver", {"name": "qemu", "type": "raw"})
    ET.SubElement(seed, "source", {"file": str(state / "seeds" / node["name"] / "seed.iso")})
    ET.SubElement(seed, "target", {"dev": "sda", "bus": "sata"})
    ET.SubElement(seed, "readonly")
    nic = ET.SubElement(devices, "interface", {"type": "network"})
    ET.SubElement(nic, "mac", {"address": node["mac"]})
    ET.SubElement(nic, "source", {"network": cfg["network"]["name"]})
    ET.SubElement(nic, "model", {"type": "virtio"})
    serial = ET.SubElement(devices, "serial", {"type": "pty"})
    ET.SubElement(serial, "target", {"port": "0"})
    console = ET.SubElement(devices, "console", {"type": "pty"})
    ET.SubElement(console, "target", {"type": "serial", "port": "0"})
    ET.SubElement(devices, "memballoon", {"model": "virtio"})
    return xml_string(root)


def cloud_config(cfg, node, public_key):
    require(public_key.startswith("ssh-ed25519 ") and "\n" not in public_key.strip(), "an ed25519 public key is required")
    return {
        "hostname": node["name"], "manage_etc_hosts": True,
        "ssh_pwauth": False, "disable_root": True,
        "users": [{"name": cfg["ssh_user"], "shell": "/bin/bash", "lock_passwd": True,
                   "sudo": "ALL=(ALL) NOPASSWD:ALL", "ssh_authorized_keys": [public_key.strip()]}],
        "disk_setup": {"/dev/vdb": {"table_type": "gpt", "layout": True, "overwrite": False}},
        "fs_setup": [{"label": "aat-data", "filesystem": "ext4", "device": "/dev/vdb1", "overwrite": False}],
        "mounts": [["LABEL=aat-data", "/var/lib/aat", "ext4", "defaults,nofail", "0", "2"]],
        "write_files": [
            {"path": "/etc/modules-load.d/aat-k3s.conf", "content": "overlay\nbr_netfilter\n"},
            {"path": "/etc/sysctl.d/90-aat-k3s.conf", "content": "net.ipv4.ip_forward=1\nnet.bridge.bridge-nf-call-iptables=1\n"}],
        "runcmd": [["modprobe", "overlay"], ["modprobe", "br_netfilter"], ["sysctl", "--system"],
                   ["sh", "-c", "findmnt /var/lib/aat && mkdir -p /var/lib/aat/local-pv"]],
    }


def node_network(node):
    return {"version": 2, "ethernets": {"aatnic": {"match": {"macaddress": node["mac"]},
            "set-name": "eth0", "dhcp4": True, "dhcp6": False}}}


def k3s_config(cfg, node, token, first=False):
    result = {"node-name": node["name"], "node-ip": node["address"], "token": token,
              "flannel-iface": "eth0", "node-label": ["platform.aat/cluster=" + cfg["cluster_id"],
              "platform.aat/physical-host=" + node["physical_host"]]}
    if node["role"] == "server":
        result.update({"tls-san": [urlparse(cfg["api_endpoint"]).hostname], "secrets-encryption": True,
                       "cluster-cidr": cfg["pod_cidr"], "service-cidr": cfg["service_cidr"],
                       "disable": ["traefik", "servicelb"], "write-kubeconfig-mode": "0600"})
        if first:
            result["cluster-init"] = True
        else:
            result["server"] = f"https://{cfg['nodes'][0]['address']}:6443"
    else:
        result["server"] = f"https://{cfg['nodes'][0]['address']}:6443"
    return result


def haproxy_config(cfg):
    endpoint = urlparse(cfg["api_endpoint"])
    text = f"""global
  log stdout format raw local0
  maxconn 2048
defaults
  mode tcp
  log global
  timeout connect 3s
  timeout client 5m
  timeout server 5m
frontend kubernetes_api
  bind {endpoint.hostname}:{endpoint.port}
  default_backend control_planes
backend control_planes
  balance roundrobin
  option httpchk GET /readyz
  http-check expect status 200
  default-server inter 2s fall 2 rise 1
"""
    for node in cfg["nodes"]:
        if node["role"] == "server":
            text += f"  server {node['name']} {node['address']}:6443 check check-ssl verify required ca-file /usr/local/etc/haproxy/server-ca.crt crt /usr/local/etc/haproxy/health.pem\n"
    return text


def health_rbac(cfg):
    name = cfg["cluster_id"] + "-api-health"
    return {"apiVersion": "v1", "kind": "List", "items": [
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
         "metadata": {"name": name}, "rules": [{"nonResourceURLs": ["/readyz"], "verbs": ["get"]}]},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "metadata": {"name": name},
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": name},
         "subjects": [{"kind": "User", "name": name, "apiGroup": "rbac.authorization.k8s.io"}]}]}


def render(cfg, output, public_key):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    (output / "network.xml").write_text(network_xml(cfg))
    (output / "haproxy.cfg").write_text(haproxy_config(cfg))
    namespace = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": cfg["namespace"],
                 "labels": {"app.kubernetes.io/part-of": cfg["cluster_id"]}}}
    (output / "namespace.json").write_text(json.dumps(namespace, indent=2) + "\n")
    for node in cfg["nodes"]:
        directory = output / node["name"]
        directory.mkdir(exist_ok=True)
        (directory / "domain.xml").write_text(domain_xml(cfg, node))
        (directory / "user-data").write_text("#cloud-config\n" + json.dumps(cloud_config(cfg, node, public_key), indent=2) + "\n")
        (directory / "meta-data").write_text(json.dumps({"instance-id": resource_uuid(cfg, node["name"]), "local-hostname": node["name"]}) + "\n")
        (directory / "network-config").write_text(json.dumps(node_network(node), indent=2) + "\n")
        # Review examples contain no real cluster token.
        (directory / "k3s.example.json").write_text(json.dumps(k3s_config(cfg, node, "<generated-private-token>", first=node == cfg["nodes"][0]), indent=2) + "\n")
    (output / "inventory.sha256").write_text(fingerprint(cfg) + "\n")
