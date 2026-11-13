import copy
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

from platform_lib.cluster import Cluster
from platform_lib.inventory import cloud_config, domain_xml, fingerprint, haproxy_config, health_rbac, k3s_config, load, memory_budget, network_xml, owned, render, validate, validate_resize

ROOT = Path(__file__).resolve().parents[2]
PUBLIC_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFPZ7djI4C6v2ohMU2HRvIIK+VLcI+uV6Kw2+EAbIhfF test-preview"


class InventoryTest(unittest.TestCase):
    def setUp(self):
        self.cfg = load(ROOT / "deployment/platform/inventory.example.json")

    def test_four_separate_vms_and_owned_local_disks(self):
        disks = set()
        ids = set()
        for node in self.cfg["nodes"]:
            xml = domain_xml(self.cfg, node)
            root = ET.fromstring(xml)
            self.assertEqual("kvm", root.get("type"))
            self.assertTrue(owned(xml, self.cfg))
            ids.add(root.findtext("uuid"))
            images = [d.find("source").get("file") for d in root.findall("./devices/disk") if d.get("device") == "disk"]
            self.assertEqual(2, len(images))
            self.assertFalse(disks.intersection(images), "nodes must not share writable root/data images")
            disks.update(images)
            config = cloud_config(self.cfg, node, PUBLIC_KEY)
            self.assertFalse(config["disk_setup"]["/dev/vdb"]["overwrite"])
            self.assertFalse(config["fs_setup"][0]["overwrite"])
        self.assertEqual(4, len(ids))

    def test_network_reservations_match_guests(self):
        root = ET.fromstring(network_xml(self.cfg))
        self.assertTrue(owned(network_xml(self.cfg), self.cfg))
        reservations = {(n.get("mac"), n.get("ip")) for n in root.findall("./ip/dhcp/host")}
        self.assertEqual({(n["mac"], n["address"]) for n in self.cfg["nodes"]}, reservations)

    def test_unrelated_resources_are_rejected(self):
        cluster = Cluster(self.cfg)
        with self.assertRaisesRegex(ValueError, "unowned"):
            cluster.assert_resource("<domain><name>aat-m2-a</name></domain>", "aat-m2-a")
        other = copy.deepcopy(self.cfg)
        other["cluster_id"] = "aat-other"
        with self.assertRaisesRegex(ValueError, "unowned"):
            cluster.assert_resource(domain_xml(other, other["nodes"][0]), "aat-m2-a")
        other = copy.deepcopy(self.cfg)
        other["state_dir"] = "/var/tmp/other-state/aat-m2"
        with self.assertRaisesRegex(ValueError, "different disk paths"):
            cluster.assert_resource(domain_xml(other, other["nodes"][0]), "aat-m2-a")

    def test_bootstrap_flags_and_stable_join_endpoint(self):
        configs = [k3s_config(self.cfg, n, "private-token", first=i == 0) for i, n in enumerate(self.cfg["nodes"])]
        self.assertTrue(configs[0]["cluster-init"])
        self.assertNotIn("server", configs[0])
        for config in configs[1:]:
            self.assertEqual("https://" + self.cfg["nodes"][0]["address"] + ":6443", config["server"])
            self.assertNotIn("cluster-init", config)
        for config in configs[:3]:
            self.assertTrue(config["secrets-encryption"])
            self.assertIn(self.cfg["network"]["gateway"], config["tls-san"])
        self.assertNotIn("secrets-encryption", configs[-1])
        proxy = haproxy_config(self.cfg)
        self.assertIn("GET /readyz", proxy)
        for node in self.cfg["nodes"][:3]:
            self.assertIn(node["address"] + ":6443", proxy)
        self.assertNotIn(self.cfg["nodes"][-1]["address"] + ":6443", proxy)

    def test_render_is_deterministic_without_credentials(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "render"
            render(self.cfg, output, PUBLIC_KEY)
            before = {p.relative_to(output): p.read_bytes() for p in output.rglob("*") if p.is_file()}
            render(self.cfg, output, PUBLIC_KEY)
            self.assertEqual(before, {p.relative_to(output): p.read_bytes() for p in output.rglob("*") if p.is_file()})
            for node in self.cfg["nodes"]:
                data = (output / node["name"] / "user-data").read_text()
                self.assertNotIn("private-token", data)
                self.assertNotIn("BEGIN OPENSSH PRIVATE", data)
                self.assertNotIn("token", json.loads(data.split("\n", 1)[1]))

    def test_invalid_topologies_fail_before_mutation(self):
        changes = [lambda c: c["nodes"].pop(),
                   lambda c: c["nodes"][1].update(role="agent"),
                   lambda c: c["nodes"][1].update(address=c["nodes"][0]["address"]),
                   lambda c: c["nodes"][0].update(address=c["network"]["gateway"]),
                   lambda c: c["nodes"][0].update(memory_mib=512),
                   lambda c: c["nodes"][0].update(name="aat-m2-invalid-"),
                   lambda c: c["nodes"][0].update(physical_host="invalid host name"),
                   lambda c: c.update(state_dir="/var/lib/libvirt/images/../aat-m2"),
                   lambda c: c.update(api_endpoint="https://192.168.127.11:7443"),
                   lambda c: c.update(pod_cidr=c["network"]["cidr"]),
                   lambda c: c.update(api_proxy_image="haproxy:latest"),
                   lambda c: c["cloud_image"].update(sha256=""),
                   lambda c: c["cloud_image"].update(url="http://example.com/image")]
        for change in changes:
            cfg = copy.deepcopy(self.cfg)
            change(cfg)
            with self.subTest(config=cfg), self.assertRaises(ValueError):
                validate(cfg)

    def test_fingerprint_tracks_resource_changes(self):
        other = copy.deepcopy(self.cfg)
        other["nodes"][0]["memory_mib"] += 512
        self.assertNotEqual(fingerprint(self.cfg), fingerprint(other))

    def test_laptop_memory_headroom_is_enforced(self):
        self.assertTrue(memory_budget(self.cfg, 15605, 10000)["memory_budget_ok"])
        heavy = copy.deepcopy(self.cfg)
        for node in heavy["nodes"]:
            node["memory_mib"] = 3072
        self.assertFalse(memory_budget(heavy, 15605, 10000)["memory_budget_ok"], "12 GiB must not pass this 16 GB host budget")
        self.assertFalse(memory_budget(self.cfg, 15605, 8000)["memory_budget_ok"], "startup must preserve current available-memory headroom")
        self.assertTrue(memory_budget(self.cfg, 15605, 8000, running_rss_mib=3000)["memory_budget_ok"])

    def test_resize_cannot_change_disks_or_cluster_identity(self):
        previous = copy.deepcopy(self.cfg)
        previous["nodes"][0]["memory_mib"] = 3072
        validate_resize(previous, self.cfg)
        for change in (lambda c: c["nodes"][0].update(data_gib=16),
                       lambda c: c["nodes"][0].update(address="192.168.127.21"),
                       lambda c: c["k3s"].update(version="v1.36.1+k3s1")):
            altered = copy.deepcopy(self.cfg)
            change(altered)
            with self.assertRaises(ValueError):
                validate_resize(previous, altered)

    def test_proxy_identity_has_only_readiness_access(self):
        role, binding = health_rbac(self.cfg)["items"]
        self.assertEqual([{"nonResourceURLs": ["/readyz"], "verbs": ["get"]}], role["rules"])
        self.assertEqual(role["metadata"]["name"], binding["roleRef"]["name"])
        proxy = haproxy_config(self.cfg)
        self.assertIn("http-check expect status 200", proxy)
        self.assertIn("verify required", proxy)
        self.assertIn("crt /usr/local/etc/haproxy/health.pem", proxy)


if __name__ == "__main__":
    unittest.main()
