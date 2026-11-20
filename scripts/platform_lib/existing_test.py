"""Existing-cluster identity checks and local-PV export safety, without live calls."""
import copy
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import runpy
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import database_integration_test as database_checks
from existing_volumes_lib import export
from platform_lib.cluster import Cluster
from platform_lib.existing import ExistingCluster, from_nodes, validate
from validation_access_lib import render as render_access, start as start_access


def nodes_document():
    items = []
    for index, (letter, memory) in enumerate(zip("abcd", (8131860, 8131844, 4009220, 4009220))):
        labels = {"platform.aat/cluster": "aat-fourhost", "platform.aat/physical-host": "host-" + letter}
        if index < 3:
            labels["node-role.kubernetes.io/control-plane"] = "true"
        items.append({"metadata": {"name": "aat-fourhost-" + letter, "uid": "original-" + letter, "labels": labels},
                      "status": {"conditions": [{"type": "Ready", "status": "True"}], "addresses": [{"type": "InternalIP", "address": "192.168.100." + str(27 + index)}],
                                 "allocatable": {"memory": str(memory) + "Ki"}, "capacity": {"cpu": "4"}}})
    return {"items": items}


class ExistingChecks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.kubeconfig = self.root / "kubeconfig"
        self.kubeconfig.write_text("private test credential\n")
        self.kubeconfig.chmod(0o600)
        self.document = nodes_document()
        self.cfg = from_nodes(self.document, cluster_id="aat-fourhost", kubeconfig=self.kubeconfig,
                              kubectl_command=["kubectl"], state_dir=self.root / "aat-fourhost")

    def test_capture_real_allocatable_capacity_without_vm_provider_fields(self):
        self.assertIsInstance(Cluster(self.cfg), ExistingCluster)
        self.assertEqual([n["memory_mib"] for n in self.cfg["nodes"]], [7941, 7941, 3915, 3915])
        self.assertNotIn("hosts", self.cfg)
        self.assertNotIn("cloud_image", self.cfg)
        self.assertFalse(hasattr(Cluster(self.cfg), "up"))
        self.assertFalse(hasattr(Cluster(self.cfg), "virsh"))

    def test_capture_refuses_wrong_labels_roles_and_unready_nodes(self):
        for change in (lambda n: n["metadata"]["labels"].update({"platform.aat/physical-host": "other"}),
                       lambda n: n["metadata"]["labels"].pop("node-role.kubernetes.io/control-plane"),
                       lambda n: n["status"]["conditions"][0].update(status="False")):
            document = copy.deepcopy(self.document)
            change(document["items"][0])
            with self.assertRaises(ValueError):
                from_nodes(document, cluster_id="aat-fourhost", kubeconfig=self.kubeconfig, kubectl_command=["kubectl"], state_dir=self.cfg["state_dir"])

    def test_private_kubeconfig_and_literal_command_arguments(self):
        cluster = Cluster(self.cfg)
        with patch.object(cluster, "run", return_value=subprocess.CompletedProcess([], 0, b"ok", b"")) as run:
            cluster.kubectl("get", "nodes")
            self.assertEqual(run.call_args.args[0], ["kubectl", "--kubeconfig", str(self.kubeconfig), "get", "nodes"])
            self.kubeconfig.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "private"):
                cluster.kubectl("get", "nodes")
            self.assertEqual(run.call_count, 1)

    def test_verification_refuses_recreated_or_moved_node(self):
        cluster = Cluster(self.cfg)
        with patch.object(cluster, "kubectl", return_value=subprocess.CompletedProcess([], 0, json.dumps(self.document).encode(), b"")):
            self.assertTrue(cluster.verify()["ready"])
        for key, value in (("uid", "replacement"), ("address", "192.168.100.99")):
            observed = copy.deepcopy(self.document)
            if key == "uid":
                observed["items"][0]["metadata"]["uid"] = value
            else:
                observed["items"][0]["status"]["addresses"][0]["address"] = value
            with patch.object(cluster, "kubectl", return_value=subprocess.CompletedProcess([], 0, json.dumps(observed).encode(), b"")):
                with self.assertRaisesRegex(ValueError, "identity"):
                    cluster.verify()

    def test_lock_refuses_changed_inventory_and_foreign_state(self):
        with patch.object(ExistingCluster, "verify", return_value={"ready": True}):
            with Cluster(self.cfg).locked():
                pass
            different = copy.deepcopy(self.cfg)
            different["nodes"][0]["memory_mib"] -= 1
            with self.assertRaisesRegex(ValueError, "changed"):
                with Cluster(different).locked():
                    pass
        self.assertEqual((Path(self.cfg["state_dir"]) / "owner.json").stat().st_mode & 0o777, 0o600)

    def test_validation_refuses_unknown_layout_and_overlapping_addresses(self):
        for change in (lambda c: c.update(storage_layout="unverified"),
                       lambda c: c["nodes"][0].update(address="10.42.0.7"),
                       lambda c: c.update(kubectl_command=["sh", "-c", "anything"])):
            cfg = copy.deepcopy(self.cfg)
            change(cfg)
            with self.assertRaises(ValueError):
                validate(cfg)

    def bundle(self, name="release", placement=None):
        check = database_checks.DatabaseIntegrationChecks()
        check.setUp()
        folder = self.root / name
        folder.mkdir()
        check.release(folder, self.cfg, placement)
        return folder / "output/bundle.json"

    def test_export_covers_all_local_pvs_with_actual_owners(self):
        bundle = self.bundle()
        output = self.root / "guest-scripts"
        report = export(self.cfg, bundle, output)
        self.assertEqual(len(report["volumes"]), 40)  # 21 PG, 9 etcd, 10 app/monitor volumes
        self.assertTrue(any(v["uid"] == 472 for v in report["volumes"]))
        self.assertTrue(any(v["uid"] == 999 for v in report["volumes"]))
        for node in self.cfg["nodes"]:
            script = output / (node["name"] + ".py")
            compile(script.read_text(), str(script), "exec")
            self.assertEqual(script.stat().st_mode & 0o777, 0o600)
            self.assertIn(node["address"], script.read_text())
            self.assertIn("refusing to adopt unmarked", script.read_text())
        with self.assertRaisesRegex(ValueError, "already exists"):
            export(self.cfg, bundle, output)

    def test_export_refuses_pv_escape_and_unknown_nodes_before_writing(self):
        bundle = self.bundle()
        path = bundle.parent / "artifacts/canonical-storage.json"
        original = json.loads(path.read_text())
        for change in (lambda p: p["spec"]["local"].update(path="/etc/unowned"),
                       lambda p: p["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0].update(values=["unrelated-node"]),
                       lambda p: p["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0].update(values=["aat-fourhost-d"])):
            resource = copy.deepcopy(original)
            change(next(o for o in resource["items"] if o["kind"] == "PersistentVolume"))
            path.write_text(json.dumps(resource))
            with self.assertRaises(ValueError):
                export(self.cfg, bundle, self.root / "refused")
            self.assertFalse((self.root / "refused").exists())

    def test_singleton_placement_preserves_topology_and_exports_new_retained_claims(self):
        original = self.bundle()
        target = self.cfg["nodes"][1]["name"]
        move = {"node": target, "volume_suffix": "move1"}
        changed = self.bundle("moved-release", {"services": {"pvmbg-mock": move},
                                                "monitoring": {"loki": move, "grafana": move}})
        for name in ("canonical-storage", "ingestion-storage", "owner-databases"):
            path = "artifacts/" + name + ".json"
            self.assertEqual((original.parent / path).read_bytes(), (changed.parent / path).read_bytes())
        old = export(self.cfg, original, self.root / "before")["volumes"]
        new = export(self.cfg, changed, self.root / "after")["volumes"]
        moved = {"pvmbg-mock", "loki", "grafana"}
        old_by_path = {entry["path"]: entry for entry in old}
        for entry in new:
            path = entry["path"]
            if path.endswith("-move1"):
                baseline = path.removesuffix("-move1")
                app = Path(baseline).name.removeprefix("aat-fourhost-").removesuffix("-a")
                moved.remove(app)
                self.assertEqual(entry, {**old_by_path.pop(baseline), "node": target, "path": path})
            else:
                self.assertEqual(entry, old_by_path.pop(path))
        self.assertFalse(moved)
        self.assertFalse(old_by_path)
        deploy = runpy.run_path(str(Path(__file__).resolve().parents[1] / "platform-deploy.py"))
        baseline = (original.parent / "artifacts/services.json").read_bytes()
        desired = (changed.parent / "artifacts/services.json").read_bytes()
        stateful = {obj["metadata"]["name"]: obj for obj in json.loads(baseline)["items"]
                    if obj["kind"] == "StatefulSet"}
        deploy["check_stateful_volumes"]({"services": baseline}, stateful)
        with self.assertRaisesRegex(ValueError, "staged cutover.*pvmbg-mock"):
            deploy["check_stateful_volumes"]({"services": desired}, stateful)

    def test_existing_access_uses_exact_external_callers_and_controller_preflight(self):
        output = self.root / "access"
        controller = "192.168.100.4"
        generator = "192.168.100.5"
        for address in (None, "0.0.0.0", "8.8.8.8", "192.168.100.0/24", self.cfg["nodes"][0]["address"]):
            with self.subTest(address=address), self.assertRaises(ValueError):
                render_access(self.cfg, output, controller_address=address)
        report = render_access(self.cfg, output, controller_address=controller, generator_address=generator)
        manifest = json.loads((output / "resources.json").read_text())
        for obj in manifest["items"]:
            if obj["kind"] == "Service":
                self.assertEqual(obj["spec"]["externalTrafficPolicy"], "Local")
            else:
                peers = obj["spec"]["ingress"][0]["from"]
                self.assertEqual([p["ipBlock"]["cidr"] for p in peers], [controller + "/32", generator + "/32"])
        self.assertEqual((output / "haproxy.cfg").read_text().count("  source " + controller), 2)
        cluster = Cluster(self.cfg)
        with patch.object(cluster, "run", return_value=SimpleNamespace(stdout=b"[]")), patch.object(cluster, "kubectl") as kube:
            with self.assertRaisesRegex(ValueError, "address is absent"):
                start_access(cluster, output)
            kube.assert_not_called()
        calls = []

        def run(args, **kwargs):
            calls.append(tuple(args))
            if args[0] == "ip":
                return SimpleNamespace(stdout=json.dumps([{"addr_info": [{"local": controller}]}]).encode())
            return SimpleNamespace(returncode=int(args[1] == "inspect"), stdout=b"")

        def kubectl(*args, **kwargs):
            calls.append(args)
            return SimpleNamespace(stdout=b"")

        with patch.object(cluster, "run", side_effect=run), patch.object(cluster, "kubectl", side_effect=kubectl):
            self.assertEqual(start_access(cluster, output), report)
        check = next(i for i, call in enumerate(calls) if call[:3] == ("docker", "run", "--rm"))
        apply = calls.index(("apply", "-f", "-"))
        start = next(i for i, call in enumerate(calls) if call[:3] == ("docker", "run", "-d"))
        self.assertLess(check, apply)
        self.assertLess(apply, start)

    def test_demo_disk_mode_keeps_volumes_and_ownership_checks_but_requires_two_gib(self):
        bundle=self.bundle();strict=export(self.cfg,bundle,self.root/"strict")
        for index,(allow,free_gib,success)in enumerate(((False,3,False),(True,1,False),(True,3,True))):
            with self.subTest(allow=allow,free_gib=free_gib):
                output=self.root/("scripts-"+str(index));report=export(self.cfg,bundle,output,allow_disk_overcommit=allow)
                self.assertEqual(report["volumes"],strict["volumes"])
                guest=self.root/("guest-"+str(index));script=(output/"aat-fourhost-a.py").read_text().replace("/var/lib/aat",str(guest))
                protected={guest,guest/"local-pv",guest/"platform-owner.json"};original_stat=Path.stat
                def owned_stat(path,*args,**kwargs):
                    observed=original_stat(path,*args,**kwargs)
                    if path in protected:
                        fields=list(observed);fields[4]=0;return os.stat_result(fields)
                    return observed
                addresses=json.dumps([{"addr_info":[{"local":self.cfg["nodes"][0]["address"]}]}])
                with patch("os.geteuid",return_value=0),patch("os.chown"),patch.object(Path,"stat",owned_stat),patch("subprocess.check_output",return_value=addresses),patch("shutil.disk_usage",return_value=SimpleNamespace(free=free_gib*2**30)),patch("sys.stdin"),redirect_stdout(io.StringIO())as printed:
                    if success:exec(compile(script,"guest-prepare.py","exec"),{})
                    else:
                        with self.assertRaisesRegex(RuntimeError,"insufficient free disk"):exec(compile(script,"guest-prepare.py","exec"),{})
                if success:
                    self.assertEqual(json.loads(printed.getvalue())["disk_capacity_check"],"free-space-only")
                    self.assertTrue((guest/"platform-owner.json").is_file())
                else:self.assertFalse((guest/"platform-owner.json").exists())
        output=self.root/"demo-wrong-node";export(self.cfg,bundle,output,allow_disk_overcommit=True)
        with patch("os.geteuid",return_value=0),patch("subprocess.check_output",return_value="[]"):
            with self.assertRaisesRegex(RuntimeError,"captured node address"):
                exec(compile((output/"aat-fourhost-a.py").read_text(),"guest-prepare.py","exec"),{})

    def test_guest_script_refuses_wrong_machine_before_storage_mutation(self):
        output = self.root / "scripts"
        export(self.cfg, self.bundle(), output)
        script = (output / "aat-fourhost-a.py").read_text()
        unrelated = json.dumps([{"addr_info": [{"local": "192.168.200.9"}]}])
        with patch("os.geteuid", return_value=0), patch("subprocess.check_output", return_value=unrelated):
            with self.assertRaisesRegex(RuntimeError, "captured node address"):
                exec(compile(script, "guest-prepare.py", "exec"), {})


if __name__ == "__main__":
    unittest.main()
