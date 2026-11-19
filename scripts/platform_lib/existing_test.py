"""Existing-cluster identity checks and local-PV export safety, without live calls."""
import copy
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import database_integration_test as database_checks
from existing_volumes_lib import export
from platform_lib.cluster import Cluster
from platform_lib.existing import ExistingCluster, from_nodes, validate


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

    def bundle(self):
        check = database_checks.DatabaseIntegrationChecks()
        check.setUp()
        folder = self.root / "release"
        folder.mkdir()
        check.release(folder, self.cfg)
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
                       lambda p: p["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0].update(values=["unrelated-node"])):
            resource = copy.deepcopy(original)
            change(next(o for o in resource["items"] if o["kind"] == "PersistentVolume"))
            path.write_text(json.dumps(resource))
            with self.assertRaises(ValueError):
                export(self.cfg, bundle, self.root / "refused")
            self.assertFalse((self.root / "refused").exists())

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
