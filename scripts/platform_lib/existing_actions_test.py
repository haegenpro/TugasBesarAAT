"""Operator-assisted VM actions and guest steps on the existing cluster, without live calls."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

from platform_lib.existing import ExistingCluster, from_nodes
from platform_lib.existing_test import nodes_document

ROOT = Path(__file__).resolve().parent.parent.parent


class Clock:
    def __init__(self):self.now = 0.0
    def __call__(self):return self.now
    def sleep(self, seconds):self.now += seconds


class OperatorActionChecks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name);kubeconfig = root / "kubeconfig";kubeconfig.write_text("x\n");kubeconfig.chmod(0o600)
        self.cfg = from_nodes(nodes_document(), cluster_id="aat-fourhost", kubeconfig=kubeconfig, kubectl_command=["kubectl"], state_dir=root / "aat-fourhost")
        self.cfg["nodes"][1]["vm_name"] = "AAT - B"
        self.up = {n["address"]: True for n in self.cfg["nodes"]}
        self.messages = [];self.clock = Clock()
        self.cluster = ExistingCluster(self.cfg, reachable=lambda address: self.up[address], notify=self.messages.append, sleep=self.clock.sleep, clock=self.clock)
        self.node = self.cfg["nodes"][1]

    def operator_after(self, seconds, value):
        """Simulate the operator: the VM changes reachability after some delay."""
        original = self.clock.sleep
        def sleep(step):
            original(step)
            if self.clock.now >= seconds:self.up[self.node["address"]] = value
        self.cluster.sleep = sleep

    def test_pause_and_resume_are_observed_not_assumed(self):
        self.assertEqual(self.cluster.vm_state(self.node), "running")
        self.operator_after(7, False)
        self.cluster.vm_action(self.node, "pause")
        self.assertIn('platform-vm-control.py pause --vm "AAT - B"', self.messages[-1])
        self.assertIn("host-b", self.messages[-1])
        self.assertEqual(self.cluster.vm_state(self.node), "paused")
        self.operator_after(self.clock.now + 4, True)
        self.cluster.vm_action(self.node, "resume")
        self.assertEqual(self.cluster.vm_state(self.node), "running")
        self.assertEqual([a["action"] for a in self.cluster.actions], ["pause", "resume"])
        self.assertTrue(all(a["observed_after_seconds"] > 0 for a in self.cluster.actions))

    def test_an_action_that_never_happens_fails_instead_of_passing(self):
        with patch.dict("os.environ", {"AAT_OPERATOR_TIMEOUT": "30"}), self.assertRaisesRegex(ValueError, "not observed"):
            self.cluster.vm_action(self.node, "pause")
        self.assertEqual(self.cluster.actions, [])
        self.assertEqual(self.cluster.vm_state(self.node), "running")

    def test_stop_declares_shut_off_and_unknown_actions_are_refused(self):
        self.operator_after(3, False)
        self.cluster.vm_action(self.node, "stop")
        self.assertEqual(self.cluster.vm_state(self.node), "shut off")
        with self.assertRaises(ValueError):self.cluster.vm_action(self.node, "reboot")
        unnamed = self.cfg["nodes"][2];self.up[unnamed["address"]] = False
        self.assertEqual(self.cluster.vm_state(unnamed), "unreachable")

    def test_tolerated_unready_node_still_needs_its_original_identity(self):
        document = nodes_document();document["items"][2]["status"]["conditions"][0]["status"] = "False"
        live = lambda *args, **kwargs: subprocess.CompletedProcess([], 0, json.dumps(document).encode(), b"")
        with patch.object(self.cluster, "kubectl", side_effect=live):
            with self.assertRaises(ValueError):self.cluster.verify()
            self.cluster.tolerate_unready.add("aat-fourhost-c")
            self.assertTrue(self.cluster.verify()["ready"])
            document["items"][2]["metadata"]["uid"] = "re-registered"
            with self.assertRaisesRegex(ValueError, "identity"):self.cluster.verify()

    def test_guest_step_waits_for_operator_confirmation(self):
        def confirm():
            folder = next((self.cluster.state / "guest" / "run1" / self.node["name"]).iterdir())
            (folder / "done").write_text("ok\n")
        original = self.clock.sleep
        def sleep(step):
            original(step)
            if self.clock.now >= 6:confirm()
        self.cluster.sleep = sleep
        result = self.cluster.guest_step(self.node, "prepare-volumes", "print('x')\n", b"{}", run_id="run1")
        folder = Path(result["exported"])
        self.assertEqual((folder / "step.py").read_text(), "print('x')\n")
        self.assertEqual((folder / "payload.bin").stat().st_mode & 0o077, 0)
        self.assertIn("touch", self.messages[-1])
        # A confirmed step is not exported or asked again on resume.
        count = len(self.messages);self.cluster.guest_step(self.node, "prepare-volumes", "changed\n", b"{}", run_id="run1")
        self.assertEqual(len(self.messages), count)

    def test_invalid_vm_names_are_refused_by_inventory_validation(self):
        from platform_lib.existing import validate
        cfg = copy.deepcopy(self.cfg);cfg["nodes"][1]["vm_name"] = 'B" && rm -rf /'
        with self.assertRaises(ValueError):validate(cfg)


class HostVmControlChecks(unittest.TestCase):
    def module(self):
        spec = importlib.util.spec_from_file_location("vm_control", ROOT / "scripts/platform-vm-control.py")
        value = importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value

    def fake(self, state):
        calls = []
        def run(args, **kwargs):
            calls.append(args[1:])
            if args[1] == "showvminfo":return subprocess.CompletedProcess(args, 0, 'name="x"\nVMState="' + state[0] + '"\n', "")
            if args[1] == "controlvm":state[0] = {"pause": "paused", "resume": "running", "poweroff": "poweroff"}[args[3]]
            if args[1] == "startvm":state[0] = "running"
            return subprocess.CompletedProcess(args, 0, "", "")
        return run, calls

    def test_freeze_always_resumes_even_when_interrupted(self):
        control = self.module();state = ["running"];run, calls = self.fake(state)
        vm = control.VM("AAT - B", run=run, binary="VBoxManage")
        record = {"events": []}
        def interrupt(seconds):raise KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):control.act(vm, "freeze", 90, record, sleep=interrupt)
        self.assertEqual(state[0], "running")
        self.assertEqual([c for c in calls if c[0] == "controlvm"], [["controlvm", "AAT - B", "pause"], ["controlvm", "AAT - B", "resume"]])
        record = {"events": []};control.act(vm, "freeze", 60, record, sleep=lambda s: None)
        self.assertTrue(record["passed"]);self.assertEqual([e["event"] for e in record["events"]], ["paused", "resumed"])

    def test_preconditions_and_names_are_checked(self):
        control = self.module();state = ["poweroff"];run, _ = self.fake(state)
        vm = control.VM("AAT - B", run=run, binary="VBoxManage")
        with self.assertRaisesRegex(RuntimeError, "running"):control.act(vm, "pause", 60, {"events": []})
        record = {"events": []};control.act(vm, "start", 60, record);self.assertEqual(record["final_state"], "running")
        with self.assertRaises(ValueError):control.VM('x"; shutdown', run=run, binary="VBoxManage")


if __name__ == "__main__":
    unittest.main()
