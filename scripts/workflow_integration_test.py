"""Workflow is offline by default and cannot guess past an interrupted action."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from platform_lib.inventory import load
from workflow_lib import plan,run
from validation_access_lib import render,start

ROOT=Path(__file__).resolve().parent.parent


class WorkflowChecks(unittest.TestCase):
    def fixture(self,root):
        value={"version":1,"inventory":"inventory.json","output":"run","image_config":"image-config.json","private_release_config":"private/release.json","external_inputs":"external.json"}
        path=root/"workflow.json";path.write_text(json.dumps(value));return path

    def test_plan_only_lists_fixed_commands_and_completed_steps_are_not_reexecuted(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder);path=self.fixture(root);prepared,_,_=plan(path)
            self.assertEqual([p["name"]for p in prepared["phases"]],["images","inputs","cluster","bootstrap","maintenance","activate","access","validate"])
            self.assertFalse((root/"run").exists())
            calls=[]
            def runner(command,**kw):calls.append(command);return subprocess.CompletedProcess(command,0)
            run(path,"images",runner);run(path,"images",runner)
            self.assertEqual(len(calls),1)
            state=json.loads((root/"run/workflow-state.json").read_text());self.assertTrue(state["steps"]["images-0"]["completed"])

    def test_failed_or_uncertain_action_blocks_automatic_retry(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder);path=self.fixture(root);calls=[]
            def runner(command,**kw):calls.append(command);return subprocess.CompletedProcess(command,1)
            with self.assertRaisesRegex(ValueError,"step failed"):run(path,"images",runner)
            with self.assertRaisesRegex(ValueError,"uncertain or failed"):run(path,"images",runner)
            self.assertEqual(len(calls),1)

    def test_validation_access_is_loopback_private_and_refuses_changed_resources(self):
        inventory=load(ROOT/"deployment/platform/inventory.fourhost.example.json")
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)/"access";report=render(inventory,root)
            self.assertEqual(report["client_base_url"],"http://127.0.0.1:18080")
            text=(root/"haproxy.cfg").read_text();self.assertIn("bind 127.0.0.1:18080",text)
            for node in inventory["nodes"]:self.assertIn(node["address"]+":31080",text)
            resources=json.loads((root/"resources.json").read_text())
            for policy in resources["items"]:
                if policy["kind"]=="NetworkPolicy":
                    self.assertTrue(all(p["ipBlock"]["cidr"].endswith("/32")for p in policy["spec"]["ingress"][0]["from"]))
            (root/"resources.json").write_text('{"kind":"List","items":[]}')
            class API:cfg=inventory
            with self.assertRaisesRegex(ValueError,"reviewed validation resources changed"):start(API(),root)


if __name__=="__main__":unittest.main()
