"""Registry bootstrap failures must retain evidence and stop promptly."""
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

from database_admin_lib import bootstrap_diagnostics,initialize,wait_registry_bootstrap
from platform_lib.inventory import load
from storage_lib import plan

ROOT=Path(__file__).resolve().parent.parent


def result(value):
    return SimpleNamespace(returncode=0,stdout=json.dumps(value).encode(),stderr=b"")


class RegistryBootstrapChecks(unittest.TestCase):
    def test_terminal_failure_does_not_wait_for_timeout(self):
        cluster=SimpleNamespace(kubectl=Mock(return_value=result({"status":{"phase":"Failed","containerStatuses":[{"state":{"terminated":{"reason":"Error","exitCode":127}}}]}})))
        with patch("database_admin_lib.time.sleep")as sleep,self.assertRaisesRegex(RuntimeError,"exit=127"):
            wait_registry_bootstrap(cluster,"ns","initializer")
        sleep.assert_not_called();self.assertEqual(cluster.kubectl.call_count,1)

    def test_running_without_readiness_keeps_waiting(self):
        cluster=SimpleNamespace(kubectl=Mock(side_effect=[result({"status":{"phase":"Running","conditions":[{"type":"Ready","status":"False"}]}}),result({"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}})]))
        with patch("database_admin_lib.time.sleep")as sleep:wait_registry_bootstrap(cluster,"ns","initializer")
        sleep.assert_called_once();self.assertEqual(cluster.kubectl.call_count,2)

    def test_readiness_wait_is_bounded(self):
        cluster=SimpleNamespace(kubectl=Mock(return_value=result({"status":{"phase":"Pending"}})))
        with patch("database_admin_lib.time.monotonic",side_effect=[0,601]),self.assertRaisesRegex(ValueError,"timed out"):
            wait_registry_bootstrap(cluster,"ns","initializer")

    def test_diagnostics_continue_after_one_api_timeout(self):
        cluster=SimpleNamespace(kubectl=Mock(side_effect=[subprocess.TimeoutExpired("kubectl",20),SimpleNamespace(returncode=0,stdout=b"fatal startup evidence",stderr=b""),result({"items":[]})]))
        with tempfile.TemporaryDirectory()as folder:
            output=Path(folder)/"diagnostics";bootstrap_diagnostics(cluster,"ns","initializer",output)
            self.assertEqual((output/"registry.log").read_bytes(),b"fatal startup evidence")
            self.assertEqual(json.loads((output/"events.json").read_text()),{"items":[]})
            self.assertTrue(all(p.stat().st_mode&0o777==0o600 for p in output.iterdir()))

    def test_failed_initializer_saves_evidence_before_cleanup(self):
        inventory=load(ROOT/"deployment/platform/inventory.fourhost.example.json")
        image="registry.invalid/fixture@sha256:"+"a"*64
        storage=plan(inventory,"postgres",image,"service-db");calls=[];pod=None
        def kubectl(*args,**kwargs):
            nonlocal pod
            calls.append(args)
            if args[:3]==("create","-f","-"):pod=json.loads(kwargs["input"]);return result(pod)
            if "pods"in args:return result({"items":[]})
            if "pod"in args:return result(pod)
            if "logs"in args:return SimpleNamespace(returncode=0,stdout=b"fatal CPU mismatch",stderr=b"")
            return result({"items":[]})
        cluster=SimpleNamespace(cfg=inventory,kubectl=kubectl)
        with tempfile.TemporaryDirectory()as folder,patch("database_admin_lib.original_primary",return_value=({"name":"leader"},[])),patch("database_admin_lib.wait_registry_bootstrap",side_effect=RuntimeError("terminal startup failure")):
            output=Path(folder)/"diagnostics"
            with self.assertRaisesRegex(RuntimeError,"terminal startup failure"):
                initialize(cluster,storage,"registry-db",image,diagnostics=output)
            self.assertEqual((output/"registry.log").read_bytes(),b"fatal CPU mismatch")
            self.assertLess(next(i for i,a in enumerate(calls)if "logs"in a),next(i for i,a in enumerate(calls)if "delete"in a))
            self.assertFalse(any("exec"in a for a in calls))
            self.assertEqual(pod["spec"]["containers"][0]["resources"]["limits"]["memory"],"256Mi")


if __name__=="__main__":unittest.main()
