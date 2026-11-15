import base64
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

from platform_lib.cluster import Cluster, LocalCluster
from platform_lib.host_agent import HostAgent, digest, serve, bounded_execute
from platform_lib.inventory import validate, load, render, cloud_config, node_network, resource_uuid, fingerprint
from platform_lib.multihost import MultiHostCluster, HostClient
from platform_lib.multihost_inventory import capacity
from platform_lib.network_evidence import reconcile, validate_probes, collector_operation, MAX_LOG, fetch, application_sample, download_collector
from platform_lib.pod_probe import objects, verify_network

ROOT = Path(__file__).resolve().parents[2]
KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFPZ7djI4C6v2ohMU2HRvIIK+VLcI+uV6Kw2+EAbIhfF synthetic-preview"


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.cfg = load(ROOT / "deployment/platform/inventory.multihost.example.json")

    def test_legacy_factory_unchanged_new_provider_selected(self):
        self.assertIsInstance(Cluster(load(ROOT / "deployment/platform/inventory.example.json")), LocalCluster)
        self.assertIsInstance(Cluster(self.cfg), MultiHostCluster)

    def test_rejects_network_paths_and_identity_ambiguity(self):
        changes = [lambda c: c["hosts"][1].update(state_dir="//server/share/aat-twohost"),
            lambda c: c["hosts"][1].update(state_dir="C:/aat/aat-twohost\nput bad"),
            lambda c: c["hosts"][1].update(ssh_user="user;whoami"),
            lambda c: c["hosts"][1].update(known_hosts="relative"),
            lambda c: c["hosts"][1].update(address=c["hosts"][0]["address"]),
            lambda c: c["hosts"][0].update(provider="virtualbox"),
            lambda c: c.update(api_endpoint="https://192.168.50.1:7443"),
            lambda c: c["nodes"][3].update(physical_host="linux"),
            lambda c: c["network"].update(mode="nat"),
            lambda c: c["hosts"][0].update(reserve_mib=1),
            lambda c: c["network"].update(cidr="10.42.0.0/16", gateway="10.42.0.1")]
        for change in changes:
            cfg = copy.deepcopy(self.cfg)
            change(cfg)
            with self.subTest(change=change), self.assertRaises((ValueError, KeyError)):
                validate(cfg)

    def test_preview_static_bridges_disks_and_networks(self):
        with tempfile.TemporaryDirectory() as tmp:
            render(self.cfg, tmp, KEY)
            self.assertFalse((Path(tmp) / "network.xml").exists())
            xml = ET.fromstring((Path(tmp) / "aat-twohost-a/domain.xml").read_text())
            self.assertEqual("bridge", xml.find("./devices/interface").get("type"))
            self.assertEqual("br0", xml.find("./devices/interface/source").get("bridge"))
            self.assertFalse((Path(tmp) / "aat-twohost-c/domain.xml").exists())
            self.assertIn("/dev/sdb", cloud_config(self.cfg, self.cfg["nodes"][2], KEY)["disk_setup"])
            net = node_network(self.cfg["nodes"][2], self.cfg)["ethernets"]["aatnic"]
            self.assertEqual(["192.168.50.33/24"], net["addresses"])
            self.assertFalse(net["dhcp4"])
            before = {str(p): p.read_bytes() for p in Path(tmp).rglob("*") if p.is_file()}
            render(self.cfg, tmp, KEY)
            self.assertEqual(before, {str(p): p.read_bytes() for p in Path(tmp).rglob("*") if p.is_file()})

    def test_each_host_budget_is_independent(self):
        linux, windows = self.cfg["hosts"]
        self.assertTrue(capacity(self.cfg, linux, 16384, 10000, 100)["memory_ok"])
        self.assertFalse(capacity(self.cfg, windows, 6000, 5000, 100)["memory_ok"])
        self.assertFalse(capacity(self.cfg, windows, 16384, 4000, 10)["disk_ok"])
        self.assertFalse(capacity(self.cfg, windows, 16384, 2000, 100)["memory_ok"])
        self.assertTrue(capacity(self.cfg, windows, 16384, 2000, 100, [n["name"] for n in self.cfg["nodes"][2:]])["memory_ok"])


class HostTests(unittest.TestCase):
    def setUp(self):
        self.cfg = load(ROOT / "deployment/platform/inventory.multihost.example.json")
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.calls = []
        def runner(args, **kwargs):
            self.calls.append(list(map(str, args)))
            raw = b'"aat","S-1-5-21-1234"' if args[0] == "whoami" else b""
            return subprocess.CompletedProcess(args, 0, raw, b"")
        self.agent = HostAgent(self.cfg, "windows", runner, Path(self.tmp.name) / "owned")
        self.agent.init()

    def test_unmarked_state_and_path_escape_refused(self):
        for name in ("../data", "/tmp/x", "C:/other", "seeds/../../x"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.agent.path(name)
        other = HostAgent(self.cfg, "windows", self.agent.run, Path(self.tmp.name) / "unmarked")
        other.root.mkdir()
        (other.root / "user-data").write_text("keep")
        with self.assertRaisesRegex(ValueError, "unmarked"):
            other.init()
        self.assertEqual("keep", (other.root / "user-data").read_text())

    def test_transfer_retry_digest_and_quota(self):
        payload = b"hand-specified-seed-bytes"
        name = self.cfg["nodes"][2]["name"] + "-seed.iso"
        import hashlib
        args = {"name": name, "offset": 0, "data": base64.b64encode(payload[:8]).decode(), "sha256": hashlib.sha256(payload).hexdigest()}
        self.assertEqual(8, self.agent.upload(args)["bytes"])
        self.assertEqual(8, self.agent.upload(args)["bytes"])
        bad = {**args, "data": base64.b64encode(b"conflict").decode()}
        with self.assertRaises(ValueError):
            self.agent.upload(bad)
        final = {**args, "offset": 8, "data": base64.b64encode(payload[8:]).decode(), "final": True}
        self.assertTrue(self.agent.upload(final)["complete"])
        self.assertEqual(payload, self.agent.artifact(name).read_bytes())
        self.agent.path("artifacts/" + name).write_bytes(b"corrupt")
        with self.assertRaises(ValueError):
            self.agent.artifact(name)
        with self.assertRaises(ValueError):
            self.agent.upload({**args, "name": "caller-selected.xml"})

    def test_completed_transfer_skips_copy_and_reconciles_missing_receipt(self):
        name = self.cfg["nodes"][2]["name"] + "-seed.iso"
        target = self.agent.path("artifacts/" + name)
        target.write_bytes(b"original-transfer")
        args = {"name": name, "sha256": digest(target)}
        self.assertTrue(self.agent.upload_status(args)["complete"])
        self.assertEqual(target, self.agent.artifact(name))
        client = MultiHostCluster(self.cfg).clients["windows"]
        with patch.object(client, "call", side_effect=lambda op, **kwargs: self.agent.upload_status(kwargs)) as call, \
                patch.object(client.cluster, "run") as transfer:
            self.assertTrue(client.transfer(target, name)["complete"])
            transfer.assert_not_called()
            self.assertEqual("upload-status", call.call_args.args[0])
        target.write_bytes(b"corrupted")
        with self.assertRaises(ValueError):
            self.agent.upload_status(args)

    def test_protocol_rejects_shell_and_wrong_os(self):
        with self.assertRaises(ValueError):
            self.agent.dispatch("delete", {})
        with patch("platform_lib.host_agent.platform.system", return_value="Linux"), self.assertRaises(ValueError):
            self.agent.dispatch("init", {})
        with self.assertRaises(ValueError):
            serve(b"x" * (128 * 1024 + 1))
        with self.assertRaises(ValueError):
            serve(json.dumps({"version": 1, "command": "powershell bad"}).encode())

    def test_virtualbox_ownership_hardware_and_lifecycle(self):
        node = self.cfg["nodes"][2]
        name = node["name"]
        values = {"UUID": resource_uuid(self.cfg, name), "CfgFile": str(self.agent.path(f"vms/{name}/{name}.vbox")),
            "VMState": "poweroff", "memory": 2048, "cpus": 2, "macaddress1": node["mac"].replace(":", ""),
            "storagecontrollername0": "SATA", "SATA-2-0": str(self.agent.path("artifacts/"+name+"-seed.iso")),
            "nic1": "bridged", "bridgeadapter1": self.cfg["hosts"][1]["bridge"],
            **{f"SATA-{i}-0": str(self.agent.path(f"disks/{name}-{k}.vdi")) for i,k in enumerate(("root", "data"))}}
        def runner(args, **kwargs):
            self.calls.append(list(map(str, args)))
            if args[1:3] == ["list", "vms"]:
                raw = f'"{name}" {{{values["UUID"]}}}'.encode()
            elif args[1] == "showvminfo":
                raw = "\n".join(k + "=" + json.dumps(v) for k,v in values.items()).encode()
            elif args[1] == "getextradata":
                raw = ("Value: " + fingerprint(self.cfg)).encode()
            else:
                raw = b""
                if args[1] == "startvm": values["VMState"] = "running"
                if args[1] == "controlvm": values["VMState"] = {"pause": "paused", "resume": "running", "acpipowerbutton": "poweroff"}[args[-1]]
            return subprocess.CompletedProcess(args, 0, raw, b"")
        self.agent.run = runner
        for action, expected in (("start", "running"), ("pause", "paused"), ("resume", "running"), ("stop", "shut off")):
            self.assertEqual(expected, self.agent.action(name, action)["state"])
        values["storagecontrollername1"] = "IDE"
        with self.assertRaisesRegex(ValueError, "storage controllers"):
            self.agent.action(name, "start")
        del values["storagecontrollername1"]
        values["SATA-0-0"] = str(Path(self.tmp.name) / "unrelated.vdi")
        with self.assertRaises(ValueError):
            self.agent.action(name, "start")
        self.assertFalse(any("delete" in c or "poweroff" in c for c in self.calls))

    def test_collector_detaches_and_retry_does_not_spawn_again(self):
        args = {"run_id": "a"*32, "seconds": 60, "probes": []}
        with patch("platform_lib.network_evidence.subprocess.Popen") as popen:
            popen.return_value.pid = 123
            result = collector_operation(self.agent, "collector-start", args)
            self.assertEqual("started", result["outcome"])
            request, log = self.agent.path("collectors/" + "a"*32 + ".request.json"), self.agent.path("collectors/" + "a"*32 + ".jsonl")
            self.assertIn("clock_domain", json.loads(request.read_text()))
            log.write_text("{}\n")
            self.assertEqual("existing", collector_operation(self.agent, "collector-start", args)["outcome"])
            self.assertEqual(1, popen.call_count)
            with self.assertRaises(ValueError):
                collector_operation(self.agent, "collector-start", {**args, "seconds": 90})

    def test_libvirt_create_is_idempotent_and_checks_transferred_base(self):
        agent = HostAgent(self.cfg, "linux", self.agent.run, Path(self.tmp.name) / "linux")
        agent.init()
        node = self.cfg["nodes"][0]
        base, seed = agent.path("artifacts/cloud.qcow2"), agent.path("artifacts/"+node["name"]+"-seed.iso")
        base.write_bytes(b"synthetic image")
        seed.write_bytes(b"synthetic iso")
        # Synthetic input pin is confined to this fake-command fixture.
        agent.cfg = copy.deepcopy(self.cfg)
        agent.cfg["cloud_image"]["sha256"] = digest(base)
        agent.owner["inventory_sha256"] = fingerprint(agent.cfg)
        agent._json("host-owner.json", agent.owner)
        for p in (base,seed): agent._json("artifacts/"+p.name+".receipt.json", {"bytes":p.stat().st_size,"sha256":digest(p)})
        from platform_lib.inventory import domain_xml
        xml = domain_xml(agent.cfg, node).replace(str(Path(agent.host["state_dir"])), str(agent.root))
        defined = []
        calls = []
        def runner(args, **kwargs):
            args = list(map(str,args));calls.append(args);raw=b""
            if args[0] == "qemu-img" and args[1] == "create": Path(args[-2]).write_bytes(b"owned")
            elif args[0] == "qemu-img" and args[1] == "info":
                kind = "root" if "-root." in args[-1] else "data"
                raw=json.dumps({"format":"qcow2","virtual-size":node[kind+"_gib"]*2**30, **({"full-backing-filename":str(base)} if kind=="root" else {})}).encode()
            elif "define" in args: defined.append(node["name"])
            elif "list" in args: raw="\n".join(defined).encode()
            elif "dumpxml" in args: raw=xml.encode()
            elif "domstate" in args: raw=b"shut off"
            return subprocess.CompletedProcess(args,0,raw,b"")
        agent.run=runner
        with patch("platform_lib.host_agent.domain_xml",return_value=xml):
            self.assertEqual("shut off",agent.create(node["name"])["state"])
            count=len([c for c in calls if "create" in c])
            agent.create(node["name"])
            self.assertEqual(count,len([c for c in calls if "create" in c]))
        self.assertEqual(b"owned",agent.path("disks/"+node["name"]+"-data.qcow2").read_bytes())

    def test_remote_json_stays_off_command_line_and_output_is_bounded(self):
        cluster=MultiHostCluster(self.cfg)
        client=cluster.clients["windows"]
        response=subprocess.CompletedProcess([],0,b'{"version":1,"result":{"ok":true}}',b"")
        with patch.object(client,"options",return_value=[]),patch("platform_lib.multihost.bounded_execute",return_value=response) as call:
            self.assertEqual({"ok":True},client.call("collector-start",token="private-secret"))
            argv,data=call.call_args.args
            self.assertEqual("aat-platform-host",argv[-1])
            self.assertNotIn("private-secret"," ".join(argv))
            self.assertIn(b"private-secret",data)
        result=bounded_execute([sys.executable,"-c","print('small')"],b"",timeout=5)
        self.assertEqual(b"small\r\n" if sys.platform=="win32" else b"small\n",result.stdout)
        with self.assertRaises(ValueError):
            bounded_execute([sys.executable,"-c","import sys;sys.stdout.buffer.write(b'x'*(3*1024*1024))"],b"",timeout=5)


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.cfg = load(ROOT / "deployment/platform/inventory.multihost.example.json")
        self.run = "a"*32
        self.logs = {}
        for host in ("linux", "windows"):
            peer = "windows" if host == "linux" else "linux"
            start = {"type": "start", "host": host, "run_id": self.run, "machine_sha256": host, "clock_domain": host,
                     "inventory_sha256": fingerprint(self.cfg), "monotonic_ns": 100}
            samples = [{"type": "sample", "monotonic_ns": 200+i, "link": {"up": up}, "tcp": [{"host": peer, "available": up, "connect_rtt_ms": 2}], "application": []} for i,up in enumerate((True, False, True))]
            self.logs[host] = [start, *samples, {"type": "end", "monotonic_ns": 400}]

    def test_network_only_is_blocked_not_bonus_pass(self):
        result = reconcile(self.cfg, self.run, self.logs)
        self.assertEqual("blocked", result["status"])
        self.assertEqual(2, result["hosts"]["linux"]["rtt_ms"]["p95"])
        self.assertTrue(result["assertions"]["real_link_loss_and_restore"])

    def test_interrupted_download_resumes_original_offset_and_verifies_digest(self):
        import hashlib
        payload = b'{"type":"start"}\n{"type":"end"}\n'
        calls = []
        def call(operation, **args):
            self.assertEqual("collector-fetch", operation)
            self.assertEqual(self.run, args["run_id"])
            calls.append(args["offset"])
            raw = payload[args["offset"]:]
            return {"data": base64.b64encode(raw).decode(), "bytes": len(raw), "done": True,
                    "terminal": {"bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}}
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "linux.jsonl"
            dest.write_bytes(payload[:10])
            download_collector(SimpleNamespace(call=call), self.run, dest)
            self.assertEqual([10, len(payload)], calls)
            self.assertEqual(payload, dest.read_bytes())
            calls.clear()
            download_collector(SimpleNamespace(call=call), self.run, dest)
            self.assertEqual([len(payload)], calls)
            dest.write_bytes(b"X" + payload[1:])
            with self.assertRaisesRegex(ValueError, "digest/count"):
                download_collector(SimpleNamespace(call=call), self.run, dest)
            self.assertEqual(b"X" + payload[1:], dest.read_bytes())

    def test_probe_cancellation_cleans_only_owned_resources(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp)
            (state / "evidence").mkdir()
            applied, deleted = [], []
            def kubectl(*args, **kwargs):
                if args[0] == "apply":
                    applied.extend(json.loads(kwargs["input"])["items"])
                elif args[0] == "wait":
                    raise KeyboardInterrupt()
                elif args[0] == "get":
                    item = copy.deepcopy(next(v for v in applied if v["kind"] == args[1] and v["metadata"]["name"] == args[2]))
                    if item == applied[0]:
                        item["metadata"]["labels"]["platform.aat/network-probe"] = "different-owner"
                    return subprocess.CompletedProcess(args, 0, json.dumps(item).encode(), b"")
                elif args[0] == "delete":
                    deleted.append(args[2])
                return subprocess.CompletedProcess(args, 0, b"", b"")
            cluster = SimpleNamespace(cfg=self.cfg, state=state, kubectl=kubectl)
            with self.assertRaises(KeyboardInterrupt):
                verify_network(cluster, "local/python@sha256:"+"a"*64)
            self.assertNotIn(applied[0]["metadata"]["name"], deleted)
            self.assertEqual(5, len(deleted))
            report = json.loads((state / "evidence/network-probe.json").read_text())
            self.assertEqual("failed", report["status"])
            self.assertEqual(1, sum(v["status"] == "failed" for v in report["cleanup"]))

    def test_no_cut_or_missing_terminal_is_failure(self):
        logs = copy.deepcopy(self.logs)
        logs["windows"].pop()
        self.assertEqual("failed", reconcile(self.cfg, self.run, logs)["status"])
        logs = copy.deepcopy(self.logs)
        for entries in logs.values():
            for v in entries:
                if v["type"] == "sample": v["link"]["up"] = True
        self.assertEqual("failed", reconcile(self.cfg, self.run, logs)["status"])

    def test_other_run_and_external_url_rejected(self):
        logs = copy.deepcopy(self.logs)
        logs["windows"][0]["run_id"] = "b"*32
        with self.assertRaises(ValueError): reconcile(self.cfg, self.run, logs)
        for url in ("http://example.com/metrics", "http://127.0.0.1/metrics", "http://user:pass@192.168.50.31/metrics", "http://192.168.50.31/admin"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                validate_probes([{"name": "bad", "kind": "metrics", "url": url}], self.cfg)

    def test_probe_objects_are_owned_bounded_and_not_tester(self):
        bundle = objects(self.cfg, "local/python@sha256:"+"a"*64, "a"*12)
        pods = [v for v in bundle if v["kind"] == "Pod"]
        self.assertEqual(4, len(pods))
        for pod in pods:
            self.assertEqual("other", pod["metadata"]["labels"]["role"])
            self.assertFalse(pod["spec"]["automountServiceAccountToken"])
            self.assertEqual(180, pod["spec"]["activeDeadlineSeconds"])
            self.assertEqual("128Mi", pod["spec"]["containers"][0]["resources"]["requests"]["memory"])

    def test_owner_reconciliation_and_same_clock_markers_are_required(self):
        from recovery_checks_test import evidence
        owner=evidence();owner["network_run_id"]=self.run
        owner["latency_markers"]=[{"host":"linux","machine_sha256":"linux","clock_domain":"linux","source":"BMKG","operation_id":"op-a","ack_monotonic_ns":150}]
        observed={"probe":"entity-a","kind":"entity","available":True,"operation_id":"op-a","source":"BMKG","source_ref_id":"a", "hazard_id":owner["acknowledgements"][0]["hazard_id"], "revision":1,"matches_expected_revision":True,"observed_monotonic_ns":200}
        self.logs["linux"][1]["application"]=[observed]
        att={"run_id":self.run,"physical_hosts":["linux","windows"],"reviewer":"fixture-reviewer","both_displays_artifact":"both-hosts.mp4#sha256-fixture","cable_disconnect_artifact":"cable.mp4#sha256-fixture"}
        result=reconcile(self.cfg,self.run,self.logs,owner,att)
        self.assertEqual("passed",result["status"])
        self.assertEqual(.00005,result["ack_to_observed_ms"]["p95"])
        bad=copy.deepcopy(owner);bad["latency_markers"][0]["clock_domain"]="other"
        with self.assertRaises(ValueError):reconcile(self.cfg,self.run,self.logs,bad,att)
        bad=copy.deepcopy(owner);bad["observed"]["checksum"]["partial"]=True
        with self.assertRaises(ValueError):reconcile(self.cfg,self.run,self.logs,bad,att)
        bad=copy.deepcopy(self.logs);bad["linux"][1]["application"][0]["hazard_id"]="wrong"
        self.assertEqual("failed",reconcile(self.cfg,self.run,bad,owner,att)["status"])

    def test_http_deadline_includes_trickled_headers_and_redacts_credentials(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path=="/slow":
                    try:
                        for b in b"HTTP/1.1 200 OK\r\n": self.connection.send(bytes([b]));time.sleep(.3)
                    except OSError:pass
                    return
                self.send_response(200);self.end_headers()
                self.wfile.write(b'app_raw_bmkg_prepared_through 7\nprivate_token secret\n')
            def log_message(self,*args):pass
        server=ThreadingHTTPServer(("127.0.0.1",0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            base="http://127.0.0.1:"+str(server.server_port)
            result=application_sample({"name":"m","kind":"metrics","url":base+"/metrics","token":"secret"})
            self.assertEqual({"raw_bmkg_prepared_through":7},result["metrics"])
            self.assertNotIn("secret",json.dumps(result))
            started=time.monotonic()
            with self.assertRaises((TimeoutError,OSError)):fetch(base+"/slow",{})
            self.assertLess(time.monotonic()-started,2.7)
        finally:
            server.shutdown();server.server_close()


if __name__ == "__main__":
    unittest.main()
