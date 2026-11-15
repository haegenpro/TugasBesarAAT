"""Ephemeral, labelled, unprivileged cross-node DNS/pod/service checks."""
import ipaddress
import json
import re
import uuid

from .inventory import require, fingerprint

SERVER = """import http.server,json,os
class H(http.server.BaseHTTPRequestHandler):
 def do_GET(self):
  b=json.dumps({'node':os.environ['NODE_NAME']}).encode();self.send_response(200);self.end_headers();self.wfile.write(b)
 def log_message(self,*a):pass
http.server.HTTPServer(('0.0.0.0',8080),H).serve_forever()
"""
CLIENT = """import json,socket,sys,urllib.request
targets=json.loads(sys.argv[1]);socket.getaddrinfo('kubernetes.default.svc.cluster.local',443)
o=urllib.request.build_opener(urllib.request.ProxyHandler({}));out=[]
for t in targets:
 with o.open(t['url'],timeout=2) as r:v=json.loads(r.read(1024))
 assert v['node'] in t['nodes'];out.append(v)
print(json.dumps({'dns':True,'peers':out}))
"""


def objects(cfg, image, run):
    require(re.fullmatch(r"[a-z0-9./:_-]+@sha256:[a-f0-9]{64}", image), "supply an immutable Python3 probe image")
    ns = cfg["namespace"]
    labels = {"platform.aat/cluster": cfg["cluster_id"], "platform.aat/network-probe": run, "role": "other"}
    items = []
    for n in cfg["nodes"]:
        name = "net-" + run + "-" + n["name"].rsplit("-", 1)[1]
        items.append({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": ns, "labels": labels}, "spec": {
            "automountServiceAccountToken": False, "restartPolicy": "Never", "activeDeadlineSeconds": 180,
            "nodeSelector": {"kubernetes.io/hostname": n["name"]},
            "securityContext": {"runAsNonRoot": True, "runAsUser": 65532, "seccompProfile": {"type": "RuntimeDefault"}},
            "containers": [{"name": "probe", "image": image, "command": ["python3", "-B", "-c", SERVER],
                "env": [{"name": "NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}}],
                "resources": {"requests": {"memory": "128Mi", "cpu": "50m"}, "limits": {"memory": "256Mi", "cpu": "500m"}},
                "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
                "readinessProbe": {"tcpSocket": {"port": 8080}, "periodSeconds": 1}}]}})
    service = "net-" + run
    items.append({"apiVersion": "v1", "kind": "Service", "metadata": {"name": service, "namespace": ns, "labels": labels},
                  "spec": {"selector": {"platform.aat/network-probe": run}, "ports": [{"port": 8080, "targetPort": 8080}]}})
    selector = {"matchLabels": {"platform.aat/network-probe": run}}
    items.append({"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": {"name": service, "namespace": ns, "labels": labels},
                  "spec": {"podSelector": selector, "policyTypes": ["Ingress", "Egress"],
                    "ingress": [{"from": [{"podSelector": selector}], "ports": [{"port": 8080, "protocol": "TCP"}]}],
                    "egress": [{"to": [{"podSelector": selector}], "ports": [{"port": 8080, "protocol": "TCP"}]},
                               {"to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}},
                                        "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}}}], "ports": [{"port": 53, "protocol": p} for p in ("UDP", "TCP")]}]}})
    return items


def verify_network(cluster, image):
    cfg, ns = cluster.cfg, cluster.cfg["namespace"]
    run = uuid.uuid4().hex[:12]
    items = objects(cfg, image, run)
    report = {"version": 1, "run_id": run, "inventory_sha256": fingerprint(cfg), "status": "failed", "image": image, "results": [], "cleanup": []}
    try:
        cluster.kubectl("apply", "-f", "-", input=json.dumps({"apiVersion": "v1", "kind": "List", "items": items}).encode())
        pods = [v for v in items if v["kind"] == "Pod"]
        cluster.kubectl("wait", "-n", ns, "--for=condition=Ready", *["pod/" + p["metadata"]["name"] for p in pods], "--timeout=90s", timeout=100)
        addresses = []
        for p, n in zip(pods, cfg["nodes"]):
            v = json.loads(cluster.kubectl("get", "pod", p["metadata"]["name"], "-n", ns, "-o", "json").stdout)
            address = v["status"]["podIP"]
            require(ipaddress.ip_address(address) in ipaddress.ip_network(cfg["pod_cidr"]) and v["spec"]["nodeName"] == n["name"], "probe placement/address differs")
            addresses.append({"url": "http://" + address + ":8080", "nodes": [n["name"]]})
        addresses.append({"url": "http://net-" + run + "." + ns + ".svc.cluster.local:8080", "nodes": [n["name"] for n in cfg["nodes"]]})
        for p, n in zip(pods, cfg["nodes"]):
            value = json.loads(cluster.kubectl("exec", "-n", ns, p["metadata"]["name"], "--", "python3", "-B", "-c", CLIENT, json.dumps(addresses), timeout=25).stdout)
            require(value.get("dns") is True and len(value.get("peers", [])) == len(addresses) and
                    all(peer.get("node") in target["nodes"] for peer, target in zip(value["peers"], addresses)), "incomplete peer/DNS proof")
            report["results"].append({"node": n["name"], "physical_host": n["physical_host"], **value})
        require(len(report["results"]) == 4, "incomplete peer matrix")
        report["status"] = "passed"
    finally:
        for item in items:
            kind, name = item["kind"], item["metadata"]["name"]
            try:
                current = cluster.kubectl("get", kind, name, "-n", ns, "-o", "json", "--ignore-not-found", timeout=10)
                if current.stdout.strip():
                    labels = json.loads(current.stdout)["metadata"]["labels"]
                    require(labels.get("platform.aat/network-probe") == run and labels.get("platform.aat/cluster") == cfg["cluster_id"], "unowned probe cleanup refused")
                    cluster.kubectl("delete", kind, name, "-n", ns, "--wait=false", timeout=10)
                report["cleanup"].append({"kind": kind, "name": name, "status": "passed"})
            except Exception:
                report["status"] = "failed"
                report["cleanup"].append({"kind": kind, "name": name, "status": "failed"})
        (cluster.state / "evidence" / "network-probe.json").write_text(json.dumps(report, indent=2))
    require(report["status"] == "passed", "network probe/cleanup failed; inspect network-probe.json")
    return report
