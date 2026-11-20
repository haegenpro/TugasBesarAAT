"""Controller-side observation and reconciliation of a real Wi-Fi cut on the existing cluster.

Host-local logs (scripts/platform-host-probe.py) prove the adapter really went
down and came back, and measure inter-node latency on the real network. The
controller log shows how the cluster saw the cut: the faulted node's kubelet
stops answering and the Kubernetes node returns to Ready afterwards. Network
evidence alone never awards the bonus: application/demo evidence is required.
"""
import json
import time

from .existing import node_ready
from .inventory import fingerprint, require

MAX_LOG = 8 << 20


def percentile(values, p):
    values = sorted(v for v in values if v is not None)
    if not values:
        return None
    return values[min(len(values) - 1, int(round(p / 100 * (len(values) - 1))))]


def observe(cluster, run_id, seconds, stream, *, sleep=time.sleep, clock=time.monotonic):
    """Every second: each node's kubelet reachability/RTT from the controller and its API Ready state."""
    started = clock();tick = 0
    stream.write(json.dumps({"type": "start", "run_id": run_id, "seconds": seconds, "wall_time_ns": time.time_ns()}) + "\n")
    while clock() - started < seconds:
        result = cluster.kubectl("get", "nodes", "-o", "json", "--request-timeout=3s", check=False)
        ready = {}
        if result.returncode == 0:
            ready = {n["metadata"]["name"]: node_ready(n) for n in json.loads(result.stdout)["items"]}
        stream.write(json.dumps({"type": "sample", "run_id": run_id, "elapsed_s": round(clock() - started, 3), "wall_time_ns": time.time_ns(),
                                 "api_available": result.returncode == 0,
                                 "nodes": [{"node": n["name"], "reachable": cluster.reachable(n["address"]), "ready": ready.get(n["name"])} for n in cluster.cfg["nodes"]]}) + "\n")
        stream.flush();tick += 1
        sleep(max(0.0, started + tick - clock()))
    stream.write(json.dumps({"type": "end", "run_id": run_id, "wall_time_ns": time.time_ns()}) + "\n")


def reconcile(cfg, run_id, host_logs, controller_log, application_evidence=None, demo_attestation=None):
    """host_logs: {physical_host: [records]}; controller_log: [records]."""
    require(cfg.get("version") == 3, "existing-cluster inventory required")
    hosts = {n["physical_host"]: n for n in cfg["nodes"]}
    result = {"version": 1, "run_id": run_id, "inventory_sha256": fingerprint(cfg), "status": "blocked", "hosts": {}, "assertions": {}}
    faulted = []
    for host, node in hosts.items():
        records = host_logs.get(host, [])
        require(all(r.get("run_id") == run_id for r in records), "host log belongs to another run: " + host)
        start = [r for r in records if r["type"] == "start"];end = [r for r in records if r["type"] == "end"]
        samples = [r for r in records if r["type"] == "sample"]
        disabled = [r for r in records if r["type"] == "link-disabled"];enabled = [r for r in records if r["type"] == "link-enabled"]
        down = [i for i, s in enumerate(samples) if s["link_up"] is False]
        recovered = bool(down and any(s["link_up"] is True for s in samples[down[-1] + 1:]))
        peers = [p["rtt_ms"] for s in samples if s["link_up"] for p in s["peers"] if p["node"] != node["name"]]
        cut = bool(start and start[0].get("disconnect"))
        if cut:faulted.append(host)
        result["hosts"][host] = {"node": node["name"], "complete": len(start) == 1 and len(end) == 1, "samples": len(samples),
                                 "disconnect_planned": cut, "adapter_disabled": len(disabled) >= 1, "adapter_restored": len(enabled) >= len(disabled) >= 1 if cut else True,
                                 "link_down_observed": bool(down), "link_recovered": recovered,
                                 "peer_rtt_ms": {"p50": percentile(peers, 50), "p95": percentile(peers, 95), "samples": sum(v is not None for v in peers)}}
    controller = [r for r in controller_log if r.get("type") == "sample"]
    require(all(r.get("run_id") == run_id for r in controller_log), "controller log belongs to another run")
    assertions = result["assertions"]
    assertions["all_hosts_logged"] = all(result["hosts"][h]["complete"] for h in hosts)
    assertions["exactly_one_faulted_host"] = len(faulted) == 1
    if len(faulted) == 1:
        host = faulted[0];entry = result["hosts"][host];name = hosts[host]["name"]
        assertions["real_link_loss_and_restore"] = entry["adapter_disabled"] and entry["adapter_restored"] and entry["link_down_observed"] and entry["link_recovered"]
        states = [next(n for n in s["nodes"] if n["node"] == name) for s in controller]
        lost = [i for i, n in enumerate(states) if not n["reachable"]]
        assertions["cluster_observed_node_loss"] = bool(lost)
        assertions["node_rejoined_ready"] = bool(lost and any(n["reachable"] and n["ready"] for n in states[lost[-1] + 1:]))
        assertions["other_nodes_stayed_reachable"] = bool(controller) and all(n["reachable"] for s in controller for n in s["nodes"] if n["node"] != name)
        result["fault"] = {"host": host, "node": name, "controller_unreachable_seconds": len(lost)}
    assertions["inter_node_latency_measured"] = all(result["hosts"][h]["peer_rtt_ms"]["samples"] > 0 for h in hosts)
    network_ok = bool(assertions) and all(assertions.values())
    if not network_ok:
        result["status"] = "failed"
    elif application_evidence and demo_attestation:
        result["application_evidence"] = application_evidence;result["demo_attestation"] = demo_attestation
        result["status"] = "passed"
    else:
        result["missing"] = [k for k, v in (("application_evidence", application_evidence), ("demo_attestation", demo_attestation)) if not v]
    return result
