"""Bounded, host-local observers. No network faults or application writes here."""
import base64
import concurrent.futures
import hashlib
import http.client
import io
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import socket
import ssl
import subprocess
import sys
import time
from urllib.parse import urlsplit, parse_qs
import uuid

from .inventory import fingerprint, require

RUN = re.compile(r"^[a-f0-9]{32}$")
MAX_LOG = 8 << 20
MAX_FETCH = 1 << 20
METRIC = re.compile(r"^app_((?:raw_bmkg|wildfire)_(?:received_through|prepared_through|applied_through)|outbox_pending)\s+([0-9.eE+-]+)$")


def download_collector(client, run_id, destination, *, deadline=None, sleep=time.sleep):
    """Resume the original bounded log; validate its final count and digest."""
    destination = Path(destination)
    require(not destination.is_symlink(), "collector download cannot be a symlink")
    offset = destination.stat().st_size if destination.exists() else 0
    require(offset <= MAX_LOG, "existing collector download exceeds quota")
    checksum = hashlib.sha256()
    if offset:
        with destination.open("rb") as existing:
            while chunk := existing.read(65536):
                checksum.update(chunk)
    with destination.open("ab") as output:
        while True:
            piece = client.call("collector-fetch", run_id=run_id, offset=offset)
            raw = base64.b64decode(piece["data"], validate=True)
            require(len(raw) <= MAX_FETCH and len(raw) == piece["bytes"] and offset + len(raw) <= MAX_LOG,
                    "collector transfer quota")
            if raw:
                output.write(raw)
                output.flush()
                os.fsync(output.fileno())
                checksum.update(raw)
                offset += len(raw)
            elif piece["done"]:
                require(piece["terminal"]["bytes"] == offset and piece["terminal"]["sha256"] == checksum.hexdigest(),
                        "collector terminal digest/count differs; preserve original download")
                return {"bytes": offset, "sha256": checksum.hexdigest()}
            else:
                require(deadline is not None and time.monotonic() < deadline,
                        "collector incomplete; recover logs under original run identity")
                sleep(1)
            require(deadline is None or time.monotonic() < deadline, "collector download deadline exceeded")


def fetch(url, headers):
    """Absolute two-second budget, including trickled headers/body; no redirects/proxies."""
    u = urlsplit(url)
    deadline = time.monotonic() + 2
    sock = socket.create_connection((u.hostname, u.port or (443 if u.scheme == "https" else 80)), timeout=2)
    class DeadlineRaw(io.RawIOBase):
        def readable(self):
            return True
        def readinto(self, buffer):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("probe deadline")
            sock.settimeout(remaining)
            return sock.recv_into(buffer)
    class Connection:
        def makefile(self, *args, **kwargs):
            return io.BufferedReader(DeadlineRaw())
    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("probe deadline")
        sock.settimeout(remaining)
        if u.scheme == "https":
            sock = ssl.create_default_context().wrap_socket(sock, server_hostname=u.hostname)
        path = u.path + (("?" + u.query) if u.query else "")
        request = f"GET {path} HTTP/1.1\r\nHost: {u.netloc}\r\nConnection: close\r\n"
        request += "".join(f"{k}: {v}\r\n" for k,v in headers.items()) + "\r\n"
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("probe deadline")
        sock.settimeout(remaining)
        sock.sendall(request.encode("ascii"))
        response = http.client.HTTPResponse(Connection())
        response.begin()
        try:
            raw = response.read((1 << 20) + 1)
            require(len(raw) <= 1 << 20, "probe response quota")
            return response.status, raw
        finally:
            response.close()
    finally:
        sock.close()


def validate_probes(probes, cfg):
    require(isinstance(probes, list) and len(probes) <= 8, "at most eight application probes")
    lan = ipaddress.ip_network(cfg["network"]["cidr"])
    names = set()
    for p in probes:
        require(re.fullmatch(r"[a-z0-9-]{1,40}", p["name"]) and p["name"] not in names, "duplicate/invalid probe name")
        names.add(p["name"])
        require(p["kind"] in ("metrics", "entity"), "only metrics or canonical entity GET probes supported")
        u = urlsplit(p["url"])
        require(u.scheme in ("http", "https") and not u.username and not u.password and not u.fragment and
                ipaddress.ip_address(u.hostname) in lan, "probe requires an explicit trusted LAN IP URL")
        require(u.path == ("/metrics" if p["kind"] == "metrics" else "/internal/canonical/entities"), "probe path is not allowlisted")
        require(not any(c in p.get("token", "") for c in "\r\n\x00"), "invalid bearer credential")
        if p["kind"] == "entity":
            require(isinstance(p.get("operation_id"), str) and p["operation_id"] and type(p.get("expected_revision")) is int and p["expected_revision"] > 0,
                    "entity probes require original operation identity and expected revision")
            query = parse_qs(u.query, strict_parsing=True)
            require(set(query) == {"source", "source_ref_id"} and all(len(v) == 1 and v[0] for v in query.values()), "entity probe requires exact source-pair scope")
    return probes


def application_sample(p):
    headers = {"Authorization": "Bearer " + p["token"]} if p.get("token") else {}
    start = time.monotonic_ns()
    out = {"probe": p["name"], "kind": p["kind"], "observed_monotonic_ns": start, "available": False}
    try:
        status, raw = fetch(p["url"], headers)
        out["status"] = status
        if status != 200:
            out["duration_ms"] = (time.monotonic_ns() - start) / 1e6
            return out
        if p["kind"] == "metrics":
            metrics = {}
            for line in raw.decode().splitlines():
                m = METRIC.fullmatch(line)
                if m:
                    value = float(m[2])
                    require(math.isfinite(value) and value >= 0, "invalid metric sample")
                    metrics[m[1]] = value
            out["metrics"] = metrics
            out["authority"] = "process telemetry; not a durable receipt/frontier proof"
        else:
            v = json.loads(raw)
            if "data" in v:
                v = v["data"]
            require(type(v.get("revision")) is int, "canonical response lacks revision")
            query = parse_qs(urlsplit(p["url"]).query)
            require(v.get("source") == query["source"][0] and v.get("source_ref_id") == query["source_ref_id"][0], "canonical identity differs from probe")
            out.update(operation_id=p["operation_id"], revision=v["revision"], hazard_id=v.get("hazard_id"), active=v.get("active"),
                       source=v["source"], source_ref_id=v["source_ref_id"],
                       expected_revision=p["expected_revision"], matches_expected_revision=v["revision"] == p["expected_revision"])
        out["available"] = True
    except Exception:
        out["reason"] = "unavailable_or_invalid_response"
    out["duration_ms"] = (time.monotonic_ns() - start) / 1e6
    out["observed_monotonic_ns"] = time.monotonic_ns()
    return out


def tcp_sample(target):
    start = time.monotonic_ns()
    available = False
    try:
        with socket.create_connection((target["address"], target["port"]), timeout=2):
            available = True
    except OSError:
        pass
    return {**target, "available": available, "connect_rtt_ms": (time.monotonic_ns() - start) / 1e6}


def collector_paths(agent, run):
    require(RUN.fullmatch(run), "invalid collector run id")
    return agent.path(f"collectors/{run}.request.json"), agent.path(f"collectors/{run}.jsonl")


def collector_operation(agent, op, args):
    request, log = collector_paths(agent, args["run_id"])
    if op == "collector-fetch":
        require(type(args.get("offset")) is int and 0 <= args["offset"] <= MAX_LOG, "invalid log offset")
        if not log.exists():
            return {"bytes": 0, "data": "", "done": False}
        require(log.stat().st_size <= MAX_LOG and args["offset"] <= log.stat().st_size, "collector quota/offset")
        with log.open("rb") as f:
            f.seek(args["offset"])
            data = f.read(MAX_FETCH)
        # The logs contain no endpoint URLs/credentials or complete application payloads.
        import base64
        terminal = agent.path(f"collectors/{args['run_id']}.done.json")
        done = json.loads(terminal.read_text()) if terminal.exists() else None
        return {"bytes": len(data), "data": base64.b64encode(data).decode(), "done": done is not None, "terminal": done}
    duration = args.get("seconds")
    require(type(duration) is int and 60 <= duration <= 600, "collector duration must be 60..600 seconds")
    probes = validate_probes(args.get("probes", []), agent.cfg)
    spec = {"inventory": agent.cfg, "host": agent.host["id"], "run_id": args["run_id"], "seconds": duration, "probes": probes}
    if request.exists():
        prior = json.loads(request.read_text())
        require({k: v for k, v in prior.items() if k != "clock_domain"} == spec, "collector identity reused with different content")
        require(log.exists(), "uncertain collector start; preserve original identity and inspect locally")
        return {"run_id": args["run_id"], "clock_domain": prior["clock_domain"], "outcome": "existing"}
    require(not log.exists(), "unmarked collector log")
    # Private request can contain bearer tokens; Windows inherits the root ACL.
    spec["clock_domain"] = uuid.uuid4().hex
    agent._json(f"collectors/{args['run_id']}.request.json", spec)
    script = Path(__file__).resolve().parents[1] / "platform-collector.py"
    kwargs = {"stdin": subprocess.DEVNULL, "stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL, "close_fds": True}
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    else:
        kwargs["start_new_session"] = True
    child = subprocess.Popen([sys.executable, script, str(request)], **kwargs)
    return {"run_id": args["run_id"], "clock_domain": spec["clock_domain"], "pid": child.pid, "outcome": "started"}


def collect(request):
    from .host_agent import HostAgent, machine_values
    spec = json.loads(Path(request).read_text())
    agent = HostAgent(spec["inventory"], spec["host"])
    agent.check_owner()
    canonical, log = collector_paths(agent, spec["run_id"])
    require(Path(request).resolve() == canonical.resolve(), "collector request outside owned state")
    validate_probes(spec["probes"], agent.cfg)
    require(type(spec["seconds"]) is int and 60 <= spec["seconds"] <= 600, "invalid collector deadline")
    total, available, machine = machine_values()
    targets = [{"node": n["name"], "host": n["physical_host"], "address": n["address"], "port": 6443 if n["role"] == "server" else 22} for n in agent.cfg["nodes"]]
    started = time.monotonic()
    def emit(f, item):
        raw = (json.dumps(item, separators=(",", ":")) + "\n").encode()
        require(f.tell() + len(raw) <= MAX_LOG, "collector log quota exhausted")
        f.write(raw)
        f.flush()
    with log.open("xb") as f:
        os.chmod(log, 0o600)
        emit(f, {"type": "start", "run_id": spec["run_id"], "host": agent.host["id"], "inventory_sha256": fingerprint(agent.cfg),
                 "clock_domain": spec["clock_domain"],
                 "machine_sha256": machine, "hostname": __import__("platform").node(), "wall_time_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns(),
                 "clock_uncertainty_ms": None, "one_way_latency": "unavailable: synchronized clock error bound not established"})
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            # Reserve the worst bounded iteration (TCP+HTTP+link) before expiry.
            while time.monotonic() - started < spec["seconds"] - 8:
                at = time.monotonic()
                samples = list(pool.map(tcp_sample, targets))
                app = list(pool.map(application_sample, spec["probes"]))
                try:
                    link = agent.link()
                except Exception:
                    link = {"up": None, "reason": "link_state_unavailable"}
                _, available, _ = machine_values()
                emit(f, {"type": "sample", "monotonic_ns": time.monotonic_ns(), "wall_time_ns": time.time_ns(), "link": link,
                         "available_mib": available, "tcp": samples, "application": app})
                # Bound the loop including slow probes; never subtract host clocks.
                time.sleep(max(0, 1 - (time.monotonic() - at)))
        emit(f, {"type": "end", "monotonic_ns": time.monotonic_ns(), "elapsed_seconds": time.monotonic() - started})
    agent._json(f"collectors/{spec['run_id']}.done.json", {"sha256": hashlib.sha256(log.read_bytes()).hexdigest(), "bytes": log.stat().st_size})


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * p) - 1)]


def reconcile(cfg, run_id, logs, application_evidence=None, demo_attestation=None):
    require(RUN.fullmatch(run_id), "invalid run id")
    result = {"version": 1, "run_id": run_id, "inventory_sha256": fingerprint(cfg), "status": "blocked", "hosts": {}, "assertions": {},
              "bonus": "physical-host evidence only; no AI/autoscaling claim", "limitations": ["TCP connect RTT is not one-way latency", "physical displays/cable removal require demo evidence"]}
    machines = set()
    for host in cfg["hosts"]:
        entries = logs.get(host["id"], [])
        starts = [x for x in entries if x.get("type") == "start"]
        require(len(starts) == 1 and starts[0]["run_id"] == run_id and starts[0]["host"] == host["id"] and starts[0]["inventory_sha256"] == fingerprint(cfg), "collector identity/scope mismatch")
        machines.add(starts[0]["machine_sha256"])
        times = [x["monotonic_ns"] for x in entries]
        require(all(type(v) is int for v in times) and all(b > a for a,b in zip(times, times[1:])), "nonmonotonic/duplicated collector records")
        samples = [x for x in entries if x.get("type") == "sample"]
        end = [x for x in entries if x.get("type") == "end"]
        peer = [p for x in samples for p in x["tcp"] if p["host"] != host["id"]]
        require(all(type(p["available"]) is bool and type(p["connect_rtt_ms"]) in (int,float) and math.isfinite(p["connect_rtt_ms"]) and p["connect_rtt_ms"] >= 0 for p in peer), "invalid RTT evidence")
        good = [p["connect_rtt_ms"] for p in peer if p["available"]]
        down = [i for i, x in enumerate(samples) if x["link"]["up"] is False]
        recovered = bool(down and any(x["link"]["up"] is True for x in samples[max(down) + 1:]))
        app = [p for x in samples for p in x.get("application", [])]
        observations = [p for p in app if p.get("kind") == "entity" and p.get("available")]
        metrics = [{"monotonic_ns": x["monotonic_ns"], "probes": [{"probe": p["probe"], "metrics": p.get("metrics", {})} for p in x.get("application", []) if p["kind"] == "metrics"]} for x in samples]
        result["hosts"][host["id"]] = {"complete": len(end) == 1, "samples": len(samples), "peer_unavailable": len(peer) - len(good),
            "rtt_ms": {"p50": percentile(good, .5), "p95": percentile(good, .95), "p99": percentile(good, .99)},
            "physical_link_down_observed": bool(down), "link_recovered": recovered,
            "entity_observations": observations, "progress_samples": metrics}
    result["assertions"]["two_machine_identities"] = len(machines) == 2
    result["assertions"]["collectors_completed"] = all(h["complete"] for h in result["hosts"].values())
    result["assertions"]["real_link_loss_and_restore"] = any(h["link_recovered"] for h in result["hosts"].values())
    result["assertions"]["both_hosts_measured_peer_traffic"] = all(h["rtt_ms"]["p50"] is not None and h["peer_unavailable"] > 0 for h in result["hosts"].values())
    # Owner evidence is the existing independent recovery schema, not a caller's passed flag.
    if application_evidence is not None:
        from recovery_checks import verify
        checked = verify(application_evidence)
        require(application_evidence.get("network_run_id") == run_id, "application evidence belongs to another network run")
        require(application_evidence.get("checkpoint", {}).get("stable") is True and application_evidence["checkpoint"].get("checksum") is not None, "stable complete population boundary required")
        result["application_reconciliation"] = checked
        result["assertions"]["application_reconciled"] = checked["status"] == "passed"
        # Source acknowledgement start markers and observer times must share one machine and boot/clock domain.
        latency = []
        for marker in application_evidence.get("latency_markers", []):
            host = marker["host"]
            start = marker["ack_monotonic_ns"]
            require(host in logs and marker["machine_sha256"] in machines and type(start) is int, "invalid acknowledgement timing origin")
            origin = next(x for x in logs[host] if x.get("type") == "start")
            require(marker["machine_sha256"] == origin["machine_sha256"] and marker["clock_domain"] == origin["clock_domain"], "cross-host/collector timing subtraction refused")
            ack = next((a for a in application_evidence["acknowledgements"] if a["operation_id"] == marker["operation_id"] and a["source"] == marker["source"]), None)
            require(ack is not None, "timing marker has no acknowledged original operation")
            require(start >= origin["monotonic_ns"], "acknowledgement predates the observer clock domain")
            seen = [p["observed_monotonic_ns"] for p in result["hosts"][host]["entity_observations"] if p["operation_id"] == marker["operation_id"] and p.get("matches_expected_revision") and
                    p["source"] == ack["source"] and p["source_ref_id"] == ack["source_ref_id"] and p["hazard_id"] == ack["hazard_id"] and p["revision"] == ack["revision"] and p["observed_monotonic_ns"] >= start]
            if seen:
                latency.append((min(seen) - start) / 1e6)
        result["ack_to_observed_ms"] = {"count": len(latency), "p95": percentile(latency, .95), "includes_polling_delay": True}
        result["assertions"]["data_lag_measured"] = bool(latency) and len(latency) == len(application_evidence.get("latency_markers", []))
    else:
        result["application_reconciliation"] = {"status": "blocked", "reason": "actual owner acknowledgements, checksum and timing markers absent"}
    if demo_attestation is not None:
        require(set(demo_attestation) == {"run_id", "physical_hosts", "both_displays_artifact", "cable_disconnect_artifact", "reviewer"}, "unknown demo attestation fields")
        require(demo_attestation.get("run_id") == run_id and demo_attestation.get("physical_hosts") == sorted(result["hosts"]), "demo attestation scope mismatch")
        require(demo_attestation.get("both_displays_artifact") and demo_attestation.get("cable_disconnect_artifact") and demo_attestation.get("reviewer"), "physical demo references/reviewer required")
        result["demo_attestation"] = demo_attestation
    required = ["two_machine_identities", "collectors_completed", "real_link_loss_and_restore", "both_hosts_measured_peer_traffic"]
    if not all(result["assertions"][k] for k in required):
        result["status"] = "failed"
    elif application_evidence is not None and result["application_reconciliation"]["status"] == "blocked":
        result["status"] = "blocked"
    elif application_evidence is not None and not application_evidence.get("latency_markers"):
        result["status"] = "blocked"
    elif application_evidence is not None and not all(result["assertions"].get(k, False) for k in ("application_reconciled", "data_lag_measured")):
        result["status"] = "failed"
    elif application_evidence is not None and demo_attestation is not None:
        result["status"] = "passed"
    return result
