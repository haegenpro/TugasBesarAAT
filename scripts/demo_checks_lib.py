"""Live M2 demo checks: trigger, observe, collect evidence and decide pass/fail.

Every check writes evidence/platform-demo/<check>-<run>/result.json. A check
passes only from observed data; missing inputs make it *blocked*, never passed.
Spec references (IF4031 M2) are given per check. Uses only public contracts
where possible: the Audit API (Lampiran A), Client API, upstream mock request
logs (acquisition request_id/epoch) and kubectl.
"""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

from storage_lib import require, write_json

ROOT = Path(__file__).resolve().parent.parent
SOURCES = ("BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class Api:
    """Small JSON client; bearer token from a private file, never logged."""
    def __init__(self, base, token=None, opener=None):
        require(isinstance(base, str) and base.startswith(("http://", "https://")), "http(s) base URL required")
        self.base, self.token, self.opener = base.rstrip("/"), token, opener or build_opener(NoRedirect)

    def request(self, method, path, body=None, params=None, headers=None, timeout=10, raw=None):
        url = self.base + path + ("?" + urlencode({k: v for k, v in params.items() if v is not None}) if params else "")
        data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
        request = Request(url, data=data, method=method)
        if self.token:
            request.add_header("Authorization", "Bearer " + self.token)
        if data is not None and "Content-Type" not in (headers or {}):
            request.add_header("Content-Type", "application/json")
        for key, value in (headers or {}).items():
            request.add_header(key, value)
        started = time.perf_counter()
        try:
            with self.opener.open(request, timeout=timeout) as response:
                status, payload = response.status, response.read()
        except HTTPError as exc:
            status, payload = exc.code, exc.read()
        except (URLError, OSError, TimeoutError) as exc:
            return {"status": None, "json": None, "ms": round((time.perf_counter() - started) * 1000, 3), "error": type(exc).__name__}
        try:
            parsed = json.loads(payload) if payload else None
        except ValueError:
            parsed = None
        return {"status": status, "json": parsed, "ms": round((time.perf_counter() - started) * 1000, 3)}


def read_token(path):
    if path is None:
        return None
    from release_config_lib import read_token as private_token
    return private_token(path)


class Evidence:
    def __init__(self, check, output=None):
        self.run_id = uuid.uuid4().hex[:12]
        self.dir = Path(output) if output else ROOT / "evidence" / "platform-demo" / (check + "-" + self.run_id)
        require(not self.dir.exists(), "evidence directory exists; keep earlier results")
        self.dir.mkdir(parents=True, mode=0o700)
        self.result = {"version": 1, "check": check, "run_id": self.run_id, "started_at": now(), "passed": False, "status": "running",
                       "assertions": {}, "events": []}

    def event(self, action, **fields):
        self.result["events"].append({"at": now(), "action": action, **fields})
        self.save()

    def save(self):
        write_json(self.dir / "result.json", self.result)

    def finish(self, blocked=()):
        assertions = self.result["assertions"]
        self.result["blocked"] = list(blocked)
        if blocked:
            self.result["status"] = "blocked"
        else:
            self.result["passed"] = bool(assertions) and all(v is True for v in assertions.values())
            self.result["status"] = "passed" if self.result["passed"] else "failed"
        self.result["finished_at"] = now()
        self.save()
        return self.result


def now():
    return datetime.now(timezone.utc).isoformat()


def checksum(ids):
    """Audit API checksum: SHA-256 of byte-sorted source_ref_ids joined by newline."""
    return hashlib.sha256("\n".join(sorted(ids, key=lambda v: v.encode())).encode()).hexdigest()


def traverse(api, path, params=None, limit=1000, max_pages=10000):
    """Follow next_cursor; return items plus partial flags seen on any page."""
    items, partial, units, cursor = [], False, set(), None
    for _ in range(max_pages):
        response = api.request("GET", path, params={**(params or {}), "limit": limit, "cursor": cursor}, timeout=30)
        require(response["status"] == 200 and isinstance(response["json"], dict), path + " returned " + str(response["status"]))
        page = response["json"]
        items += page.get("items", page.get("data", []))
        partial = partial or bool(page.get("partial"))
        units.update(page.get("unavailable_units") or [])
        cursor = page.get("next_cursor")
        if not cursor:
            return items, {"partial": partial, "unavailable_units": sorted(units)}
    raise ValueError(path + " exceeded the page bound")


def ordered(items, keys):
    values = [tuple(item.get(k) for k in keys) for item in items]
    return all(a <= b for a, b in zip(values, values[1:]))


def kubectl_json(cluster, *args):
    return json.loads(cluster.kubectl(*args, "-o", "json", "--request-timeout=10s").stdout)


def pods(cluster, namespace, app):
    found = kubectl_json(cluster, "get", "pods", "-n", namespace, "-l", "app.kubernetes.io/name=" + app)["items"]
    return [p for p in found if p["status"].get("phase") == "Running" and not p["metadata"].get("deletionTimestamp")]


def restarts(cluster, namespace):
    found = kubectl_json(cluster, "get", "pods", "-n", namespace)["items"]
    return {p["metadata"]["name"]: sum(c.get("restartCount", 0) for c in p["status"].get("containerStatuses", [])) for p in found}


def kill_pod(cluster, namespace, name):
    """Abrupt crash: no graceful shutdown, so leases must expire instead of being released."""
    cluster.kubectl("delete", "pod", name, "-n", namespace, "--grace-period=0", "--force", "--wait=false", check=False)


# --------------------------------------------------------------------------- P1

def distribution(cluster, audit, inventory, *, client=None, sample=0, evidence):
    """P1.1-1.3: no node holds every HazardEvent, load spread, ordered cross-location reads."""
    events, flags = traverse(audit, "/internal/audit/events")
    a = evidence.result["assertions"]
    a["audit_complete_not_partial"] = not flags["partial"]
    a["audit_order_ingested_at_then_hazard_id"] = ordered(events, ("ingested_at", "hazard_id"))
    a["audit_unique_hazard_ids"] = len({e["hazard_id"] for e in events}) == len(events)
    keys = [(e["source"], e["source_ref_id"]) for e in events]
    a["no_duplicate_source_ref_ids"] = len(set(keys)) == len(keys)
    sums = {}
    for source in sorted({e["source"] for e in events}):
        response = audit.request("GET", "/internal/audit/checksum", params={"source": source})
        require(response["status"] == 200, "checksum " + source + " returned " + str(response["status"]))
        mine = [e["source_ref_id"] for e in events if e["source"] == source]
        sums[source] = {"api": response["json"], "local_count": len(mine), "local_checksum": checksum(mine)}
    a["checksum_matches_event_listing"] = all(v["api"]["count"] == v["local_count"] and v["api"]["checksum"] == v["local_checksum"] for v in sums.values())
    chosen = events if not sample or sample >= len(events) else [events[i * len(events) // sample] for i in range(sample)]
    per_node = {n["name"]: {"primary": 0, "replica": 0, "any": 0} for n in inventory["nodes"]}
    single_home, missing = 0, 0
    for event in chosen:
        response = audit.request("GET", "/internal/audit/location", params={"hazard_id": event["hazard_id"]})
        if response["status"] != 200:
            missing += 1
            continue
        nodes = set()
        for location in response["json"]["locations"]:
            entry = per_node.setdefault(location["node"], {"primary": 0, "replica": 0, "any": 0})
            entry[location["role"]] = entry.get(location["role"], 0) + 1
            nodes.add(location["node"])
        for node in nodes:
            per_node[node]["any"] += 1
        single_home += len(nodes) < 2
    total = len(chosen)
    a["locations_resolved"] = missing == 0 and total > 0
    a["no_node_holds_all_data"] = total > 0 and all(v["any"] < total for v in per_node.values())
    a["primaries_on_more_than_one_node"] = sum(1 for v in per_node.values() if v["primary"]) > 1
    a["every_event_has_two_homes"] = single_home == 0
    evidence.result["distribution"] = {"events": len(events), "sampled": total, "per_node": per_node, "single_home": single_home, "checksums": sums}
    if client is not None:
        rows, page_flags = traverse(client, "/hazards", limit=50, max_pages=200)
        ids = [r.get("hazard_id") for r in rows]
        a["client_pages_unique"] = len(set(ids)) == len(ids)
        a["client_pages_cover_audit"] = set(ids) <= {e["hazard_id"] for e in events} or page_flags["partial"]
        evidence.result["client_pages"] = {"rows": len(rows), **page_flags}
    evidence.result["load"] = node_load(cluster, inventory)
    return evidence


def node_load(cluster, inventory):
    """Per-node CPU/memory and store-pod usage from metrics-server (kubectl top)."""
    nodes = cluster.kubectl("top", "nodes", "--no-headers", check=False).stdout.decode()
    store = cluster.kubectl("top", "pods", "-n", inventory["namespace"], "-l", "role=store", "--no-headers", check=False).stdout.decode()
    placement = {p["metadata"]["name"]: p["spec"].get("nodeName") for p in kubectl_json(cluster, "get", "pods", "-n", inventory["namespace"], "-l", "role=store")["items"]}
    per_node = {}
    for line in store.splitlines():
        parts = line.split()
        if len(parts) >= 3:
            node = placement.get(parts[0], "unknown")
            row = per_node.setdefault(node, {"pods": 0, "cpu_m": 0, "memory_mi": 0})
            row["pods"] += 1
            row["cpu_m"] += int(re.sub(r"\D", "", parts[1]) or 0)
            row["memory_mi"] += int(re.sub(r"\D", "", parts[2]) or 0)
    return {"captured_at": now(), "nodes": nodes.splitlines(), "store_by_node": per_node}


# --------------------------------------------------------------------------- P2

def upstream_requests(cluster, namespace, app, since):
    """Parse acquisition request logs of an upstream mock (request_id, epoch, recovery)."""
    out = cluster.kubectl("logs", "-n", namespace, "-l", "app.kubernetes.io/name=" + app, "--since-time=" + since, "--tail=-1", "--max-log-requests=10", check=False).stdout.decode(errors="replace")
    rows = []
    for line in out.splitlines():
        try:
            value = json.loads(line)
        except ValueError:
            continue
        acquisition = value.get("acquisition")
        if isinstance(acquisition, dict) and acquisition.get("request_id"):
            rows.append({"time": value.get("time"), "path": value.get("path"), "status": value.get("status_code"), **acquisition})
    return sorted(rows, key=lambda r: (r["time"] or "", r["epoch"]))


def single_acquirer(rows):
    """Upstream evidence: one first request per request_id, and no older epoch after a newer one."""
    seen, duplicates, stale, highest = set(), [], [], 0
    for row in rows:
        if row["request_id"] in seen and not row.get("recovery"):
            duplicates.append(row)
        seen.add(row["request_id"])
        if row["epoch"] < highest:
            stale.append(row)
        highest = max(highest, row["epoch"])
    return {"requests": len(rows), "epochs": sorted({r["epoch"] for r in rows}), "duplicate_requests": duplicates[:20], "stale_epoch_requests": stale[:20],
            "passed": bool(rows) and not duplicates and not stale}


def ownership(observer, scope):
    expression = ("SELECT jsonb_build_object('holder',holder,'epoch',epoch,'expires_at',expires_at,'now',clock_timestamp()) "
                  "FROM canonical_ownership WHERE scope=" + literal(scope))
    for suffix in ("storage", "ingestion"):
        try:
            value = observer.query(suffix, expression)["data"]
        except (ValueError, KeyError, RuntimeError):
            continue
        if value:
            value["database"] = suffix
            return value
    raise ValueError("ownership scope not found: " + scope)


def dispatch_after(observer, database, scope, epoch):
    expression = ("SELECT coalesce(jsonb_agg(jsonb_build_object('lane',lane,'epoch',epoch,'state',state,'completed_at',completed_at)),'[]'::jsonb) "
                  "FROM acquisition_dispatch WHERE scope=" + literal(scope) + " AND epoch>=" + str(int(epoch)))
    return observer.query(database, expression)["data"]


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def duplicates_by_source(audit, source):
    events, flags = traverse(audit, "/internal/audit/events", params={"source": source})
    keys = [e["source_ref_id"] for e in events]
    return {"events": len(events), "duplicates": len(keys) - len(set(keys)), **flags}


def takeover(cluster, observer, audit, inventory, *, scope, app, upstream, evidence, limit=60, sleep=time.sleep, clock=time.monotonic):
    """P2.2-2.3: crash the instance holding the acquisition lease; another resumes within <=60s, exactly once."""
    ns = inventory["namespace"]
    since = now()
    before = ownership(observer, scope)
    require(before.get("holder"), "no current owner for " + scope + "; start the acquisition first")
    evidence.event("owner-before", **{k: before[k] for k in ("holder", "epoch", "database")})
    candidates = [p["metadata"]["name"] for p in pods(cluster, ns, app)]
    require(len(candidates) >= 2, "need at least two running " + app + " instances")
    killed, taken, resumed = None, None, None
    for name in candidates:  # the lease holder is not named after its pod: crash until the holder changes
        current = ownership(observer, scope)
        killed_at = clock()
        kill_pod(cluster, ns, name)
        evidence.event("crashed-pod", pod=name)
        deadline = killed_at + 30 + limit
        while clock() < deadline:
            value = ownership(observer, scope)
            if value.get("holder") and value["holder"] != current["holder"]:
                taken = {"pod": name, "previous": current, "new": value, "seconds": round(clock() - killed_at, 3)}
                break
            if value.get("holder") == current["holder"] and value["epoch"] == current["epoch"] and clock() - killed_at > 20:
                break  # this pod was not the holder; its lease kept renewing elsewhere
            sleep(0.5)
        if taken:
            killed = name
            while clock() < killed_at + limit + 30:
                rows = dispatch_after(observer, taken["new"]["database"], scope, taken["new"]["epoch"])
                done = [r for r in rows if r["epoch"] == taken["new"]["epoch"] and r["state"] in ("complete", "pending", "closed")]
                if done:
                    resumed = {"seconds": round(clock() - killed_at, 3), "dispatch": done}
                    break
                sleep(0.5)
            break
    a = evidence.result["assertions"]
    a["lease_holder_crashed_and_replaced"] = taken is not None
    a["new_owner_epoch_increased"] = bool(taken) and taken["new"]["epoch"] > taken["previous"]["epoch"]
    a["acquisition_resumed_within_60s"] = bool(resumed) and resumed["seconds"] <= limit
    evidence.result["takeover"] = {"crashed": killed, "ownership": taken, "resumed": resumed}
    sleep(15)  # let the successor make several more requests before reading upstream logs
    upstream_rows = upstream_requests(cluster, ns, upstream, since)
    analysis = single_acquirer(upstream_rows)
    evidence.result["upstream"] = analysis
    a["upstream_one_requester_no_stale_epoch"] = analysis["passed"]
    source = scope.split(":", 1)[1]
    dup = duplicates_by_source(audit, source)
    evidence.result["canonical"] = dup
    a["no_duplicate_hazard_events"] = dup["duplicates"] == 0 and not dup["partial"]
    return evidence


def fencing(cluster, observer, audit, inventory, *, scope, upstream, since, evidence):
    """P2.4-2.5 analysis after a clock shift or VM freeze: no two owners stored work for the same period."""
    ns = inventory["namespace"]
    analysis = single_acquirer(upstream_requests(cluster, ns, upstream, since))
    evidence.result["upstream"] = analysis
    a = evidence.result["assertions"]
    a["upstream_one_requester_no_stale_epoch"] = analysis["passed"]
    rejected = 0
    for pod in kubectl_json(cluster, "get", "pods", "-n", ns, "-l", "role=aggregator")["items"]:
        log = cluster.kubectl("logs", "-n", ns, pod["metadata"]["name"], "--since-time=" + since, "--all-containers=true", check=False).stdout.decode(errors="replace")
        rejected += log.count("ownership expired before commit") + log.count("P0F01")
    evidence.result["stale_commits_rejected_in_logs"] = rejected
    dup = duplicates_by_source(audit, scope.split(":", 1)[1])
    evidence.result["canonical"] = dup
    a["no_duplicate_hazard_events"] = dup["duplicates"] == 0 and not dup["partial"]
    evidence.result["owner_now"] = ownership(observer, scope)
    return evidence


# --------------------------------------------------------------------------- P3

MALFORMED = {
    "truncated_json": b'{"report_type":"field_observation","occurred_at":"2026-11-16T03:00:00Z","latit',
    "deep_nesting": b'{"a":' * 2000 + b"1" + b"}" * 2000,
    "invalid_utf8": b'{"report_type":"field_observation","description":"\xff\xfe\xfd"}',
    "oversized": b'{"report_type":"field_observation","description":"' + b"x" * (2 << 20) + b'"}',
}


def field_report_body(index):
    return {"report_type": "field_observation", "occurred_at": now()[:19] + "Z", "latitude": -6.9, "longitude": 107.6, "description": "demo-" + str(index)}


def malformed(cluster, client_field, inventory, *, evidence, sleep=time.sleep):
    """P3.3: broken messages are refused without crashing anything; valid messages still pass."""
    ns = inventory["namespace"]
    before = restarts(cluster, ns)
    answers = {}
    for name, body in MALFORMED.items():
        response = client_field.request("POST", "/field-reports", raw=body, headers={"Idempotency-Key": "malformed-" + name + "-" + evidence.run_id}, timeout=30)
        answers[name] = response["status"]
    sleep(10)
    valid = client_field.request("POST", "/field-reports", body=field_report_body(0), headers={"Idempotency-Key": "valid-" + evidence.run_id})
    after = restarts(cluster, ns)
    a = evidence.result["assertions"]
    a["malformed_rejected_with_4xx"] = all(isinstance(s, int) and 400 <= s < 500 for s in answers.values())
    a["no_container_restarted"] = all(after.get(k, v) == v for k, v in before.items())
    a["valid_report_still_accepted"] = valid["status"] == 202
    evidence.result["responses"] = answers
    return evidence


def ops_only(audit_url, ingestion_url, tokens, *, evidence):
    """P3.5: failed messages are visible only to BNPB Internal Ops."""
    results = {}
    for role, token in tokens.items():
        if token is None:
            continue
        results[role] = {"audit_unprocessed": Api(audit_url, token).request("GET", "/internal/audit/unprocessed")["status"],
                         "ops_failures": Api(ingestion_url, token).request("GET", "/internal/ingestion/failures")["status"]}
    a = evidence.result["assertions"]
    own = {"ops": "ops_failures", "audit": "audit_unprocessed"}
    for role, statuses in results.items():
        for name, status in statuses.items():
            if own.get(role) == name:
                a[role + "_can_read_" + name] = status == 200
            elif role not in own:
                a[role + "_denied_" + name] = status in (401, 403)
    evidence.result["access"] = results
    return evidence


def parked(audit, source):
    items, _ = traverse(audit, "/internal/audit/unprocessed", params={"source": source})
    return items


def schema_break(pvmbg_admin, audit, *, mode, evidence, wait=120, sleep=time.sleep, clock=time.monotonic):
    """P3.1-3.2: an incompatible PVMBG change is parked intact with reason/source/time, not guessed."""
    require(mode in ("rename", "type_change", "missing_required"), "incompatible mode required")
    response = pvmbg_admin.request("POST", "/admin/schema-version", body={"mode": mode})
    evidence.event("schema-mode", mode=mode, status=response["status"])
    deadline, items = clock() + wait, []
    while clock() < deadline:
        items = [i for i in parked(audit, "PVMBG") if i.get("status") == "parked"]
        if items:
            break
        sleep(5)
    a = evidence.result["assertions"]
    a["mode_switched"] = response["status"] in (200, 204)
    a["incompatible_messages_parked"] = bool(items)
    a["parked_have_reason_source_time"] = bool(items) and all(i.get("reason") and i.get("source") == "PVMBG" and i.get("received_at") for i in items)
    a["parked_marked_not_retryable"] = bool(items) and all(i.get("retryable") is False for i in items)
    evidence.result["parked"] = items[:50]
    return evidence


def replay(ingestion_ops, audit, *, namespace, adapter, evidence, wait=300, sleep=time.sleep, clock=time.monotonic):
    """P3.4: after adapting, replay the parked messages; all recover, no duplicates."""
    items = [i for i in parked(audit, "PVMBG") if i.get("status") == "parked"]
    require(items, "no parked PVMBG messages to replay")
    before = duplicates_by_source(audit, "PVMBG")
    body = {"request_id": "demo-" + evidence.run_id, "source": "PVMBG", "namespace": namespace, "adapter_version": adapter, "message_ids": [i["message_id"] for i in items]}
    started = ingestion_ops.request("POST", "/internal/ingestion/replays", body=body)
    require(started["status"] in (200, 201, 202) and isinstance(started["json"], dict), "replay request returned " + str(started["status"]))
    job = started["json"].get("job_id") or started["json"].get("id")
    deadline, state = clock() + wait, None
    while clock() < deadline and job:
        state = ingestion_ops.request("GET", "/internal/ingestion/replays/" + str(job))["json"]
        if isinstance(state, dict) and state.get("status") in ("completed", "complete", "failed"):
            break
        sleep(3)
    again = ingestion_ops.request("POST", "/internal/ingestion/replays", body=body)  # same request id: idempotent
    remaining = [i for i in parked(audit, "PVMBG") if i["message_id"] in set(body["message_ids"])]
    after = duplicates_by_source(audit, "PVMBG")
    a = evidence.result["assertions"]
    a["replay_completed"] = isinstance(state, dict) and state.get("status") in ("completed", "complete")
    a["all_parked_recovered"] = not remaining
    a["no_duplicate_hazard_events"] = after["duplicates"] == 0
    a["events_added_not_doubled"] = after["events"] - before["events"] <= len(items)
    a["repeated_replay_request_idempotent"] = again["status"] in (200, 201, 202, 409)
    evidence.result["replay"] = {"job": state, "messages": len(items), "before": before, "after": after}
    return evidence


def field_reports(cluster, client_field, inventory, *, count, evidence, wait=300, sleep=time.sleep, clock=time.monotonic):
    """P3.6: every 202-accepted report survives receiver crashes and is processed exactly once."""
    ns = inventory["namespace"]
    receipts, crashed = {}, []
    for index in range(count):
        key = "demo-" + evidence.run_id + "-" + str(index)
        response = client_field.request("POST", "/field-reports", body=field_report_body(index), headers={"Idempotency-Key": key})
        if response["status"] == 202:
            receipts[key] = response["json"]["receipt_id"]
        if index == count // 2:
            for pod in pods(cluster, ns, "ingestion"):
                kill_pod(cluster, ns, pod["metadata"]["name"]);crashed.append(pod["metadata"]["name"])
            evidence.event("receivers-crashed", pods=crashed)
    repeated = {}
    for index in range(count):  # client retries after the crash: same key and body
        key = "demo-" + evidence.run_id + "-" + str(index)
        for _ in range(20):
            response = client_field.request("POST", "/field-reports", body=field_report_body(index), headers={"Idempotency-Key": key})
            if response["status"] == 202:
                repeated[key] = response["json"]["receipt_id"];break
            sleep(2)
    deadline, statuses = clock() + wait, {}
    while clock() < deadline:
        for key, receipt in receipts.items():
            if statuses.get(key) != "processed":
                value = client_field.request("GET", "/field-reports/" + receipt)["json"] or {}
                statuses[key] = value.get("status")
        if receipts and all(v == "processed" for v in statuses.values()):
            break
        sleep(3)
    a = evidence.result["assertions"]
    a["receivers_crashed_mid_stream"] = bool(crashed)
    a["every_202_has_one_stable_receipt"] = bool(receipts) and all(repeated.get(k) == v for k, v in receipts.items())
    a["every_accepted_report_processed"] = bool(receipts) and all(statuses.get(k) == "processed" for k in receipts)
    a["late_retries_accepted"] = len(repeated) == count
    evidence.result["reports"] = {"sent": count, "accepted": len(receipts), "statuses": statuses}
    return evidence


# --------------------------------------------------------------------------- P4

def processor_crash(cluster, observer, audit, inventory, *, app, reference, evidence, wait=600, sleep=time.sleep, clock=time.monotonic):
    """P4.5: crash the wildfire processor mid-stream; the final result equals the undisturbed reference."""
    ns = inventory["namespace"]
    before = observer.progress()["data"]
    victims = [p["metadata"]["name"] for p in pods(cluster, ns, app)]
    require(victims, "no running " + app + " pod")
    for name in victims:
        kill_pod(cluster, ns, name)
    evidence.event("processor-crashed", pods=victims, progress=before)
    deadline, progress = clock() + wait, None
    while clock() < deadline:
        progress = observer.progress()["data"]
        if progress and progress.get("received_through") is not None and progress.get("applied_through") == progress.get("received_through") \
                and progress["received_through"] > (before or {}).get("applied_through", -1):
            break
        sleep(5)
    events, flags = traverse(audit, "/internal/audit/events", params={"source": "SATELIT"})
    ids = [e["source_ref_id"] for e in events]
    api = audit.request("GET", "/internal/audit/checksum", params={"source": "SATELIT"})["json"] or {}
    a = evidence.result["assertions"]
    a["caught_up_after_crash"] = bool(progress) and progress.get("applied_through") == progress.get("received_through")
    a["no_duplicate_fires"] = len(ids) == len(set(ids))
    a["not_partial"] = not flags["partial"]
    if reference:
        a["count_matches_reference"] = api.get("count") == reference["count"]
        a["checksum_matches_reference"] = api.get("checksum") == reference["checksum"]
    evidence.result["satellite"] = {"progress_before": before, "progress_after": progress, "checksum": api, "reference": reference}
    return evidence


# --------------------------------------------------------------------------- P5

def coastal_breaker(prometheus, client, *, paths, rate_query, evidence, seconds=120, sleep=time.sleep, clock=time.monotonic):
    """P5.4: while the sensor is unhealthy, requests to it do not surge and dependent reads answer fast."""
    def rate():
        if not (prometheus and rate_query):
            return None
        value = prometheus.request("GET", "/api/v1/query", params={"query": rate_query})["json"] or {}
        result = value.get("data", {}).get("result", [])
        return sum(float(r["value"][1]) for r in result) if result else 0.0
    baseline = rate()
    latencies, statuses, rates = [], {}, []
    started = clock()
    while clock() - started < seconds:
        for path in paths:
            response = client.request("GET", path, timeout=10)
            latencies.append(response["ms"])
            statuses[str(response["status"])] = statuses.get(str(response["status"]), 0) + 1
        value = rate()
        if value is not None:
            rates.append(value)
        sleep(2)
    ordered_ms = sorted(latencies)
    p95 = ordered_ms[min(len(ordered_ms) - 1, int(0.95 * (len(ordered_ms) - 1)))] if ordered_ms else None
    a = evidence.result["assertions"]
    a["dependent_reads_fast_p95_under_1s"] = p95 is not None and p95 < 1000
    a["no_timeouts_or_5xx"] = not any(k in ("None", "500", "502", "504") for k in statuses)
    if rate_query:
        a["sensor_request_rate_did_not_surge"] = bool(rates) and baseline is not None and max(rates) <= max(1.0, baseline * 1.5)
    evidence.result["breaker"] = {"p95_ms": p95, "statuses": statuses, "baseline_rate": baseline, "max_rate": max(rates) if rates else None, "samples": len(latencies)}
    return evidence


# --------------------------------------------------------------------------- Tester Agent

def tester(cluster, audit, inventory, *, scenario, mode, extra, recovery_seconds, evidence, sleep=time.sleep):
    """Run a Tester Agent scenario, then only observe (no intervention) during the recovery window."""
    require(mode in ("practice", "exam", "list"), "mode must be practice, exam or list")
    command = ["exec", "-n", "tester-agent", "daemonset/tester-agent", "--", "/app"]
    if mode == "list":
        listing = cluster.kubectl(*command, "list", check=False)
        evidence.result["scenarios"] = listing.stdout.decode(errors="replace")
        evidence.result["assertions"]["listed"] = listing.returncode == 0
        return evidence
    require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,80}", scenario or ""), "scenario name required")
    since = now()
    before = {s: audit.request("GET", "/internal/audit/checksum", params={"source": s})["json"] for s in SOURCES}
    evidence.result["before"] = {"checksums": before, "restarts": restarts(cluster, inventory["namespace"])}
    started = cluster.kubectl(*command, "run", mode, scenario, *extra, check=False, timeout=900)
    evidence.event("scenario-finished", exit_code=started.returncode)
    (evidence.dir / "tester-output.txt").write_bytes(started.stdout + b"\n" + started.stderr)
    samples = []
    for _ in range(max(1, recovery_seconds // 10)):  # observation only; the group must not intervene
        health = {s: audit.request("GET", "/internal/audit/checksum", params={"source": s})["json"] for s in SOURCES}
        samples.append({"at": now(), "partial": sorted(s for s, v in health.items() if not v or v.get("partial"))})
        sleep(10)
    with (evidence.dir / "tester-agent.log").open("wb") as stream:  # every node's agent pod
        for pod in kubectl_json(cluster, "get", "pods", "-n", "tester-agent")["items"]:
            name = pod["metadata"]["name"]
            log = cluster.kubectl("logs", "-n", "tester-agent", name, "--since-time=" + since, "--all-containers=true", check=False)
            stream.write(("==== " + name + " (" + str(pod["spec"].get("nodeName")) + ")\n").encode() + log.stdout + b"\n")
    after = {s: audit.request("GET", "/internal/audit/checksum", params={"source": s})["json"] for s in SOURCES}
    lost = [s for s in SOURCES if before.get(s) and after.get(s) and after[s].get("count", 0) < before[s].get("count", 0)]
    a = evidence.result["assertions"]
    a["scenario_ran"] = started.returncode == 0
    a["no_hazard_events_lost"] = not lost
    a["recovered_not_partial_at_end"] = bool(samples) and not samples[-1]["partial"]
    evidence.result["after"] = {"checksums": after, "restarts": restarts(cluster, inventory["namespace"]), "recovery_samples": samples}
    return evidence
