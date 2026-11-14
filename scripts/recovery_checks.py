"""Version-one recovery evidence checks. No faults, writes, or dataset repair."""
import hashlib
import json
from datetime import datetime


def require(value, message):
    if not value:
        raise ValueError(message)


def content_digest(event):
    """Mutable canonical wire content, JSON UTF-8, sorted keys, no whitespace."""
    if event is None:
        return None
    fields = ("hazard_type", "severity", "area_name", "latitude", "longitude", "occurred_at", "attributes")
    require(all(k in event for k in fields), "incomplete canonical event")
    raw = json.dumps({k: event[k] for k in fields}, ensure_ascii=False, sort_keys=True,
                     separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def audit_digest(refs):
    return hashlib.sha256(b"\n".join(sorted(r.encode("utf-8") for r in refs))).hexdigest()


def placement_checks(topology, require_four=False):
    """Union physical holdings, including transient destinations; replicas count once per node."""
    require(topology.get("verified") is True, "topology identity/placement is unverified")
    units = topology.get("units", [])
    require(units and len({u["name"] for u in units}) == len(units), "missing/duplicate physical units")
    holdings, groups, all_shards = {}, {}, set()
    for u in units:
        require(u.get("identity_verified") is True, "physical member identity is unverified")
        require(u.get("node") and isinstance(u.get("shards"), list), "physical holdings missing")
        require(u.get("holdings_complete") is True, "physical/transient inventory incomplete")
        require(len(u["shards"]) == len(set(u["shards"])), "duplicate shard in physical member")
        holdings.setdefault(u["node"], set()).update(u["shards"])
        all_shards.update(u["shards"])
        groups.setdefault(u["group"], []).append(u)
    require(all_shards, "no nonempty logical shards observed")
    require(all(v != all_shards for v in holdings.values()), "a node holds the complete dataset, including transient copies")
    require(all(any(u["shards"] for u in members) for members in groups.values()), "empty worker group")
    if require_four:
        require(len(holdings) == 4 and len(groups) == 4, "four nodes and four nonempty groups required")
        for members in groups.values():
            require(len(members) == 3 and len({m["node"] for m in members}) == 3, "three distinct physical copies required")
            require(all(m.get("complete") is True for m in members), "rebuilding member is not a complete copy")
            require(len({tuple(sorted(m["shards"])) for m in members}) == 1, "physical group holdings disagree")
    return {node: sorted(shards) for node, shards in sorted(holdings.items())}


def verify(document, require_four=False):
    require(document.get("version") == 1, "unsupported evidence version")
    dataset = document.get("dataset", {})
    require(dataset.get("id") and len(dataset.get("sha256", "")) == 64, "dataset identity/digest missing")
    checkpoint = document.get("checkpoint", {})
    require(checkpoint.get("id"), "checkpoint identity missing")
    expected = document.get("acknowledgements", [])
    require(expected, "no acknowledged operations to reconcile")
    observed = document.get("observed", {})
    entities = observed.get("entities", [])
    keys = [(v["source"], v["source_ref_id"]) for v in entities]
    require(len(set(keys)) == len(keys), "duplicate logical entity/replica inflation")
    actual = dict(zip(keys, entities))
    ops = observed.get("operations", [])
    opkeys = [(v["source"], v["operation_id"]) for v in ops]
    require(len(set(opkeys)) == len(opkeys), "duplicate logical operation")
    operations = dict(zip(opkeys, ops))
    expected_ops = set()
    latest = {}
    for ack in expected:
        key = (ack["source"], ack["operation_id"])
        require(key not in expected_ops, "duplicate acknowledgement identity")
        expected_ops.add(key)
        op = operations.get(key)
        require(op is not None and op.get("batch_id") == ack["batch_id"], "acknowledged operation missing or reassigned")
        require(op.get("result") == ack.get("result"), "recorded operation outcome changed")
        entity_key = (ack["source"], ack["source_ref_id"])
        old = latest.get(entity_key)
        if old is None or ack["revision"] >= old["revision"]:
            latest[entity_key] = ack
    for key, ack in latest.items():
        entity = actual.get(key)
        require(entity is not None, "acknowledged entity missing")
        for field in ("hazard_id", "revision", "active"):
            require(entity.get(field) == ack.get(field), "canonical " + field + " changed")
        before_time, after_time = (datetime.fromisoformat(v.replace("Z", "+00:00")) for v in (ack["ingested_at"], entity["ingested_at"]))
        require(before_time.tzinfo is not None and after_time.tzinfo is not None and before_time == after_time,
                "canonical ingested_at changed")
        if "event" in entity:
            digest = content_digest(entity["event"])
            require((entity["event"] is not None) == bool(entity["active"]), "inactive payload or absent active payload")
        else:
            require("content_sha256" in entity, "observed content digest missing")
            digest = entity["content_sha256"]
            require((digest is not None) == bool(entity["active"]), "inactive digest or absent active digest")
        require(digest == ack.get("content_sha256"), "canonical content changed")
    checksum = observed.get("checksum")
    if checkpoint.get("checksum") is not None:
        require(checkpoint.get("stable") is True, "checksum requires an explicitly stable population")
        require(checksum and checksum.get("partial") is False, "partial checksum cannot establish complete recovery")
        for field in ("source", "count", "checksum"):
            require(checksum.get(field) == checkpoint["checksum"].get(field), "audit checksum population/content differs")
    holdings = None
    if observed.get("control") is not None:
        before, after = observed["control"]["before"], observed["control"]["after"]
        require(before == after and before.get("primary") is True and before.get("pending") is False
                and isinstance(before.get("generation"), int), "uncertain or overlapping canonical control state")
    if observed.get("topology") is not None:
        holdings = placement_checks(observed["topology"], require_four)
    elif require_four:
        raise ValueError("four-node physical inventory missing")
    unresolved = document.get("uncertain_operations", [])
    return {"version": 1, "status": "blocked" if unresolved else "passed",
            "scope": "four-node assertions" if require_four else "local/component assertions",
            "acknowledged_operations": len(expected), "reconciled_entities": len(latest),
            "uncertain_operations": unresolved, "physical_holdings": holdings,
            "timings": document.get("timings", {}),
            "limitations": ["Assertions do not independently establish deployment, fault execution, or acknowledgement durability."]}
