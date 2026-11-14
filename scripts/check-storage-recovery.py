#!/usr/bin/env python3
"""Read-only recovery plan/collection and offline verification. Never repairs data."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from recovery_checks import content_digest, require, verify

ROOT = Path(__file__).resolve().parent.parent


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


def read_json(path, limit=2 << 20):
    require(path.stat().st_size <= limit, "evidence input exceeds configured bound")
    return json.loads(path.read_text(encoding="utf-8"))


def platform(args):
    from platform_lib.inventory import load
    spec = importlib.util.spec_from_file_location("recovery_platform", ROOT / "scripts/platform-recovery.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    require(args.storage_plan and args.node, "--storage-plan and an owned --node required")
    inventory, storage = load(args.inventory), read_json(args.storage_plan)
    config = read_json(args.config) if args.config else None
    if config is not None:
        module.validate_config(storage, config)
    plan = module.recovery_plan(inventory, storage, args.node)
    cluster = None
    if args.command == "observe":
        from platform_lib.cluster import Cluster
        cluster = Cluster(inventory)
    return cluster, storage, config, plan


def observe(args, expected):
    """Use trusted inventory, bounded Pod probes and read-only SQL. No remote URL in a payload."""
    cluster, storage, config, _ = platform(args)
    require(config is not None, "observe requires the original --config with captured system identities")
    url = urlsplit(args.api_url)
    require(url.scheme in ("http", "https") and url.hostname and not url.username and not url.password
            and not url.query and not url.fragment, "invalid configured API URL")
    audit_token, canonical_token = os.environ.get("AUDIT_BEARER_TOKEN"), os.environ.get("CANONICAL_SERVICE_TOKEN")
    require(audit_token and canonical_token, "dedicated audit and canonical credentials required")
    require(not any(c in audit_token+canonical_token for c in "\r\n"), "invalid credential header characters")
    acks = expected.get("acknowledgements", [])
    require(0 < len(acks) <= args.max_operations <= 1000, "acknowledgement collection quota exceeded")
    deadline, remaining = time.monotonic() + args.seconds, args.max_bytes
    opener = build_opener(NoRedirects)

    def budget():
        require(time.monotonic() < deadline, "collection deadline exceeded")

    def get(path, token):
        nonlocal remaining
        budget()
        request = Request(args.api_url.rstrip("/") + path, headers={"Authorization": "Bearer " + token})
        with opener.open(request, timeout=min(2, deadline - time.monotonic())) as response:
            raw = response.read(min(remaining, 2 << 20) + 1)
        require(len(raw) <= min(remaining, 2 << 20), "collection byte quota exceeded")
        remaining -= len(raw)
        return json.loads(raw)

    statuses, primary = [], None
    group_by_id = {g["id"]: g for g in config["groups"]}
    for group in storage["groups"]:
        for member in group["members"]:
            budget()
            try:
                pod = json.loads(cluster.kubectl("get", "pod", member["name"], "-n", storage["namespace"],
                    "-o", "json", "--request-timeout=2s", timeout=4).stdout)
                require(pod["metadata"]["labels"].get("platform.aat/formation") == storage["formation"]
                        and pod["spec"]["nodeName"] == member["node"], "Pod ownership/placement mismatch")
                raw = cluster.kubectl("exec", "-n", storage["namespace"], member["name"], "-c", "postgres", "--",
                    "psql", "-qAt", "-U", "postgres", "-d", "hazards", "-c",
                    "BEGIN READ ONLY; SET LOCAL statement_timeout='2s'; SELECT public.platform_replication_status(); COMMIT;", timeout=4).stdout
                require(len(raw) <= 1 << 20, "member observation exceeds bound")
                status = json.loads(raw)
                require(status["system_id"] == group_by_id[group["id"]]["system_id"]
                        and status["formation"] == storage["formation"] and status["group_id"] == group["id"]
                        and status["member"] == member["name"], "physical member identity mismatch")
                statuses.append({"name": member["name"], "node": member["node"], "status": status})
                if group["id"] == 0 and status["primary"]:
                    require(primary is None, "multiple coordinator primaries")
                    primary = member["name"]
            except subprocess.SubprocessError:
                statuses.append({"name": member["name"], "node": member["node"], "unavailable": True})
    require(primary, "authoritative coordinator unavailable")
    def query(sql):
        nonlocal remaining
        budget()
        raw = cluster.kubectl("exec", "-n", storage["namespace"], primary, "-c", "postgres", "--",
            "psql", "-qAt", "-U", "postgres", "-d", "hazards", "-c",
            "BEGIN READ ONLY; SET LOCAL statement_timeout='2s'; " + sql + "; COMMIT;", timeout=4).stdout
        require(len(raw) <= min(remaining, 2 << 20), "SQL observation exceeds byte quota")
        remaining -= len(raw)
        return json.loads(raw)

    control_sql = "SELECT jsonb_build_object('generation',generation,'pending',EXISTS(SELECT 1 FROM canonical_attempts WHERE state='pending'),'primary',NOT pg_is_in_recovery()) FROM canonical_visibility WHERE singleton"
    control_before = query(control_sql)
    require(control_before["primary"] and not control_before["pending"], "canonical control state unavailable/pending")
    # SQL literals are escaped; identifiers and executable paths are fixed, never caller selected.
    identities = json.dumps([[a["source"], a["operation_id"]] for a in acks], separators=(",", ":")).encode().hex()
    sql = """
SELECT coalesce(jsonb_agg(jsonb_build_object('source',source,'operation_id',operation_id,'batch_id',batch_id,'result',result)),'[]')
FROM canonical_operations WHERE (source,operation_id) IN
(SELECT v->>0,v->>1 FROM jsonb_array_elements(convert_from(decode('%s','hex'),'UTF8')::jsonb) v)""" % identities
    operations = query(sql)
    entity_ids = json.dumps(list({(a["source"], a["source_ref_id"]) for a in acks}), separators=(",", ":")).encode().hex()
    metadata = query("SELECT coalesce(jsonb_agg(jsonb_build_object('source',source,'source_ref_id',source_ref_id,'ingested_at',ingested_at)),'[]') FROM canonical_entities WHERE (source,source_ref_id) IN (SELECT v->>0,v->>1 FROM jsonb_array_elements(convert_from(decode('" + entity_ids + "','hex'),'UTF8')::jsonb) v)")
    ingestion_times = {(v["source"], v["source_ref_id"]): v["ingested_at"] for v in metadata}
    entities = []
    for source, ref in sorted({(a["source"], a["source_ref_id"]) for a in acks}):
        result = get("/internal/canonical/entities?" + urlencode({"source": source, "source_ref_id": ref}), canonical_token)
        result["content_sha256"] = content_digest(result.pop("event"))
        result.update(source=source, source_ref_id=ref, ingested_at=ingestion_times[(source,ref)])
        entities.append(result)
    observed = {"entities": entities, "operations": operations, "physical_members": statuses,
                "topology_limitation": "Member identities observed; complete transient holdings require owner catalog evidence."}
    if expected.get("checkpoint", {}).get("checksum") is not None:
        source = expected["checkpoint"]["checksum"].get("source")
        observed["checksum"] = get("/internal/audit/checksum" + ("?" + urlencode({"source": source}) if source else ""), audit_token)
    observed["control"] = {"before": control_before, "after": query(control_sql)}
    expected["observed"] = observed
    expected["collection"] = {"elapsed_seconds": args.seconds - max(0, deadline-time.monotonic()), "bytes": args.max_bytes-remaining}
    return expected


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=["plan", "observe", "verify"])
    p.add_argument("--inventory", type=Path, default=ROOT / "deployment/platform/inventory.example.json")
    p.add_argument("--storage-plan", type=Path)
    p.add_argument("--config", type=Path)
    p.add_argument("--node")
    p.add_argument("--evidence", type=Path)
    p.add_argument("--output", type=Path)
    p.add_argument("--api-url", default="")
    p.add_argument("--four-node", action="store_true")
    p.add_argument("--max-operations", type=int, default=1000)
    p.add_argument("--max-bytes", type=int, default=8 << 20)
    p.add_argument("--seconds", type=int, default=120)
    a = p.parse_args()
    require(0 < a.seconds <= 300 and 0 < a.max_bytes <= 32 << 20, "collection bounds invalid")
    if a.command == "plan":
        _, _, _, plan = platform(a)
        result = {"version": 1, "status": "blocked", "infrastructure_plan": plan,
                  "owner_checkpoints": ["stable acknowledged baseline", "two-survivor canonical progress before rebuild",
                                        "empty replacement catch-up and transient placement", "subsequent permitted loss"],
                  "limitations": ["Offline plan does not execute faults or establish acceptance."]}
    else:
        require(a.evidence, "--evidence required")
        document = read_json(a.evidence, a.max_bytes)
        if a.command == "observe":
            require(a.output and a.output.resolve() != a.evidence.resolve(), "observe requires a distinct --output evidence file")
            document = observe(a, document)
            a.output.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        result = verify(document, a.four_node)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "passed" or a.command == "plan" else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(json.dumps({"version": 1, "status": "failed", "error": str(error)}), file=sys.stderr)
        sys.exit(1)
