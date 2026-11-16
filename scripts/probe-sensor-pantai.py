#!/usr/bin/env python3
import argparse
import collections
import json
import os
import time
import urllib.error
import urllib.request

FIELDS = ("critical", "observation_id", "observed_at", "quality", "sea_level_anomaly_m", "station_id", "wave_height_m")


def get(base, token, path, timeout):
    req = urllib.request.Request(base + path, headers={"Authorization": "Bearer " + token})
    started = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read(), time.time() - started
    except urllib.error.HTTPError as e:
        return e.code, e.read(), time.time() - started
    except Exception as e:
        return -1, str(e).encode(), time.time() - started


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://localhost:18080")
    parser.add_argument("--seconds", type=float, default=120)
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    token = os.environ.get("PANTAI_AUTH_TOKEN", "")
    stations = json.loads(get(args.base, token, "/v1/stations", args.timeout)[1]).get("items", [])
    known = {s.get("station_id") for s in stations}
    statuses, health, anomalies, cursor = collections.Counter(), collections.Counter(), collections.Counter(), collections.Counter()
    latencies, seen, examples, criticals = [], {}, {}, []
    duplicates = changed = 0
    position = None
    started = time.time()
    while time.time() - started < args.seconds:
        health[get(args.base, token, "/healthz", 5)[0]] += 1
        status, body, elapsed = get(args.base, token, "/v1/observations?limit=100" + (f"&cursor={position}" if position else ""), args.timeout)
        statuses[status] += 1
        latencies.append(elapsed)
        if status != 200:
            examples.setdefault(f"status_{status}", body[:300].decode(errors="replace"))
            time.sleep(0.5)
            continue
        try:
            page = json.loads(body)
        except ValueError:
            anomalies["body_not_json"] += 1
            examples.setdefault("body_not_json", body[:300].decode(errors="replace"))
            time.sleep(0.5)
            continue
        items = page.get("items") if isinstance(page, dict) else None
        if not isinstance(items, list):
            anomalies["items_not_list"] += 1
            continue
        for item in items:
            if not isinstance(item, dict):
                anomalies["item_not_object"] += 1
                continue
            if tuple(sorted(item)) != FIELDS:
                key = "keys:" + ",".join(sorted(item))
                anomalies[key] += 1
                examples.setdefault(key, item)
            anomalies["quality:" + str(item.get("quality"))] += 1
            if item.get("station_id") not in known:
                anomalies["unknown_station"] += 1
                examples.setdefault("unknown_station", item)
            if not isinstance(item.get("critical"), bool):
                anomalies["critical_not_boolean"] += 1
                examples.setdefault("critical_not_boolean", item)
            identity = item.get("observation_id")
            if identity in seen:
                if seen[identity] == item:
                    duplicates += 1
                else:
                    changed += 1
                    examples.setdefault("changed_same_id", {"before": seen[identity], "after": item})
            else:
                seen[identity] = item
            if item.get("critical") is True:
                criticals.append(item)
        following = page.get("next_cursor")
        cursor["null" if following is None else "same" if following == position else "advanced"] += 1
        cursor["empty_page"] += 0 if items else 1
        position = following or position
        time.sleep(0.25 if items else 1.0)
    latencies.sort()
    pick = lambda q: round(latencies[int(q * (len(latencies) - 1))], 3) if latencies else None
    print(json.dumps({"seconds": args.seconds, "requests": sum(statuses.values()), "statuses": statuses, "health": health,
                      "latency_p50": pick(0.5), "latency_p95": pick(0.95), "latency_max": pick(1.0),
                      "unique_observations": len(seen), "exact_duplicates": duplicates, "changed_same_id": changed,
                      "criticals": len(criticals), "critical_examples": criticals[:3], "anomalies": anomalies,
                      "examples": examples, "cursor": cursor}, indent=1, default=str))


if __name__ == "__main__":
    main()
