#!/usr/bin/env python3
"""Reproduce historical v1 and deterministic v2 replay without external services."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parent.parent
HISTORICAL_BRIDGE = "86b06182753069167e1b07d8e5ece4fc6911d2a13d7d6038bcff2161a6bb4bbf"


def main():
    run = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dt%H%M%S") + "-" + uuid.uuid4().hex[:8]
    evidence = ROOT / "evidence" / "role1-m6" / run
    evidence.mkdir(parents=True)
    binary = evidence / ("wildfire-reference.exe" if os.name == "nt" else "wildfire-reference")
    build = ["go", "build", "-mod=readonly", "-o", str(binary), "./cmd/wildfire-reference"]
    completed = subprocess.run(build, cwd=ROOT / "aggregator", capture_output=True, timeout=120)
    (evidence / "build.log").write_bytes(completed.stdout + completed.stderr)
    if completed.returncode:
        print("Reference build failed: " + str(evidence))
        return 1
    checks = []
    for name, version, fixture, blocked in [
        ("v1-bridge", 5, "bridge", False),
        ("v1-mixed", 5, "mixed-merge-split", True),
        ("v2-mixed", 6, "mixed-merge-split", False),
    ]:
        config = ROOT / "docs" / "fixtures" / f"wildfire-m{version}" / ("config.json" if version == 5 else "reference-config.json")
        command = [str(binary), "replay", "--config", str(config), "--input", str(ROOT / "docs/fixtures/wildfire-m5" / (fixture + ".jsonl"))]
        outputs = []
        codes = []
        for n in (1, 2):
            result = subprocess.run(command, capture_output=True, timeout=60)
            (evidence / f"{name}-{n}.jsonl").write_bytes(result.stdout)
            (evidence / f"{name}-{n}.stderr").write_bytes(result.stderr)
            outputs.append(result.stdout)
            codes.append(result.returncode)
        digest = hashlib.sha256(outputs[0]).hexdigest()
        valid = outputs[0] == outputs[1]
        if blocked:
            valid = valid and all(code != 0 for code in codes) and b"ambiguous_merge_split" in outputs[0]
        else:
            valid = valid and codes == [0, 0]
        if name == "v1-bridge":
            valid = valid and digest == HISTORICAL_BRIDGE
        checks.append({"name": name, "status": "passed" if valid else "failed", "command": command, "return_codes": codes, "sha256": digest, "byte_identical": outputs[0] == outputs[1], "scope": "historical blocked reproduction" if blocked else "deterministic replay"})
    report = {"build_command": build, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "checks": checks}
    (evidence / "summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("Evidence: " + str(evidence))
    for check in checks:
        print(check["name"] + ": " + check["status"] + " sha256=" + check["sha256"])
    return 0 if all(check["status"] == "passed" for check in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
