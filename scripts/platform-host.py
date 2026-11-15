#!/usr/bin/env python3
"""Fixed SSH entry point. Install an aat-platform-host wrapper invoking this file.
No command arguments or caller-provided executable/shell text are accepted.
"""
import json
import sys
from platform_lib.host_agent import MAX_REQUEST, serve

if __name__ == "__main__":
    try:
        print(serve(sys.stdin.buffer.read(MAX_REQUEST + 1)).decode())
    except Exception as exc:
        # Keep provider stderr, arbitrary request fields and credentials private.
        print(json.dumps({"version": 1, "error": type(exc).__name__, "message": "host operation refused; inspect local configuration/state"}))
        sys.exit(1)
