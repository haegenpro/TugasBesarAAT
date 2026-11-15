#!/usr/bin/env python3
"""Private detached observer entry point; only fixed host-helper paths are valid."""
import sys
from platform_lib.network_evidence import collect

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(2)
    collect(sys.argv[1])
