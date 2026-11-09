#!/usr/bin/env python3
"""Run each independent Go module. Integration settings are inherited, never printed."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import argparse
import os
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--race', action='store_true')
    parser.add_argument('--vet', action='store_true', help='Run go vet instead of go test')
    parser.add_argument('--jobs', type=int, default=3)
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error('--jobs must be positive')
    modules = sorted({p.parent for p in ROOT.glob('*/go.mod')} | {ROOT / 'monitoring/launcher'})
    def run(module):
        command = ['go', 'vet', './...'] if args.vet else ['go', 'test', '-count=1', '-timeout=180s', *(['-race'] if args.race else []), './...']
        result = subprocess.run(command, cwd=module, text=True, capture_output=True)
        name = str(module.relative_to(ROOT))
        print(f'{name}: {"PASS" if result.returncode == 0 else "FAIL"}', flush=True)
        if result.returncode:
            print(result.stdout + result.stderr, flush=True)
        elif not args.vet:
            print(result.stdout.strip(), flush=True)
        return result.returncode
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        failures = list(pool.map(run, modules))
    return int(any(failures))

if __name__ == '__main__':
    sys.exit(main())
