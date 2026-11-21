#!/usr/bin/env python3
"""Checksum-verified JSONL catalog import through the canonical HTTP owner."""
import argparse
import hashlib
import json
from pathlib import Path
import time
import urllib.error
import urllib.request
from common import LOCAL, read_config, runtime_secrets
from verify import forward


def records(path):
    with Path(path).open('rb') as source:
        number = 0
        while line := source.readline((1<<20)+1):
            number += 1
            if len(line) > 1<<20:
                raise ValueError(f'catalog line {number} exceeds 1 MiB')
            value = json.loads(line)
            required = {'source', 'source_ref_id', 'hazard_type', 'severity', 'area_name',
                        'latitude', 'longitude', 'occurred_at', 'attributes'}
            if (not isinstance(value, dict) or not required <= value.keys()
                    or value['source'] != 'BMKG' or value['hazard_type'] != 'SEISMIC'):
                raise ValueError(f'catalog line {number} is not a normalized BMKG seismic event')
            # Server validates the actual contract; IDs/timestamps assigned by it
            # cannot be substituted by values from an unrelated catalog format.
            yield {key: value[key] for key in required}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--input', required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    cfg = read_config(args.config)
    with Path(args.input).open('rb') as source:
        digest = hashlib.file_digest(source, 'sha256').hexdigest()
    if digest != args.sha256.lower(): raise ValueError('catalog checksum mismatch')
    count = sum(1 for _ in records(args.input))
    print(f'Catalog: {count} normalized events; SHA-256 {digest}', flush=True)
    if not args.execute:
        print('Dry run. Add --execute to submit events through the canonical owner.')
        return
    credentials = runtime_secrets(LOCAL/'secrets.json')
    with forward(cfg, 'aggregator', 8080) as port:
        batch = []
        completed = 0
        def send(events):
            payload = json.dumps({'events': events}, allow_nan=False).encode()
            for attempt in range(6):
                request = urllib.request.Request(f'http://127.0.0.1:{port}/internal/ops/catalog',
                    data=payload, headers={'Authorization': 'Bearer '+credentials['admin'],
                                          'Content-Type': 'application/json'})
                try:
                    with urllib.request.urlopen(request, timeout=15) as response:
                        result = json.load(response)
                        if result.get('accepted') != len(events): raise RuntimeError('catalog acknowledgment mismatch')
                        return
                except urllib.error.HTTPError as error:
                    if error.code not in (429, 502, 503, 504): raise RuntimeError(f'catalog rejected (HTTP {error.code})') from None
                except (urllib.error.URLError, TimeoutError): pass
                time.sleep(min(2**attempt, 5))
            raise RuntimeError('catalog import interrupted; rerun the same verified file safely')
        for event in records(args.input):
            batch.append(event)
            if len(batch) == 20:
                send(batch); completed += len(batch); batch = []
                print(f'Acknowledged {completed}/{count}', flush=True)
        if batch: send(batch); completed += len(batch)
    print(f'Acknowledged {completed} events. Compare Audit checksum/count and shard placement.')


if __name__ == '__main__': main()
