#!/usr/bin/env python3
"""Validate and install a checksum-pinned province reference for one feed session."""
import argparse
import hashlib
import json
import math
from pathlib import Path
from common import LOCAL, NAMESPACE, kubectl, read_config


def validate(raw, digest):
    if len(raw) > 4*1024*1024 or hashlib.sha256(raw).hexdigest() != digest.lower():
        raise ValueError('reference size or SHA-256 mismatch')
    rows = json.loads(raw)
    if not isinstance(rows, list) or not rows or len(rows) > 1000:
        raise ValueError('expected a nonempty province JSON array, at most 1000 rows')
    seen = set()
    for row in rows:
        if (not isinstance(row, dict) or set(row) != {'id', 'name', 'latitude', 'longitude'}
                or not isinstance(row['id'], str) or not row['id'] or row['id'] in seen
                or not isinstance(row['name'], str) or not row['name']):
            raise ValueError('invalid or repeated province identity')
        for key, bound in [('latitude', 90), ('longitude', 180)]:
            value = row[key]
            if type(value) not in (float, int) or not math.isfinite(value) or abs(value) > bound:
                raise ValueError('invalid province coordinate')
        seen.add(row['id'])
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--file', required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    cfg = read_config(args.config)
    raw = Path(args.file).read_bytes()
    rows = validate(raw, args.sha256)
    namespace = cfg['satellite']['namespace']
    print(f'Namespace: {namespace}; provinces: {len(rows)}; SHA-256: {args.sha256.lower()}')
    if not args.execute:
        print('Dry run. Add --execute to install this verified artifact.')
        return
    from lifecycle import require_offline
    require_offline(cfg)
    pods = json.loads(kubectl(cfg, '-n', NAMESPACE, 'get', 'pods', '-l',
                             'formation=ingestion,database-role=primary', '-o', 'json'))['items']
    if len(pods) != 1:
        raise ValueError('ingestion primary unavailable')
    script = '''import json,os,psycopg2
namespace=NAMESPACE
rows=json.loads(ROWS)
c=psycopg2.connect(host='127.0.0.1',dbname='ingestion',user='ingestion_app',password=os.environ['INGESTION_PASSWORD'])
with c.cursor() as cur:
 cur.execute('SELECT id,payload FROM provinces WHERE namespace=%s ORDER BY id',(namespace,))
 existing={identity:payload for identity,payload in cur.fetchall()}
 desired={r['id']:r for r in rows}
 cur.execute('SELECT EXISTS(SELECT 1 FROM wildfire_observations WHERE namespace=%s)',(namespace,))
 if cur.fetchone()[0] and existing!=desired: raise RuntimeError('reference is immutable after processing starts; use a new source namespace')
 cur.execute('DELETE FROM provinces WHERE namespace=%s',(namespace,))
 for row in rows: cur.execute('INSERT INTO provinces(namespace,id,payload) VALUES(%s,%s,%s)',(namespace,row['id'],json.dumps(row)))
c.commit()
'''.replace('NAMESPACE', repr(namespace)).replace('ROWS', repr(json.dumps(rows)))
    kubectl(cfg, '-n', NAMESPACE, 'exec', '-i', pods[0]['metadata']['name'], '-c', 'postgres',
            '--', '/opt/patroni/bin/python', '-', input_text=script)
    print('Verified province reference installed. Restart writers with manage.py up.')


if __name__ == '__main__': main()
