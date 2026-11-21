#!/usr/bin/env python3
"""Plan reprocessing of inactive satellite updates saved under the previous rule."""
import argparse
import json

from common import LOCAL, NAMESPACE, kubectl, private_json, read_config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--output', help='optional private evidence JSON file')
    args = parser.parse_args()
    cfg = read_config(args.config)
    if args.execute:
        from lifecycle import require_offline
        require_offline(cfg)
    pods = json.loads(kubectl(cfg, '-n', NAMESPACE, 'get', 'pods', '-l',
                             'formation=ingestion,database-role=primary', '-o', 'json'))['items']
    if len(pods) != 1:
        raise ValueError('ingestion primary is not unambiguous')
    program = '''import json,os,psycopg2
c=psycopg2.connect(host='127.0.0.1',dbname='ingestion',user='ingestion_app',password=os.environ['INGESTION_PASSWORD'])
namespace=NAMESPACE
execute=EXECUTE
with c.cursor() as cur:
 cur.execute("""SELECT i.message_id::text,i.position,i.entity_key,i.reason
 FROM raw_inputs i JOIN wildfire_observations w
 ON w.namespace=i.namespace AND w.key=i.entity_key
 WHERE i.source='SATELIT' AND i.namespace=%s AND i.state='applied'
 AND i.metadata->>'event_type'='update' AND NOT w.active
 AND w.last_position=i.position AND w.payload->'detection'<>'null'::jsonb
 AND NOT EXISTS(SELECT 1 FROM canonical_outbox o WHERE o.message_id=i.message_id)
 ORDER BY i.position LIMIT 1001""",(namespace,))
 rows=cur.fetchall()
 if len(rows)>1000: raise RuntimeError('more than 1000 candidates; review and migrate in explicit batches')
 if execute and rows:
  cur.execute("""UPDATE raw_inputs SET state='parked',
   reason='lifecycle rule changed: a later update reactivates its retained detection'
   WHERE state='applied' AND message_id::text=ANY(%s)""",([r[0] for r in rows],))
  if cur.rowcount!=len(rows): raise RuntimeError('candidate state changed; no migration committed')
 if execute: c.commit()
 else: c.rollback()
 print(json.dumps({'namespace':namespace,'execute':execute,'candidates':[
  {'message_id':r[0],'offset':r[1],'entity_key':r[2],'previous_reason':r[3]} for r in rows]}))
c.close()
'''.replace('NAMESPACE', repr(cfg['satellite']['namespace'])).replace('EXECUTE', repr(args.execute))
    result = json.loads(kubectl(cfg, '-n', NAMESPACE, 'exec', '-i',
                              pods[0]['metadata']['name'], '-c', 'postgres', '--',
                              '/opt/patroni/bin/python', '-', input_text=program))
    if args.output:
        from pathlib import Path
        private_json(Path(args.output), result)
    print(json.dumps(result, indent=2))
    if args.execute:
        print('Selected inputs are parked for review. Start the corrected workers, then replay through Internal Ops.')
    else:
        print('Dry run. No input state changed. --execute requires stopped writers.')


if __name__ == '__main__':
    try:
        main()
    except (OSError, RuntimeError, ValueError):
        raise SystemExit('Lifecycle reprocessing refused; inspect configuration, stopped writers, and ingestion readiness.')
