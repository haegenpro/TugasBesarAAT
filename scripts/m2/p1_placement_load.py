#!/usr/bin/env python3
"""Problem 1 evidence: shard/replica placement, node load and a one-region write spike.

Criteria: P1-1, P1-2, P1-5, P1-6 (shard/replica placement, node load, one-region spike).

distribution  sample HazardEvents and report primary/replica placement per node and unit
nodeload      sample kubelet CPU/memory per node over time (run while loading or spiking)
spike         write clustered synthetic BMKG events through the canonical owner (dry run by default)
"""
import argparse
import datetime
import json
from pathlib import Path
import sys
import time
import urllib.error
import urllib.request

from evidence_audit import AuditClient, pages
from common import LOCAL, node_usage, private_json, read_config, runtime_secrets


def summarize_locations(rows, nodes):
    """Placement verdict from Audit /location answers, one entry per sampled HazardEvent."""
    primaries = {node: 0 for node in nodes}
    replicas = {node: 0 for node in nodes}
    units, no_replica, same_node, unreachable = {}, [], [], []
    for row in rows:
        if row.get('partial') or not row.get('locations'):
            unreachable.append(row.get('hazard_id'))
            continue
        primary = [x for x in row['locations'] if x['role'] == 'primary']
        replica = [x for x in row['locations'] if x['role'] == 'replica']
        for location in primary:
            primaries[location['node']] = primaries.get(location['node'], 0)+1
            units[location['unit']] = units.get(location['unit'], 0)+1
        for location in replica:
            replicas[location['node']] = replicas.get(location['node'], 0)+1
        if not replica:
            no_replica.append(row['hazard_id'])
        elif primary and all(p['node'] == r['node'] for p in primary for r in replica):
            same_node.append(row['hazard_id'])
    sampled = len(rows)-len(unreachable)
    return {'sampled': sampled, 'unreachable': unreachable, 'primaries_per_node': primaries,
            'replicas_per_node': replicas, 'events_per_primary_unit': dict(sorted(units.items())),
            'without_replica': no_replica, 'replica_on_primary_node': same_node,
            'primary_nodes_in_use': sum(1 for v in primaries.values() if v),
            'no_single_unit_holds_everything': len(units) > 1 or sampled == 0,
            'largest_unit_share': (max(units.values())/sampled) if units and sampled else None}


def placement_ok(report):
    return (report['sampled'] > 0 and not report['unreachable'] and not report['without_replica']
            and not report['replica_on_primary_node'] and report['primary_nodes_in_use'] > 1
            and report['no_single_unit_holds_everything'])


def sample_ids(client, wanted):
    ids = []
    for page in pages(client, '/internal/audit/events', {}, limit=1000):
        ids.extend(item['hazard_id'] for item in page['items'])
    if len(ids) <= wanted:
        return ids
    stride = len(ids)/wanted
    return [ids[int(i*stride)] for i in range(wanted)]


def citus_placement(cfg):
    try:
        from acceptance_support import canonical_sql
        return canonical_sql(cfg, 'SELECT nodename, count(*), coalesce(sum(shard_size),0) FROM citus_shards GROUP BY nodename ORDER BY nodename')
    except (RuntimeError, ValueError, OSError):
        return None


def cmd_distribution(cfg, args):
    nodes = list(cfg['nodes'].values())
    with AuditClient(cfg, base_url=args.base_url) as client:
        ids = sample_ids(client, args.sample)
        rows = []
        for hazard_id in ids:
            status, body, _ = client.get('/internal/audit/location', {'hazard_id': hazard_id})
            rows.append(body if status == 200 and body else {'hazard_id': hazard_id, 'partial': True})
    report = summarize_locations(rows, nodes)
    print(f"sampled {report['sampled']} of {len(ids)} chosen HazardEvents ({len(rows)-report['sampled']} unreachable)")
    print(f"{'node':20} {'primary events':>15} {'replica events':>15}")
    for node in nodes:
        print(f"{node:20} {report['primaries_per_node'].get(node, 0):>15} {report['replicas_per_node'].get(node, 0):>15}")
    print('events per primary unit: '+', '.join(f'{u}={n}' for u, n in report['events_per_primary_unit'].items()))
    if report['largest_unit_share'] is not None:
        print(f"largest unit share: {report['largest_unit_share']*100:.1f}% (no single unit holds everything: "
              f"{report['no_single_unit_holds_everything']})")
    print(f"primary roles span {report['primary_nodes_in_use']} node(s); events without replica: {len(report['without_replica'])}; "
          f"replica on the primary's node: {len(report['replica_on_primary_node'])}")
    shards = citus_placement(cfg)
    if shards is None:
        print('Citus shard placement: unavailable (check /internal/audit/location results above instead)')
    else:
        for node, count, size in shards:
            print(f'Citus shards on {node}: {count} placements, {int(size)/1048576:.1f} MiB')
    print('PLACEMENT OK' if placement_ok(report) else 'PLACEMENT CHECK FAILED')
    return 0 if placement_ok(report) else 1


def cmd_nodeload(cfg, args):
    nodes = list(cfg['nodes'].values())
    peaks = {node: {'cpu': 0, 'mem': 0} for node in nodes}
    deadline = time.monotonic()+args.duration
    print(f"{'time (UTC)':10} "+' '.join(f'{n[:18]:>18} cpu(m)/mem(MiB)' for n in nodes))
    samples = []
    while True:
        usage = node_usage(cfg)
        cells = []
        for node in nodes:
            item = usage.get(node)
            if not item or item['cpu_usage_nano_cores'] is None:
                cells.append(f"{'unavailable':>35}")
                continue
            cpu, mem = item['cpu_usage_nano_cores']/1e6, item['memory_working_set_bytes']/1048576
            peaks[node]['cpu'], peaks[node]['mem'] = max(peaks[node]['cpu'], cpu), max(peaks[node]['mem'], mem)
            cells.append(f'{cpu:>18.0f} / {mem:>10.0f}')
        samples.append({'at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'usage': usage})
        print(f"{datetime.datetime.now(datetime.timezone.utc):%H:%M:%S}   "+' '.join(cells), flush=True)
        if time.monotonic() >= deadline:
            break
        time.sleep(args.interval)
    print('peak per node: '+'; '.join(f"{n}: {p['cpu']:.0f} mCPU, {p['mem']:.0f} MiB" for n, p in peaks.items()))
    if args.output:
        private_json(Path(args.output), {'nodes': nodes, 'samples': samples, 'peaks': peaks})
    return 0


def spike_events(run_id, count, area, lat, lon, now):
    for index in range(count):
        yield {'source': 'BMKG', 'source_ref_id': f'spike-{run_id}-{index:06d}', 'hazard_type': 'SEISMIC',
               'severity': 'SIAGA', 'area_name': area,
               # Tiny deterministic jitter keeps every event in one region.
               'latitude': round(lat+(index % 7)*0.001, 5), 'longitude': round(lon+(index % 11)*0.001, 5),
               'occurred_at': now, 'attributes': {'magnitude': 5.0, 'depth_km': 10, 'synthetic': True}}


def cmd_spike(cfg, args):
    run_id = args.run_id or datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')
    now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    events = list(spike_events(run_id, args.count, args.area, args.latitude, args.longitude, now))
    print(f'{len(events)} synthetic BMKG events in "{args.area}" with identities spike-{run_id}-NNNNNN')
    if not args.execute:
        print('Dry run. These events become part of the canonical dataset and Audit checksum; add --execute to send.')
        return 0
    from evidence_integration_tests import forward
    token = runtime_secrets(LOCAL/'secrets.json')['admin']
    started, accepted = time.monotonic(), 0
    with forward(cfg, 'aggregator', 8080) as port:
        for start in range(0, len(events), 20):
            batch = events[start:start+20]
            request = urllib.request.Request(f'http://127.0.0.1:{port}/internal/ops/catalog',
                data=json.dumps({'events': batch}).encode(), headers={'Authorization': 'Bearer '+token,
                                                                    'Content-Type': 'application/json'})
            for attempt in range(6):
                try:
                    with urllib.request.urlopen(request, timeout=15) as response:
                        accepted += json.load(response)['accepted']
                        break
                except urllib.error.HTTPError as error:
                    if error.code not in (429, 502, 503, 504):
                        raise RuntimeError(f'spike rejected (HTTP {error.code})') from None
                except (urllib.error.URLError, TimeoutError):
                    pass
                time.sleep(min(2**attempt, 5))
            else:
                raise RuntimeError('spike interrupted; rerun with the same --run-id (identities are idempotent)')
    elapsed = time.monotonic()-started
    print(f'acknowledged {accepted}/{len(events)} in {elapsed:.1f}s ({accepted/elapsed:.0f} events/s)')
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--base-url')
    commands = parser.add_subparsers(dest='command', required=True)
    sub = commands.add_parser('distribution')
    sub.add_argument('--sample', type=int, default=200)
    sub = commands.add_parser('nodeload')
    sub.add_argument('--duration', type=float, default=60)
    sub.add_argument('--interval', type=float, default=5)
    sub.add_argument('--output')
    sub = commands.add_parser('spike')
    sub.add_argument('--count', type=int, default=2000)
    sub.add_argument('--area', default='Sumatera Barat')
    sub.add_argument('--latitude', type=float, default=-0.95)
    sub.add_argument('--longitude', type=float, default=100.35)
    sub.add_argument('--run-id')
    sub.add_argument('--execute', action='store_true')
    args = parser.parse_args(argv)
    if args.command == 'spike' and not 1 <= args.count <= 200000:
        parser.error('count must be 1..200000')
    cfg = read_config(args.config)
    return {'distribution': cmd_distribution, 'nodeload': cmd_nodeload, 'spike': cmd_spike}[args.command](cfg, args)


if __name__ == '__main__':
    sys.exit(main())
