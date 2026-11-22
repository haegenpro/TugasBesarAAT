#!/usr/bin/env python3
"""Bounded local k6 runner with resource evidence and optional scaling/rollout.

Criteria: P2-6 (10,000 VUs, 3 -> 5 -> 2, error rate), P2-8 (rolling image replacement); P5-5 is judged from the Field/Media rows of its summary.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.request

from common import (LOCAL, NAMESPACE, ROOT, is_minikube, kubectl, node_usage, private_json,
                    read_config, runtime_secrets)
from tuning import application_env, settings

K6_IMAGE = 'grafana/k6@sha256:3d8e5c7f1d53ec59408c2ffbd9943602a0985ffcbd4a2430ec00f3ffca89796e'
# A paced run that delivers less than this share of the offered rate was limited by the system
# under test (or the generator), not by the offered load.
PACED_SATURATION = 0.95


def available_memory():
    values = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    return int(values['MemAvailable'].split()[0]) * 1024


def metric_value(summary, name, field='count'):
    return summary.get('metrics', {}).get(name, {}).get('values', {}).get(field)


def throughput(summary):
    """Complete hazard pages per second plus validity counters.

    A paced run (`target_rps` in the summary configuration) counts only the hold window, so the
    login ramp and the ramp-down never enter the figure; an unpaced run counts the whole run.
    """
    configuration = summary.get('configuration', {})
    target = configuration.get('target_rps') or 0
    if target > 0:
        seconds = configuration.get('hold_seconds') or 0
        complete = metric_value(summary, 'm2_paced_complete_hazard_reads') or 0
    else:
        seconds = summary.get('state', {}).get('testRunDurationMs', 0)/1000
        complete = metric_value(summary, 'm2_complete_hazard_reads') or 0
    rate = complete/seconds if seconds else 0.0
    result = {'complete_pages': complete, 'seconds': seconds, 'pages_per_second': rate,
              'partial_pages': metric_value(summary, 'm2_partial_hazard_reads') or 0,
              'invalid_responses': metric_value(summary, 'm2_invalid_hazard_responses') or 0,
              'unexpected_rate': metric_value(summary, 'm2_unexpected_errors', 'rate'),
              'transport_failures': metric_value(summary, 'm2_transport_failures') or 0,
              'target_rps': target or None}
    if target > 0:
        result.update({'paced_requests': metric_value(summary, 'm2_paced_requests') or 0,
                       'vus_exercised': metric_value(summary, 'm2_paced_vus_exercised') or 0,
                       'configured_vus': configuration.get('vus'),
                       'saturated': rate < PACED_SATURATION*target})
    return result


def evaluate(summary, report, args, duplicates=None):
    """Pass/fail verdicts for the criteria this run can evidence; nothing is excluded to pass."""
    checks = []

    def check(criterion, ok, detail):
        checks.append({'criterion': criterion, 'ok': bool(ok), 'detail': detail})
    rate = metric_value(summary, 'm2_unexpected_errors', 'rate')
    check('P2-6 unexpected error rate below 1% (controlled 429 excluded)', rate is not None and rate < 0.01,
          'missing' if rate is None else f'{rate*100:.4f}%')
    if args.vus >= 10000:
        held = report.get('observed_target_vu_seconds', 0)
        check('P2-6 at least 120 s continuously at the target VUs', held >= 120,
              f"{held:.1f}s continuous, {report.get('target_vu_first_to_last_seconds', 0):.1f}s first-to-last")
    invalid = metric_value(summary, 'm2_invalid_hazard_responses')
    check('hazard responses are complete or explicitly partial', invalid == 0, f'{invalid} invalid')
    if getattr(args, 'target_rps', 0) > 0:
        exercised = metric_value(summary, 'm2_paced_vus_exercised') or 0
        check('paced run: every VU sent a measured request', exercised >= args.vus,
              f'{exercised} of {args.vus} VUs')
    actions = {a['name'] for a in report.get('actions', [])}
    if args.scale:
        check('P2-6 scaled 3 -> 5 -> 2 during the hold', {'scale-5', 'scale-2'} <= actions, ', '.join(sorted(actions)) or 'none')
    if args.roll_from:
        check('P2-8 Aggregator image replaced during the hold', 'roll-version' in actions, ', '.join(sorted(actions)) or 'none')
    if getattr(args, 'strict_zero_failures', False):
        server = sum(metric_value(summary, f'm2_http_status{{status:{s}}}') or 0 for s in (500, 502, 503, 504))
        transport = metric_value(summary, 'm2_transport_failures') or 0
        failed = metric_value(summary, 'm2_unexpected_errors', 'passes') or 0
        check('P2-8 no failed request (controlled 429 with Retry-After excluded)', server == 0 and transport == 0 and failed == 0,
              f'{server} 5xx, {transport} transport failures, {failed} unexpected failures')
    if duplicates is not None:
        check('P2-2 no duplicate HazardEvent after the run', not duplicates['duplicate_ids'] and not duplicates['duplicate_refs'],
              f"{duplicates['duplicate_ids']} duplicate ids, {duplicates['duplicate_refs']} duplicate source identities")
    return checks


def audit_duplicates(cfg):
    """Scan every source through the Audit API and count duplicate identities."""
    from evidence_audit import AuditClient, SOURCES, analyze, scan_source
    ids = refs = 0
    with AuditClient(cfg) as client:
        for source in SOURCES:
            report = analyze(scan_source(client, source)['items'])
            ids += len(report['duplicate_hazard_ids'])
            refs += len(report['duplicate_source_refs'])
    return {'duplicate_ids': ids, 'duplicate_refs': refs}


def request_json(port, path, container=None):
    """Read a k6 REST API answer; fall back to `docker exec` when the host cannot reach it.

    Docker Desktop runs --network=host containers in the Docker VM's network namespace, so the
    API bound to 127.0.0.1 inside the container is unreachable from a WSL distro. Without the
    fallback no VU observation is possible and the scale/rollout actions never start.
    """
    try:
        with urllib.request.urlopen(f'http://127.0.0.1:{port}'+path, timeout=5) as response:
            return json.load(response)
    except OSError:
        if not container:
            raise
        result = subprocess.run(['docker', 'exec', container, 'wget', '-qO-', f'http://127.0.0.1:{port}{path}'],
                                capture_output=True, text=True, timeout=8)
        if result.returncode:
            raise OSError('k6 REST API unreachable, even through docker exec')
        return json.loads(result.stdout)


def run(args):
    cfg = read_config(args.config)
    output = Path(args.output).resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    credentials = runtime_secrets(LOCAL/'secrets.json')
    private_json(output/'credentials.json', {key: credentials[key] for key in ('media', 'field')})
    service = json.loads(kubectl(cfg, '-n', NAMESPACE, 'get', 'service/client-api', '-o', 'json'))
    node = json.loads(kubectl(cfg, 'get', 'node', cfg['nodes']['a'], '-o', 'json'))
    host = next(a['address'] for a in node['status']['addresses'] if a['type']=='InternalIP')
    base = args.base_url or f"http://{host}:{service['spec']['ports'][0]['nodePort']}"
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0)); api_port = probe.getsockname()[1]
    name = 'aat-m2-load-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')
    # Minikube nodes are Docker containers whose stats/limits Docker can report.
    # On k3s hosts the same data comes from the kubelet instead.
    container_names = [name, *cfg['nodes'].values()] if is_minikube(cfg) else [name]
    target_image = cfg['image_prefix'].rstrip('/')+'/aggregator:'+cfg['image_tag']
    commands = ['docker', 'run', '--name', name, '--pull=never', '--network=host',
                '--user', f'{os.getuid()}:{os.getgid()}', '--memory', str(args.memory_mib)+'m',
                '--memory-swap', str(args.memory_mib)+'m', '--cpus', str(args.cpus),
                '--ulimit', 'nofile=65536:65536', '--read-only', '--tmpfs', '/tmp:rw,size=64m',
                '--mount', f'type=bind,src={ROOT}/scripts/m2/p2_load_test.js,dst=/test/load.js,readonly',
                '--mount', f'type=bind,src={output},dst=/results',
                '-e', 'M2_CREDENTIALS_FILE=/results/credentials.json',
                '-e', 'M2_BASE_URL='+base, '-e', 'M2_SUMMARY_FILE=/results/summary.json',
                '-e', 'M2_VUS='+str(args.vus), '-e', 'M2_RAMP='+str(args.ramp_seconds)+'s',
                '-e', 'M2_HOLD='+str(args.hold_seconds)+'s', '-e', 'M2_THINK_SECONDS='+str(args.think_seconds),
                '-e', 'M2_REPORT_EVERY='+str(args.report_every), '-e', 'M2_RUN_ID='+name,
                '-e', 'M2_TARGET_RPS='+str(getattr(args, 'target_rps', 0)),
                '-e', 'GOMEMLIMIT='+str(max(128, args.memory_mib-512))+'MiB', '-e', 'GOGC=50',
                '-e', 'K6_NO_USAGE_REPORT=true', K6_IMAGE, 'run', '--address', f'127.0.0.1:{api_port}',
                '--no-color', '/test/load.js']
    report = {'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'revision': subprocess.check_output(['git','rev-parse','HEAD'], cwd=ROOT, text=True).strip(),
              'colocated_generator': True, 'base_url': base, 'image_tag': cfg['image_tag'],
              'k6_image': K6_IMAGE, 'requested_vus': args.vus, 'ramp_seconds': args.ramp_seconds,
              'hold_seconds': args.hold_seconds, 'think_seconds': args.think_seconds,
              'target_rps': getattr(args, 'target_rps', 0),
              'generator_memory_mib': args.memory_mib, 'generator_cpus': args.cpus,
              'generator_container_name': name,
              'report_every': args.report_every, 'samples': [], 'actions': [], 'host_memory_guard': False}
    report['tuning']=settings(cfg)
    report['source_configuration'] = {
        'satellite_configured': bool(cfg['external'].get('satellite_token')),
        'coastal_profile': cfg['coastal']['profile'],
        'coastal_speed': cfg['coastal']['speed']}
    report['uncommitted_files']=subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True).splitlines()
    report['source_diff_sha256']=hashlib.sha256(subprocess.check_output(['git','diff','HEAD'],cwd=ROOT)).hexdigest()
    report['load_script_sha256']=hashlib.sha256((ROOT/'scripts/m2/p2_load_test.js').read_bytes()).hexdigest()
    report['load_runner_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    controllers=json.loads(kubectl(cfg,'-n',NAMESPACE,'get','deployments','-o','json'))['items']
    report['controllers_before']={o['metadata']['name']:{'replicas':o['spec']['replicas'],
        'ready':o['status'].get('readyReplicas',0),
        'images': {container['name']: container['image']
                   for container in o['spec']['template']['spec']['containers']}}
        for o in controllers}
    pods = json.loads(kubectl(cfg, '-n', NAMESPACE, 'get', 'pods', '-o', 'json'))['items']
    report['runtime_images_before'] = {
        pod['metadata']['name']: [
            {key: container.get(key) for key in ('name', 'image', 'imageID')}
            for container in pod['status'].get('containerStatuses', [])]
        for pod in pods}
    # Record explicit overrides as well as the operator configuration. Only
    # allowlisted nonsecret tuning keys may enter the evidence file.
    report['runtime_tuning_overrides'] = {}
    for controller in controllers:
        component = controller['metadata']['name']
        safe_keys = set(application_env(cfg, component)) | {
            'M2_WORKER_READS', 'AGGREGATOR_WORKER_READ_CONNS',
            'AGGREGATOR_POLL_ENABLED', 'AGGREGATOR_REPLAY_ENABLED'}
        report['runtime_tuning_overrides'][component] = {
            item['name']: item['value']
            for container in controller['spec']['template']['spec']['containers']
            for item in container.get('env', [])
            if item['name'] in safe_keys and 'value' in item
        }
    # The P2-7 comparison pauses acquisition and processors on purpose (fixed dataset), so it opts out.
    if args.vus>=10000 and not getattr(args,'fixed_dataset',False):
        for processor in ('wildfire-processor','coastal-processor','field-report-processor'):
            if report['controllers_before'][processor]['replicas']<1:
                raise ValueError('restore processors before the 10,000-VU acceptance run')
        for o in controllers:
            env=o['spec']['template']['spec']['containers'][0].get('env',[])
            if o['metadata']['name']=='aggregator' and any(e['name'] in ('AGGREGATOR_POLL_ENABLED','AGGREGATOR_REPLAY_ENABLED') and e.get('value')=='false' for e in env):
                raise ValueError('restore source acquisition before the 10,000-VU acceptance run')
            if o['metadata']['name']=='ingestion' and any(e['name']=='PANTAI_AUTH_TOKEN' and e.get('value')=='' for e in env):
                raise ValueError('restore coastal acquisition before the 10,000-VU acceptance run')
    report['profile'] = cfg.get('profile')
    report['node_container_limits'] = {}
    if is_minikube(cfg):
        for node_name in cfg['nodes'].values():
            host_config = json.loads(subprocess.check_output(
                ['docker','inspect','--format','{{json .HostConfig}}',node_name],text=True))
            # Only public resource fields, never Docker environment or mounts.
            report['node_container_limits'][node_name] = {
                key: host_config.get(key)
                for key in ('Memory','NanoCpus','CpuQuota','CpuPeriod','CpusetCpus','CpuShares')}
    else:
        nodes = json.loads(kubectl(cfg,'get','nodes','-o','json'))['items']
        report['node_capacity'] = {n['metadata']['name']: {'capacity': n['status'].get('capacity'),
                                                            'allocatable': n['status'].get('allocatable')}
                                   for n in nodes if n['metadata']['name'] in cfg['nodes'].values()}
    high_since = None
    high_last = None
    current_high = None
    longest_high = 0
    process = None
    started = time.monotonic()
    with (output/'generator.log').open('w') as log:
        try:
            if args.roll_from:
                kubectl(cfg, '-n', NAMESPACE, 'set', 'image', 'deployment/aggregator', 'aggregator='+args.roll_from)
                kubectl(cfg, '-n', NAMESPACE, 'rollout', 'status', 'deployment/aggregator', '--timeout=180s', timeout=190)
            process = subprocess.Popen(commands, stdout=log, stderr=subprocess.STDOUT)
            generator_started = time.monotonic()
            while process.poll() is None:
                elapsed = time.monotonic()-started
                sample = {'elapsed_seconds': elapsed, 'host_available_bytes': available_memory()}
                try:
                    sample['vus'] = request_json(api_port, '/v1/metrics/vus', name)['data']['attributes']['sample']['value']
                except (OSError, ValueError, KeyError, subprocess.TimeoutExpired):
                    sample['vus'] = None
                sample['metrics']={}
                for metric in ('m2_unexpected_errors','m2_admission_rejections','m2_successful_hazard_reads'):
                    try:sample['metrics'][metric]=request_json(api_port,'/v1/metrics/'+metric,name)['data']['attributes']['sample']
                    except (OSError,ValueError,KeyError,subprocess.TimeoutExpired):pass
                if sample['vus'] is not None and sample['vus'] >= args.vus:
                    if high_since is None:
                        high_since = elapsed
                        print(f'Observed {args.vus} active VUs at {elapsed:.1f}s.', flush=True)
                    high_last = elapsed
                    if current_high is None: current_high = elapsed
                    longest_high = max(longest_high, elapsed-current_high)
                else:
                    current_high = None
                if args.scale or args.roll_from:
                    deployment = json.loads(kubectl(cfg,'-n',NAMESPACE,'get','deployment/aggregator','-o','json'))
                    sample['aggregator'] = {'desired':deployment['spec']['replicas'],
                        'ready':deployment['status'].get('readyReplicas',0),
                        'updated':deployment['status'].get('updatedReplicas',0),
                        'image':deployment['spec']['template']['spec']['containers'][0]['image']}
                try:
                    stats = subprocess.run(['docker','stats','--no-stream','--format','{{json .}}',*container_names],
                                           capture_output=True, text=True, timeout=8)
                    sample['docker'] = [json.loads(line) for line in stats.stdout.splitlines() if line]
                except (subprocess.TimeoutExpired, ValueError):
                    sample['docker'] = []
                if not is_minikube(cfg):
                    sample['nodes'] = node_usage(cfg)
                report['samples'].append(sample)
                if sample['host_available_bytes'] < 512*1024**2:
                    report['host_memory_guard'] = True
                    subprocess.run(['docker','stop','-t','5',name], stdout=log, stderr=log)
                    break
                if time.monotonic()-generator_started > args.ramp_seconds+args.hold_seconds+180:
                    report['deadline_exceeded'] = True
                    subprocess.run(['docker','stop','-t','5',name], stdout=log, stderr=log)
                    break
                if (args.scale or args.roll_from) and high_since is not None:
                    age = elapsed-high_since
                    schedule = ([(15,'scale-5',5)] if args.scale else []) + \
                               ([(45,'roll-version',None)] if args.roll_from else []) + \
                               ([(80,'scale-2',2)] if args.scale else [])
                    action = next(((label,replicas) for after,label,replicas in schedule
                                   if age>=after and not any(a['name']==label for a in report['actions'])),None)
                    if action:
                        label, replicas = action
                        if replicas is None:
                            kubectl(cfg, '-n', NAMESPACE, 'set', 'image', 'deployment/aggregator', 'aggregator='+target_image)
                        else:
                            kubectl(cfg, '-n', NAMESPACE, 'scale', 'deployment/aggregator', '--replicas='+str(replicas))
                        report['actions'].append({'name': label, 'elapsed_seconds': elapsed, 'vus': sample['vus']})
                        print(f'{label} at {elapsed:.1f}s; observed VUs={sample["vus"]}.', flush=True)
                private_json(output/'run.json', report)
                time.sleep(3)
            report['exit_code'] = process.wait(timeout=20)
        finally:
            if process is not None and process.poll() is None:
                subprocess.run(['docker','stop','-t','5',name], stdout=log, stderr=log)
                process.wait(timeout=20)
            state = subprocess.run(['docker','inspect','--format','{{json .State}}',name], capture_output=True, text=True)
            if state.returncode==0:
                report['generator_state'] = json.loads(state.stdout)
                subprocess.run(['docker','rm',name], check=True, stdout=log)
            if args.scale or args.roll_from:
                kubectl(cfg,'-n',NAMESPACE,'scale','deployment/aggregator','--replicas=3')
                kubectl(cfg,'-n',NAMESPACE,'set','image','deployment/aggregator','aggregator='+target_image)
            report['observed_target_vu_seconds'] = longest_high
            report['target_vu_first_to_last_seconds'] = 0 if high_since is None else high_last-high_since
            report['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            summary_path = output/'summary.json'
            if summary_path.exists():
                duplicates = None
                if getattr(args, 'audit_check', False):
                    try:
                        duplicates = audit_duplicates(cfg)
                    except (RuntimeError, OSError, ValueError, KeyError) as error:
                        report['audit_check_error'] = type(error).__name__
                report['throughput'] = throughput(json.loads(summary_path.read_text()))
                report['evaluation'] = evaluate(json.loads(summary_path.read_text()), report, args, duplicates)
                for item in report['evaluation']:
                    print(('PASS ' if item['ok'] else 'FAIL ')+item['criterion']+' ('+item['detail']+')', flush=True)
            private_json(output/'run.json',report)
    print(json.dumps({'output':str(output),'exit_code':report.get('exit_code'),
                      'target_vu_seconds':report['observed_target_vu_seconds'],
                      'host_memory_guard':report['host_memory_guard']}, indent=2),flush=True)
    code=report.get('exit_code',1)
    if code==0 and any(not item['ok'] for item in report.get('evaluation',[])):
        return 1
    return code


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',default=str(LOCAL/'config.json'))
    parser.add_argument('--output',required=True)
    parser.add_argument('--base-url')
    parser.add_argument('--vus',type=int,default=10000)
    parser.add_argument('--ramp-seconds',type=int,default=30)
    parser.add_argument('--hold-seconds',type=int,default=135)
    parser.add_argument('--think-seconds',type=float,default=1)
    parser.add_argument('--memory-mib',type=int,default=4096)
    parser.add_argument('--cpus',type=float,default=2)
    parser.add_argument('--report-every',type=int,default=10)
    parser.add_argument('--target-rps',type=float,default=0,
                        help='offer this many requests per second in total during the hold, spread evenly over all VUs '
                             '(0 keeps every VU looping with --think-seconds)')
    parser.add_argument('--scale',action='store_true')
    parser.add_argument('--roll-from',help='actual previous Aggregator image to roll forward from')
    parser.add_argument('--strict-zero-failures',action='store_true',
                        help='fail the P2-8 verdict on any 5xx, transport failure or unexpected failure')
    parser.add_argument('--audit-check',action='store_true',
                        help='scan the Audit API after the run and fail on duplicate HazardEvents (P2-2)')
    args=parser.parse_args()
    if args.vus<2 or args.ramp_seconds<1 or args.hold_seconds<1 or args.think_seconds<0 or args.memory_mib<768 or args.cpus<=0 or args.report_every<0:
        parser.error('invalid workload or generator bounds')
    if args.scale and args.hold_seconds<120:
        parser.error('scaling rehearsal requires at least a 120-second hold')
    if args.target_rps<0 or (args.target_rps>0 and args.hold_seconds<args.vus/args.target_rps+10):
        parser.error('a paced hold must let every VU send a measured request: hold >= vus/target-rps + 10 seconds')
    return run(args)


if __name__=='__main__':raise SystemExit(main())
