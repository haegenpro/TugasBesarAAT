#!/usr/bin/env python3
"""Capture a small private status/Audit/resource snapshot without Secret manifests.

Criteria: Platform (topology, requests/limits, role labels) and P1-4 (checksum before/after).
"""
import argparse
import datetime
import json
from pathlib import Path
import subprocess
import urllib.request
from common import APPLICATION, LABEL, LOCAL, NAMESPACE, kubectl, private_json, read_config, runtime_secrets
from evidence_integration_tests import forward


def collect(cfg):
    report={'captured_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'context':cfg['context'],'profile':cfg['profile'],'nodes':cfg['nodes'],
            'image_tag':cfg['image_tag'],'satellite_namespace':cfg['satellite']['namespace'],
            'errors':[]}
    report['revision']=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
    report['uncommitted_files']=subprocess.check_output(['git','status','--porcelain'],text=True).splitlines()
    nodes=json.loads(kubectl(cfg,'get','nodes','-o','json'))['items']
    report['node_status']=[{'name':n['metadata']['name'],'capacity':n['status'].get('capacity'),
                           'allocatable':n['status'].get('allocatable'),'conditions':n['status'].get('conditions')}
                          for n in nodes if n['metadata']['name'] in cfg['nodes'].values()]
    pods=json.loads(kubectl(cfg,'-n',NAMESPACE,'get','pods','-l',f'{LABEL}={APPLICATION}','-o','json'))['items']
    report['pods']=[{'name':p['metadata']['name'],'labels':p['metadata']['labels'],
                     'node':p['spec'].get('nodeName'),'phase':p['status']['phase'],
                     'conditions':p['status'].get('conditions'),
                     'containers':[{'name':c['name'],'image':c['image'],'resources':c.get('resources')}
                                   for c in p['spec']['containers']],
                     'container_status':p['status'].get('containerStatuses')}
                    for p in pods]
    report['working_set']={}
    for node in cfg['nodes'].values():
        try:
            summary=json.loads(kubectl(cfg,'get','--raw','/api/v1/nodes/'+node+'/proxy/stats/summary',timeout=30))
            report['working_set'][node]=[{'pod':p['podRef']['name'],
                'working_set_bytes':p.get('memory',{}).get('workingSetBytes'),
                'containers':[{'name':c['name'],'working_set_bytes':c.get('memory',{}).get('workingSetBytes')}
                              for c in p.get('containers',[])]}
                for p in summary.get('pods',[]) if p['podRef']['namespace']==NAMESPACE]
        except (RuntimeError,ValueError,subprocess.TimeoutExpired): report['errors'].append('working set unavailable: '+node)
    credentials=runtime_secrets(LOCAL/'secrets.json')
    try:
        with forward(cfg,'aggregator',8080) as port:
            token=cfg['external'].get('audit_token') or credentials['audit']
            request=urllib.request.Request(f'http://127.0.0.1:{port}/internal/audit/checksum',headers={'Authorization':'Bearer '+token})
            with urllib.request.urlopen(request,timeout=8) as response:report['audit_checksum']=json.load(response)
    except (OSError,RuntimeError,ValueError):report['errors'].append('Audit checksum unavailable')
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',default=str(LOCAL/'config.json'))
    parser.add_argument('--output',required=True,help='new private evidence directory')
    args=parser.parse_args();cfg=read_config(args.config)
    directory=Path(args.output);directory.mkdir(mode=0o700,parents=False,exist_ok=False)
    report=collect(cfg);private_json(directory/'status.json',report)
    print('Saved private status evidence: '+str(directory/'status.json'))
    if report['errors']:print('Unavailable evidence: '+', '.join(report['errors']))


if __name__=='__main__':main()
