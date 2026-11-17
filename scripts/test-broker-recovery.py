#!/usr/bin/env python3
"""Run three real isolated brokers inside one temporary network-isolated test runner."""
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
from platform_lib.inventory import load
from service_lib import render

ROOT=Path(__file__).resolve().parent.parent

def main():
    os.umask(0o077);run=uuid.uuid4().hex[:12];prefix='aat-broker-check-'+run
    output=ROOT/'evidence/platform-broker-recovery'/run;output.mkdir(parents=True)
    created=[];report={'passed':False,'scope':'three real broker processes on one isolated runner; not four-host acceptance'}
    def docker(*args,check=True):return subprocess.run(['docker',*map(str,args)],check=check,capture_output=True,text=True,timeout=240)
    try:
        image=docker('image','inspect','nats:2.15.0-alpine3.22','--format','{{.Id}}').stdout.strip();report['broker_image']=image
        export=prefix+'-export'
        docker('create','--name',export,'--label','aat.broker.check='+run,image);created.append(export)
        docker('cp',export+':/usr/local/bin/nats-server',output/'nats-server');(output/'nats-server').chmod(0o700)
        inventory=load(ROOT/'deployment/platform/inventory.fourhost.example.json');inventory['namespace']='TEST_NAMESPACE'
        catalog=json.loads((ROOT/'deployment/services/catalog.json').read_text())
        fixture='fixture.invalid/image@sha256:'+'a'*64
        resources,_=render(inventory,catalog,{s['image_key']:fixture for s in catalog['services']if 'image_key'in s})
        config=next(o for o in resources['items']if o['kind']=='ConfigMap'and o['metadata']['name']=='broker-config')['data']['nats.conf']
        (output/'nats-template.conf').write_text(config)
        runner=prefix+'-go';created.append(runner)
        args=['run','--name',runner,'--label','aat.broker.check='+run,'--network','none','--memory','2g','--cpus','3','--mount','type=bind,src='+str(ROOT)+',dst=/src,readonly','--mount','type=bind,src='+str(output)+',dst=/checks,readonly','-v','r3-go-mod:/go/pkg/mod','-v','r3-go-build:/root/.cache/go-build','-e','NATS_SERVER_BINARY=/checks/nats-server','-e','NATS_RECOVERY_CONFIG=/checks/nats-template.conf','-w','/src/broker','golang:1.27.1-alpine3.24','go','test','-mod=readonly','-count=1','-timeout=180s','-json','./cmd/recover-node']
        result=docker(*args,check=False)
        (output/'tests.log').write_text(result.stdout+result.stderr)
        counts={'pass':0,'fail':0,'skip':0}
        for line in result.stdout.splitlines():
            try:event=json.loads(line)
            except ValueError:continue
            if event.get('Test')and event.get('Action')in counts:counts[event['Action']]+=1
        report.update(passed=result.returncode==0 and counts['pass']>=2 and not counts['skip'],counts=counts,returncode=result.returncode)
        print(json.dumps({'evidence':str(output),'passed':report['passed'],'counts':counts}))
    except subprocess.CalledProcessError as exc:
        report['setup_error']=exc.stderr
        raise
    finally:
        cleanup=[]
        for name in reversed(created):
            identity=docker('inspect','--format','{{ index .Config.Labels "aat.broker.check" }}',name,check=False).stdout.strip()
            if identity==run and docker('rm','-f','-v',name,check=False).returncode:cleanup.append(name)
        report['cleanup_required']=cleanup
        if cleanup:report['passed']=False
        (output/'result.json').write_text(json.dumps(report,indent=2)+'\n')
    return int(not report['passed'])

if __name__=='__main__':
    try:sys.exit(main())
    except (ValueError,KeyError,OSError,subprocess.SubprocessError)as exc:print('Broker checks stopped: '+type(exc).__name__,file=sys.stderr);sys.exit(1)
