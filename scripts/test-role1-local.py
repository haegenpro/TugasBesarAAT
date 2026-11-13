#!/usr/bin/env python3
"""Record local M3/M4 checks. External settings are removed; this never uses an application DB."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT=Path(__file__).resolve().parent.parent

def source_digest():
    digest=hashlib.sha256()
    paths=sorted(p for folder in ('aggregator','experiments/m1','store/migrations','scripts')
                 for p in (ROOT/folder).rglob('*')
                 if p.is_file() and (p.suffix in ('.go','.sql','.py') or p.name in ('go.mod','go.sum')))
    for path in paths:
        digest.update(path.relative_to(ROOT).as_posix().encode()+b'\0'+path.read_bytes()+b'\0')
    return digest.hexdigest()

def main():
    run=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dt%H%M%S')+'-'+uuid.uuid4().hex[:8]
    output=ROOT/'evidence'/'role1-m3-m4'/run
    output.mkdir(parents=True,mode=0o700)
    env={k:v for k,v in os.environ.items() if not k.startswith(('M2_','M3_')) and k!='TEST_POSTGRES_URL'}
    steps=[('aggregator-native',ROOT/'aggregator',['go','test','-mod=readonly','-count=1','-json','./...']),
           ('aggregator-vet',ROOT/'aggregator',['go','vet','-mod=readonly','./...']),
           ('aggregator-race',ROOT/'aggregator',['go','test','-mod=readonly','-count=1','-race','-json','./...']),
           ('experiment-native',ROOT/'experiments/m1',['go','test','-mod=readonly','-count=1','-json','./...']),
           ('experiment-vet',ROOT/'experiments/m1',['go','vet','-mod=readonly','./...']),
           ('experiment-race',ROOT/'experiments/m1',['go','test','-mod=readonly','-count=1','-race','-json','./...']),
           ('renderers',ROOT,[sys.executable,'-m','unittest','discover','-s','scripts','-p','*renderer_test.py'])]
    result={'run_id':run,'checks':[],'container_acceptance':'blocked: requires executable isolated database/broker and actual cluster metadata; native skips are not passing integration evidence'}
    print('Evidence: '+str(output),flush=True)
    for name,cwd,command in steps:
        start=datetime.datetime.now(datetime.timezone.utc)
        before=source_digest()
        log=output/(name+'.log')
        with log.open('wb') as stream:
            try:
                completed=subprocess.run(command,cwd=cwd,env=env,stdout=stream,stderr=subprocess.STDOUT,timeout=900)
                status='passed' if completed.returncode==0 else 'failed'
            except (OSError,subprocess.TimeoutExpired) as exc:
                stream.write(str(exc).encode());status='blocked'
        after=source_digest()
        if before!=after: status='failed'
        counts={'passed_tests':0,'skipped_tests':0,'failed_tests':0}
        for line in log.read_text(encoding='utf-8',errors='replace').splitlines():
            try:event=json.loads(line)
            except ValueError:continue
            if not event.get('Test'):continue
            key={'pass':'passed_tests','skip':'skipped_tests','fail':'failed_tests'}.get(event.get('Action'))
            if key:counts[key]+=1
        item={'name':name,'status':status,'command':command,'cwd':str(cwd.relative_to(ROOT)),'source_sha256_before':before,'source_sha256_after':after,'elapsed_seconds':(datetime.datetime.now(datetime.timezone.utc)-start).total_seconds(),**counts}
        result['checks'].append(item)
        (output/'summary.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
        print(name+': '+status+' '+json.dumps(counts),flush=True)
    version=subprocess.run(['go','version'],cwd=ROOT/'aggregator',capture_output=True,text=True)
    result['go_version']=version.stdout.strip()
    (output/'summary.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    return 0 if all(v['status']=='passed' for v in result['checks']) else 1

if __name__=='__main__':sys.exit(main())
