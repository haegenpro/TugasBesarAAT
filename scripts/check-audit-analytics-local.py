#!/usr/bin/env python3
"""One final native/vet/race pass per changed module; external tests stay explicit."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT=Path(__file__).resolve().parent.parent
def digest():
    value=hashlib.sha256()
    for folder in ('aggregator','client-api','experiments/m1','store/migrations','scripts','docs/fixtures'):
        for path in sorted((ROOT/folder).rglob('*')):
            if path.is_file() and (path.suffix in ('.go','.sql','.py','.js','.css','.html','.json','.jsonl','.avsc') or path.name in ('go.mod','go.sum')):
                value.update(path.relative_to(ROOT).as_posix().encode()+b'\0'+path.read_bytes()+b'\0')
    return value.hexdigest()
def main():
    run=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dt%H%M%S')+'-'+uuid.uuid4().hex[:8]
    output=ROOT/'evidence'/'role1-audit-analytics'/run;output.mkdir(parents=True)
    env={k:v for k,v in os.environ.items() if not k.startswith(('M2_','M3_','M89_','AUDIT_CAPTURE_','ANALYTICS_BROWSER_')) and k!='TEST_POSTGRES_URL'}
    steps=[]
    for folder in ('aggregator','client-api','experiments/m1'):
        name=folder.replace('/','-')
        steps += [(name+'-native',folder,['go','test','-mod=readonly','-count=1','-json','./...']),
                  (name+'-vet',folder,['go','vet','-mod=readonly','./...']),
                  (name+'-race',folder,['go','test','-mod=readonly','-count=1','-race','-json','./...'])]
    steps.append(('renderers','.',[sys.executable,'-m','unittest','discover','-s','scripts','-p','*renderer_test.py']))
    result={'run_id':run,'checks':[],'scope':'Native skips do not establish database, replicated or Kubernetes acceptance; consult separate component evidence.'}
    print('Evidence: '+str(output),flush=True)
    for name,folder,command in steps:
        start=datetime.datetime.now(datetime.timezone.utc);before=digest();path=output/(name+'.log')
        with path.open('wb') as log:
            try:code=subprocess.run(command,cwd=ROOT/folder,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=600).returncode;status='passed' if code==0 else 'failed'
            except (OSError,subprocess.TimeoutExpired) as exc:log.write(str(exc).encode());status='blocked'
        after=digest()
        if before!=after:status='failed'
        counts={'pass':0,'skip':0,'fail':0}
        for line in path.read_text(encoding='utf-8',errors='replace').splitlines():
            try:event=json.loads(line)
            except ValueError:continue
            if event.get('Test') and event.get('Action') in counts:counts[event['Action']]+=1
        item={'name':name,'status':status,'command':command,'source_sha256_before':before,'source_sha256_after':after,'elapsed_seconds':(datetime.datetime.now(datetime.timezone.utc)-start).total_seconds(),'tests':counts}
        result['checks'].append(item);(output/'summary.json').write_text(json.dumps(result,indent=2)+'\n');print(name+': '+status+' '+str(counts),flush=True)
    result['go_version']=subprocess.run(['go','version'],capture_output=True,text=True).stdout.strip()
    (output/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
    return 0 if all(x['status']=='passed' for x in result['checks']) else 1
if __name__=='__main__':sys.exit(main())
