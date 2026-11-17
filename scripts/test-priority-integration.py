#!/usr/bin/env python3
"""Focused priority regressions with an owned temporary PostgreSQL and Go runner."""
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import uuid
ROOT=Path(__file__).resolve().parent.parent

def main():
 os.umask(0o077);run=uuid.uuid4().hex[:12];network='aat-priority-check-'+run;created=[];net_created=False
 output=ROOT/'evidence/platform-priority'/run;output.mkdir(parents=True)
 password=secrets.token_hex(24);env={**os.environ,'POSTGRES_PASSWORD':password,'TEST_POSTGRES_URL':'postgres://postgres:'+password+'@postgres:5432/postgres?sslmode=disable'}
 report={'passed':False,'scope':'isolated scheduling and connection-pool saturation, not four-host CPU/network or load acceptance','modules':{}}
 def docker(*args,check=True):return subprocess.run(['docker',*map(str,args)],env=env,capture_output=True,text=True,timeout=240,check=check)
 try:
  docker('network','create','--internal','--label','aat.priority.check='+run,network);net_created=True
  pg=network+'-pg';docker('run','-d','--name',pg,'--label','aat.priority.check='+run,'--network',network,'--network-alias','postgres','--memory','256m','-e','POSTGRES_PASSWORD','postgres:16-alpine3.24');created.append(pg)
  deadline=time.monotonic()+30
  while docker('exec',pg,'pg_isready','-U','postgres',check=False).returncode:
   if time.monotonic()>deadline:raise RuntimeError('temporary PostgreSQL startup deadline')
   time.sleep(.2)
  runner=network+'-go';docker('run','-d','--name',runner,'--label','aat.priority.check='+run,'--network',network,'--memory','2g','--cpus','3','--mount','type=bind,src='+str(ROOT)+',dst=/src,readonly','-v','r3-go-mod:/go/pkg/mod','-v','r3-go-build:/root/.cache/go-build','golang:1.27.1-alpine3.24','tail','-f','/dev/null');created.append(runner)
  for module in ('auth-service','client-api','aggregator'):
   result=docker('exec','-e','TEST_POSTGRES_URL','-w','/src/'+module,runner,'go','test','-mod=readonly','-count=1','-timeout=90s','-json',*(['./internal/store','./internal/server']if module=='aggregator'else['./...']),'-run','TestPriority|TestSignedField|TestVerifiedField|TestSessionSurvives|TestConcurrentRefresh|TestPerClient',check=False)
   (output/(module+'.log')).write_text((result.stdout+result.stderr).replace(password,'[redacted]'))
   counts={'pass':0,'fail':0,'skip':0}
   for line in result.stdout.splitlines():
    try:event=json.loads(line)
    except ValueError:continue
    if event.get('Test')and event.get('Action')in counts:counts[event['Action']]+=1
   report['modules'][module]={'passed':result.returncode==0 and counts['pass']>0 and not counts['skip'],'counts':counts}
  report['passed']=all(v['passed']for v in report['modules'].values())
  print(json.dumps({'evidence':str(output),'passed':report['passed'],'modules':report['modules']}))
 finally:
  cleanup=[]
  for name in reversed(created):
   identity=docker('inspect','--format','{{ index .Config.Labels "aat.priority.check" }}',name,check=False).stdout.strip()
   if identity==run and docker('rm','-f','-v',name,check=False).returncode:cleanup.append(name)
  if net_created:
   identity=docker('network','inspect','--format','{{ index .Labels "aat.priority.check" }}',network,check=False).stdout.strip()
   if identity==run and docker('network','rm',network,check=False).returncode:cleanup.append(network)
  report['cleanup_required']=cleanup
  if cleanup:report['passed']=False
  (output/'result.json').write_text(json.dumps(report,indent=2)+'\n')
 return int(not report['passed'])
if __name__=='__main__':
 try:sys.exit(main())
 except (ValueError,OSError,RuntimeError,subprocess.SubprocessError)as exc:print('Priority checks stopped: '+type(exc).__name__,file=sys.stderr);sys.exit(1)
