"""Small helpers for local acceptance probes; never serialize credentials."""
import json
import time
import urllib.error
import urllib.request

from common import NAMESPACE, kubectl


def nodeport(cfg):
    service = json.loads(kubectl(cfg,'-n',NAMESPACE,'get','service/client-api','-o','json'))
    node = json.loads(kubectl(cfg,'get','node',cfg['nodes']['a'],'-o','json'))
    host = next(a['address'] for a in node['status']['addresses'] if a['type']=='InternalIP')
    return f"http://{host}:{service['spec']['ports'][0]['nodePort']}"


def http(base, path, body=None, token=None, headers=None, timeout=5, method=None):
    outgoing = dict(headers or {})
    if token: outgoing['Authorization']='Bearer '+token
    if body is not None:
        outgoing['Content-Type']='application/json'
        body = body if isinstance(body,bytes) else json.dumps(body).encode()
    request=urllib.request.Request(base+path,data=body,headers=outgoing,method=method)
    started=time.monotonic()
    try:
        with urllib.request.urlopen(request,timeout=timeout) as response:
            return response.status,response.read(),dict(response.headers),time.monotonic()-started
    except urllib.error.HTTPError as error:
        return error.code,error.read(),dict(error.headers),time.monotonic()-started
    except (OSError,TimeoutError):
        return 0,b'',{},time.monotonic()-started


class Session:
    def __init__(self,base,credentials,role='field'):
        self.base,self.credentials,self.role=base,credentials,role
        self.session=None
        self.until=0

    def request(self,path,body=None,headers=None,timeout=5,method=None):
        if not self.session or time.monotonic()>=self.until:
            refresh=self.session and self.session.get('refresh_token')
            status,payload,_,duration=http(self.base,'/auth/refresh' if refresh else '/auth/login',
                {'refresh_token':refresh} if refresh else {'client_id':self.role+'-demo' if self.role!='internal' else 'internal-demo',
                                                         'client_secret':self.credentials[self.role]},timeout=timeout)
            if status!=200:
                if status==401:self.session=None
                return status,payload,{},duration
            self.session=json.loads(payload)
            self.until=time.monotonic()+max(1,int(self.session['expires_in'])-5)
        result=http(self.base,path,body,self.session['access_token'],headers,timeout,method)
        if result[0]==401:self.session=None
        return result


def owned_sql(cfg, formation, database, role, password_env, query, params=(), group=None):
    selector=f'formation={formation},database-role=primary'
    if group is not None:selector+=f',citus-group={group}'
    pods=json.loads(kubectl(cfg,'-n',NAMESPACE,'get','pods','-l',selector,'-o','json'))['items']
    if len(pods)!=1:raise RuntimeError('owner primary is not unambiguous')
    program='''import os,json,psycopg2
c=psycopg2.connect(host='127.0.0.1',dbname=DATABASE,user=ROLE,password=os.environ[PASSWORD_ENV])
c.autocommit=True
with c.cursor() as cur:
 cur.execute(QUERY,PARAMS)
 print(json.dumps(cur.fetchall() if cur.description else [],default=str))
c.close()
'''
    for placeholder,value in [('DATABASE',database),('ROLE',role),('PASSWORD_ENV',password_env),('QUERY',query),('PARAMS',list(params))]:
        program=program.replace(placeholder,repr(value))
    raw=kubectl(cfg,'-n',NAMESPACE,'exec','-i',pods[0]['metadata']['name'],'-c','postgres','--',
                '/opt/patroni/bin/python','-',input_text=program,timeout=40)
    return json.loads(raw)


def canonical_sql(cfg,query,params=()):
    return owned_sql(cfg,'canonical','hazards','canonical_app','CANONICAL_PASSWORD',query,params,0)


def ingestion_sql(cfg,query,params=()):
    return owned_sql(cfg,'ingestion','ingestion','ingestion_app','INGESTION_PASSWORD',query,params)


def quiesce_sources(cfg):
    kubectl(cfg,'-n',NAMESPACE,'set','env','deployment/aggregator',
            'AGGREGATOR_POLL_ENABLED=false','AGGREGATOR_REPLAY_ENABLED=false')
    kubectl(cfg,'-n',NAMESPACE,'set','env','deployment/ingestion','PANTAI_AUTH_TOKEN=')
    for name in ('aggregator','ingestion'):
        kubectl(cfg,'-n',NAMESPACE,'rollout','status','deployment/'+name,'--timeout=180s',timeout=190)
    for name in ('wildfire-processor','coastal-processor','field-report-processor'):
        kubectl(cfg,'-n',NAMESPACE,'scale','deployment/'+name,'--replicas=0')
    deadline=time.monotonic()+90
    while time.monotonic()<deadline:
        pods=json.loads(kubectl(cfg,'-n',NAMESPACE,'get','pods','-o','json'))['items']
        pending=[p for p in pods if p['metadata'].get('labels',{}).get('app') in
                 ('wildfire-processor','coastal-processor','field-report-processor')
                 and p['status']['phase'] not in ('Succeeded','Failed')]
        if not pending:return
        time.sleep(2)
    raise RuntimeError('processor shutdown did not complete before the measurement')


def restore_sources(cfg):
    kubectl(cfg,'-n',NAMESPACE,'set','env','deployment/aggregator',
            'AGGREGATOR_POLL_ENABLED-','AGGREGATOR_REPLAY_ENABLED-')
    kubectl(cfg,'-n',NAMESPACE,'set','env','deployment/ingestion','PANTAI_AUTH_TOKEN-')
    for name in ('wildfire-processor','coastal-processor','field-report-processor'):
        kubectl(cfg,'-n',NAMESPACE,'scale','deployment/'+name,'--replicas=2')
