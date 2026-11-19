"""Install Citus worker credentials from existing Secrets on captured groups."""
import hashlib
import json

from storage_lib import durability_config,require


SCRIPT=r'''import json,os,time
import psycopg2
from psycopg2.extensions import make_dsn
groups=json.loads(os.environ['CAPTURED_GROUPS'])
role=os.environ['RUNTIME_ROLE']
def connect(group,user,password):
    deadline=time.monotonic()+90
    while True:
        try:
            return psycopg2.connect(host=group['host'],port=5432,dbname=os.environ['PGDATABASE'],user=user,password=password,connect_timeout=3,sslmode='disable',application_name='platform-citus-authentication')
        except psycopg2.OperationalError:
            if time.monotonic()>=deadline:raise SystemExit('Citus authentication could not reach group '+str(group['id']))from None
            time.sleep(2)
for group in groups:
    connection=connect(group,'postgres',os.environ['POSTGRES_PASSWORD'])
    try:
        with connection:
            with connection.cursor()as cursor:
                cursor.execute("SET LOCAL statement_timeout='15s'")
                cursor.execute('SELECT public.platform_replication_status()')
                status=cursor.fetchone()[0]
                if not(status['primary']and status['formation']==os.environ['FORMATION']and status['group_id']==group['id']and status['system_id']==group['system_id']):
                    raise SystemExit('Citus authentication refused an unexpected database identity')
                for user,key in (('postgres','POSTGRES_PASSWORD'),(role,'APP_PASSWORD')):
                    cursor.execute('INSERT INTO pg_catalog.pg_dist_authinfo(nodeid,rolename,authinfo) VALUES(0,%s,%s) ON CONFLICT(nodeid,rolename) DO UPDATE SET authinfo=EXCLUDED.authinfo',(user,make_dsn(password=os.environ[key])))
        print('Citus worker authentication configured for group '+str(group['id']),flush=True)
    finally:connection.close()
connection=connect(groups[0],role,os.environ['APP_PASSWORD'])
try:
    with connection.cursor()as cursor:
        cursor.execute("SET statement_timeout='30s'")
        cursor.execute("SELECT success,result FROM run_command_on_workers('SELECT current_user')")
        results=cursor.fetchall()
        if len(results)!=len(groups)-1 or not all(ok and value==role for ok,value in results):
            raise SystemExit('Citus runtime worker authentication verification failed')
    print('Citus runtime authentication verified on every worker',flush=True)
finally:connection.close()
'''


def render(storage,captured):
    expected=durability_config(storage)
    require(storage['mode']=='citus'and len(captured.get('groups',[]))==len(expected['groups']),'Citus authentication needs captured canonical groups')
    groups=[]
    for planned,actual in zip(expected['groups'],captured['groups']):
        require(isinstance(actual.get('system_id'),str)and actual['system_id'].isdigit(),'captured PostgreSQL identity required')
        planned['system_id']=actual['system_id']
        groups.append({'id':planned['id'],'system_id':actual['system_id'],'host':storage['formation']+'-g'+str(planned['id'])+'-primary.'+storage['namespace']+'.svc.cluster.local'})
    require(expected==captured,'Citus authentication capture differs from storage plan')
    tag=hashlib.sha256((SCRIPT+storage['image']+json.dumps(groups,sort_keys=True)).encode()).hexdigest()[:12]
    name=storage['cluster_id']+'-citus-auth-'+tag
    env=[{'name':key,'value':value}for key,value in {'FORMATION':storage['formation'],'PGDATABASE':storage.get('database','hazards'),'RUNTIME_ROLE':storage.get('runtime_role','aggregator_app'),'CAPTURED_GROUPS':json.dumps(groups,separators=(',',':'))}.items()]
    env += [{'name':key,'valueFrom':{'secretKeyRef':{'name':storage['formation']+'-credentials','key':key}}}for key in ('POSTGRES_PASSWORD','APP_PASSWORD')]
    labels={'app.kubernetes.io/name':'canonical-migrate','role':'other','platform.aat/cluster':storage['cluster_id']}
    job={'apiVersion':'batch/v1','kind':'Job','metadata':{'name':name,'namespace':storage['namespace'],'labels':labels},'spec':{'backoffLimit':0,'activeDeadlineSeconds':600,'template':{'metadata':{'labels':labels},'spec':{'restartPolicy':'Never','automountServiceAccountToken':False,'securityContext':{'runAsNonRoot':True,'runAsUser':10001,'runAsGroup':10001},'containers':[{'name':'citus-authentication','image':storage['image'],'command':['/opt/patroni/bin/python','-c',SCRIPT],'env':env,'resources':{'requests':{'memory':'128Mi','cpu':'100m'},'limits':{'memory':'256Mi','cpu':'1'}}}]}}}}
    return {'apiVersion':'v1','kind':'List','items':[job]},name
