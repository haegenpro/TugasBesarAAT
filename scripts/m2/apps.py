"""Application placement and least-privilege runtime configuration."""
from urllib.parse import quote
import hashlib
import json
from common import NAMESPACE, ROOT, labels, local_volume, resource, resources
from render import configmap, image, pod_env, policy, secret, service
from tuning import application_env


def database(credentials,owner,formation='services',name=None):
    password=quote(credentials[owner+'_db'],safe='')
    if formation in ('services','ingestion'):formation+='-db'
    return f"postgres://{owner}_app:{password}@{formation}:5432/{name or owner}?sslmode=disable"


def workload(cfg,name,role,port,env,replicas=2,component=None,node=None,volume=None,command=None):
    env={**env,**application_env(cfg,name)}
    private={key:value for key,value in env.items() if key in ('DATABASE_URL','NATS_URL') or key.endswith(('_TOKEN','_KEY','_SECRET','_PASSWORD'))}
    public={key:value for key,value in env.items() if key not in private}
    result=[secret(name+'-runtime',private),configmap(name+'-config',public)]
    container={'name':name,'image':image(cfg,component or name),'imagePullPolicy':'IfNotPresent',
               # Legacy Secrets may retain old public tuning keys. ConfigMap
               # values must win when both sources contain the same key.
               'env':pod_env({}), 'envFrom':[{'secretRef':{'name':name+'-runtime'}},{'configMapRef':{'name':name+'-config'}}],
               'resources':resources(role),'ports':[{'name':'metrics','containerPort':port}],
               'readinessProbe':{'httpGet':{'path':'/ready','port':port},'periodSeconds':3,'failureThreshold':2},
               'livenessProbe':{'httpGet':{'path':'/health','port':port},'periodSeconds':10,'failureThreshold':6},
               'startupProbe':{'httpGet':{'path':'/health','port':port},'periodSeconds':3,'failureThreshold':60}}
    if command:container['command']=command
    if role=='mock':container['readinessProbe']['httpGet']['path']='/health'
    pod={'containers':[container],'terminationGracePeriodSeconds':40,'automountServiceAccountToken':False}
    if node:pod['nodeSelector']={'kubernetes.io/hostname':cfg['nodes'][node]}
    else:
        pod['affinity']={'nodeAffinity':{'requiredDuringSchedulingIgnoredDuringExecution':{'nodeSelectorTerms':[{'matchExpressions':[{'key':'kubernetes.io/hostname','operator':'In','values':list(cfg['nodes'].values())}]}]},
            'preferredDuringSchedulingIgnoredDuringExecution':[{'weight':10,'preference':{'matchExpressions':[{'key':'kubernetes.io/hostname','operator':'In','values':[cfg['nodes']['a']]}]}}]},
            'podAntiAffinity':{'preferredDuringSchedulingIgnoredDuringExecution':[{'weight':100,'podAffinityTerm':{'labelSelector':{'matchLabels':{'app':name}},'topologyKey':'kubernetes.io/hostname'}}]}}
    if volume:
        result+=local_volume(cfg,name,cfg['nodes'][node],1)
        container['volumeMounts']=[{'name':'data','mountPath':volume}]
        pod['volumes']=[{'name':'data','persistentVolumeClaim':{'claimName':name}}]
    strategy={'type':'Recreate'} if volume else {'type':'RollingUpdate','rollingUpdate':{'maxSurge':1,'maxUnavailable':0}}
    result.append(resource('Deployment',name,spec={'replicas':replicas,'strategy':strategy,
        'selector':{'matchLabels':{'app':name}},'template':{'metadata':{'labels':labels(name,role),'annotations':{
            'prometheus.io/scrape':'true','prometheus.io/port':str(port),
            'aat.bnpb/runtime-sha256':hashlib.sha256(json.dumps(env,sort_keys=True).encode()).hexdigest()}},'spec':pod}}))
    result.append(service(name,{'app':name},{'http':port},public=name=='client-api'))
    return result


def applications(cfg,credentials):
    nats='nats://application:'+quote(credentials['nats'],safe='')+'@broker:4222'
    registry='http://schema-registry:8080/apis/ccompat/v7'
    common={'DB_MAX_IDLE_CONNS':'2','MAX_PAYLOAD_BYTES':str(4<<20),
        'MAX_JSON_DEPTH':'64','SHUTDOWN_TIMEOUT':'30s','OTEL_EXPORTER_OTLP_ENDPOINT':'http://jaeger:4318',
        'FAILED_INPUT_QUOTA_BYTES':str(cfg['limits']['quarantine_mib']<<20),
        'NATS_URL':nats,'SCHEMA_REGISTRY_URL':registry,
        'NATS_STREAM':'HAZARDS','NATS_SUBJECT':'hazard.events','NATS_LIFECYCLE_STREAM':'LIFECYCLE',
        'NATS_LIFECYCLE_SUBJECT':'hazard.lifecycle','NATS_DLQ_STREAM':'HAZARDS_DLQ',
        'NATS_MAX_ACK_PENDING':'32','NATS_ACK_WAIT':'30s','CONSUMER_WORKERS':'1'}
    result=[]
    env={**common,'M2_ENABLED':'true','DATABASE_URL':database(credentials,'canonical','canonical-0','hazards'),
        'AGGREGATOR_PORT':'8080','AGGREGATOR_ADMIN_TOKEN':credentials['admin'],
        'BMKG_BASE_URL':'http://bmkg-mock:8081','BMKG_API_KEY':credentials['bmkg'],
        'PVMBG_BASE_URL':'http://pvmbg-mock:8082','PVMBG_TOKEN':credentials['pvmbg'],
        'POLL_INTERVAL':'3','SOURCE_RETRY_MAX_SEC':'30','BMKG_FRESHNESS_SEC':'15','PVMBG_FRESHNESS_SEC':'30',
        'BMKG_SEISMIC_READER_SCHEMA':'/schemas/seismic.avsc','BMKG_TSUNAMI_READER_SCHEMA':'/schemas/tsunami.avsc',
        'PVMBG_READER_SCHEMA':'/schemas/volcanic-v1.avsc','AUDIT_API_TOKEN':cfg['external'].get('audit_token') or credentials['audit'],
        'CANONICAL_SERVICE_TOKEN':credentials['canonical_internal'],'ANALYTICS_SERVICE_TOKEN':credentials['canonical_internal'],
        'FIELD_READ_TOKEN':credentials['field_read'],'M2_NODE_A':cfg['nodes']['a'],'M2_NODE_B':cfg['nodes']['b'],
        'FAILED_INPUT_QUOTA_BYTES':str(cfg['limits']['quarantine_mib']<<20),
        'INGESTION_BASE_URL':'http://ingestion:8080','INGESTION_SERVICE_TOKEN':credentials['ingestion_internal']}
    result+=workload(cfg,'aggregator','aggregator',8080,env,3)
    result.append(policy('aggregator-api',{'app':'aggregator'},['client-api','ingestion','wildfire-processor','coastal-processor','situation-summarizer'],[8080]))
    auth={**common,'AUTH_PORT':'8083','DATABASE_URL':database(credentials,'auth'),'AUTH_INTERNAL_KEY':credentials['auth_internal'],
        'ACCESS_TOKEN_TTL_SEC':'60','REFRESH_TOKEN_TTL_SEC':'86400','MEDIA_CLIENT_ID':'media-demo','MEDIA_CLIENT_SECRET':credentials['media'],
        'FIELD_CLIENT_ID':'field-demo','FIELD_CLIENT_SECRET':credentials['field'],'INTERNAL_CLIENT_ID':'internal-demo','INTERNAL_CLIENT_SECRET':credentials['internal'],
        'AUTH_PRIORITY_ENABLED':'true','AUTH_PRIORITY_KEY':credentials['priority']}
    result+=workload(cfg,'auth-service','api',8083,auth)
    result.append(policy('auth-api',{'app':'auth-service'},['client-api'],[8083]))
    client={**common,'CLIENT_API_PORT':'8080','AUTH_BASE_URL':'http://auth-service:8083','AGGREGATOR_BASE_URL':'http://aggregator:8080',
        'AUTH_INTERNAL_KEY':credentials['auth_internal'],
        'CLIENT_API_PRIORITY_ENABLED':'true',
        'AUTH_PRIORITY_KEY':credentials['priority'],'FIELD_READ_TOKEN':credentials['field_read'],'ANALYTICS_ENABLED':'true','ANALYTICS_SERVICE_TOKEN':credentials['canonical_internal'],
        'INGESTION_BASE_URL':'http://ingestion:8080','FIELD_REPORT_SERVICE_TOKEN':credentials['ingestion_internal'],'OPS_SERVICE_TOKEN':credentials['admin']}
    if cfg['features'].get('ai'):client['SITUATION_BASE_URL']='http://situation-summarizer:8094'
    result+=workload(cfg,'client-api','api',8080,client)
    result.append(policy('client-api-public',{'app':'client-api'},[],[8080],public=True))
    for owner,port in [('dashboard',8091),('notifier',8092)]:
        name='consumer-'+owner
        env={**common,'CONSUMER_PORT':str(port),'CONSUMER_ROLE':owner,'DATABASE_URL':database(credentials,owner),
             'CONSUMER_ADMIN_SOCKET':'/tmp/consumer-admin/consumer.sock','NATS_DURABLE':owner+'-v1','NATS_DLQ_SUBJECT':'hazard.dlq.'+owner}
        if owner=='notifier':env.update(WARNING_DURABLE='coastal-warning-v1',WARNING_STREAM='COASTAL_WARNINGS',WARNING_SUBJECT='coastal.warning',
            WARNING_EXPECTATIONS_URL='http://ingestion:8080',COASTAL_SLO_TOKEN=credentials['coastal_slo'],WARNING_SLO_METRICS_ENABLED='true')
        result+=workload(cfg,name,'consumer',port,env)
        result.append(policy(name,{'app':name},['client-api'],[port]))
    for name,port in [('bmkg-mock',8081),('pvmbg-mock',8082)]:
        env={**common,'SOURCE_ARCHIVE_PATH':'/data/history.db'}
        if name=='bmkg-mock':env.update(BMKG_PORT=str(port),BMKG_API_KEY=credentials['bmkg'],BMKG_DELAY_MS='50',BMKG_EVENT_INTERVAL_MS='1000')
        else:env.update(PVMBG_PORT=str(port),PVMBG_TOKEN=credentials['pvmbg'],PVMBG_ADMIN_TOKEN=credentials['admin'],PVMBG_MIN_DELAY_MS='500',PVMBG_MAX_DELAY_MS='3000',PVMBG_REPORT_INTERVAL_MS='1000')
        result+=workload(cfg,name,'mock',port,env,1,node='a',volume='/data')
        result.append(policy(name,{'app':name},['aggregator','client-api'],[port]))
    registry_env={'APICURIO_STORAGE_KIND':'sql','APICURIO_STORAGE_SQL_KIND':'postgresql',
        'APICURIO_DATASOURCE_URL':'jdbc:postgresql://services-db:5432/registry','APICURIO_DATASOURCE_USERNAME':'registry_app',
        'APICURIO_DATASOURCE_PASSWORD':credentials['registry_db'],'REGISTRY_RETRY_DELAY_SEC':'5',
        'QUARKUS_HTTP_PORT':'8080','QUARKUS_MANAGEMENT_PORT':'9000',
        'JAVA_OPTS_APPEND':'-Xms32m -Xmx96m -XX:MaxMetaspaceSize=96m -XX:ReservedCodeCacheSize=16m -XX:MaxDirectMemorySize=8m -Xss256k -XX:ActiveProcessorCount=2'}
    registry_objects=workload(cfg,'schema-registry','other',8080,registry_env,1,node='a')
    registry_template=next(o for o in registry_objects if o['kind']=='Deployment')['spec']['template']
    registry_template['metadata']['annotations']['prometheus.io/port']='9000'
    pod=registry_template['spec']['containers'][0]
    for probe,path in [('readinessProbe','/health/ready'),('livenessProbe','/health/live'),('startupProbe','/health/live')]:pod[probe]['httpGet']={'path':path,'port':9000}
    pod['ports'][0]['name']='http'
    pod['ports'].append({'name':'metrics','containerPort':9000})
    result+=registry_objects
    result.append(policy('registry-api',{'app':'schema-registry'},['aggregator','ingestion','coastal-processor','wildfire-processor','field-report-processor','bmkg-mock','pvmbg-mock','consumer-dashboard','consumer-notifier'],[8080,9000]))
    ingestion={**common,'DATABASE_URL':database(credentials,'ingestion','ingestion'),'FIELD_REPORT_SERVICE_TOKEN':credentials['ingestion_internal'],
        'COASTAL_SLO_TOKEN':credentials['coastal_slo'],'SATELLITE_TOKEN':cfg['external'].get('satellite_token',''),
        'SATELLITE_URL':cfg['satellite']['url'],'SATELLITE_NAMESPACE':cfg['satellite']['namespace'],
        'SATELLITE_INITIAL_OFFSET':str(cfg['satellite']['initial_offset']),'PANTAI_BASE_URL':'http://sensor-pantai:8080',
        'PANTAI_AUTH_TOKEN':credentials['coastal'],'PANTAI_NAMESPACE':'coastal-'+cfg['coastal']['profile']}
    result+=workload(cfg,'ingestion','other',8080,ingestion)
    result.append(policy('ingestion-api',{'app':'ingestion'},['client-api','aggregator','consumer-notifier'],[8080]))
    for kind in ['wildfire','coastal','field-report']:
        name=kind+'-processor'
        env={**common,'DATABASE_URL':database(credentials,'ingestion','ingestion'),'WORKER_KIND':'field' if kind=='field-report' else kind,
            'AGGREGATOR_BASE_URL':'http://aggregator:8080','CANONICAL_SERVICE_TOKEN':credentials['canonical_internal'],
            'SATELLITE_ALLOWED_LATENESS':str(cfg['satellite']['lateness_seconds'])+'s'}
        result+=workload(cfg,name,'consumer',8080,env,component='aggregator',command=['/usr/local/bin/m2-worker'])
        result.append(policy(name,{'app':name},[],[8080]))
    # Ingestion and processors share an image, but have separate processes,
    # resource classes and ownership boundaries.
    for o in result:
        if o['kind']=='Deployment' and o['metadata']['name']=='ingestion':
            o['spec']['template']['spec']['containers'][0]['image']=image(cfg,'aggregator')
            o['spec']['template']['spec']['containers'][0]['command']=['/usr/local/bin/m2-ingestion']
    sensor=workload(cfg,'sensor-pantai','sensor',8080,{'PANTAI_AUTH_TOKEN':credentials['coastal'],
        'PANTAI_PROFILE':cfg['coastal']['profile'],'PANTAI_SPEED':str(cfg['coastal']['speed'])},1,node='a',volume='/data')
    sensor_spec=next(o for o in sensor if o['kind']=='Deployment')['spec']['template']['spec']
    next(o for o in sensor if o['kind']=='Deployment')['spec']['template']['metadata']['annotations']['prometheus.io/scrape']='false'
    sensor_pod=sensor_spec['containers'][0]
    sensor_pod['image']='sustuna/sensor-pantai@sha256:51fbd90d3891ad6e9f5768ad330ffb65205644cb29e50da65c7538f2e0b4397d'
    for key in ['readinessProbe','livenessProbe','startupProbe']:sensor_pod[key]['httpGet']['path']='/healthz'
    sensor_spec['securityContext']={'fsGroup':65532}
    sensor_spec['initContainers']=[{'name':'prepare-data','image':image(cfg,'storage'),
        'command':['sh','-ec','chown 65532:65532 /data; chmod 700 /data'],
        'securityContext':{'runAsUser':0},'resources':resources('other'),'volumeMounts':[{'name':'data','mountPath':'/data'}]}]
    result+=sensor;result.append(policy('sensor-api',{'app':'sensor-pantai'},['ingestion'],[8080]))
    if cfg['features'].get('ai'):
        external=cfg['external']
        if not external.get('openrouter_api_key') or not external.get('summary_model'):raise ValueError('AI requires an explicit model and API key in private configuration')
        env={**common,'SUMMARIZER_PORT':'8094','AGGREGATOR_BASE_URL':'http://aggregator:8080','OPENROUTER_API_KEY':external['openrouter_api_key'],
            'SUMMARY_MODEL':external['summary_model'],'SUMMARY_SERVICE_TOKEN':credentials['canonical_internal']}
        result+=workload(cfg,'situation-summarizer','other',8094,env,1,node='b')
        result.append(policy('situation-api',{'app':'situation-summarizer'},['client-api'],[8094]))
    return result


def broker_job(cfg,credentials):
    name='broker-initialize'
    job_revision=hashlib.sha256((ROOT/'broker/cmd/m2-init/main.go').read_bytes()+
                                b'\0'+image(cfg,'broker').encode()).hexdigest()[:10]
    result = [secret(name,{'NATS_URL':'nats://application:'+quote(credentials['nats'],safe='')+'@broker:4222'}),
        resource('Job',name+'-'+job_revision,spec={'backoffLimit':6,'ttlSecondsAfterFinished':86400,'template':{'metadata':{'labels':labels(name,'other')},'spec':{
            'restartPolicy':'OnFailure','nodeSelector':{'kubernetes.io/hostname':cfg['nodes']['a']},'automountServiceAccountToken':False,
            'containers':[{'name':name,'image':image(cfg,'broker'),'command':['/usr/local/bin/m2-init'],
                           'envFrom':[{'secretRef':{'name':name}}],'resources':resources('other')}]}}})]
    result[-1]['metadata']['labels']['app'] = name
    return result
