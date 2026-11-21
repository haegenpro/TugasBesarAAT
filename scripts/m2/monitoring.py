"""Small browser observability stack with explicit memory and retention bounds."""
import json
import hashlib
from common import LABEL, APPLICATION, NAMESPACE, ROOT, labels, resource, resources, local_volume
from render import configmap, secret, service, policy, pod_env, image


def monitoring(cfg,credentials):
    result=[]
    targets={8080:['aggregator','client-api','ingestion','wildfire-processor','coastal-processor','field-report-processor'],
             8081:['bmkg-mock'],8082:['pvmbg-mock'],8083:['auth-service'],8091:['consumer-dashboard'],8092:['consumer-notifier'],9000:['schema-registry']}
    if cfg['features'].get('ai'):targets[8094]=['situation-summarizer']
    # Discover each process directly. Scraping a load-balanced Service would
    # splice different pods' cumulative counters into a single time series.
    jobs=[{'job_name':'application-pods','kubernetes_sd_configs':[{'role':'pod','namespaces':{'names':[NAMESPACE]}}],
        'relabel_configs':[
            {'source_labels':['__meta_kubernetes_pod_annotation_prometheus_io_scrape'],'action':'keep','regex':'true'},
            {'source_labels':['__meta_kubernetes_pod_phase'],'action':'keep','regex':'Running'},
            {'source_labels':['__meta_kubernetes_pod_container_port_name'],'action':'keep','regex':'metrics'},
            {'source_labels':['__address__','__meta_kubernetes_pod_annotation_prometheus_io_port'],
             'regex':'([^:]+)(?::[0-9]+)?;([0-9]+)','target_label':'__address__','replacement':'$1:$2'},
            {'source_labels':['__meta_kubernetes_pod_label_app'],'target_label':'service'},
            {'source_labels':['__meta_kubernetes_pod_name'],'target_label':'pod'}]}]
    prometheus={'global':{'scrape_interval':'15s','evaluation_interval':'15s'},'scrape_configs':jobs,'rule_files':['/config/slo.yaml']}
    # Go services expose the same fixed metric contract. SLOs include server
    # failures; overload rejections remain separately visible, never erased.
    rules={'groups':[{'name':'m2-availability','rules':[{'record':'service:request_rate5m',
        'expr':'sum by(service) (rate(app_http_requests_total[5m]))'},
        {'record':'service:error_ratio5m','expr':'sum by(service) (rate(app_http_requests_total{status=~"5.."}[5m])) / clamp_min(sum by(service) (rate(app_http_requests_total[5m])), 0.001)'},
        {'alert':'ServiceErrorBudgetBurn','expr':'service:error_ratio5m > 0.01','for':'2m','labels':{'severity':'warning'},
         'annotations':{'summary':'More than 1% server errors over the last five minutes'}}]}]}
    files={'prometheus':{'prometheus.yaml':json.dumps(prometheus),'slo.yaml':json.dumps(rules)},
           'loki':{'loki.yaml':(ROOT/'deployment/m2/monitoring/loki.yaml').read_text()},
           'jaeger':{'jaeger.yaml':(ROOT/'deployment/m2/monitoring/jaeger.yaml').read_text()}}
    datasources={'apiVersion':1,'datasources':[{'name':'Prometheus','uid':'prometheus','type':'prometheus','url':'http://prometheus:9090','access':'proxy','isDefault':True},
        {'name':'Loki','uid':'loki','type':'loki','url':'http://loki:3100','access':'proxy','jsonData':{'maxLines':500,'derivedFields':[{'name':'TraceID','matcherRegex':'"trace_id":"([a-f0-9]{32})"','datasourceUid':'jaeger','url':'${__value.raw}'}]}},
        {'name':'Jaeger','uid':'jaeger','type':'jaeger','url':'http://jaeger:16686','access':'proxy'}]}
    panels=[]
    expressions=[('Requests / second','sum by(service) (rate(app_http_requests_total[1m]))'),
        ('Server error ratio','service:error_ratio5m'),
        ('HTTP p95 (seconds)','histogram_quantile(0.95, sum by(service,le) (rate(app_http_request_duration_seconds_bucket[5m])))'),
        ('Admission rejections','sum by(service) (rate(app_admission_role_capacity_rejected_total[1m]))'),
        ('Canonical lag (pending outbox)','app_outbox_pending'),
        ('Coastal warning p95 (seconds)','app_coastal_slo_p95_delivered_seconds'),
        ('Error budget remaining (1% / demo)','clamp_min(1 - service:error_ratio5m / 0.01, 0)')]
    for i,(title,expr) in enumerate(expressions):panels.append({'id':i+1,'type':'timeseries','title':title,'datasource':{'type':'prometheus','uid':'prometheus'},'gridPos':{'x':(i%2)*12,'y':(i//2)*8,'w':12,'h':8},'targets':[{'expr':expr,'legendFormat':'{{service}}'}]})
    dashboard={'uid':'m2-operations','title':'M2 operations','schemaVersion':39,'version':1,'refresh':'15s','time':{'from':'now-30m','to':'now'},'panels':panels}
    result += [configmap(name+'-config',content) for name,content in files.items()]
    result += [resource('ServiceAccount','prometheus'),
        resource('Role','prometheus',rules=[{'apiGroups':[''],'resources':['pods'],'verbs':['get','list','watch']}]),
        resource('RoleBinding','prometheus',subjects=[{'kind':'ServiceAccount','name':'prometheus','namespace':NAMESPACE}],
                 roleRef={'apiGroup':'rbac.authorization.k8s.io','kind':'Role','name':'prometheus'})]
    result += [configmap('grafana-datasources',{'datasources.yaml':json.dumps(datasources)}),
        configmap('grafana-dashboards',{'provider.yaml':json.dumps({'apiVersion':1,'providers':[{'name':'M2','type':'file','options':{'path':'/dashboards'}}]})}),
        configmap('grafana-dashboard-content',{'operations.json':json.dumps(dashboard)}),
        secret('grafana-runtime',{'GF_SECURITY_ADMIN_PASSWORD':credentials['grafana']}),
        configmap('grafana-environment',{'GF_SECURITY_ADMIN_USER':'admin',
            'GF_USERS_ALLOW_SIGN_UP':'false','GF_ANALYTICS_REPORTING_ENABLED':'false','GF_ANALYTICS_CHECK_FOR_UPDATES':'false',
            'GF_PLUGINS_PREINSTALL_DISABLED':'true','GF_PLUGINS_PREINSTALL_AUTO_UPDATE':'false',
            'GF_UNIFIED_ALERTING_ENABLED':'false','GOMEMLIMIT':'128MiB','GOGC':'50'})]
    definitions=[('prometheus','prom/prometheus:v3.15.0',9090,['--config.file=/config/prometheus.yaml','--storage.tsdb.path=/data','--storage.tsdb.retention.time=6h','--storage.tsdb.retention.size=256MB','--query.max-concurrency=2','--query.max-samples=1000000','--query.timeout=5s','--storage.tsdb.min-block-duration=30m','--storage.tsdb.max-block-duration=30m'],'/-/ready'),
        ('loki','grafana/loki:3.7.8',3100,['-config.file=/config/loki.yaml'],'/ready'),
        ('jaeger','jaegertracing/jaeger:2.22.0',16686,['--config=/config/jaeger.yaml'],'/'),
        ('grafana','grafana/grafana:13.2.3',3000,None,'/api/health')]
    for name,img,port,args,ready in definitions:
        volumes=[];mounts=[]
        if name!='grafana':volumes.append({'name':'config','configMap':{'name':name+'-config'}});mounts.append({'name':'config','mountPath':'/config','readOnly':True})
        if name in ('loki','prometheus','grafana'):
            result+=local_volume(cfg,name,cfg['nodes']['a'],1)
            volumes.append({'name':'data','persistentVolumeClaim':{'claimName':name}})
            path={'prometheus':'/data','loki':'/loki','grafana':'/var/lib/grafana'}[name];mounts.append({'name':'data','mountPath':path})
        container={'name':name,'image':img,'resources':resources('other'),'env':pod_env({'GOMEMLIMIT':'192MiB','GOMAXPROCS':'2'}),
            'ports':[{'containerPort':port}],'volumeMounts':mounts,
            'readinessProbe':{'httpGet':{'path':ready,'port':port},'periodSeconds':5},
            'startupProbe':{'httpGet':{'path':ready,'port':port},'periodSeconds':5,'failureThreshold':60}}
        if args:container['args']=args
        if name=='jaeger':container['ports'].append({'containerPort':4318})
        if name=='grafana':
            container['env']=pod_env({'GOMEMLIMIT':'128MiB','GOGC':'50','GOMAXPROCS':'2'})
            container['envFrom']=[{'configMapRef':{'name':'grafana-environment'}},{'secretRef':{'name':'grafana-runtime'}}]
            for volume,path in [('datasources','/etc/grafana/provisioning/datasources'),('dashboards','/etc/grafana/provisioning/dashboards'),('dashboard-content','/dashboards')]:
                volumes.append({'name':volume,'configMap':{'name':'grafana-'+volume}});mounts.append({'name':volume,'mountPath':path,'readOnly':True})
        pod={'nodeSelector':{'kubernetes.io/hostname':cfg['nodes']['a']},'automountServiceAccountToken':False,'containers':[container],'volumes':volumes}
        if name=='prometheus':
            pod['serviceAccountName']='prometheus'
            pod['automountServiceAccountToken']=True
        if name in ('loki','prometheus','grafana'):
            uid={'loki':10001,'prometheus':65534,'grafana':472}[name]
            pod['securityContext']={'fsGroup':uid}
            pod['initContainers']=[{'name':'prepare-data','image':image(cfg,'storage'),'command':['sh','-ec',f'chown {uid}:{uid} /data; chmod 700 /data'],
                'securityContext':{'runAsUser':0},'resources':resources('other'),'volumeMounts':[{'name':'data','mountPath':'/data'}]}]
        runtime=next((o['stringData'] for o in result if o['kind']=='Secret' and o['metadata']['name']=='grafana-runtime'),{}) if name=='grafana' else {}
        if name=='grafana':runtime={**runtime,**next(o['data'] for o in result if o['kind']=='ConfigMap' and o['metadata']['name']=='grafana-environment')}
        checksum=hashlib.sha256(json.dumps({'config':files.get(name,datasources),'runtime':runtime},sort_keys=True).encode()).hexdigest()
        result.append(resource('Deployment',name,spec={'replicas':1,'strategy':{'type':'Recreate'},'selector':{'matchLabels':{'app':name}},'template':{'metadata':{'labels':labels(name,'other'),'annotations':{'aat.bnpb/config-sha256':checksum}},'spec':pod}}))
        ports={'http':port};callers=['grafana','prometheus']
        if name=='jaeger':ports['otlp']=4318;callers+=sum(targets.values(),[])
        if name=='loki':callers+=['fluent-bit']
        result.append(service(name,{'app':name},ports))
        result.append(policy(name,{'app':name},callers,list(ports.values())))
    result.append(policy('monitor-scrape',{},['prometheus'],list(targets)))
    fluent='''[SERVICE]
    Flush 2
    Log_Level warn
    Parsers_File /fluent-bit/etc/parsers.conf
    storage.path /buffers
    storage.max_chunks_up 8
[INPUT]
    Name tail
    Tag kube.*
    Path /var/log/containers/*_NAMESPACE_*.log
    multiline.parser docker, cri
    DB /buffers/tail.db
    Mem_Buf_Limit 8MB
    Inotify_Watcher Off
    Refresh_Interval 5
    Skip_Long_Lines On
    storage.type filesystem
[FILTER]
    Name kubernetes
    Match kube.*
    Kube_Tag_Prefix kube.var.log.containers.
    Merge_Log On
    Keep_Log Off
    K8S-Logging.Parser Off
    K8S-Logging.Exclude Off
[OUTPUT]
    Name loki
    Match kube.*
    Host loki
    Port 3100
    Labels namespace=$kubernetes['namespace_name'],app=$kubernetes['labels']['app']
    Line_Format json
    storage.total_limit_size 32M
'''.replace('NAMESPACE',NAMESPACE)
    result+=[configmap('fluent-bit-config',{'fluent-bit.conf':fluent}),resource('ServiceAccount','fluent-bit'),
        resource('Role','fluent-bit',rules=[{'apiGroups':[''],'resources':['pods'],'verbs':['get','list','watch']}]),
        resource('RoleBinding','fluent-bit',subjects=[{'kind':'ServiceAccount','name':'fluent-bit','namespace':NAMESPACE}],roleRef={'apiGroup':'rbac.authorization.k8s.io','kind':'Role','name':'fluent-bit'})]
    for node_id,node in cfg['nodes'].items():
        name='fluent-bit-'+node_id
        result.append(resource('Deployment',name,spec={'replicas':1,'selector':{'matchLabels':{'app':name}},'template':{'metadata':{'labels':{**labels(name,'other'),'app':'fluent-bit','instance':name},'annotations':{'aat.bnpb/config-sha256':hashlib.sha256(fluent.encode()).hexdigest()}},'spec':{
            'serviceAccountName':'fluent-bit','nodeSelector':{'kubernetes.io/hostname':node},
            'containers':[{'name':'fluent-bit','image':'fluent/fluent-bit:4.2.3','resources':resources('other'),
                'args':['-c','/config/fluent-bit.conf'],'volumeMounts':[{'name':'logs','mountPath':'/var/log','readOnly':True},{'name':'config','mountPath':'/config','readOnly':True},{'name':'buffers','mountPath':'/buffers'}]}],
            'volumes':[{'name':'logs','hostPath':{'path':'/var/log','type':'Directory'}},{'name':'config','configMap':{'name':'fluent-bit-config'}},{'name':'buffers','emptyDir':{'sizeLimit':'64Mi'}}]}}}))
        # Selector must match the distinct per-node controller, while Loki and
        # policy selection share app=fluent-bit.
        result[-1]['spec']['selector']={'matchLabels':{'instance':name}}
    return result
