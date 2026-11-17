"""Offline metrics, logs and traces integration for the four-node development cluster."""
import copy
import json
from pathlib import Path

from service_lib import DIGEST, render as render_services
from storage_lib import require

ROOT = Path(__file__).resolve().parent.parent
IMAGES = ("prometheus", "loki", "tempo", "grafana", "alloy", "nats-exporter", "node-exporter", "platform-metrics")


def configs(inventory, storage, additional_storages=()):
    ns = inventory["namespace"]
    relabel = [
        {"source_labels":["__meta_kubernetes_pod_annotation_prometheus_io_scrape"],"action":"keep","regex":"true"},
        # A pod can expose multiple ports. Select only its annotated port to avoid
        # double-counting, then use the pod IP supplied by Kubernetes discovery.
        {"source_labels":["__meta_kubernetes_pod_container_port_number"],"target_label":"__tmp_port"},
        {"source_labels":["__meta_kubernetes_pod_annotation_prometheus_io_port"],"target_label":"__tmp_port","action":"keepequal"},
        {"source_labels":["__meta_kubernetes_pod_annotation_prometheus_io_path"],"target_label":"__metrics_path__"},
    ]
    for source, label in (("namespace","namespace"),("pod_name","pod"),("pod_node_name","node"),("pod_label_app_kubernetes_io_name","service"),("pod_label_role","role")):
        relabel.append({"source_labels":["__meta_kubernetes_"+source],"target_label":label})
    jobs = [{"job_name":"applications","kubernetes_sd_configs":[{"role":"pod","namespaces":{"names":[ns]}}],"relabel_configs":relabel}]
    formations=(storage,*additional_storages)
    jobs.append({"job_name":"patroni","static_configs":[{"targets":[m["host"]+":8008"],"labels":{"formation":s["formation"],"node":m["node"],"group":str(g["id"]),"member":m["name"]}} for s in formations for g in s["groups"] for m in g["members"]]})
    jobs.append({"job_name":"storage-etcd","static_configs":[{"targets":[f"{s['formation']}-etcd-{letter}-0.{s['formation']}-etcd-members.{ns}.svc.cluster.local:2381"],"labels":{"formation":s["formation"]}} for s in formations for letter in "abc"]})
    jobs.append({"job_name":"nodes","static_configs":[{"targets":[n["address"]+":9100"],"labels":{"node":n["name"]}} for n in inventory["nodes"]]})
    jobs.append({"job_name":"prometheus","static_configs":[{"targets":["localhost:9090"]}]})
    prom = {"global":{"scrape_interval":"15s","evaluation_interval":"15s"},"rule_files":["/etc/prometheus/alerts.json"],"scrape_configs":jobs}
    alerts = {"groups":[{"name":"platform","rules":[
        {"alert":"ScrapeUnavailable","expr":"up == 0","for":"1m","labels":{"severity":"warning"},"annotations":{"summary":"Target unavailable; investigate before interpreting missing metrics as zero"}},
        {"alert":"MetricsCollectionStale","expr":"platform_metrics_collection_success == 0 or platform_metrics_snapshot_age_seconds > 30","for":"30s","labels":{"severity":"warning"}},
        {"alert":"ApplicationUnexpectedErrors","expr":'sum by (service) (rate(app_http_requests_total{status=~"5.."}[2m])) / clamp_min(sum by (service) (rate(app_http_requests_total{status!="429"}[2m])), 0.001) > 0.01',"for":"1m","labels":{"severity":"critical"}},
        {"alert":"OutboxBacklog","expr":"app_outbox_pending > 1000","for":"2m","labels":{"severity":"warning"}},
        {"alert":"SatelliteLagGrowing","expr":"app_satellite_feed_lag_offsets > 1000","for":"1m","labels":{"severity":"warning"}},
        {"alert":"WildfireOutputBlocked","expr":"app_wildfire_blocked_output > 0","for":"30s","labels":{"severity":"critical"}},
        {"alert":"CoastalSourceUnavailable","expr":"app_coastal_source_stale == 2","for":"30s","labels":{"severity":"warning"}},
        {"alert":"CoastalSLOCollectionUnavailable","expr":"app_coastal_slo_collection_success == 0","for":"1m","labels":{"severity":"warning"}},
        {"alert":"CoastalTimelinessBudgetExhausted","expr":"app_coastal_slo_error_budget_used > 1 and on(pod) (app_coastal_slo_collection_success == 1) and on(pod) (app_coastal_slo_coverage_complete == 1) and on(pod) (app_coastal_slo_clock_anomalies == 0)","for":"30s","labels":{"severity":"critical"}},
    ]}]}
    loki = {"auth_enabled":False,"server":{"http_listen_port":3100,"grpc_listen_port":9096},
        "common":{"instance_addr":"127.0.0.1","path_prefix":"/loki","replication_factor":1,"ring":{"kvstore":{"store":"inmemory"}},"storage":{"filesystem":{"chunks_directory":"/loki/chunks","rules_directory":"/loki/rules"}}},
        "ingester":{"wal":{"dir":"/loki/wal"}},"schema_config":{"configs":[{"from":"2024-01-01","store":"tsdb","object_store":"filesystem","schema":"v13","index":{"prefix":"index_","period":"24h"}}]},
        "storage_config":{"tsdb_shipper":{"active_index_directory":"/loki/index","cache_location":"/loki/index-cache"}},"compactor":{"working_directory":"/loki/compactor","retention_enabled":True,"delete_request_store":"filesystem"},
        "limits_config":{"retention_period":"24h","allow_structured_metadata":True},"analytics":{"reporting_enabled":False}}
    tempo = {"server":{"http_listen_port":3200},"distributor":{"receivers":{"otlp":{"protocols":{"http":{"endpoint":"0.0.0.0:4318"},"grpc":{"endpoint":"0.0.0.0:4317"}}}}},"ingester":{"max_block_duration":"5m"},"compactor":{"compaction":{"block_retention":"24h"}},"storage":{"trace":{"backend":"local","wal":{"path":"/var/tempo/wal"},"local":{"path":"/var/tempo/blocks"}}}}
    alloy = '''discovery.kubernetes "pods" {
  role = "pod"
  namespaces { names = ["NAMESPACE"] }
}
discovery.relabel "logs" {
  targets = discovery.kubernetes.pods.targets
  rule { source_labels = ["__meta_kubernetes_namespace"]
    target_label = "namespace" }
  rule { source_labels = ["__meta_kubernetes_pod_name"]
    target_label = "pod" }
  rule { source_labels = ["__meta_kubernetes_pod_node_name"]
    target_label = "node" }
  rule { source_labels = ["__meta_kubernetes_pod_label_app_kubernetes_io_name"]
    target_label = "service" }
}
loki.source.kubernetes "pods" {
  targets = discovery.relabel.logs.output
  forward_to = [loki.write.local.receiver]
}
loki.write "local" {
  endpoint { url = "http://loki:3100/loki/api/v1/push" }
}
'''.replace("NAMESPACE",ns)
    sources = {"apiVersion":1,"datasources":[{"name":name,"uid":name,"type":kind,"access":"proxy","url":url,"isDefault":name=="prometheus"} for name,kind,url in (("prometheus","prometheus","http://prometheus:9090"),("loki","loki","http://loki:3100"),("tempo","tempo","http://tempo:3200"))]}
    provider = {"apiVersion":1,"providers":[{"name":"platform","type":"file","options":{"path":"/etc/platform-dashboards"}}]}
    panels=[]
    signals=(("Scrape health","up"),("Pod memory by role","sum by (role) (platform_pod_memory_bytes)"),("Pod CPU by role","sum by (role) (platform_pod_cpu_cores)"),("HTTP request rate","sum by (service,status) (rate(app_http_requests_total[1m]))"),("Pending outbox","app_outbox_pending"),("Patroni roles","patroni_primary"),("Collector age","platform_metrics_snapshot_age_seconds"),
        ("Satellite acquisition lag (offsets)","app_satellite_feed_lag_offsets"),
        ("Wildfire received / prepared / applied","{__name__=~\"app_wildfire_(received_through|prepared_through|applied_through)\"}"),
        ("Oldest wildfire correction age","app_wildfire_oldest_correction_age_seconds"),
        ("Coastal source state: fresh=0 stale=1 unavailable=2","app_coastal_source_stale"),
        ("Coastal source lag","app_coastal_observation_lag_seconds"),
        ("Critical warning delivery duration (latest receipt only)","app_coastal_warning_last_delivery_seconds"),
        ("Critical receipts delivered and on-time (local counters)","{__name__=~\"app_coastal_warnings_(delivered|on_time)_total\"}"),
        ("Acquired critical cohort: expected / pending / overdue","max by (__name__) ({__name__=~\"app_coastal_slo_(expected|pending|pending_overdue)\"} and on(pod) (app_coastal_slo_collection_success == 1))"),
        ("Acquired cohort error-budget use (review coverage)","max(app_coastal_slo_error_budget_used and on(pod) (app_coastal_slo_collection_success == 1))"),
        ("Cohort coverage / p95 completeness / clock anomalies","{__name__=~\"app_coastal_slo_(coverage_complete|p95_complete|clock_anomalies|collection_success)\"}"),
        ("Critical cohort p95 lower bound (seconds)","max(app_coastal_slo_p95_lower_bound_seconds and on(pod) (app_coastal_slo_collection_success == 1))"),
        ("Auth reserved/general DB connections","{__name__=~\"app_auth_(field|general)_db_connections_in_use\"}"),
        ("Reserved Field read DB connections","{__name__=~\"app_field_read_db_connections_(in_use|max)\"}"),
        ("Controlled API rejections","sum by (service) (rate(app_http_requests_total{status=\"429\"}[1m]))"),
        ("Field reserve / authentication rejections","{__name__=~\"app_admission_(role_capacity|authentication_capacity)_rejected_total\"}"))
    for title,expr in signals:
        i=len(panels);panels.append({"id":i+1,"title":title,"type":"timeseries","datasource":{"type":"prometheus","uid":"prometheus"},"gridPos":{"h":8,"w":12,"x":(i%2)*12,"y":(i//2)*8},"targets":[{"expr":expr,"refId":"A"}]})
    dashboard={"uid":"platform-m2","title":"M2 platform","schemaVersion":39,"version":1,"refresh":"15s","time":{"from":"now-30m","to":"now"},"panels":panels}
    return {"prometheus-config":{"prometheus.json":json.dumps(prom),"alerts.json":json.dumps(alerts)},"loki-config":{"config.json":json.dumps(loki)},"tempo-config":{"config.json":json.dumps(tempo)},"alloy-config":{"config.alloy":alloy},"grafana-datasources":{"sources.json":json.dumps(sources)},"grafana-providers":{"providers.json":json.dumps(provider)},"platform-dashboards":{"platform.json":json.dumps(dashboard),"application.json":(ROOT/"monitoring/templates/grafana/application.json").read_text()}}


def render(inventory, storage, images, additional_storages=()):
    require(all(s["namespace"]==inventory["namespace"] and s["cluster_id"]==inventory["cluster_id"]for s in(storage,*additional_storages)),"storage plan belongs to another cluster")
    for name in IMAGES:
        require(isinstance(images.get(name),str) and DIGEST.fullmatch(images[name]),"supply immutable image for "+name)
    definitions = [
        ("prometheus",9090,"/bin/prometheus",["--config.file=/etc/prometheus/prometheus.json","--storage.tsdb.path=/prometheus","--storage.tsdb.retention.time=24h"],"/prometheus","/etc/prometheus","/-/ready","/-/healthy"),
        ("loki",3100,"/usr/bin/loki",["-config.file=/etc/loki/config.json"],"/loki","/etc/loki","/ready","/ready"),
        ("tempo",3200,"/tempo",["-config.file=/etc/tempo/config.json"],"/var/tempo","/etc/tempo","/ready","/ready"),
        ("grafana",3000,"/run.sh",[],"/var/lib/grafana",None,"/api/health","/api/health"),
        ("alloy",12345,"/bin/alloy",["run","/etc/alloy/config.alloy","--storage.path=/var/lib/alloy/data","--server.http.listen-addr=0.0.0.0:12345"],None,"/etc/alloy","/-/ready","/-/healthy"),
        ("platform-metrics",9101,"/usr/local/bin/platform-metrics",[],None,None,"/ready","/health"),
    ]
    for letter in "abc":
        endpoint=f"http://broker-{letter}-0.broker-members.{inventory['namespace']}.svc.cluster.local:8222"
        definitions.append(("nats-exporter-"+letter,7777,"/usr/local/bin/prometheus-nats-exporter",["-addr=0.0.0.0","-port=7777","-path=/metrics","-varz","-jsz=all",endpoint],None,None,"/metrics","/metrics"))
    services=[]
    for name,port,binary,args,path,config,ready,live in definitions:
        exporter=name.startswith("nats-exporter-")
        s={"name":name,"kind":"StatefulSet" if path else "Deployment","role":"other","replicas":1,"image_key":"nats-exporter"if exporter else name,"ports":{"http":port},"metrics_port":port if exporter or name in ("alloy","platform-metrics") else None,"environment":{},"command":[binary]+args,"ready":{"path":ready,"port":port},"live":{"path":live,"port":port},"config_mounts":[]}
        if config:s["config_mounts"].append({"name":name+"-config","path":config})
        if path:s["storage"]={"path":path,"size":"1Gi","uid":{"grafana":472,"prometheus":65534,"loki":10001,"tempo":10001}[name]}
        if name=="grafana":
            s["secret"]="grafana-runtime"
            s["environment"]={"GF_PATHS_PROVISIONING":"/etc/grafana/provisioning","GF_USERS_ALLOW_SIGN_UP":"false","GF_AUTH_ANONYMOUS_ENABLED":"false"}
            s["config_mounts"] += [{"name":"grafana-datasources","path":"/etc/grafana/provisioning/datasources"},{"name":"grafana-providers","path":"/etc/grafana/provisioning/dashboards"},{"name":"platform-dashboards","path":"/etc/platform-dashboards"}]
        if name=="tempo":s["ports"].update({"otlp-http":4318,"otlp-grpc":4317})
        services.append(s)
    resources, report=render_services(inventory,{"version":1,"services":services},images)
    ns=inventory["namespace"];items=resources["items"]
    def add(api,kind,name,**fields):
        items.append({"apiVersion":api,"kind":kind,"metadata":{"name":name,"namespace":ns},**fields})
    for name,data in configs(inventory,storage,additional_storages).items():add("v1","ConfigMap",name,data=data)
    add("v1","ServiceAccount","platform-monitoring",automountServiceAccountToken=False)
    add("rbac.authorization.k8s.io/v1","Role","platform-monitoring",rules=[{"apiGroups":[""],"resources":["pods","services","endpoints"],"verbs":["get","list","watch"]},{"apiGroups":[""],"resources":["pods/log"],"verbs":["get"]},{"apiGroups":["metrics.k8s.io"],"resources":["pods"],"verbs":["get","list"]}])
    add("rbac.authorization.k8s.io/v1","RoleBinding","platform-monitoring",roleRef={"apiGroup":"rbac.authorization.k8s.io","kind":"Role","name":"platform-monitoring"},subjects=[{"kind":"ServiceAccount","name":"platform-monitoring","namespace":ns}])
    for obj in items:
        if obj["kind"] not in ("Deployment","StatefulSet"):continue
        pod=obj["spec"]["template"]["spec"];container=pod["containers"][0];name=container["name"]
        if name in ("prometheus","alloy","platform-metrics"):
            pod.update({"serviceAccountName":"platform-monitoring","automountServiceAccountToken":True})
        if name=="platform-metrics":container["env"].append({"name":"POD_NAMESPACE","valueFrom":{"fieldRef":{"fieldPath":"metadata.namespace"}}})
        if name=="alloy":
            pod["volumes"].append({"name":"alloy-data","emptyDir":{"sizeLimit":"128Mi"}})
            container["volumeMounts"].append({"name":"alloy-data","mountPath":"/var/lib/alloy/data"})
    selector={"app.kubernetes.io/name":"node-exporter","platform.aat/cluster":inventory["cluster_id"]}
    node_pod={"automountServiceAccountToken":False,"hostNetwork":True,"hostPID":True,"dnsPolicy":"ClusterFirstWithHostNet","nodeSelector":{"platform.aat/cluster":inventory["cluster_id"]},"securityContext":{"runAsNonRoot":True,"runAsUser":65534,"seccompProfile":{"type":"RuntimeDefault"}},"containers":[{"name":"node-exporter","image":images["node-exporter"],"args":["--path.procfs=/host/proc","--path.sysfs=/host/sys","--path.rootfs=/host/root","--web.listen-address=:9100"],"ports":[{"name":"metrics","containerPort":9100}],"resources":{"requests":{"memory":"128Mi","cpu":"50m"},"limits":{"memory":"256Mi","cpu":"500m"}},"securityContext":{"allowPrivilegeEscalation":False,"readOnlyRootFilesystem":True,"capabilities":{"drop":["ALL"]}},"volumeMounts":[{"name":n,"mountPath":"/host/"+p,"readOnly":True} for n,p in (("proc","proc"),("sys","sys"),("root","root"))]}],"volumes":[{"name":n,"hostPath":{"path":p,"type":"Directory"}}for n,p in (("proc","/proc"),("sys","/sys"),("root","/"))]}
    add("apps/v1","DaemonSet","node-exporter",spec={"selector":{"matchLabels":selector},"template":{"metadata":{"labels":{**selector,"role":"other"}},"spec":node_pod}})
    report["memory_requests_mib"]+=128*len(inventory["nodes"])
    report.update({"profile":"development-single-copy-observability","external_dependencies":["metrics-server","grafana-runtime Secret","broker"],"note":"Local observability volumes are single-copy development storage. Full application plus monitoring does not fit the lightweight VM allocation."})
    return resources, report
