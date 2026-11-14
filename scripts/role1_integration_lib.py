"""Offline M3/M4 assembly. No infrastructure, credential or VM side effects."""
import copy
import hashlib
import json
import re
from pathlib import Path
from aggregator_lib import render as aggregator_render
from storage_lib import require


def render(inventory, aggregator_config, config, aggregator_image, ingestion_image):
    require(config.get("version") == 1, "role1 config version must be 1")
    for key in ("bmkg_initial_offset", "satellite_initial_offset", "payload_quota_bytes"):
        require(type(config.get(key)) is int and config[key] >= (1 if key == "payload_quota_bytes" else 0), "explicit offset/quota required")
    for key in ("bmkg_namespace", "satellite_namespace"):
        require(isinstance(config.get(key), str) and config[key] and "\x00" not in config[key], "explicit source namespace required")
    for key in ("runtime_secret", "ingestion_storage_secret", "ingestion_durability_configmap"):
        require(isinstance(config.get(key), str) and re.fullmatch(r"[a-z0-9][a-z0-9.-]*", config[key]), "named private Secret/ConfigMap required")
    require(re.fullmatch(r"[a-z0-9./:_-]+@sha256:[0-9a-f]{64}", ingestion_image), "immutable ingestion image required")
    result = aggregator_render(inventory, aggregator_config, aggregator_image)
    ns, cluster = inventory["namespace"], inventory["cluster_id"]
    labels = {"platform.aat/cluster": cluster, "role": "other"}
    def obj(api, kind, name, **fields):
        return {"apiVersion": api, "kind": kind, "metadata": {"name": name, "namespace": ns, "labels": labels.copy()}, **fields}
    def secret_env(key, name):
        return {"name": key, "valueFrom": {"secretKeyRef": {"name": name, "key": key}}}
    def values(data):
        return [{"name": key, "value": str(value)} for key, value in data.items()]
    deployment = next(x for x in result["items"] if x["kind"] == "Deployment")
    pod = deployment["spec"]["template"]["spec"]
    container = pod["containers"][0]
    for feature in ("analytics_enabled", "audit_sessions_enabled"):
        require(type(config.get(feature, False)) is bool, feature + " must be boolean")
    if config.get("analytics_enabled", False):
        container["env"] += values({"ANALYTICS_ENABLED": "true"})
        container["env"].append(secret_env("ANALYTICS_SERVICE_TOKEN", config["runtime_secret"]))
    if config.get("audit_sessions_enabled", False):
        container["env"] += values({"AUDIT_SESSIONS_ENABLED": "true"})
    sessions = [{"source": source, "namespace": config[name + "_namespace"], "initial_offset": config[name + "_initial_offset"]} for source, name in (("BMKG", "bmkg"), ("SATELIT", "satellite"))]
    container["env"] += values({"BMKG_RAW_ENABLED": "true", "RAW_PIPELINE_ENABLED": "true", "SATELLITE_HISTORY_ENABLED": "true", "RAW_INPUT_SESSIONS": json.dumps(sessions, separators=(",", ":")), "RAW_INPUT_SCHEMA": "/etc/role1/raw-input.avsc", "INPUT_PAYLOAD_QUOTA_BYTES": config["payload_quota_bytes"], "INGESTION_BASE_URL": "http://ingestion:8080", "AUDIT_ENABLED": "true"})
    for key in ("HISTORY_SERVICE_TOKEN", "INGESTION_SERVICE_TOKEN", "AUDIT_TOKEN", "AUDIT_CURSOR_KEY"):
        container["env"].append(secret_env(key, config["runtime_secret"]))
    container["env"].append({"name": "POD_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}})
    account = cluster + "-audit-reader"
    pod["serviceAccountName"] = account
    # Explicit projected, rotating token; automount remains disabled.
    pod["volumes"] += [{"name": "audit-api", "projected": {"sources": [{"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}}, {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}}]}}, {"name": "raw-schema", "configMap": {"name": cluster + "-raw-schema"}}]
    container["volumeMounts"] += [{"name": "audit-api", "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount", "readOnly": True}, {"name": "raw-schema", "mountPath": "/etc/role1", "readOnly": True}]
    result["items"] += [obj("v1", "ServiceAccount", account, automountServiceAccountToken=False), obj("rbac.authorization.k8s.io/v1", "Role", account, rules=[{"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]}]), obj("rbac.authorization.k8s.io/v1", "RoleBinding", account, subjects=[{"kind": "ServiceAccount", "name": account, "namespace": ns}], roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": account})]
    raw_schema = (Path(__file__).resolve().parent.parent / "docs/fixtures/raw-input-v1.avsc").read_text(encoding="utf-8")
    result["items"].append(obj("v1", "ConfigMap", cluster + "-raw-schema", data={"raw-input.avsc": raw_schema}))
    if "wildfire" in config:
        wildfire = config["wildfire"]
        require(isinstance(wildfire, dict), "wildfire requires explicit normalized processor inputs")
        config_text, province_text = wildfire.get("config_json"), wildfire.get("province_json")
        require(isinstance(config_text, str) and isinstance(province_text, str), "wildfire config_json/province_json must preserve UTF-8 contents")
        require(len(config_text.encode("utf-8")) <= 1 << 20 and len(province_text.encode("utf-8")) <= 1 << 20, "wildfire configuration exceeds bounded startup inputs")
        processor = json.loads(config_text)
        require(processor.get("version") == 1 and processor.get("algorithm_version") == 2, "wildfire requires algorithm version 2")
        require(processor.get("namespace") == config["satellite_namespace"] and processor.get("initial_offset") == config["satellite_initial_offset"], "wildfire must match the configured satellite history")
        require(type(processor.get("allowed_lateness_seconds")) is int and processor["allowed_lateness_seconds"] >= 0, "fixed explicit wildfire lateness required")
        for key in ("page_rows", "cache_bytes", "staging_bytes", "prepared_bytes"):
            require(type(processor.get(key)) is int and processor[key] > 0, "explicit wildfire execution budgets required")
        require(processor["page_rows"] <= 1000 and processor["cache_bytes"] >= 4096 and processor["staging_bytes"] >= 4096 and 1024 <= processor["prepared_bytes"] <= processor["cache_bytes"] // 4 and processor["page_rows"] <= processor["cache_bytes"] // 256, "wildfire execution budgets are incompatible")
        require(hashlib.sha256(province_text.encode("utf-8")).hexdigest() == processor.get("province_sha256"), "wildfire province checksum mismatch")
        require(isinstance(json.loads(province_text), list) and json.loads(province_text), "immutable province table required")
        processor["province_file"] = "provinces.json"
        encoded = json.dumps(processor, ensure_ascii=False, separators=(",", ":"))
        identity = hashlib.sha256((encoded + "\0" + province_text).encode("utf-8")).hexdigest()[:12]
        name = cluster + "-wildfire-" + identity
        result["items"].append(obj("v1", "ConfigMap", name, immutable=True, data={"config.json": encoded, "provinces.json": province_text}))
        pod["volumes"].append({"name": "wildfire", "configMap": {"name": name}})
        container["volumeMounts"].append({"name": "wildfire", "mountPath": "/etc/wildfire", "readOnly": True})
        container["env"] += values({"WILDFIRE_PROCESSOR_ENABLED": "true", "WILDFIRE_PROCESSOR_CONFIG": "/etc/wildfire/config.json"})
    owned = copy.deepcopy(pod)
    owned.pop("serviceAccountName", None)
    owned["volumes"] = [{"name": "durability", "configMap": {"name": config["ingestion_durability_configmap"]}}, {"name": "raw-schema", "configMap": {"name": cluster + "-raw-schema"}}, {"name": "tmp", "emptyDir": {"sizeLimit": "16Mi"}}]
    owned["containers"] = [{"name": "ingestion", "image": ingestion_image, "command": ["/usr/local/bin/raw-ingestion"], "ports": [{"name": "http", "containerPort": 8080}], "resources": {"requests": {"memory": "128Mi", "cpu": "100m"}, "limits": {"memory": "256Mi", "cpu": "1"}}, "securityContext": container["securityContext"], "env": values({"INGESTION_NAMESPACE": config["bmkg_namespace"], "INGESTION_INITIAL_OFFSET": config["bmkg_initial_offset"], "INGESTION_PAYLOAD_QUOTA_BYTES": config["payload_quota_bytes"], "INGESTION_DURABILITY_CONFIG": "/etc/aat/durability.json", "INGESTION_PORT": "8080", "BMKG_BASE_URL": "http://bmkg-mock:8081", "SCHEMA_REGISTRY_URL": "http://schema-registry:8080/apis/ccompat/v7", "AGGREGATOR_BASE_URL": "http://aggregator:8080", "AUTH_BASE_URL": "http://auth-service:8083", "RAW_INPUT_SCHEMA": "/etc/role1/raw-input.avsc", "BMKG_SEISMIC_READER_SCHEMA": "/schemas/seismic.avsc", "BMKG_TSUNAMI_READER_SCHEMA": "/schemas/tsunami.avsc", "OTEL_EXPORTER_OTLP_ENDPOINT": "http://tempo:4318"}) + [secret_env(key, config["runtime_secret"]) for key in ("INGESTION_SERVICE_TOKEN", "INGESTION_CURSOR_KEY", "AUTH_INTERNAL_KEY", "BMKG_API_KEY", "NATS_URL")] + [secret_env(key, config["ingestion_storage_secret"]) for key in ("INGESTION_DATABASE_URL", "INGESTION_GROUP_0_URL")], "volumeMounts": [{"name": "durability", "mountPath": "/etc/aat", "readOnly": True}, {"name": "raw-schema", "mountPath": "/etc/role1", "readOnly": True}, {"name": "tmp", "mountPath": "/tmp"}], "startupProbe": container["startupProbe"], "livenessProbe": container["livenessProbe"], "readinessProbe": container["readinessProbe"]}]
    selector = {"app.kubernetes.io/name": "ingestion", "platform.aat/cluster": cluster}
    owned["topologySpreadConstraints"][0]["labelSelector"] = {"matchLabels": selector}
    result["items"] += [obj("apps/v1", "Deployment", cluster + "-ingestion", spec={"replicas": 3, "selector": {"matchLabels": selector}, "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1}}, "template": {"metadata": {"labels": {**selector, "role": "consumer"}}, "spec": owned}}), obj("v1", "Service", "ingestion", spec={"selector": selector, "ports": [{"name": "http", "port": 8080, "targetPort": "http"}]})]
    def peer(name): return {"podSelector": {"matchLabels": {"app.kubernetes.io/name": name}}}
    def flow(names, ports): return {"to": [peer(name) for name in names], "ports": [{"protocol": "TCP", "port": port} for port in ports]}
    def policy(name, selector, ingress=(), egress=()):
        result["items"].append(obj("networking.k8s.io/v1", "NetworkPolicy", cluster + "-role1-" + name, spec={"podSelector": {"matchLabels": selector}, "policyTypes": ["Ingress", "Egress"], "ingress": list(ingress), "egress": list(egress)}))
    def incoming(names, ports): return {"from": [peer(name) for name in names], "ports": [{"protocol": "TCP", "port": port} for port in ports]}
    form = cluster + "-ingestion"
    storage = {"platform.aat/formation": form, "component": "storage-postgres"}
    router = {"platform.aat/formation": form, "component": "storage-router"}
    etcd = {"platform.aat/formation": form, "component": "storage-etcd"}
    def group_peer(labels): return {"podSelector": {"matchLabels": labels}}
    def group_flow(labels, ports, direction="to"): return {direction: [group_peer(label) for label in labels], "ports": [{"protocol": "TCP", "port": port} for port in ports]}
    policy("ingestion", selector, [incoming(["aggregator", "prometheus", "alloy"], [8080])], [flow(["aggregator", "schema-registry", "tempo"], [8080, 4318]), flow(["auth-service"], [8083]), flow(["broker"], [4222]), flow(["bmkg-mock"], [8081]), group_flow([storage, router], [5432])])
    policy("bmkg", {"app.kubernetes.io/name": "bmkg-mock"}, [incoming(["ingestion"], [8081])])
    api_egress = [{"to": [{"ipBlock": {"cidr": inventory["service_cidr"]}}], "ports": [{"protocol": "TCP", "port": 443}]}, {"to": [{"ipBlock": {"cidr": n["address"] + "/32"}} for n in inventory["nodes"]], "ports": [{"protocol": "TCP", "port": 6443}]}]
    policy("aggregator", {"app.kubernetes.io/name": "aggregator"}, [incoming(["ingestion"], [8080])], [flow(["ingestion"], [8080]), group_flow([{"platform.aat/formation": cluster + "-storage", "component": "storage-postgres"}], [8008])] + api_egress)
    policy("owned-postgres", storage, [incoming(["ingestion", "ingestion-migrate"], [5432]), group_flow([storage, router], [5432, 8008], "from")], [group_flow([storage], [5432, 8008]), group_flow([etcd], [2379])])
    policy("owned-router", router, [incoming(["ingestion", "ingestion-migrate"], [5432])], [group_flow([storage], [5432, 8008])])
    policy("owned-etcd", etcd, [group_flow([storage, etcd], [2379], "from"), group_flow([etcd], [2380], "from")], [group_flow([etcd], [2379, 2380])])
    return result
