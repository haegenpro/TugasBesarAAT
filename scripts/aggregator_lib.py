"""Offline application manifest renderer; no cluster calls or secret reads."""
import re
from urllib.parse import urlsplit

from storage_lib import require

PUBLIC = {
    "BMKG_BASE_URL", "PVMBG_BASE_URL", "NATS_URL", "NATS_STREAM", "NATS_SUBJECT",
    "NATS_LIFECYCLE_STREAM", "NATS_LIFECYCLE_SUBJECT", "SCHEMA_REGISTRY_URL",
    "POLL_INTERVAL", "SOURCE_RETRY_MAX_SEC", "BMKG_FRESHNESS_SEC", "PVMBG_FRESHNESS_SEC",
    "ACQUISITION_LEASE_TTL", "OWNERSHIP_LEASE_TTL",
}


def render(inventory, config, image, replicas=3):
    require(re.fullmatch(r"[a-z0-9./:_-]+@sha256:[0-9a-f]{64}", image), "supply an actual built Aggregator image by immutable digest")
    require(replicas in (2, 3, 5), "replica profiles are 2, 3 and 5; default is 3")
    require(set(config) == {"version", "storage_mode", "runtime_secret", "storage_secret", "durability_configmap", "environment"}, "unknown/missing Aggregator configuration fields")
    require(config["version"] == 1 and config["storage_mode"] in ("postgres", "citus"), "unsupported configuration version/storage mode")
    for key in ("runtime_secret", "storage_secret", "durability_configmap"):
        name = config[key]
        require(isinstance(name, str) and len(name) <= 253 and
                all(len(label) <= 63 and re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", label) for label in name.split(".")),
                "invalid Secret/ConfigMap name")
    public = config["environment"].copy()
    require(set(public) == PUBLIC and all(isinstance(v, str) and v and "\n" not in v for v in public.values()), "supply exactly the documented nonsecret environment settings")
    for key in ("BMKG_BASE_URL", "PVMBG_BASE_URL", "NATS_URL", "SCHEMA_REGISTRY_URL"):
        uri = urlsplit(public[key])
        allowed = ("nats", "tls", "ws", "wss") if key == "NATS_URL" else ("http", "https")
        require(uri.scheme in allowed and uri.hostname and not uri.username and not uri.password and not uri.query and not uri.fragment,
                "public service URLs must not include credentials, query strings or fragments")
    public.update({
        "AGGREGATOR_PORT": "8080", "ACQUISITION_ENABLED": "true", "PLATFORM_COORDINATION_ENABLED": "true",
        "CANONICAL_ADAPTER_ENABLED": "true", "CANONICAL_OWNERSHIP_ENABLED": "true",
        "CANONICAL_DURABILITY_CONFIG": "/etc/aat/durability.json",
        "AGGREGATOR_POLL_ENABLED": "true", "AGGREGATOR_REPLAY_ENABLED": "true", "AGGREGATOR_PUBLISH_ENABLED": "true",
        "DB_IDLE_TRANSACTION_TIMEOUT": "15s", "DB_ADMISSION_IDLE_TIMEOUT": "30s",
        "SHUTDOWN_TIMEOUT": "30s", "DB_MAX_OPEN_CONNS": "16", "DB_STATEMENT_TIMEOUT": "5s", "DB_LOCK_TIMEOUT": "2s",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://tempo:4318",
        "BMKG_SEISMIC_READER_SCHEMA": "/schemas/seismic.avsc", "BMKG_TSUNAMI_READER_SCHEMA": "/schemas/tsunami.avsc",
        "PVMBG_READER_SCHEMA": "/schemas/volcanic-v1.avsc",
    })
    namespace, cluster = inventory["namespace"], inventory["cluster_id"]
    name = cluster + "-aggregator"
    selector = {"app.kubernetes.io/name": "aggregator", "platform.aat/cluster": cluster}
    labels = {**selector, "role": "aggregator"}

    def resource(api, kind, resource_name, **fields):
        return {"apiVersion": api, "kind": kind,
                "metadata": {"name": resource_name, "namespace": namespace, "labels": labels.copy()}, **fields}

    env = [{"name": key, "valueFrom": {"configMapKeyRef": {"name": name + "-config", "key": key}}} for key in sorted(public)]
    for key in ("AGGREGATOR_ADMIN_TOKEN", "BMKG_API_KEY", "PVMBG_TOKEN", "CANONICAL_SERVICE_TOKEN", "PROCESSOR_SERVICE_TOKEN"):
        env.append({"name": key, "valueFrom": {"secretKeyRef": {"name": config["runtime_secret"], "key": key}}})
    keys = ["DATABASE_URL"] + [f"STORAGE_GROUP_{i}_URL" for i in range(5 if config["storage_mode"] == "citus" else 1)]
    for key in keys:
        env.append({"name": key, "valueFrom": {"secretKeyRef": {"name": config["storage_secret"], "key": key}}})
    env.append({"name": "INSTANCE_ID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}})

    deployment = resource("apps/v1", "Deployment", name, spec={
        "replicas": replicas, "minReadySeconds": 5, "progressDeadlineSeconds": 300,
        "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1}},
        "selector": {"matchLabels": selector}, "template": {"metadata": {"labels": labels, "annotations": {"prometheus.io/scrape": "true", "prometheus.io/port": "8080", "prometheus.io/path": "/metrics"}}, "spec": {
            "automountServiceAccountToken": False, "terminationGracePeriodSeconds": 45,
            "securityContext": {"runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001,
                                "seccompProfile": {"type": "RuntimeDefault"}},
            "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [{
                "matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In", "values": [n["name"] for n in inventory["nodes"]]}]}]}}},
            "topologySpreadConstraints": [{"maxSkew": 1, "minDomains": 3, "topologyKey": "kubernetes.io/hostname",
                "whenUnsatisfiable": "DoNotSchedule", "labelSelector": {"matchLabels": selector}}],
            "containers": [{"name": "aggregator", "image": image, "ports": [{"name": "http", "containerPort": 8080}], "env": env,
                "resources": {"requests": {"memory": "256Mi", "cpu": "100m"}, "limits": {"memory": "512Mi", "cpu": "1"}},
                "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
                "startupProbe": {"httpGet": {"path": "/health", "port": "http"}, "periodSeconds": 2, "failureThreshold": 30},
                "livenessProbe": {"httpGet": {"path": "/health", "port": "http"}, "periodSeconds": 10, "timeoutSeconds": 2, "failureThreshold": 3},
                "readinessProbe": {"httpGet": {"path": "/ready", "port": "http"}, "periodSeconds": 3, "timeoutSeconds": 2, "failureThreshold": 2},
                "lifecycle": {"preStop": {"exec": {"command": ["/bin/sh", "-c", "sleep 5"]}}},
                "volumeMounts": [{"name": "durability", "mountPath": "/etc/aat", "readOnly": True}, {"name": "tmp", "mountPath": "/tmp"}]}],
            "volumes": [{"name": "durability", "configMap": {"name": config["durability_configmap"], "items": [{"key": "durability.json", "path": "durability.json"}]}},
                        {"name": "tmp", "emptyDir": {"sizeLimit": "64Mi"}}],
        }}})
    objects = [
        resource("v1", "ConfigMap", name + "-config", data=public),
        resource("v1", "Service", "aggregator", spec={"type": "ClusterIP", "selector": selector,
                 "ports": [{"name": "http", "port": 8080, "targetPort": "http"}]}),
        deployment,
        resource("policy/v1", "PodDisruptionBudget", name, spec={"minAvailable": 2, "selector": {"matchLabels": selector}}),
    ]
    return {"apiVersion": "v1", "kind": "List", "items": objects}
