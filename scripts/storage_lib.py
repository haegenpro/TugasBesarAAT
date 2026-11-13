"""Offline Kubernetes storage renderer. Importing this module has no effects."""
import copy
import json
from pathlib import Path
import re

ETCD_IMAGE = "gcr.io/etcd-development/etcd@sha256:159cf260a40becd61d289ef582fe9339a53c7d1bd22d7cffebd213e095625183"
PROXY_IMAGE = "haproxy@sha256:1c3568e520453cc8ba98c39501febcf450429c80e6cc27ecad779961d76e597c"


def require(value, message):
    if not value:
        raise ValueError(message)


def plan(inventory, mode, image):
    require(mode in ("postgres", "citus"), "mode must be postgres or citus")
    require(re.fullmatch(r"[a-z0-9./:_-]+@sha256:[0-9a-f]{64}", image), "supply the built Patroni image by immutable digest")
    nodes = [n["name"] for n in inventory["nodes"]]
    placement = {0: [0, 1, 2]}
    if mode == "citus":
        placement.update({1: [1, 2, 3], 2: [0, 2, 3], 3: [0, 1, 3], 4: [0, 1, 2]})
    formation = inventory["cluster_id"] + "-storage"
    namespace = inventory["namespace"]
    groups = []
    for group, indexes in placement.items():
        members = []
        for i in indexes:
            workload = f"{formation}-g{group}-{chr(97+i)}"
            name = workload + "-0"
            members.append({"name": name, "workload": workload, "node": nodes[i],
                            "host": f"{name}.{formation}-members.{namespace}.svc.cluster.local", "port": 5432})
        groups.append({"id": group, "system_id": "RECORD_AFTER_BOOTSTRAP", "dsn_env": f"STORAGE_GROUP_{group}_URL", "members": members})
    per_node = {node: 0 for node in nodes}
    for group in groups:
        for member in group["members"]:
            per_node[member["node"]] += 512
    for node in nodes[:3]:
        per_node[node] += 128  # etcd
    return {"version": 1, "formation": formation, "namespace": namespace,
            "mode": mode, "image": image, "groups": groups,
            "nodes": nodes, "pod_cidr": inventory["pod_cidr"], "cluster_id": inventory["cluster_id"],
            "requests_mib_before_routers": per_node, "router_requests_mib": 256,
            "vm_memory_mib": {n["name"]: n["memory_mib"] for n in inventory["nodes"]}}


def durability_config(storage):
    groups = copy.deepcopy(storage["groups"])
    for group in groups:
        for member in group["members"]:
            for field in ("workload", "node"):
                member.pop(field)
    return {"version": 1, "mode": storage["mode"], "formation": storage["formation"], "groups": groups}


def resources(role):
    request, limit = ("512Mi", "1Gi") if role == "store" else ("128Mi", "256Mi")
    return {"requests": {"memory": request, "cpu": "100m"}, "limits": {"memory": limit, "cpu": "1"}}


def manifests(storage):
    ns, formation = storage["namespace"], storage["formation"]
    owner = {"platform.aat/formation": formation}
    objects = [{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}}]

    def obj(kind, name, spec=None, **fields):
        api = "apps/v1" if kind in ("StatefulSet", "Deployment") else "v1"
        meta = {"name": name, "labels": owner.copy()}
        if kind != "PersistentVolume":
            meta["namespace"] = ns
        result = {"apiVersion": api, "kind": kind, "metadata": meta, **fields}
        if spec is not None:
            result["spec"] = spec
        objects.append(result)
        return result

    def service(name, selector, ports, headless=False):
        spec = {"selector": selector, "ports": [{"name": n, "port": p, "targetPort": t} for n, p, t in ports]}
        if headless:
            spec.update({"clusterIP": "None", "publishNotReadyAddresses": True})
        obj("Service", name, spec)

    def persistent(name, node, size):
        affinity = {"required": {"nodeSelectorTerms": [{"matchExpressions": [
            {"key": "kubernetes.io/hostname", "operator": "In", "values": [node]}]}]}}
        obj("PersistentVolume", name, {"capacity": {"storage": size}, "volumeMode": "Filesystem",
            "accessModes": ["ReadWriteOnce"], "persistentVolumeReclaimPolicy": "Retain",
            "storageClassName": "", "local": {"path": f"/var/lib/aat/local-pv/{formation}/{name}"},
            "nodeAffinity": affinity, "claimRef": {"namespace": ns, "name": name}})
        obj("PersistentVolumeClaim", name, {"accessModes": ["ReadWriteOnce"], "storageClassName": "",
            "volumeName": name, "resources": {"requests": {"storage": size}}})

    etcd_service = formation + "-etcd-members"
    etcd_names = [formation + "-etcd-" + letter + "-0" for letter in "abc"]
    etcd_hosts = [f"{name}.{etcd_service}.{ns}.svc.cluster.local" for name in etcd_names]
    service(etcd_service, {**owner, "component": "storage-etcd"}, [("client", 2379, 2379), ("peer", 2380, 2380)], True)
    initial = ",".join(f"{name}=https://{host}:2380" for name, host in zip(etcd_names, etcd_hosts))
    for i, (name, host) in enumerate(zip(etcd_names, etcd_hosts)):
        workload, node = name[:-2], storage["nodes"][i]
        persistent(workload, node, "512Mi")
        labels = {**owner, "role": "other", "component": "storage-etcd", "member": name}
        obj("StatefulSet", workload, {"replicas": 1, "serviceName": etcd_service,
            "selector": {"matchLabels": {"member": name}}, "template": {"metadata": {"labels": labels}, "spec": {
                "nodeSelector": {"kubernetes.io/hostname": node}, "automountServiceAccountToken": False,
                "securityContext": {"runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000},
                "containers": [{"name": "etcd", "image": ETCD_IMAGE, "command": ["/usr/local/bin/etcd"],
                    "args": [f"--name={name}", "--data-dir=/data/member", f"--initial-cluster={initial}",
                             f"--initial-cluster-token={formation}", "--initial-cluster-state=new",
                             "--listen-client-urls=https://0.0.0.0:2379", f"--advertise-client-urls=https://{host}:2379",
                             "--listen-peer-urls=https://0.0.0.0:2380", f"--initial-advertise-peer-urls=https://{host}:2380",
                             "--client-cert-auth=true", "--peer-client-cert-auth=true", "--trusted-ca-file=/tls/ca.crt",
                             "--cert-file=/tls/tls.crt", "--key-file=/tls/tls.key", "--peer-trusted-ca-file=/tls/ca.crt",
                             "--peer-cert-file=/tls/tls.crt", "--peer-key-file=/tls/tls.key", "--quota-backend-bytes=134217728",
                             "--auto-compaction-retention=1", "--listen-metrics-urls=http://0.0.0.0:2381"],
                    "ports": [{"containerPort": 2379}, {"containerPort": 2380}], "resources": resources("other"),
                    "readinessProbe": {"exec": {"command": ["/usr/local/bin/etcdctl", "--endpoints=https://127.0.0.1:2379",
                        "--cacert=/tls/ca.crt", "--cert=/tls/tls.crt", "--key=/tls/tls.key", "endpoint", "health"]}, "periodSeconds": 5},
                    "volumeMounts": [{"name": "data", "mountPath": "/data"}, {"name": "tls", "mountPath": "/tls", "readOnly": True}]}],
                "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": workload}},
                            {"name": "tls", "secret": {"secretName": workload + "-tls", "defaultMode": 0o440}}]}}})

    service(formation + "-members", {**owner, "component": "storage-postgres"}, [("postgres", 5432, 5432), ("patroni", 8008, 8008)], True)
    for group in storage["groups"]:
        for member in group["members"]:
            name, workload, node = member["name"], member["workload"], member["node"]
            persistent(workload, node, "1Gi")
            labels = {**owner, "role": "store", "component": "storage-postgres", "member": name, "storage-group": str(group["id"])}
            values = {"FORMATION": formation, "STORAGE_MODE": storage["mode"], "GROUP_ID": str(group["id"]),
                      "MEMBER_SERVICE": formation + "-members", "POSTGRES_DB": "hazards", "POD_CIDR": storage["pod_cidr"],
                      "ETCD_ENDPOINTS": ",".join(h + ":2379" for h in etcd_hosts)}
            env = [{"name": k, "value": v} for k, v in values.items()]
            env += [{"name": k, "valueFrom": {"fieldRef": {"fieldPath": v}}} for k, v in
                    (("POD_NAME", "metadata.name"), ("POD_NAMESPACE", "metadata.namespace"))]
            obj("StatefulSet", workload, {"replicas": 1, "serviceName": formation + "-members",
                "selector": {"matchLabels": {"member": name}}, "template": {"metadata": {"labels": labels}, "spec": {
                    "nodeSelector": {"kubernetes.io/hostname": node}, "automountServiceAccountToken": False,
                    "terminationGracePeriodSeconds": 45, "securityContext": {"runAsUser": 999, "runAsGroup": 999, "fsGroup": 999},
                    "containers": [{"name": "postgres", "image": storage["image"], "env": env,
                        "envFrom": [{"secretRef": {"name": formation + "-credentials"}}], "resources": resources("store"),
                        "ports": [{"containerPort": 5432}, {"containerPort": 8008}],
                        "readinessProbe": {"httpGet": {"path": "/health", "port": 8008}, "periodSeconds": 3},
                        "volumeMounts": [{"name": "data", "mountPath": "/var/lib/postgresql/data"},
                                         {"name": "etcd-tls", "mountPath": "/etcd-tls", "readOnly": True}]}],
                    "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": workload}},
                        {"name": "etcd-tls", "secret": {"secretName": formation + "-patroni-tls", "defaultMode": 0o440}}]}}})

    proxy = ["global", "  log stdout format raw local0", "defaults", "  mode tcp", "  log global",
             "  timeout connect 2s", "  timeout client 65s", "  timeout server 65s", "resolvers cluster_dns",
             "  parse-resolv-conf", "  resolve_retries 3", "  timeout resolve 1s", "  timeout retry 1s", "  hold valid 5s"]
    for group in storage["groups"]:
        port = 5432 + group["id"]
        proxy += [f"listen group_{group['id']}", f"  bind *:{port}", "  option httpchk GET /primary", "  http-check expect status 200"]
        for member in group["members"]:
            proxy.append(f"  server {member['name']} {member['host']}:5432 check port 8008 inter 1s fall 2 rise 2 resolvers cluster_dns init-addr none on-marked-down shutdown-sessions")
        service(f"{formation}-g{group['id']}-primary", {**owner, "component": "storage-router"}, [("postgres", 5432, port)])
    obj("ConfigMap", formation + "-router", data={"haproxy.cfg": "\n".join(proxy) + "\n"})
    router_labels = {**owner, "role": "other", "component": "storage-router"}
    obj("Deployment", formation + "-router", {"replicas": 2, "selector": {"matchLabels": {**owner, "component": "storage-router"}},
        "template": {"metadata": {"labels": router_labels}, "spec": {"automountServiceAccountToken": False,
            "affinity": {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{
                "labelSelector": {"matchLabels": {**owner, "component": "storage-router"}}, "topologyKey": "kubernetes.io/hostname"}]}},
            "containers": [{"name": "router", "image": PROXY_IMAGE, "resources": resources("other"),
                            "volumeMounts": [{"name": "config", "mountPath": "/usr/local/etc/haproxy", "readOnly": True}]}],
            "volumes": [{"name": "config", "configMap": {"name": formation + "-router"}}]}}})
    return {"apiVersion": "v1", "kind": "List", "items": objects}


def write_json(path, value):
    path = Path(path)
    path.write_text(json.dumps(value, indent=2) + "\n")
    path.chmod(0o600)
