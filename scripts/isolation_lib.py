"""Default-deny application isolation with explicit service data ownership."""
import ipaddress
from storage_lib import require


def render(inventory,extra_flows=()):
    ns,cluster=inventory["namespace"],inventory["cluster_id"]
    formation=cluster+"-storage";items=[]
    def app(name):return {"app.kubernetes.io/name":name}
    def peer(labels):return {"podSelector":{"matchLabels":labels}}
    def ports(values):return [{"protocol":"TCP","port":v}for v in values]
    def policy(name,selector,ingress=None,egress=None):
        spec={"podSelector":{"matchLabels":selector},"policyTypes":["Ingress","Egress"]}
        spec["ingress"]=ingress or [];spec["egress"]=egress or []
        items.append({"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":cluster+"-"+name,"namespace":ns},"spec":spec})
    def incoming(names,values):return {"from":[peer(app(n))for n in names],"ports":ports(values)}
    def outgoing(names,values):return {"to":[peer(app(n))for n in names],"ports":ports(values)}
    policy("default-deny",{})
    dns={"to":[{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":"kube-system"}},"podSelector":{"matchLabels":{"k8s-app":"kube-dns"}}}],
         "ports":[{"protocol":protocol,"port":53}for protocol in("UDP","TCP")]}
    policy("dns",{},egress=[dns])
    postgres={"platform.aat/formation":formation,"component":"storage-postgres"}
    router={"platform.aat/formation":formation,"component":"storage-router"}
    etcd={"platform.aat/formation":formation,"component":"storage-etcd"}
    monitor_names=["prometheus","alloy","loki","tempo","grafana"]
    policy("canonical-postgres",postgres,
        ingress=[incoming(["aggregator","canonical-migrate"],[5432]),{"from":[peer(postgres),peer(router)],"ports":ports([5432,8008])},incoming(["prometheus"],[8008,9187])],
        egress=[{"to":[peer(postgres)],"ports":ports([5432,8008])},{"to":[peer(etcd)],"ports":ports([2379])}])
    policy("canonical-router",router,
        ingress=[incoming(["aggregator","canonical-migrate"],list(range(5432,5437)))],
        egress=[{"to":[peer(postgres)],"ports":ports([5432,8008])}])
    policy("canonical-etcd",etcd,
        ingress=[{"from":[peer(postgres),peer(etcd)],"ports":ports([2379])},
                 incoming(["registry-db","auth-db","dashboard-db","notifier-db"],[2379]),
                 {"from":[peer(etcd)],"ports":ports([2380])},incoming(["prometheus"],[2381])],
        egress=[{"to":[peer(etcd)],"ports":ports([2379,2380])}])
    canonical_clients=["client-api","wildfire-processor","ingestion","coastal-processor"]
    data_egress=[{"to":[peer(postgres),peer(router)],"ports":ports(list(range(5432,5437)))}]
    policy("aggregator",app("aggregator"),
        ingress=[incoming(canonical_clients+["prometheus","alloy"],[8080])],
        egress=data_egress+[outgoing(["bmkg-mock"],[8081]),outgoing(["pvmbg-mock"],[8082]),outgoing(["broker"],[4222]),outgoing(["schema-registry"],[8080]),outgoing(["tempo"],[4318])])
    policy("canonical-migrate",app("canonical-migrate"),egress=data_egress)
    for source,port in (("bmkg-mock",8081),("pvmbg-mock",8082)):
        policy(source,app(source),ingress=[incoming(["aggregator","prometheus","alloy"],[port])],egress=[outgoing(["schema-registry"],[8080]),outgoing(["tempo"],[4318])])
    broker_clients=["aggregator","consumer-dashboard","consumer-notifier","broker-init","ingestion","wildfire-processor","coastal-processor"]
    policy("broker",app("broker"),ingress=[incoming(broker_clients,[4222]),incoming(["broker"],[6222]),incoming(["prometheus","alloy","nats-exporter"],[8222])],egress=[outgoing(["broker"],[6222])])
    policy("broker-init",app("broker-init"),egress=[outgoing(["broker"],[4222]),outgoing(["tempo"],[4318])])
    policy("registry",app("schema-registry"),ingress=[incoming(broker_clients+["bmkg-mock","pvmbg-mock"],[8080]),incoming(["prometheus","alloy"],[9000])],egress=[outgoing(["registry-db"],[5432]),outgoing(["tempo"],[4318])])
    owners={"registry-db":"schema-registry","auth-db":"auth-service","dashboard-db":"consumer-dashboard","notifier-db":"consumer-notifier"}
    for database,owner in owners.items():
        policy(database,app(database),ingress=[incoming([owner,database+"-init"],[5432]),incoming([database],[5432,8008]),incoming(["prometheus"],[8008,9187])],
            egress=[outgoing([database],[5432,8008]),{"to":[peer(etcd)],"ports":ports([2379])},outgoing([database+"-etcd"],[2379])])
        policy(database+"-init",app(database+"-init"),egress=[outgoing([database],[5432])])
        policy(database+"-etcd",app(database+"-etcd"),ingress=[incoming([database,database+"-etcd"],[2379,2380]),incoming(["prometheus"],[2381])],egress=[outgoing([database+"-etcd"],[2379,2380])])
    policy("auth",app("auth-service"),ingress=[incoming(["client-api","prometheus","alloy"],[8083])],egress=[outgoing(["auth-db"],[5432]),outgoing(["tempo"],[4318])])
    policy("client-api",app("client-api"),ingress=[incoming(["client-tui","load-generator","ingress","prometheus","alloy"],[8080])],egress=[outgoing(["auth-service"],[8083]),outgoing(["aggregator"],[8080]),outgoing(["tempo"],[4318])])
    for consumer,database,port in (("consumer-dashboard","dashboard-db",8091),("consumer-notifier","notifier-db",8092)):
        policy(consumer,app(consumer),ingress=[incoming(["prometheus","alloy","load-generator"],[port])],egress=[outgoing([database],[5432]),outgoing(["broker"],[4222]),outgoing(["schema-registry"],[8080]),outgoing(["tempo"],[4318])])
    policy("sensor",app("sensor-pantai"),ingress=[incoming(["coastal-processor","ingestion","prometheus","alloy"],[8080])])
    for name in("wildfire-processor","ingestion","coastal-processor"):
        policy(name,app(name),ingress=[incoming(["prometheus","alloy"],[8080])],egress=[outgoing(["aggregator","schema-registry","sensor-pantai"],[8080]),outgoing(["broker"],[4222]),outgoing(["tempo"],[4318])])
    policy("load-generator",app("load-generator"),egress=[outgoing(["client-api","aggregator"],[8080])])
    metric_ports=[8080,8081,8082,8083,8091,8092,8008,8222,9187,2381,9090,12345,3100,3200,3000,4317,4318,9100,9101,7777]
    for name in monitor_names+["nats-exporter","node-exporter","platform-metrics"]:
        policy(name,app(name),ingress=[{"from":[{"podSelector":{}}],"ports":ports(metric_ports)}],
            egress=[{"to":[{"podSelector":{}}],"ports":ports(metric_ports)},
                    {"to":[{"ipBlock":{"cidr":inventory["service_cidr"]}}],"ports":ports([443])},
                    {"to":[{"ipBlock":{"cidr":n["address"]+"/32"}}for n in inventory["nodes"]],"ports":ports([6443,9100])}])
    # Owner additions use an explicit app, CIDR and bounded ports. FQDN egress
    # needs CNI-specific handling; generic NetworkPolicy cannot express names.
    for i,flow in enumerate(extra_flows):
        network=ipaddress.ip_network(flow["cidr"])
        require(network.prefixlen>0 and NAME_SAFE(flow["app"]),"external egress must be scoped to a named app and bounded CIDR")
        require(flow["ports"] and all(type(p)is int and 1<=p<=65535 for p in flow["ports"]),"explicit external ports required")
        policy("external-"+str(i),app(flow["app"]),egress=[{"to":[{"ipBlock":{"cidr":str(network)}}],"ports":ports(flow["ports"])}])
    return {"apiVersion":"v1","kind":"List","items":items}

def NAME_SAFE(name):
    import re
    return bool(re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?",name))
