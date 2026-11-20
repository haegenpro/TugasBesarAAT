"""Owned, loopback validation entry points balanced over four VM NodePorts."""
import hashlib
import ipaddress
import json
import os
from pathlib import Path

from platform_lib.inventory import fingerprint
from storage_lib import PROXY_IMAGE,require,write_json

PRIVATE_IPV4=tuple(ipaddress.ip_network(cidr) for cidr in ("10.0.0.0/8","172.16.0.0/12","192.168.0.0/16"))


def private_address(value):
    require(isinstance(value,str),"an explicit private IPv4 address is required")
    address=ipaddress.IPv4Address(value)
    require(any(address in network for network in PRIVATE_IPV4),"an RFC 1918 IPv4 address is required")
    return str(address)


def render(inventory,output,client_port=18080,audit_port=18081,client_node_port=31080,audit_node_port=31081,*,controller_address=None,generator_address=None):
    require(type(client_port)is int and type(audit_port)is int and 1024<=client_port<=65535 and 1024<=audit_port<=65535 and client_port!=audit_port,"distinct unprivileged validation ports required")
    require(type(client_node_port)is int and type(audit_node_port)is int and 30000<=client_node_port<=32767 and 30000<=audit_node_port<=32767 and client_node_port!=audit_node_port,"distinct validation NodePorts in 30000..32767 required")
    output=Path(output);require(not output.exists(),"access output exists; preserve reviewed configuration")
    cluster=inventory["cluster_id"];ns=inventory["namespace"];labels={"platform.aat/cluster":cluster,"platform.aat/validation-access":"owned"}
    v3=inventory.get("version")==3
    node_addresses={n["address"]for n in inventory["nodes"]}
    if v3:
        controller=private_address(controller_address)
        addresses={controller}
        if generator_address is not None:addresses.add(private_address(generator_address))
        require(addresses.isdisjoint(node_addresses),"use controller/generator addresses outside the guest nodes")
        require(all(private_address(address)==address for address in node_addresses),"validation requires private VM addresses")
    else:
        require(controller_address is None and generator_address is None,"explicit validation callers require existing-cluster inventory v3")
        controller=next(h["address"]for h in inventory["hosts"]if h["id"]==inventory["controller_host"])if inventory.get("version")==2 else inventory["network"]["gateway"]
        addresses={controller,*node_addresses}
        require(all(ipaddress.ip_address(a).version==4 and ipaddress.ip_address(a).is_private for a in addresses),"validation access is limited to this private IPv4 topology")
    traffic_policy="Local" if v3 else "Cluster"
    objects=[];text=["global","  log stdout format raw local0","  maxconn 20000","defaults","  mode http","  timeout connect 2s","  timeout client 15s","  timeout server 15s","  option http-server-close"]
    for app,node_port,port in(("client-api",client_node_port,client_port),("aggregator",audit_node_port,audit_port)):
        name=cluster+"-validation-"+app
        selector={"app.kubernetes.io/name":app,"platform.aat/cluster":cluster}
        objects.append({"apiVersion":"v1","kind":"Service","metadata":{"name":name,"namespace":ns,"labels":labels},"spec":{"type":"NodePort","externalTrafficPolicy":traffic_policy,"selector":selector,"ports":[{"name":"http","port":8080,"targetPort":8080,"nodePort":node_port}]}})
        objects.append({"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":name,"namespace":ns,"labels":labels},"spec":{"podSelector":{"matchLabels":selector},"policyTypes":["Ingress"],"ingress":[{"from":[{"ipBlock":{"cidr":address+"/32"}}for address in sorted(addresses)],"ports":[{"protocol":"TCP","port":8080}]}]}})
        text += ["frontend "+app,"  bind 127.0.0.1:"+str(port),"  default_backend "+app+"-nodes","backend "+app+"-nodes","  balance roundrobin","  option httpchk GET /ready","  http-check expect status 200","  default-server inter 1s fall 2 rise 1"]
        if v3:text.append("  source "+controller)
        text += ["  server "+n["name"]+" "+n["address"]+":"+str(node_port)+" check"for n in inventory["nodes"]]
    output.mkdir(parents=True,mode=0o700);write_json(output/"resources.json",{"apiVersion":"v1","kind":"List","items":objects})
    (output/"haproxy.cfg").write_text("\n".join(text)+"\n");(output/"haproxy.cfg").chmod(0o600)
    report={"version":1,"cluster_id":cluster,"image":PROXY_IMAGE,"container":cluster+"-validation-access","config_sha256":hashlib.sha256((output/"haproxy.cfg").read_bytes()).hexdigest(),"client_base_url":"http://127.0.0.1:"+str(client_port),"audit_base_url":"http://127.0.0.1:"+str(audit_port),"host_memory_limit_mib":256,"scope":"Linux controller loopback proxy; allowed NodePort callers are only controller and owned VM IPv4 addresses"}
    report["resources_sha256"]=hashlib.sha256((output/"resources.json").read_bytes()).hexdigest()
    if v3:
        report.update(inventory_sha256=fingerprint(inventory),controller_address=controller,
                      caller_cidrs=[address+"/32" for address in sorted(addresses)],
                      scope="Linux controller loopback proxy with source-preserving NodePorts for declared private callers")
    write_json(output/"access.json",report);return report


def start(cluster,output):
    output=Path(output).resolve();report=json.loads((output/"access.json").read_text())
    require(report["cluster_id"]==cluster.cfg["cluster_id"]and report["image"]==PROXY_IMAGE and report["container"]==cluster.cfg["cluster_id"]+"-validation-access","access plan belongs to another cluster or image")
    require(report["config_sha256"]==hashlib.sha256((output/"haproxy.cfg").read_bytes()).hexdigest(),"reviewed proxy configuration changed")
    require(report["resources_sha256"]==hashlib.sha256((output/"resources.json").read_bytes()).hexdigest(),"reviewed validation resources changed")
    if cluster.cfg.get("version")==3:
        require(report.get("inventory_sha256")==fingerprint(cluster.cfg),"validation access inventory changed; render a reviewed plan")
        controller=private_address(report.get("controller_address"))
        interfaces=json.loads(cluster.run(["ip","-json","-4","address","show"]).stdout)
        require(any(a.get("local")==controller for interface in interfaces for a in interface.get("addr_info",[])),
                "reviewed controller address is absent from this Linux controller")
    resource=json.loads((output/"resources.json").read_text())
    for obj in resource["items"]:
        meta=obj["metadata"]
        labels=meta.get("labels",{})
        require(obj["kind"]in("Service","NetworkPolicy")and meta["namespace"]==cluster.cfg["namespace"]and labels.get("platform.aat/cluster")==cluster.cfg["cluster_id"]and labels.get("platform.aat/validation-access")=="owned","unowned validation access resource")
        current=cluster.kubectl("get",obj["kind"],obj["metadata"]["name"],"-n",cluster.cfg["namespace"],"--ignore-not-found","-o","json")
        if current.stdout.strip():
            meta=json.loads(current.stdout)["metadata"]
            require(meta.get("labels",{}).get("platform.aat/cluster")==cluster.cfg["cluster_id"]and meta.get("labels",{}).get("platform.aat/validation-access")=="owned","refusing to adopt an existing validation resource")
    manifest=json.dumps(resource).encode()
    args=["--network","host","--user",str(os.geteuid())+":"+str(os.getegid()),"--read-only","--cap-drop","ALL","--security-opt","no-new-privileges","--memory","256m","--mount","type=bind,src="+str(output/"haproxy.cfg")+",dst=/usr/local/etc/haproxy/haproxy.cfg,readonly"]
    existing=cluster.docker("inspect",report["container"],check=False)
    if existing.returncode==0:
        value=json.loads(existing.stdout)[0];labels=value["Config"].get("Labels",{})
        require(labels.get("aat.validation.cluster")==cluster.cfg["cluster_id"]and labels.get("aat.validation.config")==report["config_sha256"]and value["Config"]["Image"]==PROXY_IMAGE,"existing validation proxy is unowned or differs")
        require(value["HostConfig"]["NetworkMode"]=="host","validation proxy network differs")
        mounts=[m for m in value["Mounts"]if m["Destination"]=="/usr/local/etc/haproxy/haproxy.cfg"]
        require(len(mounts)==1 and not mounts[0]["RW"]and hashlib.sha256(Path(mounts[0]["Source"]).read_bytes()).hexdigest()==report["config_sha256"],"existing validation proxy mount changed")
    else:
        cluster.docker("run","--rm",*args,PROXY_IMAGE,"haproxy","-c","-f","/usr/local/etc/haproxy/haproxy.cfg")
    cluster.kubectl("apply","--dry-run=server","-f","-",input=manifest)
    cluster.kubectl("apply","-f","-",input=manifest)
    if existing.returncode==0:
        if not value["State"]["Running"]:cluster.docker("start",report["container"])
        return report
    cluster.docker("run","-d","--name",report["container"],"--label","aat.validation.cluster="+cluster.cfg["cluster_id"],"--label","aat.validation.config="+report["config_sha256"],"--log-driver","local","--log-opt","max-size=10m","--log-opt","max-file=2",*args,PROXY_IMAGE,"haproxy","-W","-db","-f","/usr/local/etc/haproxy/haproxy.cfg")
    return report
