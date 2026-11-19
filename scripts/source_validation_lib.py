"""Measured source/recipient reconciliation and bounded 60x backlog recovery."""
from datetime import datetime,timezone
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
import time
from urllib.error import URLError,HTTPError
from urllib.parse import urlencode,urlsplit
from urllib.request import HTTPRedirectHandler,ProxyHandler,Request,build_opener

from maintenance_lib import verify_receipt
from release_config_lib import read_token
from storage_lib import SERVICE_DATABASES,SERVICE_FORMATION,require,write_json


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None


def http_json(url,token="",payload=None):
    parsed=urlsplit(url)
    require(parsed.scheme in("http","https")and parsed.hostname and not parsed.username and not parsed.password and not parsed.fragment,"source/measurement URL must not contain credentials")
    headers={"User-Agent":"aat-m2-satellite/1.0","Accept":"application/json"}
    if token:headers["Authorization"]="Bearer "+token
    if payload is not None:headers["Content-Type"]="application/json"
    request=Request(url,data=json.dumps(payload).encode()if payload is not None else None,headers=headers)
    try:
        with build_opener(ProxyHandler({}),NoRedirect()).open(request,timeout=5)as response:
            raw=response.read((1<<20)+1);require(len(raw)<=1<<20,"measurement response exceeds 1 MiB")
            if response.status==204:return {}
            return json.loads(raw)
    except (URLError,HTTPError,TimeoutError)as exc:raise ValueError("source/measurement HTTP request failed; no response body retained")from None


def literal(value):
    text=str(value);require("\x00"not in text,"NUL is invalid in SQL input")
    tag="$aat_input$"
    while tag in text:tag=tag[:-1]+"x$"
    return tag+text+tag


def timestamp(value):
    require(isinstance(value,str),"timestamp must be text")
    date=datetime.fromisoformat(value.replace("Z","+00:00"));require(date.tzinfo is not None,"timestamp timezone required")
    return date.timestamp()


def load_reference(path,owner):
    path=Path(path);require(path.stat().st_size<=32<<20,"reference exceeds 32 MiB")
    value=json.loads(path.read_text());require(value.get("version")==1,"reference version must be 1")
    sat=value.get("satellite",{});rows=sat.get("records",[])
    require(sat.get("namespace")==owner["satellite_namespace"]and isinstance(rows,list)and 2<=len(rows)<=100000,"nonempty bounded satellite reference for this namespace required")
    previous=None
    for row in rows:
        offset=row.get("offset");digest=row.get("payload_sha256")
        require(type(offset)is int and offset>=0 and(previous is None or offset==previous+1),"satellite reference offsets must be ordered and contiguous")
        require(isinstance(digest,str)and re.fullmatch(r"[0-9a-f]{64}",digest),"reference must hash normalized operation bytes, not a received success flag")
        previous=offset
    coast=value.get("coastal",{});ids=coast.get("critical_notification_ids",[])
    require(coast.get("namespace")==owner["r3"]["coastal_namespace"]and isinstance(ids,list)and 1<=len(ids)<=100000 and all(isinstance(i,str)and re.fullmatch(r"warning-v1:[0-9a-f]{64}",i)for i in ids)and len(set(ids))==len(ids),"distinct bounded source critical notification identities required")
    audit=value.get("audit",{})
    require(audit.get("source")in("BMKG","PVMBG","SATELIT","SENSOR_PANTAI")and type(audit.get("count"))is int and audit["count"]>=0 and isinstance(audit.get("checksum"),str)and re.fullmatch(r"[0-9a-f]{64}",audit["checksum"]),"course/reference Audit count and checksum required")
    return value,hashlib.sha256(path.read_bytes()).hexdigest()


def load_inputs(inventory,path):
    path=Path(path).resolve();config=json.loads(path.read_text());base=path.parent
    require(config.get("version")==1,"source validation configuration version must be 1")
    release_path=(base/config["release_config"]).resolve();release=json.loads(release_path.read_text());release_base=release_path.parent
    def read(key):return json.loads((release_base/release[key]).read_text())
    aggregator=read("aggregator");owner=read("role1");images=read("images");receipt=read("initialization_receipt")
    verify_receipt(receipt,inventory,aggregator,owner,images)
    reference,sha=load_reference(base/config["source_reference"],owner)
    require(type(config.get("settle_seconds",60))is int and 1<=config.get("settle_seconds",60)<=300,"settle bound must be 1..300 seconds")
    require(type(config.get("max_clock_uncertainty_ms",500))in(int,float)and 0<config.get("max_clock_uncertainty_ms",500)<=5000,"clock uncertainty limit must be positive and at most 5000 ms")
    return config,base,owner,receipt,reference,sha


def dispatch_status(observer):
    """Read original control primaries; include metadata, never receipt bodies."""
    result={}
    expression="""SELECT coalesce(jsonb_agg(jsonb_build_object(
      'scope',d.scope,'lane',d.lane,'dispatch_id',d.dispatch_id,
      'epoch',d.epoch,'holder',d.holder,'state',d.state,'streaming',d.streaming,
      'started_at',d.started_at,'completed_at',d.completed_at,'status',d.status,
      'receipt_bytes',octet_length(d.body),
      'attempt_started_at',d.attempt_started_at,'retry_after',d.retry_after,
      'stream_open',d.stream_open,'interrupted',d.interrupted,
      'recovery_count',d.recovery_count,'last_recovery_at',d.last_recovery_at,
      'previous_dispatch_id',d.previous_dispatch_id,'last_failure',d.last_failure,
      'recovery_ready',d.state='pending' AND d.retry_after<=clock_timestamp() AND
        o.holder IS NOT NULL AND o.expires_at>clock_timestamp() AND
        (o.epoch<>d.epoch OR o.holder IS DISTINCT FROM d.holder OR d.interrupted OR NOT d.stream_open),
      'blocking',d.state='pending' AND (
        o.epoch<>d.epoch OR o.holder IS DISTINCT FROM d.holder OR
        o.expires_at IS NULL OR o.expires_at<=clock_timestamp() OR d.interrupted OR
        (NOT d.stream_open AND d.retry_after<=clock_timestamp()))
      ) ORDER BY d.scope,d.lane),'[]'::jsonb)
      FROM acquisition_dispatch d JOIN canonical_ownership o USING(scope)"""
    for suffix in("storage","ingestion"):
        result[suffix]=observer.query(suffix,expression)["data"]
    return result


class Observer:
    def __init__(self,cluster,owner,receipt):
        self.cluster=cluster;self.owner=owner;self.receipt=receipt;self.origin_wall=time.time();self.origin_mono=time.monotonic();self.preferred={}

    def query(self,suffix,expression,database="hazards"):
        formation=self.receipt["durability"][suffix];group=formation["groups"][0];ns=self.cluster.cfg["namespace"]
        members=sorted(group["members"],key=lambda m:m["name"]!=self.preferred.get(suffix))
        sql="BEGIN READ ONLY; SET LOCAL statement_timeout='3s'; SELECT jsonb_build_object('physical',public.platform_replication_status(),'clock_epoch',extract(epoch from clock_timestamp()),'data',("+expression+")); COMMIT;"
        for member in members:
            before=time.monotonic()
            try:
                result=self.cluster.kubectl("exec","-i","-n",ns,member["name"],"-c","postgres","--","psql","-qAt","-U","postgres","-d",database,"-v","ON_ERROR_STOP=1","-f","-",input=sql.encode(),timeout=5,check=False)
            except (OSError,RuntimeError,subprocess.SubprocessError):continue
            after=time.monotonic()
            if result.returncode:continue
            require(len(result.stdout)<=2<<20,"database measurement page exceeds 2 MiB")
            try:value=json.loads(result.stdout)
            except ValueError:continue
            physical=value["physical"]
            require(physical["formation"]==formation["formation"]and physical["system_id"]==group["system_id"]and physical["member"]==member["name"]and physical["group_id"]==0,"measurement database is not the original captured physical member")
            if physical["primary"]is not True:continue
            self.preferred[suffix]=member["name"]
            clock=float(value["clock_epoch"]);require(math.isfinite(clock),"invalid database clock sample")
            low=clock-(self.origin_wall+after-self.origin_mono);high=clock-(self.origin_wall+before-self.origin_mono)
            return {"data":value["data"],"clock_offset_interval_seconds":[low,high],"member":member["name"],"observed_monotonic":after}
        raise ValueError("no captured primary is reachable for "+suffix)

    def progress(self):
        namespace=literal(self.owner["satellite_namespace"])
        return self.query("storage","SELECT to_jsonb(s) FROM input_sessions s WHERE source='SATELIT' AND namespace="+namespace)

    def node_clocks(self):
        # Pods and PostgreSQL use their VM's kernel clock. Capture one original
        # canonical member on each VM, including standbys, without SSH guesses.
        formation=self.receipt["durability"]["storage"];chosen={};intervals=[]
        for group in formation["groups"]:
            for member in group["members"]:
                match=re.search(r"-([a-d])-0$",member["name"])
                if match and match[1]not in chosen:chosen[match[1]]=(group,member)
        require(len(chosen)==4,"four original VM clock witnesses required for satellite entry timing")
        for letter,(group,member)in sorted(chosen.items()):
            before=time.monotonic()
            result=self.cluster.kubectl("exec","-n",self.cluster.cfg["namespace"],member["name"],"-c","postgres","--","psql","-qAt","-U","postgres","-d","hazards","-v","ON_ERROR_STOP=1","-c","SELECT jsonb_build_object('physical',public.platform_replication_status(),'clock_epoch',extract(epoch from clock_timestamp()))",timeout=5)
            after=time.monotonic();value=json.loads(result.stdout);physical=value["physical"]
            require(physical["formation"]==formation["formation"]and physical["system_id"]==group["system_id"]and physical["member"]==member["name"]and physical["group_id"]==group["id"],"clock witness is not an original captured member")
            clock=float(value["clock_epoch"])
            intervals.append([clock-(self.origin_wall+after-self.origin_mono),clock-(self.origin_wall+before-self.origin_mono)])
        return intervals

    def collect(self,reference):
        out={"satellite":[],"coastal_expected":[],"coastal_receipts":[],"clock_samples":{"ingestion":[],"notifier-db":[]}}
        out["progress"]=self.progress()["data"]
        out["dispatch"]=dispatch_status(self)
        rows=reference["satellite"]["records"];namespace=literal(reference["satellite"]["namespace"])
        for start in range(rows[0]["offset"],rows[-1]["offset"]+1,500):
            end=min(start+499,rows[-1]["offset"])
            value=self.query("storage","SELECT coalesce(jsonb_agg(jsonb_build_object('offset',position,'payload_sha256',encode(sha256(original_payload),'hex'),'state',state,'first_received_at',envelope->>'received_at') ORDER BY position),'[]'::jsonb) FROM input_history WHERE source='SATELIT' AND namespace="+namespace+" AND position BETWEEN "+str(start)+" AND "+str(end))
            for record in value["data"]:record["applied_observed_epoch"]=self.origin_wall+value["observed_monotonic"]-self.origin_mono
            out["satellite"]+=value["data"]
        ids=reference["coastal"]["critical_notification_ids"];namespace=literal(reference["coastal"]["namespace"])
        for start in range(0,len(ids),500):
            selected=literal(json.dumps(ids[start:start+500]));selection="SELECT jsonb_array_elements_text("+selected+"::jsonb)"
            value=self.query("ingestion","SELECT coalesce(jsonb_agg(jsonb_build_object('notification_id',notification_id,'observation_id',observation_id,'first_received_at',first_received_at,'rejected',rejected_at IS NOT NULL)),'[]'::jsonb) FROM coastal_warning_expected WHERE namespace="+namespace+" AND notification_id IN ("+selection+")")
            out["coastal_expected"]+=value["data"];out["clock_samples"]["ingestion"].append(value["clock_offset_interval_seconds"])
            value=self.query(SERVICE_FORMATION,"SELECT jsonb_build_object('receipts',(SELECT coalesce(jsonb_agg(jsonb_build_object('notification_id',notification_id,'observation_id',observation_id,'first_received_at',first_received_at,'committed_at',committed_at)),'[]'::jsonb) FROM coastal_warning_receipts WHERE notification_id IN ("+selection+")),'conflicts',(SELECT count(*) FROM coastal_warning_conflicts WHERE notification_id IN ("+selection+")))",database=SERVICE_DATABASES["notifier-db"][0])
            out["coastal_receipts"]+=value["data"]["receipts"];out["clock_samples"]["notifier-db"].append(value["clock_offset_interval_seconds"])
            out["coastal_conflicts"]=out.get("coastal_conflicts",0)+value["data"]["conflicts"]
        return out


def analyze(reference,observed,max_clock_uncertainty_ms=500):
    failures=[]
    def check(condition,reason):
        if not condition:failures.append(reason)
    dispatch=observed.get("dispatch")
    check(isinstance(dispatch,dict)and set(dispatch)=={"storage","ingestion"}and all(isinstance(v,list)and all(type(row.get("blocking"))is bool and not row["blocking"]for row in v)for v in dispatch.values()),"source dispatch evidence is missing or automatic acquisition recovery remains unfinished")
    expected={r["offset"]:r["payload_sha256"]for r in reference["satellite"]["records"]};rows=observed.get("satellite",[]);actual={r["offset"]:r for r in rows}
    check(len(actual)==len(rows)and set(actual)==set(expected),"satellite reference identities are missing, duplicated or unexpected")
    check(all(actual.get(offset,{}).get("payload_sha256")==digest and actual.get(offset,{}).get("state")=="applied"for offset,digest in expected.items()),"satellite payload changed or canonical work remains unapplied")
    progress=observed.get("progress")or{}
    check(all(type(progress.get(k))is int for k in("received_through","prepared_through","applied_through"))and progress.get("received_through",-1)>=progress.get("prepared_through",-1)>=progress.get("applied_through",-1)>=max(expected),"satellite frontiers have not applied the reference range")
    ids=set(reference["coastal"]["critical_notification_ids"])
    acquired=observed.get("coastal_expected",[]);receipts=observed.get("coastal_receipts",[])
    required={r["notification_id"]:r for r in acquired};delivered={r["notification_id"]:r for r in receipts}
    missing_source=sorted(ids-required.keys());missing_receipts=sorted(ids-delivered.keys())
    check(not missing_source and len(required)==len(acquired)and set(required)==ids,"source critical reference was not completely acquired")
    check(not missing_receipts and len(delivered)==len(receipts)and set(delivered)==ids,"critical operations are missing durable recipient receipts")
    check(observed.get("coastal_conflicts") == 0,"recipient content conflicts exist or conflict evidence is missing")
    samples=observed.get("clock_samples",{});source=samples.get("ingestion",[]);recipient=samples.get("notifier-db",[])
    intervals=source+recipient
    valid=bool(source and recipient)and all(isinstance(v,list)and len(v)==2 and all(type(x)in(int,float)and math.isfinite(x)for x in v)and v[0]<=v[1]for v in intervals)
    uncertainty=None
    if valid:
        uncertainty=max(abs(a-b)for s in source for r in recipient for a in s for b in r)*1000
        check(uncertainty<=max_clock_uncertainty_ms,"observed database clock uncertainty exceeds configured limit")
    else:check(False,"source/recipient database clock measurements are missing or invalid")
    latencies=[]
    for identity in sorted(ids&required.keys()&delivered.keys()):
        source_row,receipt=required[identity],delivered[identity]
        check(not source_row.get("rejected"),"required critical operation was rejected")
        check(source_row.get("observation_id")==receipt.get("observation_id"),"recipient observation identity changed")
        first=timestamp(source_row["first_received_at"])
        check(abs(first-timestamp(receipt["first_received_at"]))<.001,"recipient first-acquisition timestamp changed")
        latency=(timestamp(receipt["committed_at"])-first)*1000
        check(uncertainty is not None and latency>=-(uncertainty or 0),"negative latency exceeds measured clock uncertainty")
        if uncertainty is not None:latencies.append(max(0,latency+uncertainty))
    latencies.sort();p95=latencies[math.ceil(.95*len(latencies))-1]if latencies else None
    timely=sum(v<=5000 for v in latencies)
    check(p95 is not None and p95<=5000 and timely/len(ids)>=.95,"conservative critical delivery p95/timely fraction exceeds five-second target")
    expected_audit=reference["audit"];audit=observed.get("audit",{})
    check(audit.get("partial")is False and not audit.get("unavailable_units")and all(audit.get(k)==expected_audit[k]for k in("source","count","checksum")),"Audit result is partial or differs from the independent reference")
    burst=observed.get("burst")
    entry=None
    if burst is not None:
        check(burst.get("speed")==60 and burst.get("new_source_offsets",0)>0,"60x source activity was not observed during disconnect")
        seconds=burst.get("catchup_seconds")
        check(type(seconds)in(int,float)and math.isfinite(seconds)and 0<=seconds<=60 and burst.get("applied_through",-1)>=burst.get("target_offset",0),"satellite backlog did not catch up within 60 seconds")
        intervals=observed.get("satellite_node_clock_intervals",[]);start=burst.get("initial_applied_through");target=burst.get("target_offset")
        valid_clocks=len(intervals)==4 and all(isinstance(v,list)and len(v)==2 and all(type(x)in(int,float)and math.isfinite(x)for x in v)and v[0]<=v[1]for v in intervals)
        timing=[];count=target-start if type(start)is int and type(target)is int else 0
        complete=valid_clocks and 0<count<=100000 and min(expected)<=start+1 and max(expected)>=target
        span=(max(v[1]for v in intervals)-min(v[0]for v in intervals))*1000 if valid_clocks else None
        if complete:
            for offset in range(start+1,target+1):
                row=actual.get(offset,{})
                if row.get("state")!="applied"or not row.get("first_received_at")or type(row.get("applied_observed_epoch"))not in(int,float):complete=False;break
                timing.append((row["applied_observed_epoch"]-timestamp(row["first_received_at"])+max(v[1]for v in intervals))*1000)
            complete=complete and all(math.isfinite(v)and v>=0 for v in timing)
        check(complete and span<=max_clock_uncertainty_ms,"satellite entry timing lacks complete burst reference coverage, timestamps or bounded VM clock witnesses")
        timing.sort();entry_p95=timing[math.ceil(.95*len(timing))-1]if timing else None
        entry={"complete":complete,"operations":len(timing),"p95_upper_bound_ms":entry_p95,"clock_span_ms":span,"scope":"first BNPB acquisition to observed committed applied state for offsets newly applied across this disconnect; historical timings are not fabricated"}
        limit=reference["satellite"].get("entry_p95_max_ms")
        if limit is not None:check(type(limit)in(int,float)and math.isfinite(limit)and limit>0 and entry_p95 is not None and entry_p95<=limit,"satellite entry p95 exceeds supplied evaluator limit")
    return {"version":1,"passed":not failures,"failures":failures,"scope":"declared independent satellite/coastal/Audit reference dataset; bounded 60x disconnect if requested","satellite_operations":len(expected),"satellite_entry_timing":entry,"critical_required":len(ids),"missing_source":missing_source,"missing_receipts":missing_receipts,"clock_uncertainty_ms":uncertainty,"critical_p95_upper_bound_ms":p95,"critical_timely_fraction":timely/len(ids),"burst":burst,"limitations":["Does not infer completeness outside the supplied reference dataset.","Clock measurements bound current query-time uncertainty; clock-shift scenarios require separate timing evidence.","Load, all storage groups, placement and physical fault acceptance use their separate runners."]}


def patch_replicas(cluster,deployment,uid,before,after):
    patch=[{"op":"test","path":"/metadata/uid","value":uid},{"op":"test","path":"/spec/replicas","value":before},{"op":"replace","path":"/spec/replicas","value":after}]
    cluster.kubectl("patch","deployment",deployment,"-n",cluster.cfg["namespace"],"--type=json","-p",json.dumps(patch))


def burst(cluster,observer,feed_url,token,disconnect_seconds,output):
    require(type(disconnect_seconds)is int and 2<=disconnect_seconds<=30,"disconnect_seconds must be 2..30")
    name=cluster.cfg["cluster_id"]+"-ingestion";ns=cluster.cfg["namespace"]
    original=json.loads(cluster.kubectl("get","deployment",name,"-n",ns,"-o","json").stdout);uid=original["metadata"]["uid"];replicas=original["spec"]["replicas"]
    labels=original["spec"]["template"]["metadata"]["labels"]
    require(labels.get("platform.aat/cluster")==cluster.cfg["cluster_id"]and labels.get("app.kubernetes.io/name")=="ingestion"and replicas>=1,"owned running ingestion deployment required")
    first=http_json(feed_url.rstrip("/")+"/v1/session",token)
    require(first.get("speed")==60 and isinstance(first.get("group"),str)and type(first.get("latest_offset"))is int,"live 60x satellite session metadata required")
    initial=(observer.progress()["data"]or{}).get("applied_through",observer.owner.get("satellite_initial_offset",0)-1)if observer is not None else -1
    state={"deployment":name,"uid":uid,"original_replicas":replicas,"hold_intent":True,"restored":False,"source_session":{key:first.get(key)for key in("group","profile","speed","latest_offset")}};write_json(output/"burst-state.json",state)
    try:
        patch_replicas(cluster,name,uid,replicas,0)
        # Wait for all acquisition pods to terminate before timing a disconnected
        # interval. A Deployment scale response alone does not prove no reader.
        drained=time.monotonic()+60
        while True:
            pods=json.loads(cluster.kubectl("get","pods","-n",ns,"-l","app.kubernetes.io/name=ingestion,platform.aat/cluster="+cluster.cfg["cluster_id"],"-o","json").stdout)["items"]
            if not pods:break
            require(time.monotonic()<drained,"ingestion pods did not terminate before disconnect test")
            time.sleep(.5)
        deadline=time.monotonic()+disconnect_seconds
        while time.monotonic()<deadline:time.sleep(min(.5,max(0,deadline-time.monotonic())))
        latest=http_json(feed_url.rstrip("/")+"/v1/session",token)
        require((latest.get("group"),latest.get("profile"),latest.get("speed"))==(first.get("group"),first.get("profile"),60),"satellite session changed during disconnect")
        target=latest["latest_offset"];require(type(target)is int and target>first["latest_offset"],"source produced no new offsets during disconnect")
        started=time.monotonic();patch_replicas(cluster,name,uid,0,replicas);state["restored"]=True;write_json(output/"burst-state.json",state)
        samples=[];progress={}
        while time.monotonic()-started<=60:
            try:
                progress=observer.progress()["data"]or{};samples.append({"seconds":time.monotonic()-started,"progress":progress})
            except (ValueError,OSError,RuntimeError,subprocess.SubprocessError):samples.append({"seconds":time.monotonic()-started,"unavailable":True})
            write_json(output/"burst-samples.json",samples)
            if progress.get("applied_through",-1)>=target:break
            time.sleep(.5)
        value={"group":first["group"],"profile":first.get("profile"),"speed":60,"initial_applied_through":initial,"target_offset":target,"new_source_offsets":target-first["latest_offset"],"catchup_seconds":time.monotonic()-started,"applied_through":progress.get("applied_through",-1)}
        write_json(output/"burst-result.json",value);return value
    finally:
        # Reconcile an API reply lost after applying the hold/restore. Refuse a
        # recreated or concurrently scaled controller rather than guessing.
        try:
            current=json.loads(cluster.kubectl("get","deployment",name,"-n",ns,"-o","json").stdout)
            if current["metadata"]["uid"]==uid and current["spec"]["replicas"]==replicas:state["restored"]=True
            elif current["metadata"]["uid"]==uid and current["spec"]["replicas"]==0:
                patch_replicas(cluster,name,uid,0,replicas);state["restored"]=True
            else:state["cleanup_required"]="inspect original ingestion controller; UID or replica count changed"
        except (ValueError,KeyError,OSError,RuntimeError,subprocess.SubprocessError):state["cleanup_required"]="inspect original ingestion controller; cleanup API request failed"
        write_json(output/"burst-state.json",state)
        require(state.get("restored")and not state.get("cleanup_required"),"ingestion cleanup requires explicit inspection")


def run(cluster,config_path,output,scenario="reconcile"):
    config,base,owner,receipt,reference,sha=load_inputs(cluster.cfg,config_path)
    output=Path(output);require(not output.exists(),"source validation output exists; use a new evidence directory")
    output.mkdir(parents=True,mode=0o700);observer=Observer(cluster,owner,receipt)
    summary={"version":1,"passed":False,"reference_sha256":sha,"scenario":scenario}
    write_json(output/"result.json",summary)
    try:
        measured_burst=None
        if scenario=="satellite-burst":
            token=read_token(base/config["satellite_token_file"])
            measured_burst=burst(cluster,observer,owner["r3"]["satellite_feed_url"],token,config.get("disconnect_seconds",10),output)
        deadline=time.monotonic()+config.get("settle_seconds",60)
        while True:
            observed=observer.collect(reference)
            audit_token=read_token(base/config["audit_token_file"])
            observed["audit"]=http_json(config["audit_base_url"].rstrip("/")+"/internal/audit/checksum?"+urlencode({"source":reference["audit"]["source"]}),audit_token)
            if measured_burst is not None:
                observed["burst"]=measured_burst;observed["satellite_node_clock_intervals"]=observer.node_clocks()
            write_json(output/"observation.json",observed)
            summary={**analyze(reference,observed,config.get("max_clock_uncertainty_ms",500)),"reference_sha256":sha,"scenario":scenario}
            write_json(output/"result.json",summary)
            if summary["passed"]or time.monotonic()>=deadline:return summary
            time.sleep(.5)
    except BaseException:
        summary["passed"]=False;summary["execution_incomplete"]=True;write_json(output/"result.json",summary);raise
