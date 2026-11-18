"""Measure load-generator CPU, available memory and network rate on Linux."""
from pathlib import Path
import threading
import time
from storage_lib import write_json


def counters(root=Path("/proc")):
    cpu=[int(x)for x in(root/"stat").read_text().splitlines()[0].split()[1:]]
    memory={line.split(":")[0]:int(line.split()[1])for line in(root/"meminfo").read_text().splitlines()}
    network=0
    for line in(root/"net/dev").read_text().splitlines()[2:]:
        name,data=line.split(":",1)
        if name.strip()=="lo":continue
        fields=data.split();network+=int(fields[0])+int(fields[8])
    # guest/guest_nice are already included in user/nice; exclude double counts.
    return {"monotonic":time.monotonic(),"cpu_total":sum(cpu[:8]),"cpu_idle":sum(cpu[3:5]),"available_memory_mib":memory["MemAvailable"]/1024,"network_bytes":network}


def health(samples,network_mbps=None):
    if len(samples)<2:return {"passed":False,"reason":"at least two generator samples required"}
    cpu=[];rates=[];valid=True
    for a,b in zip(samples,samples[1:]):
        ticks=b["cpu_total"]-a["cpu_total"];elapsed=b["monotonic"]-a["monotonic"]
        idle=b["cpu_idle"]-a["cpu_idle"];network_delta=b["network_bytes"]-a["network_bytes"]
        if ticks<=0 or elapsed<=0 or not 0<=idle<=ticks or network_delta<0:valid=False
        if ticks>0:cpu.append(1-idle/ticks)
        if elapsed>0:rates.append(max(0,network_delta)*8/elapsed/1e6)
    peak=max(cpu,default=1);network=max(rates,default=0);memory=min(s["available_memory_mib"]for s in samples)
    known=type(network_mbps)in(int,float)and network_mbps>0
    passed=valid and bool(cpu and rates)and peak<.95 and memory>=256 and known and network<.8*network_mbps
    return {"passed":passed,"peak_host_cpu_fraction":peak,"minimum_available_memory_mib":memory,"peak_network_mbps":network,"declared_network_capacity_mbps":network_mbps,"network_capacity_known":known,"note":"Measurements concern this Linux load-generator host. Background/virtual-interface traffic is included conservatively; this does not prove remote-link capacity."}


class Sampler:
    """Continue sampling while slow Kubernetes snapshots/actions are in flight."""
    def __init__(self,path,interval=1):
        self.path=Path(path);self.interval=interval;self.samples=[];self.errors=[];self.done=threading.Event();self.thread=None

    def start(self):
        def collect():
            while not self.done.is_set():
                try:
                    self.samples.append(counters());write_json(self.path,self.samples)
                except (ValueError,KeyError,OSError)as exc:self.errors.append(type(exc).__name__)
                self.done.wait(self.interval)
        self.thread=threading.Thread(target=collect,daemon=True);self.thread.start()

    def stop(self):
        self.done.set()
        if self.thread:
            self.thread.join(timeout=5)
            if self.thread.is_alive():self.errors.append("sampler did not stop")
