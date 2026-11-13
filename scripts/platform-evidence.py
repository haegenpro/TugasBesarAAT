#!/usr/bin/env python3
"""Analyze captured M2 evidence later; never generate success from missing data."""
import argparse
import json
import math
from pathlib import Path
import sys
from storage_lib import require


def load(path):return json.loads(Path(path).read_text())

def counter(report,name):
    value=report["metrics"][name]["values"]["count"]
    require(type(value)in(int,float) and math.isfinite(value) and value>=0,"missing/invalid count "+name)
    return value

def acceptance(directory,reconciliation):
    run=load(directory/"result.json");work=load(directory/"load.json");owner=load(reconciliation)
    require(run["run_id"]==work["configuration"]["run_id"]==owner["run_id"],"evidence run IDs differ")
    require(run["dataset_sha256"]==owner["dataset_sha256"],"owner reconciliation describes another dataset")
    require(run.get("load_exit_code")==0 and run["status"]=="needs-owner-reconciliation","load was not completed")
    require(work["configuration"]["vus"]>=10000 and work["configuration"]["duration_seconds"]>=120,"insufficient sustained workload")
    expected=owner["expected_critical_ids"]
    require(expected and all(isinstance(i,str)and i for i in expected) and len(set(expected))==len(expected),"nonempty distinct required critical operation IDs needed")
    receipts=owner["critical_receipts"];seen={};duplicates=0
    for receipt in receipts:
        op=receipt["operation_id"];latency=receipt["latency_ms"]
        require(op in set(expected),"receipt outside declared critical denominator")
        require(type(latency)in(int,float) and math.isfinite(latency) and latency>=0,"invalid original-event receipt latency")
        if op in seen:duplicates+=1
        seen[op]=min(seen.get(op,latency),latency)
    missing=sorted(set(expected)-set(seen));timely=sum(v<=5000 for v in seen.values())
    require(not missing,"critical operations are missing")
    require(timely/len(expected)>=.95,"critical timely delivery below 95 percent")
    p95=sorted(seen.values())[math.ceil(.95*len(seen))-1]
    require(p95<=5000,"critical receipt p95 exceeds five seconds")
    require(owner["polling_unique"] is True and owner["logical_reconciliation_passed"] is True,"polling/canonical reconciliation incomplete")
    for key in ("acquisition_takeover_max_seconds","freshness_catchup_seconds"):
        value=owner[key];require(type(value)in(int,float)and math.isfinite(value)and 0<=value<=60,key+" exceeds bound or is missing")
    generator=owner["load_generator"]
    require(generator["saturated"] is False and generator.get("samples_artifact"),"load-generator headroom evidence required")
    artifact=(directory/generator["samples_artifact"]).resolve()
    require(artifact.is_relative_to(directory.resolve()) and artifact.is_file(),"load-generator sample artifact missing or outside run directory")
    for name in ("m2_successful_reads{role:field}","m2_successful_reads{role:media}"):require(counter(work,name)>0,"no successful role traffic")
    error=work["metrics"]["m2_unexpected_failure_rate"]["values"]["rate"]
    require(type(error)in(int,float) and math.isfinite(error) and 0<=error<.01,"unexpected error rate >=1 percent")
    return {"scope":"declared load and owner-reconciled critical dataset only","run_id":run["run_id"],"passed":True,"critical_required":len(expected),"critical_missing":missing,"critical_duplicate_receipts":duplicates,"critical_p95_ms":p95,"timely_fraction":timely/len(expected),"timely_budget_fraction_consumed":(len(expected)-timely)/(.05*len(expected)),"note":"Does not establish every disk-loss, clock-shift, snapshot or combined game-day criterion."}

def compare(one,three):
    a,b=load(one/"result.json"),load(three/"result.json")
    x,y=load(one/"load.json"),load(three/"load.json")
    for run,n in ((a,1),(b,3)):
        require(run["scenario"]=="baseline" and run["original_deployment"]["replicas"]==n and run["load_exit_code"]==0,"compare completed 1 and 3 replica baseline runs")
    for key in ("dataset_sha256","script_sha256","git_revision"):require(a[key]==b[key],"comparison differs in "+key)
    require(not a["worktree_dirty"] and not b["worktree_dirty"],"comparison requires clean recorded revision")
    for key in ("vus","duration_seconds","field_vus","think_time_seconds","workload"):require(x["configuration"][key]==y["configuration"][key],"workload configuration differs")
    for report in (x,y):require(report["metrics"]["m2_unexpected_failure_rate"]["values"]["rate"]<.01,"comparison has excessive errors")
    low=counter(x,"m2_successful_reads")/x["configuration"]["duration_seconds"]
    high=counter(y,"m2_successful_reads")/y["configuration"]["duration_seconds"]
    require(low>0,"single-replica throughput is zero")
    # Resource/headroom and unchanged upstream/data evidence must accompany the
    # numeric result; the script cannot infer those facts from HTTP counts.
    return {"successful_rps_one":low,"successful_rps_three":high,"ratio":high/low,"numeric_criterion_met":high>=2*low,"passed":False,"status":"needs-environment-and-generator-review"}

def main():
    p=argparse.ArgumentParser(description=__doc__);subs=p.add_subparsers(dest="action",required=True)
    a=subs.add_parser("reconcile");a.add_argument("run",type=Path);a.add_argument("owner_reconciliation",type=Path)
    a=subs.add_parser("compare");a.add_argument("one",type=Path);a.add_argument("three",type=Path)
    args=p.parse_args()
    result=acceptance(args.run,args.owner_reconciliation)if args.action=="reconcile"else compare(args.one,args.three)
    print(json.dumps(result,indent=2))

if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,OSError,TypeError)as exc:print(f"Evidence incomplete or failed: {exc}",file=sys.stderr);sys.exit(1)
