"""Build/publish tracked source contexts and capture registry-confirmed digests."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

from monitoring_lib import IMAGES
from service_lib import DIGEST
from storage_lib import require,write_json

ROOT=Path(__file__).resolve().parent.parent
BUILDS={name:(name,"Dockerfile",None)for name in("broker","bmkg-mock","pvmbg-mock","auth-service","client-api","consumer-dashboard","consumer-notifier","platform-metrics")}
BUILDS.update({"schema-registry":("registry","Dockerfile",None),"storage":(".","deployment/storage/Dockerfile",None),"role1":(".","deployment/role1/Dockerfile",None)})
BUILDS.update({name:("monitoring","Dockerfile",name)for name in("prometheus","grafana","loki","alloy","nats-exporter")})
REQUIRED=set(BUILDS)|set(IMAGES)


def source_files(root=ROOT):
    result=subprocess.run(["git","ls-files","-z"],cwd=root,capture_output=True,check=True)
    paths=[]
    for name in result.stdout.decode().split("\0"):
        if not name:continue
        path=Path(name)
        if any(part in(".git","evidence",".aws",".codex")or part.startswith(".env")for part in path.parts):continue
        source=root/path
        require(not source.is_symlink(),"tracked symlink cannot enter a release build context")
        require(source.is_file(),"tracked source is missing: "+name)
        paths.append(path)
    return sorted(paths)


def plan(config,root=ROOT):
    require(config.get("version")==1,"image release version must be 1")
    registry=config.get("registry","")
    require(isinstance(registry,str)and re.fullmatch(r"[a-z0-9][a-z0-9.:/-]*[a-z0-9]",registry)and "://"not in registry and ".."not in registry,"supply a registry/repository prefix without credentials or scheme")
    require(config.get("platform","linux/amd64")in("linux/amd64","linux/arm64"),"explicit supported Linux image platform required")
    external=config.get("external_images",{})
    require(isinstance(external,dict)and set(external)<=REQUIRED,"unknown supplied image key")
    require(all(isinstance(v,str)and DIGEST.fullmatch(v)for v in external.values()),"supply external images by immutable registry digest")
    missing=REQUIRED-set(BUILDS)-external.keys()
    require(not missing,"supply external images: "+", ".join(sorted(missing)))
    paths=source_files(root);digest=hashlib.sha256()
    for path in paths:
        digest.update(path.as_posix().encode()+b"\0"+(root/path).read_bytes()+b"\0")
    revision=subprocess.check_output(["git","rev-parse","HEAD"],cwd=root,text=True).strip()
    source=digest.hexdigest();builds=[]
    for key,(context,dockerfile,target)in sorted(BUILDS.items()):
        if key in external:continue
        require(root.joinpath(context,dockerfile).is_file(),"missing Dockerfile for "+key)
        builds.append({"key":key,"context":context,"dockerfile":dockerfile,"target":target,"tag":registry.rstrip("/")+"/"+key+":"+revision[:12]+"-"+source[:12]})
    return {"version":1,"revision":revision,"source_sha256":source,"platform":config.get("platform","linux/amd64"),"builds":builds,"external_images":external},paths


def publish(config,output,*,root=ROOT,run=subprocess.run):
    report,paths=plan(config,root);output=Path(output)
    require(not output.exists(),"image output exists; preserve original registry evidence")
    output.mkdir(parents=True,mode=0o700);stage=output/"context";stage.mkdir(mode=0o700)
    images=dict(report["external_images"]);report.update(completed=[],passed=False)
    write_json(output/"result.json",report)
    try:
        # Docker sees only tracked, filtered files, never local private inputs.
        for path in paths:
            destination=stage/path;destination.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(root/path,destination);destination.chmod((root/path).stat().st_mode&0o777)
        for build in report["builds"]:
            key=build["key"];context=stage/build["context"]
            args=["docker","build","--platform",report["platform"],"--tag",build["tag"],"--file",str(context/build["dockerfile"])]
            if build["target"]:args += ["--target",build["target"]]
            args.append(str(context))
            log=output/(key+".log")
            with log.open("wb")as stream:
                result=run(args,stdout=stream,stderr=subprocess.STDOUT,timeout=3600)
            log.chmod(0o600);require(result.returncode==0,"image build failed for "+key+"; inspect private build log")
            result=run(["docker","push",build["tag"]],capture_output=True,timeout=1200)
            with log.open("ab")as stream:stream.write(result.stdout+result.stderr)
            require(result.returncode==0,"image push failed for "+key+"; inspect private build log")
            digests=set(re.findall(rb"digest:\s*(sha256:[0-9a-f]{64})",result.stdout+result.stderr))
            require(len(digests)==1,"registry push did not confirm one image digest for "+key)
            reference=build["tag"].rsplit(":",1)[0]+"@"+digests.pop().decode()
            require(DIGEST.fullmatch(reference),"invalid registry digest reference")
            checked=run(["docker","image","inspect",build["tag"],"--format","{{json .RepoDigests}}"],capture_output=True,timeout=30)
            require(checked.returncode==0 and reference in json.loads(checked.stdout),"local repository digest disagrees with registry push")
            images[key]=reference;report["completed"].append({"key":key,"image":reference})
            write_json(output/"images.json",images);write_json(output/"result.json",report)
            print("Published immutable image: "+key,flush=True)
        require(set(images)==REQUIRED,"image map is incomplete")
        report["passed"]=True;write_json(output/"result.json",report)
        return report
    finally:
        # Contexts contain only tracked source; preserve logs/receipts, not a
        # second checkout. Failed pushes and already published images remain.
        shutil.rmtree(stage)

