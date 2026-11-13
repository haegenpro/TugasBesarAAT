#!/usr/bin/env python3
"""Render storage offline; explicit subcommands prepare disks/secrets or inspect bootstrap.

No command starts VMs, builds images, applies Kubernetes objects or runs tests.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
from urllib.parse import quote

from platform_lib.cluster import Cluster
from platform_lib.inventory import load
from storage_lib import durability_config, manifests, plan, require, write_json

ROOT = Path(__file__).resolve().parent.parent


def read_plan(path, inventory):
    storage = json.loads(Path(path).read_text())
    require(storage == plan(inventory, storage["mode"], storage["image"], storage.get("formation_suffix", "storage")), "storage plan does not match this inventory/renderer")
    return storage


def private_directory(path):
    path = Path(path)
    require(not path.is_symlink(), "output must not be a symlink")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    require(path.stat().st_uid == os.geteuid(), "output must belong to the invoking user")
    path.chmod(0o700)
    return path


def make_secrets(storage, output):
    resolved = Path(output).resolve()
    require(not resolved.is_relative_to(ROOT) or resolved.is_relative_to(ROOT / "evidence"),
            "keep repository-local generated secrets under the ignored evidence/ directory")
    output = private_directory(output)
    require(not list(output.iterdir()), "secret output must be empty; implicit credential rotation is forbidden")
    formation, namespace = storage["formation"], storage["namespace"]
    pki = output / "pki"
    pki.mkdir(mode=0o700)

    def openssl(*args):
        result = subprocess.run(["openssl", *args], cwd=pki, capture_output=True, timeout=60)
        require(result.returncode == 0, "openssl certificate generation failed")

    openssl("req", "-x509", "-newkey", "rsa:3072", "-nodes", "-days", "365", "-sha256",
            "-subj", f"/CN={formation}-etcd-ca", "-keyout", "ca.key", "-out", "ca.crt",
            "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign")
    items = []

    def secret(name, data):
        items.append({"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
            "metadata": {"name": name, "namespace": namespace, "labels": {"platform.aat/formation": formation}},
            "data": {key: base64.b64encode(value if isinstance(value, bytes) else value.encode()).decode() for key, value in data.items()}})

    names = [(formation + "-patroni", None)]
    for letter in "abc":
        name = formation + "-etcd-" + letter
        host = f"{name}-0.{formation}-etcd-members.{namespace}.svc.cluster.local"
        names.append((name, host))
    for index, (name, host) in enumerate(names, 1):
        extensions = ["basicConstraints=critical,CA:FALSE", "keyUsage=critical,digitalSignature,keyEncipherment",
                      "extendedKeyUsage=clientAuth" if host is None else "extendedKeyUsage=serverAuth,clientAuth"]
        if host:
            extensions.append(f"subjectAltName=DNS:{host},DNS:localhost,IP:127.0.0.1")
        (pki / (name + ".ext")).write_text("\n".join(extensions) + "\n")
        openssl("req", "-new", "-newkey", "rsa:3072", "-nodes", "-sha256", "-subj", f"/CN={name}",
                "-keyout", name + ".key", "-out", name + ".csr")
        openssl("x509", "-req", "-in", name + ".csr", "-CA", "ca.crt", "-CAkey", "ca.key",
                "-set_serial", str(index), "-days", "90", "-sha256", "-extfile", name + ".ext", "-out", name + ".crt")
        secret(name + "-tls", {"ca.crt": (pki / "ca.crt").read_bytes(), "tls.crt": (pki / (name + ".crt")).read_bytes(),
                               "tls.key": (pki / (name + ".key")).read_bytes()})
    credentials = {key: secrets.token_hex(32) for key in
                   ("POSTGRES_PASSWORD", "REPLICATION_PASSWORD", "PATRONI_PASSWORD", "APP_PASSWORD")}
    secret(formation + "-credentials", credentials)
    write_json(output / "secrets.json", {"apiVersion": "v1", "kind": "List", "items": items})
    env = []
    for group in storage["groups"]:
        dsn = f"postgresql://aggregator_app:{quote(credentials['APP_PASSWORD'], safe='')}@{formation}-g{group['id']}-primary.{namespace}.svc.cluster.local:5432/hazards?sslmode=disable"
        env.append(f"{group['dsn_env']}={dsn}")
        if group["id"] == 0:
            env.append(("INGESTION_DATABASE_URL=" if storage.get("formation_suffix")=="ingestion" else "DATABASE_URL=") + dsn)
    env.append(("INGESTION_DURABILITY_CONFIG=" if storage.get("formation_suffix")=="ingestion" else "CANONICAL_DURABILITY_CONFIG=")+"/etc/aat/durability.json")
    (output / "application.env").write_text("\n".join(env) + "\n")
    for path in output.rglob("*"):
        path.chmod(0o700 if path.is_dir() else 0o600)


PREPARE_GUEST = r'''
import json, os, pathlib, sys
data = json.load(sys.stdin)
root = pathlib.Path('/var/lib/aat/local-pv') / data['formation']
marker = json.loads(pathlib.Path('/var/lib/aat/platform-owner.json').read_text())
if marker['cluster_id'] != data['cluster_id']:
    raise RuntimeError('guest does not belong to this cluster')
if os.path.realpath('/var/lib/aat/local-pv') != '/var/lib/aat/local-pv':
    raise RuntimeError('unexpected local-pv symlink')
root.mkdir(mode=0o755, exist_ok=True)
if root.is_symlink():
    raise RuntimeError('formation directory must not be a symlink')
owner = root / '.owner.json'
expected = {'cluster_id': data['cluster_id'], 'formation': data['formation']}
if owner.exists():
    if json.loads(owner.read_text()) != expected:
        raise RuntimeError('storage owner mismatch')
elif list(root.iterdir()):
    raise RuntimeError('refusing to adopt unmarked existing storage')
else:
    owner.write_text(json.dumps(expected))
for item in data['paths']:
    target = root / item['name']
    if target.exists():
        if target.is_symlink() or not target.is_dir() or target.stat().st_uid != item['uid']:
            raise RuntimeError('existing data path has unexpected ownership/type')
    else:
        target.mkdir(mode=0o700)
        os.chown(target, item['uid'], item['uid'])
'''


def prepare_disks(cluster, storage):
    for node in cluster.cfg["nodes"]:
        paths = []
        for group in storage["groups"]:
            paths += [{"name": m["workload"], "uid": 999} for m in group["members"] if m["node"] == node["name"]]
        index = storage["nodes"].index(node["name"])
        if index < 3:
            paths.append({"name": storage["formation"] + "-etcd-" + chr(97+index), "uid": 1000})
        payload = {"cluster_id": storage["cluster_id"], "formation": storage["formation"], "paths": paths}
        cluster.ssh(node, ["findmnt", "--mountpoint", "/var/lib/aat"])
        cluster.ssh(node, ["sudo", "-n", "python3", "-c", PREPARE_GUEST], input=json.dumps(payload).encode())


def capture_config(cluster, storage, output):
    cfg = durability_config(storage)
    for group, result in zip(storage["groups"], cfg["groups"]):
        identities = set()
        for member in group["members"]:
            status = json.loads(cluster.kubectl("exec", "-n", storage["namespace"], member["name"], "-c", "postgres", "--",
                "psql", "-U", "postgres", "-d", "hazards", "-Atc", "SELECT public.platform_replication_status()").stdout)
            require(status["formation"] == storage["formation"] and status["group_id"] == group["id"] and status["member"] == member["name"],
                    "bootstrap identity does not match planned member")
            identities.add(status["system_id"])
        require(len(identities) == 1, "all three copies must share the bootstrap PostgreSQL system ID")
        result["system_id"] = identities.pop()
    require(len({g["system_id"] for g in cfg["groups"]}) == len(cfg["groups"]), "physical groups must have distinct system IDs")
    write_json(output, cfg)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["render", "create-secrets", "prepare-disks", "capture-config"])
    parser.add_argument("--inventory", type=Path, default=ROOT / "deployment/platform/inventory.example.json")
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--mode", choices=["postgres", "citus"], default="postgres")
    parser.add_argument("--formation-suffix", choices=["storage","ingestion"], default="storage")
    parser.add_argument("--image", help="built Patroni image repository@sha256:...")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    inventory = load(args.inventory)
    if args.command == "render":
        require(args.image and args.output, "render requires --image and --output")
        storage = plan(inventory, args.mode, args.image,args.formation_suffix)
        output = private_directory(args.output)
        write_json(output / "plan.json", storage)
        write_json(output / "resources.json", manifests(storage))
        write_json(output / "durability.template.json", durability_config(storage))
        print(f"Rendered {len(storage['groups'])} physical groups; no infrastructure changed. System IDs must be captured after bootstrap.")
        print(json.dumps({"requests_mib_before_routers": storage["requests_mib_before_routers"], "router_requests_mib": 256,
                          "vm_memory_mib": storage["vm_memory_mib"], "note": "Excludes Kubernetes, applications and host overhead. Full Citus exceeds the light VM budget."}, indent=2))
    else:
        require(args.plan, "command requires --plan")
        storage = read_plan(args.plan, inventory)
        if args.command == "create-secrets":
            require(args.output, "create-secrets requires --output")
            make_secrets(storage, args.output)
            print("Private credentials and etcd certificates written; nothing applied.")
        else:
            cluster = Cluster(inventory)
            with cluster.locked():
                if args.command == "prepare-disks":
                    prepare_disks(cluster, storage)
                else:
                    require(args.output, "capture-config requires --output")
                    capture_config(cluster, storage, args.output)
            print("Storage operation complete.")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, RuntimeError, subprocess.SubprocessError) as exc:
        print(f"Storage operation failed: {exc}", file=sys.stderr)
        sys.exit(1)
