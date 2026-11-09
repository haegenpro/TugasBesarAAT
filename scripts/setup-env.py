#!/usr/bin/env python3
"""Create a coherent demo configuration; never replace an existing .env file."""
import argparse
from pathlib import Path
import re
import secrets
import sys
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent
STREAMS = "NATS_STREAM NATS_SUBJECT NATS_DLQ_STREAM NATS_LIFECYCLE_STREAM NATS_LIFECYCLE_SUBJECT"
FILES = {
    "bmkg-mock/.env": "BMKG_API_KEY BMKG_DELAY_MS BMKG_EVENT_INTERVAL_MS SOURCE_ARCHIVE_PATH",
    "pvmbg-mock/.env": "PVMBG_TOKEN PVMBG_ADMIN_TOKEN PVMBG_MIN_DELAY_MS PVMBG_MAX_DELAY_MS PVMBG_REPORT_INTERVAL_MS SOURCE_ARCHIVE_PATH",
    "aggregator/.env": f"AGGREGATOR_ADMIN_TOKEN BMKG_SEISMIC_READER_SCHEMA BMKG_TSUNAMI_READER_SCHEMA PVMBG_READER_SCHEMA BMKG_API_KEY PVMBG_TOKEN POLL_INTERVAL DATABASE_URL BMKG_FRESHNESS_SEC PVMBG_FRESHNESS_SEC SOURCE_RETRY_MAX_SEC {STREAMS}",
    "store/.env": "POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD APP_USER APP_PASSWORD",
    "broker/.env": f"{STREAMS} NATS_STREAM_MAX_AGE NATS_DLQ_SUBJECT_PREFIX NATS_DLQ_MAX_AGE NATS_LIFECYCLE_MAX_AGE NATS_REPLICAS NATS_MAX_BYTES",
    "auth-service/.env": "AUTH_INTERNAL_KEY ACCESS_TOKEN_TTL_SEC REFRESH_TOKEN_TTL_SEC MEDIA_CLIENT_ID MEDIA_CLIENT_SECRET FIELD_CLIENT_ID FIELD_CLIENT_SECRET INTERNAL_CLIENT_ID INTERNAL_CLIENT_SECRET",
    "client-api/.env": "AUTH_INTERNAL_KEY CLIENT_API_MAX_INFLIGHT CLIENT_API_MAX_INFLIGHT_PER_CLIENT",
    "client-tui/.env": "MEDIA_CLIENT_ID FIELD_CLIENT_ID INTERNAL_CLIENT_ID",
    "consumer-dashboard/.env": f"CONSUMER_ROLE CONSUMER_ADMIN_SOCKET NATS_DURABLE NATS_DLQ_SUBJECT {STREAMS}",
    "consumer-notifier/.env": f"CONSUMER_ROLE CONSUMER_ADMIN_SOCKET NATS_DURABLE NATS_DLQ_SUBJECT {STREAMS}",
    "monitoring/grafana/.env": "GF_SECURITY_ADMIN_USER GF_SECURITY_ADMIN_PASSWORD DLQ_STREAM_NAME",
    "monitoring/prometheus/.env": "METRICS_RETENTION_DAYS",
    "monitoring/loki/.env": "LOKI_GRPC_PORT LOG_RETENTION_DAYS",
}


def configuration(template):
    values, registry, ports = {}, {}, {}
    section = ""
    for line in template.splitlines():
        header = re.fullmatch(r"# --- (.+) ---", line)
        if header:
            section = header[1]
        entry = re.fullmatch(r"([A-Z_]+)=(.*)", line)
        if not entry:
            continue
        key, value = entry.groups()
        (registry if section.startswith("registry/.env") else values)[key] = value
        if section.startswith("root .env"):
            ports[key] = value
    for key in ("BMKG_API_KEY", "PVMBG_TOKEN", "PVMBG_ADMIN_TOKEN", "AGGREGATOR_ADMIN_TOKEN",
                "POSTGRES_PASSWORD", "APP_PASSWORD", "AUTH_INTERNAL_KEY", "MEDIA_CLIENT_SECRET",
                "FIELD_CLIENT_SECRET", "INTERNAL_CLIENT_SECRET", "GF_SECURITY_ADMIN_PASSWORD"):
        values[key] = secrets.token_hex(24)
    registry["POSTGRES_PASSWORD"] = secrets.token_hex(24)
    registry["APICURIO_DATASOURCE_PASSWORD"] = registry["POSTGRES_PASSWORD"]
    values["DATABASE_URL"] = (
        f"postgres://{values['APP_USER']}:{values['APP_PASSWORD']}"
        f"@postgres:${{POSTGRES_PORT:?Set POSTGRES_PORT in root .env}}/{values['POSTGRES_DB']}?sslmode=disable"
    )
    files = {".env": ports, "registry/.env": registry}
    for name, keys in FILES.items():
        files[name] = {key: values[key] for key in keys.split()}
    files["consumer-notifier/.env"].update(
        CONSUMER_ROLE="notifier",
        NATS_DURABLE="notifier-v1", NATS_DLQ_SUBJECT="hazard.dlq.notifier",
    )
    for service, prefix, database_host in [("auth-service", "AUTH", "auth-db"), ("consumer-dashboard", "DASHBOARD", "dashboard-db"), ("consumer-notifier", "NOTIFIER", "notifier-db")]:
        username=values[prefix+"_DATABASE_USER"]
        database=values[prefix+"_DATABASE_NAME"]
        password=secrets.token_hex(24)
        files[database_host+"/.env"]={"POSTGRES_DB":database,"POSTGRES_USER":database+"_admin","POSTGRES_PASSWORD":secrets.token_hex(24),"APP_USER":username,"APP_PASSWORD":password}
        files[service+"/.env"]["DATABASE_URL"]=f"postgres://{username}:{password}@{database_host}:${{POSTGRES_PORT:?Set POSTGRES_PORT in root .env}}/{database}?sslmode=disable"
    demo=dict(files["consumer-dashboard/.env"])
    password=secrets.token_hex(24)
    demo.update(DATABASE_URL=f"postgres://dashboard_demo_app:{password}@dashboard-demo-db:${{POSTGRES_PORT:?Set POSTGRES_PORT in root .env}}/dashboard_demo?sslmode=disable",NATS_DURABLE="dashboard-demo-v1",NATS_DLQ_SUBJECT="hazard.dlq.dashboard-demo")
    files["consumer-dashboard-demo/.env"]=demo
    files["dashboard-demo-db/.env"]={"POSTGRES_DB":"dashboard_demo","POSTGRES_USER":"dashboard_demo_admin","POSTGRES_PASSWORD":secrets.token_hex(24),"APP_USER":"dashboard_demo_app","APP_PASSWORD":password}
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=ROOT,
                        help="Target checkout directory (default: repository root)")
    parser.add_argument("--fresh", action="store_true", help="Back up existing local configuration and generate a fresh, consistent set")
    args = parser.parse_args()
    files = configuration((ROOT / ".env.example").read_text())
    if args.fresh:
        backup=args.output_root/".local-config-backup"/datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        for name in files:
            old=args.output_root/name
            if old.exists():
                target=backup/name;target.parent.mkdir(parents=True,exist_ok=True,mode=0o700);old.rename(target);target.chmod(0o600)
        if backup.exists(): print("Previous local configuration backed up under .local-config-backup")
    present = [name for name in files if (args.output_root / name).exists()]
    if len(present) == len(files):
        print("All configuration .env files already exist; preserved without changes.")
        return 0
    if present:
        print("Partial configuration found; no files written. Complete it using README configuration tables.", file=sys.stderr)
        print("Missing: " + ", ".join(name for name in files if name not in present), file=sys.stderr)
        return 1
    for name, values in files.items():
        path = args.output_root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        # Exclusive creation protects existing secrets even if another process creates a file.
        with path.open("x", encoding="utf-8") as output:
            path.chmod(0o600)
            output.write("# Local demo configuration. Do not commit.\n")
            output.writelines(f"{key}={value}\n" for key, value in sorted(values.items()))
        print(f"Created {name}")
    print("Random credentials generated consistently across services; values are not printed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
