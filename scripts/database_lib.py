"""Owned HA PostgreSQL formations and explicit administrator schema initialization."""
import hashlib
from pathlib import Path

from storage_lib import SERVICE_DATABASES,manifests,plan,require

ROOT=Path(__file__).resolve().parent.parent
OWNERS={"auth-db":"auth-service","registry-db":"schema-registry","dashboard-db":"consumer-dashboard","notifier-db":"consumer-notifier"}
SCHEMAS={"auth-db":"auth-service/schema.sql","dashboard-db":"consumer-dashboard/schema.sql","notifier-db":"consumer-notifier/schema.sql"}


def render(inventory,image):
    items=[];plans=[];ns=inventory["namespace"]
    def peer(labels):return {"podSelector":{"matchLabels":labels}}
    def ports(*values):return [{"protocol":"TCP","port":value}for value in values]
    for suffix in SERVICE_DATABASES:
        storage=plan(inventory,"postgres",image,suffix);plans.append(storage)
        items += [o for o in manifests(storage)["items"]if o["kind"]!="Namespace"]
        formation=storage["formation"]
        pg={"platform.aat/formation":formation,"component":"storage-postgres"}
        router={"platform.aat/formation":formation,"component":"storage-router"}
        etcd={"platform.aat/formation":formation,"component":"storage-etcd"}
        owner={"app.kubernetes.io/name":OWNERS[suffix]};admin={"app.kubernetes.io/name":suffix+"-init"};monitor={"app.kubernetes.io/name":"prometheus"}
        items.append({"apiVersion":"v1","kind":"Service","metadata":{"name":suffix,"namespace":ns},"spec":{"selector":router,"ports":[{"name":"postgres","port":5432,"targetPort":5432}]}})
        rules=[(suffix+"-postgres",pg,[{"from":[peer(owner),peer(admin),peer(pg),peer(router)],"ports":ports(5432)},{"from":[peer(pg),peer(router),peer(monitor)],"ports":ports(8008)}],[{"to":[peer(pg)],"ports":ports(5432,8008)},{"to":[peer(etcd)],"ports":ports(2379)}]),
               (suffix+"-router",router,[{"from":[peer(owner),peer(admin)],"ports":ports(5432)}],[{"to":[peer(pg)],"ports":ports(5432,8008)}]),
               (suffix+"-etcd",etcd,[{"from":[peer(pg),peer(etcd)],"ports":ports(2379)},{"from":[peer(etcd)],"ports":ports(2380)},{"from":[peer(monitor)],"ports":ports(2381)}],[{"to":[peer(etcd)],"ports":ports(2379,2380)}]),
               (suffix+"-admin",admin,[],[{"to":[peer(pg),peer(router)],"ports":ports(5432)}])]
        for name,selector,ingress,egress in rules:items.append({"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":inventory["cluster_id"]+"-owned-"+name,"namespace":ns},"spec":{"podSelector":{"matchLabels":selector},"policyTypes":["Ingress","Egress"],"ingress":ingress,"egress":egress}})
    return {"apiVersion":"v1","kind":"List","items":items},{"formations":plans,"memory_requests_mib":sum(sum(p["requests_mib_before_routers"].values())+p["router_requests_mib"]for p in plans),"schema_initialization":"explicit administrator step against running databases, with owning applications stopped"}


def schema_sql(storage):
    suffix=storage["formation_suffix"];require(suffix in SERVICE_DATABASES,"unknown service database")
    role=storage["runtime_role"];require(role==SERVICE_DATABASES[suffix][1],"unexpected runtime role")
    if suffix=="registry-db":
        # Registry schema is initialized by its exact image as an explicit admin
        # pod first, not a guessed hand-written copy of the vendor's DDL.
        body="DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='r' AND c.relname NOT IN ('platform_storage_identity','service_schema_migrations')) THEN RAISE EXCEPTION 'bootstrap registry schema with the pinned image first'; END IF; END $$;"
    else:
        body="""DO $$ BEGIN
IF EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='public' AND c.relkind IN ('r','p')
AND c.relname NOT IN ('platform_storage_identity','service_schema_migrations')) THEN
RAISE EXCEPTION 'untracked owner schema exists; explicit adoption or migration required';
END IF; END $$;
"""+(ROOT/SCHEMAS[suffix]).read_text()
    digest=hashlib.sha256(body.encode()).hexdigest()
    return f'''BEGIN;
SELECT pg_advisory_xact_lock(726120);
CREATE TABLE IF NOT EXISTS service_schema_migrations(version text PRIMARY KEY,checksum text NOT NULL);
DO $guard$ BEGIN
  IF EXISTS(SELECT 1 FROM service_schema_migrations WHERE version='owner-v1' AND checksum<>'{digest}') THEN
    RAISE EXCEPTION 'owner schema checksum differs; explicit upgrade required';
  END IF;
END $guard$;
SELECT NOT EXISTS(SELECT 1 FROM service_schema_migrations WHERE version='owner-v1') AS needs_initialization \\gset
\\if :needs_initialization
{body}
INSERT INTO service_schema_migrations(version,checksum) VALUES('owner-v1','{digest}');
\\endif
GRANT USAGE ON SCHEMA public TO "{role}";
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO "{role}";
GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO "{role}";
REVOKE ALL ON platform_storage_identity,service_schema_migrations FROM "{role}";
REVOKE CREATE ON SCHEMA public FROM "{role}";
COMMIT;
'''
