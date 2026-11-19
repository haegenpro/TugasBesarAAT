"""Shared HA PostgreSQL formation for owned service databases, plus explicit schema initialization.

One replicated PostgreSQL formation hosts the four service databases. Each owner
keeps its own database, runtime role, credential, Service name and NetworkPolicy
entry; the platform-rendered pg_hba.conf admits a role only to its own database.
"""
import hashlib
from pathlib import Path

from storage_lib import SERVICE_DATABASES,SERVICE_FORMATION,SERVICE_OWNERS,manifests,plan,require

ROOT=Path(__file__).resolve().parent.parent
OWNERS=SERVICE_OWNERS
SCHEMAS={"auth-db":"auth-service/schema.sql","dashboard-db":"consumer-dashboard/schema.sql","notifier-db":"consumer-notifier/schema.sql"}


def render(inventory,image):
    ns=inventory["namespace"]
    def peer(labels):return {"podSelector":{"matchLabels":labels}}
    def ports(*values):return [{"protocol":"TCP","port":value}for value in values]
    storage=plan(inventory,"postgres",image,SERVICE_FORMATION)
    items=[o for o in manifests(storage)["items"]if o["kind"]!="Namespace"]
    formation=storage["formation"]
    pg={"platform.aat/formation":formation,"component":"storage-postgres"}
    router={"platform.aat/formation":formation,"component":"storage-router"}
    etcd={"platform.aat/formation":formation,"component":"storage-etcd"}
    owners=[peer({"app.kubernetes.io/name":OWNERS[s]})for s in SERVICE_DATABASES]
    admins=[peer({"app.kubernetes.io/name":s+"-init"})for s in SERVICE_DATABASES]
    monitor=peer({"app.kubernetes.io/name":"prometheus"})
    for service in SERVICE_DATABASES:
        # Owner-facing names are unchanged; all route to the shared primary.
        items.append({"apiVersion":"v1","kind":"Service","metadata":{"name":service,"namespace":ns},"spec":{"selector":router,"ports":[{"name":"postgres","port":5432,"targetPort":5432}]}})
    rules=[("postgres",pg,[{"from":[*owners,*admins,peer(pg),peer(router)],"ports":ports(5432)},{"from":[peer(pg),peer(router),monitor],"ports":ports(8008)}],[{"to":[peer(pg)],"ports":ports(5432,8008)},{"to":[peer(etcd)],"ports":ports(2379)}]),
           ("router",router,[{"from":[*owners,*admins],"ports":ports(5432)}],[{"to":[peer(pg)],"ports":ports(5432,8008)}]),
           ("etcd",etcd,[{"from":[peer(pg),peer(etcd)],"ports":ports(2379)},{"from":[peer(etcd)],"ports":ports(2380)},{"from":[monitor],"ports":ports(2381)}],[{"to":[peer(etcd)],"ports":ports(2379,2380)}]),
           *((service+"-admin",{"app.kubernetes.io/name":service+"-init"},[],[{"to":[peer(pg),peer(router)],"ports":ports(5432)}])for service in SERVICE_DATABASES)]
    for name,selector,ingress,egress in rules:items.append({"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":inventory["cluster_id"]+"-owned-"+SERVICE_FORMATION+"-"+name,"namespace":ns},"spec":{"podSelector":{"matchLabels":selector},"policyTypes":["Ingress","Egress"],"ingress":ingress,"egress":egress}})
    report={"formations":[storage],"databases":storage["databases"],
            "memory_requests_mib":sum(storage["requests_mib_before_routers"].values())+storage["router_requests_mib"],
            "schema_initialization":"explicit administrator step per database against the running formation, with owning applications stopped",
            "isolation":"per-owner database, role, credential and pg_hba pair; other application components cannot reach the formation"}
    return {"apiVersion":"v1","kind":"List","items":items},report


def schema_sql(service):
    require(service in SERVICE_DATABASES,"unknown service database")
    role=SERVICE_DATABASES[service][1]
    if service=="registry-db":
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
"""+(ROOT/SCHEMAS[service]).read_text()
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


def isolation_sql(storage):
    """Administrator query (home database) describing owner separation on one member."""
    require(storage.get("formation_suffix")==SERVICE_FORMATION,"shared service database plan required")
    pairs=",".join(f"('{d['database']}','{d['runtime_role']}')"for d in storage["databases"])
    return f"""WITH owners(db,role) AS (VALUES {pairs})
SELECT jsonb_build_object(
 'connect',(SELECT jsonb_object_agg(r.role||'@'||d.db,has_database_privilege(r.role,d.db,'CONNECT')) FROM owners r CROSS JOIN owners d),
 'admin_connect',(SELECT jsonb_object_agg(r.role||'@'||a.datname,has_database_privilege(r.role,a.datname,'CONNECT')) FROM owners r CROSS JOIN pg_database a WHERE a.datallowconn AND a.datname NOT IN (SELECT db FROM owners)),
 'public_connect',(SELECT coalesce(jsonb_agg(datname ORDER BY datname),'[]'::jsonb) FROM pg_database WHERE datallowconn AND has_database_privilege('public',datname,'CONNECT')),
 'roles',(SELECT jsonb_object_agg(rolname,jsonb_build_object('login',rolcanlogin,'superuser',rolsuper,'createdb',rolcreatedb,'createrole',rolcreaterole,'replication',rolreplication,'bypassrls',rolbypassrls,'inherit',rolinherit,'connection_limit',rolconnlimit)) FROM pg_roles WHERE rolname IN (SELECT role FROM owners)),
 'memberships',(SELECT count(*) FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member OR r.oid=m.roleid WHERE r.rolname IN (SELECT role FROM owners)),
 'max_connections',current_setting('max_connections')::int,
 'hba',(SELECT jsonb_agg(jsonb_build_object('type',type,'database',database,'user',user_name,'method',auth_method,'error',error) ORDER BY rule_number) FROM pg_hba_file_rules))"""


EXTENSION_SQL="SELECT coalesce(jsonb_agg(extname ORDER BY extname),'[]'::jsonb) FROM pg_extension WHERE extname IN ('dblink','postgres_fdw')"


def check_isolation(storage,observed,extensions):
    """Fail unless every owner can reach only its own database through every layer."""
    require(storage.get("formation_suffix")==SERVICE_FORMATION,"shared service database plan required")
    owners=storage["databases"];pairs={(d["database"],d["runtime_role"])for d in owners}
    expected_connect={d["runtime_role"]+"@"+o["database"]:d is o for d in owners for o in owners}
    require(observed.get("connect")==expected_connect,"an owner role can connect to another owner's database")
    admin=observed.get("admin_connect")
    require(isinstance(admin,dict)and admin and not any(admin.values()),"an owner role can connect to an administrator database")
    require(observed.get("public_connect")==[],"PUBLIC retains CONNECT on a database")
    roles=observed.get("roles",{})
    for d in owners:
        role=roles.get(d["runtime_role"])
        require(role=={"login":True,"superuser":False,"createdb":False,"createrole":False,"replication":False,"bypassrls":False,"inherit":False,"connection_limit":d["connection_limit"]},"owner role attributes differ: "+d["runtime_role"])
    require(observed.get("memberships")==0,"owner roles must not be members of other roles")
    require(observed.get("max_connections")==storage["max_connections"],"shared formation max_connections differs")
    require(sum(d["connection_limit"]for d in owners)<storage["max_connections"],"owner connection limits exceed the shared server")
    hba=observed.get("hba") or []
    require(hba and not any(rule.get("error")for rule in hba),"pg_hba.conf has invalid rules")
    for rule in hba:
        databases,users=rule.get("database")or[],rule.get("user")or[]
        if rule.get("method")=="reject":continue
        if users==["postgres"]:continue
        if databases==["replication"]and users==["replicator"]:continue
        require(len(databases)==1 and len(users)==1 and (databases[0],users[0])in pairs,"pg_hba.conf admits a role beyond its own database")
    for d in owners:
        require(any(rule.get("database")==[d["database"]]and rule.get("user")==[d["runtime_role"]]and rule.get("type")=="host"for rule in hba),"pg_hba.conf lacks the owner network rule for "+d["database"])
    require(hba[-1].get("type")=="host"and hba[-1].get("method")=="reject"and hba[-1].get("database")==["all"]and hba[-1].get("user")==["all"],"pg_hba.conf must end by rejecting every other network connection")
    require(set(extensions)==set(d["database"]for d in owners)and not any(extensions.values()),"cross-database extensions (dblink/postgres_fdw) are installed")
    return {"passed":True,"databases":sorted(d["database"]for d in owners),"max_connections":observed["max_connections"],
            "connection_limits":{d["runtime_role"]:d["connection_limit"]for d in owners}}
