"""A-side durability policy. Never disables strict sync for a B primary.

Patroni owns elections, timelines, replication and Citus address updates. This
controller only selects the explicitly accepted A-only demo durability policy.
"""
import base64
import json
import os
import ssl
import time
import urllib.request

GROUPS = [('canonical', 0), ('canonical', 1), ('canonical', 2), ('ingestion', None), ('services', None)]
NS = os.environ['POD_NAMESPACE']
TOKEN = open('/var/run/secrets/kubernetes.io/serviceaccount/token').read().strip()
TLS = ssl.create_default_context(cafile='/var/run/secrets/kubernetes.io/serviceaccount/ca.crt')
AUTH = 'Basic '+base64.b64encode(('postgres:'+os.environ['POSTGRES_PASSWORD']).encode()).decode()


def request(url, body=None, kubernetes=False, method=None):
    headers = {'Authorization': 'Bearer '+TOKEN if kubernetes else AUTH}
    if body is not None:
        headers['Content-Type'] = 'application/json'
    r = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None,
                               headers=headers, method=method or ('PATCH' if body is not None else 'GET'))
    with urllib.request.urlopen(r, timeout=2, context=TLS if kubernetes else None) as response:
        return json.load(response)


def cycle():
    nodes = request('https://kubernetes.default.svc/api/v1/nodes', kubernetes=True)['items']
    ready = {n['metadata']['name']: any(c['type']=='Ready' and c['status']=='True' for c in n['status']['conditions']) for n in nodes}
    if not ready.get(os.environ['NODE_A']):
        raise RuntimeError('A is not Ready; refusing a durability-policy change')
    groups = []
    for formation, group in GROUPS:
        stem = f'canonical-{group}' if group is not None else formation
        url = f'http://{stem}-a-0.{stem}-a-peer:8008'
        cluster = request(url+'/cluster')
        leader = next((m for m in cluster['members'] if m['role']=='leader'), None)
        if not leader or leader.get('state') != 'running':
            raise RuntimeError('formation has no running primary')
        groups.append((url, leader, cluster, group))
    for url,leader,cluster,group in groups:
        # An A primary is the accepted sole-node authority when B is absent.
        # synchronous_mode remains enabled: a healthy B still participates in
        # flush-before-commit. Non-strict mode lets Patroni unblock A after its
        # synchronous connection disappears, without waiting for Kubernetes'
        # slower Node-Ready timeout. A B primary must ALWAYS remain strict.
        strict = not leader['name'].endswith('-a-0')
        candidate=next((m for m in cluster['members'] if m['name'].endswith('-b-0') and m.get('role')=='sync_standby' and m.get('state') in ('streaming','running')),None)
        restore=group==2 and not strict and ready.get(os.environ['NODE_B']) and candidate is not None
        if restore:strict=True
        config = request(url+'/config')
        desired={'tcp_keepalives_idle':2,'tcp_keepalives_interval':1,'tcp_keepalives_count':3,
                 'wal_sender_timeout':'5s','wal_receiver_timeout':'5s'}
        if group is not None:
            desired['citus.node_conninfo']='connect_timeout=1'
            desired['citus.max_adaptive_executor_pool_size']=1
        current=config.get('postgresql',{}).get('parameters',{})
        changes={k:v for k,v in desired.items() if current.get(k)!=v}
        update={}
        if config.get('synchronous_mode_strict') != strict: update['synchronous_mode_strict']=strict
        if changes: update['postgresql']={'parameters':changes}
        if update:
            request(url+'/config', update)
            print(json.dumps({'service':'storage-guardian','event':'durability_policy',
                              'leader':leader['name'],'strict':strict}), flush=True)
        if restore:
            request(url+'/switchover',{'leader':leader['name'],'candidate':candidate['name']},method='POST')
            print(json.dumps({'service':'storage-guardian','event':'restoring_worker_2_on_b'}),flush=True)


def main():
    while True:
        try:
            cycle()
        except Exception as error:
            # Configuration and authentication contents are intentionally omitted.
            print(json.dumps({'service':'storage-guardian','event':'waiting','reason':str(error)}), flush=True)
        time.sleep(2)


if __name__ == '__main__':
    main()
