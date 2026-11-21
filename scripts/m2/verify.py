#!/usr/bin/env python3
"""Run isolated database integration suites through owned local port forwards."""
import argparse
from contextlib import contextmanager, ExitStack
import json
import os
import socket
import subprocess
import time
from urllib.parse import quote
from common import LOCAL, NAMESPACE, ROOT, read_config, runtime_secrets


@contextmanager
def forward(cfg,service,remote_port=5432):
    target=service if '/' in service else 'service/'+service
    with socket.socket() as probe:
        probe.bind(('127.0.0.1',0));port=probe.getsockname()[1]
    env=os.environ.copy()
    if cfg['profile']=='minikube':env['KUBECONFIG']=str(LOCAL/'kubeconfig')
    process=subprocess.Popen([str(LOCAL/'bin/kubectl'),'--context',cfg['context'],'-n',NAMESPACE,
        'port-forward','--address=127.0.0.1',target,f'{port}:{remote_port}'],env=env,
        stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    try:
        deadline=time.monotonic()+20
        while True:
            try:
                with socket.create_connection(('127.0.0.1',port),timeout=.2):break
            except OSError:
                if process.poll() is not None or time.monotonic()>deadline:raise RuntimeError(service+' port forward did not become ready')
                time.sleep(.1)
        yield port
    finally:
        process.terminate()
        try:process.wait(timeout=5)
        except subprocess.TimeoutExpired:process.kill();process.wait()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',default=str(LOCAL/'config.json'))
    parser.add_argument('--suite',choices=('canonical','ingestion','services','broker','all'),default='all')
    args=parser.parse_args();cfg=read_config(args.config);credentials=runtime_secrets(LOCAL/'secrets.json')
    def dsn(owner,port,name=None):return f"postgres://{owner}_app:{quote(credentials[owner+'_db'],safe='')}@127.0.0.1:{port}/{name or owner}?sslmode=disable"
    def run(module,env,*packages):
        subprocess.run(['go','test','-race',*packages,'-count=1','-timeout=180s','-v'],cwd=ROOT/module,env=env,check=True)
    if args.suite in ('canonical','all'):
        with ExitStack() as forwards:
            port=forwards.enter_context(forward(cfg,'canonical-0'))
            addresses={}
            for group in (1,2):
                for node in ('a','b'):
                    member=f'canonical-{group}-{node}'
                    local=forwards.enter_context(forward(cfg,'pod/'+member+'-0'))
                    host=f'{member}-0.{member}-peer.{NAMESPACE}.svc.cluster.local:5432'
                    addresses[host]=f'127.0.0.1:{local}'
            env=os.environ.copy();env['M2_TEST_DATABASE_URL']=dsn('canonical',port,'hazards')
            env['M2_TEST_WORKER_ADDRESSES']=json.dumps(addresses)
            run('aggregator',env,'./internal/store','-run','TestM2')
    if args.suite in ('ingestion','all'):
        with forward(cfg,'ingestion-db') as port:
            env=os.environ.copy();env['M2_TEST_INGESTION_URL']=dsn('ingestion',port)
            run('aggregator',env,'./internal/ingest','-run','TestM2')
    if args.suite in ('services','all'):
        with forward(cfg,'services-db') as port:
            for owner,module in [('auth','auth-service'),('dashboard','consumer-dashboard'),('notifier','consumer-notifier')]:
                env=os.environ.copy();env['TEST_POSTGRES_URL']=dsn(owner,port)
                run(module,env,'./...')
    if args.suite in ('broker','all'):
        with forward(cfg,'broker',4222) as nats_port, forward(cfg,'services-db') as port:
            for owner in ('dashboard','notifier'):
                env=os.environ.copy()
                env['TEST_POSTGRES_URL']=dsn(owner,port)
                env['TEST_NATS_URL']=f"nats://application:{quote(credentials['nats'],safe='')}@127.0.0.1:{nats_port}"
                run('consumer-'+owner,env,'./...','-run',
                    'TestFetchTraceMeasuresRealBrokerWait|TestDLQRetainsOneEntryAfterLostAckAndAmbiguousPublish|TestWarningDeliveryThroughRealBroker')


if __name__=='__main__':main()
