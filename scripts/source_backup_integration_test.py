"""Actual SQLite WAL snapshots, restore round-trip and stopped-writer refusal."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
from types import SimpleNamespace
import unittest

from platform_lib.inventory import load,fingerprint
from replacement_lib import RESTORE,validate_archive,validate_checkpoint
from service_lib import render
from source_backup_lib import capture,plan

ROOT=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('capture_source',ROOT/'deployment/platform/capture_source.py')
guest=importlib.util.module_from_spec(spec);spec.loader.exec_module(guest)

class SourceBackupChecks(unittest.TestCase):
    def fixture(self,folder):
        root=Path(folder);base=root/'volumes';formation=base/'fixture-services';volume=formation/'fixture-bmkg-mock-a'
        volume.mkdir(parents=True)
        (formation/'.owner.json').write_text(json.dumps({'cluster_id':'fixture','formation':'fixture-services'}))
        marker=root/'guest-owner.json';marker.write_text(json.dumps({'cluster_id':'fixture'}))
        return base,marker,volume,{'cluster_id':'fixture','claim':volume.name,'path':str(volume),'uid':os.getuid()}

    def test_sqlite_snapshot_includes_committed_wal_and_round_trips_restore(self):
        with tempfile.TemporaryDirectory()as folder:
            base,marker,volume,payload=self.fixture(folder)
            database=volume/'archive.json';writer=sqlite3.connect(database)
            writer.execute('PRAGMA journal_mode=WAL');writer.execute('PRAGMA wal_autocheckpoint=0')
            writer.execute('CREATE TABLE history(sequence INTEGER PRIMARY KEY,payload TEXT)')
            writer.execute("INSERT INTO history VALUES(7,'committed in WAL')");writer.commit()
            self.assertTrue(Path(str(database)+'-wal').exists())
            try:metadata,raw=guest.capture(payload,base,marker)
            finally:writer.close()
            archive=Path(folder)/'snapshot.tar';archive.write_bytes(raw);validate_archive(archive)
            with tarfile.open(archive)as source:
                self.assertEqual(source.getnames(),['archive.json'])
            self.assertEqual(metadata['sqlite'][0]['history_highwater'],7)
            destination=Path(folder)/'restored';destination.mkdir()
            args={'path':str(destination),'uid':os.getuid(),'sha256':metadata['sha256']}
            restored=subprocess.run([sys.executable,'-c',RESTORE,json.dumps(args)],input=raw,capture_output=True)
            self.assertEqual(restored.returncode,0,restored.stderr.decode())
            with sqlite3.connect(destination/'archive.json')as db:
                self.assertEqual(db.execute('SELECT sequence,payload FROM history').fetchall(),[(7,'committed in WAL')])
                self.assertEqual(db.execute('PRAGMA integrity_check').fetchone(),('ok',))

    def test_foreign_owner_and_links_are_refused_before_capture(self):
        with tempfile.TemporaryDirectory()as folder:
            base,marker,volume,payload=self.fixture(folder)
            (volume/'link').symlink_to(marker)
            with self.assertRaisesRegex(ValueError,'links'):guest.capture(payload,base,marker)
            (volume/'link').unlink()
            wrong=copy.deepcopy(payload);wrong['cluster_id']='foreign'
            with self.assertRaisesRegex(ValueError,'another cluster'):guest.capture(wrong,base,marker)

    def test_corrupt_sqlite_is_never_an_accepted_checkpoint(self):
        with tempfile.TemporaryDirectory()as folder:
            base,marker,volume,payload=self.fixture(folder)
            (volume/'archive.json').write_bytes(b'SQLite format 3\x00'+b'invalid database')
            with self.assertRaises(sqlite3.DatabaseError):guest.capture(payload,base,marker)

    def test_plan_finds_only_source_volumes_and_capture_refuses_live_writer(self):
        inventory=load(ROOT/'deployment/platform/inventory.fourhost.example.json')
        image='fixture.invalid/test@sha256:'+'a'*64
        catalog=json.loads((ROOT/'deployment/services/catalog.json').read_text())
        resources,requirements=render(inventory,catalog,{s['image_key']:image for s in catalog['services']if 'image_key'in s})
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)
            (root/'resources.json').write_text(json.dumps(resources));(root/'requirements.json').write_text(json.dumps(requirements))
            (root/'bundle.json').write_text(json.dumps({'version':1,'node_reserve_mib':512,'stages':[{'id':'services','manifest':'resources.json'}]}))
            prepared=plan(inventory,root/'bundle.json',[root/'requirements.json'])
            self.assertEqual({v['app']for v in prepared['volumes']},{'bmkg-mock','pvmbg-mock','sensor-pantai'})
            controller=next(o for o in resources['items']if o['kind']=='StatefulSet'and o['metadata']['name']==prepared['volumes'][0]['controller'])
            controller['metadata']['uid']='original-uid'
            class Cluster:
                cfg=inventory
                def kubectl(self,*args):return SimpleNamespace(stdout=json.dumps(controller).encode())
                def ssh(self,*args,**kwargs):raise AssertionError('remote data must not be accessed with live writers')
            with self.assertRaisesRegex(ValueError,'scaled to zero'):capture(Cluster(),prepared,root/'backup')
            self.assertFalse((root/'backup').exists())

    def test_receipt_cannot_be_reused_for_another_owner_or_archive(self):
        inventory=load(ROOT/'deployment/platform/inventory.fourhost.example.json')
        volume={'claim':'source','path':'/owned/source','uid':10001,'node':inventory['nodes'][0]['name'],'controller':{'app':'bmkg-mock','name':'bmkg-mock-a'}}
        receipt={'version':1,'writers_stopped':True,'cluster_id':inventory['cluster_id'],'namespace':inventory['namespace'],'inventory_sha256':fingerprint(inventory),'volume':{**{k:volume[k]for k in('claim','path','uid','node')},'app':'bmkg-mock','controller':'bmkg-mock-a'},'sha256':'a'*64,'captured_at':'recorded-time'}
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder);(root/'receipt.json').write_text(json.dumps(receipt))
            policy={'receipt':'receipt.json','sha256':'a'*64}
            self.assertEqual(validate_checkpoint(inventory,volume,policy,root)['sha256'],'a'*64)
            for changed in ('owner','archive'):
                candidate=copy.deepcopy(volume);rule=copy.deepcopy(policy)
                if changed=='owner':candidate['controller']['app']='pvmbg-mock'
                else:rule['sha256']='b'*64
                with self.assertRaises(ValueError):validate_checkpoint(inventory,candidate,rule,root)

if __name__=='__main__':unittest.main()
