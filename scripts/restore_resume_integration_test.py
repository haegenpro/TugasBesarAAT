"""Actual filesystem interruption checks for atomic source checkpoint restores."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('restore_source',ROOT/'deployment/platform/restore_source.py')
restore=importlib.util.module_from_spec(spec);spec.loader.exec_module(restore)

class RestoreResumeChecks(unittest.TestCase):
    def archive(self):
        stream=io.BytesIO()
        with tarfile.open(fileobj=stream,mode='w')as archive:
            entry=tarfile.TarInfo('nested/state.db');raw=b'consistent checkpoint';entry.size=len(raw);archive.addfile(entry,io.BytesIO(raw))
        return stream.getvalue()

    def test_lost_reply_after_directory_commit_is_verified_without_overwrite(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)/'volume';root.mkdir();raw=self.archive();sha=hashlib.sha256(raw).hexdigest();payload={'path':str(root),'uid':os.getuid(),'sha256':sha}
            original=os.replace
            def fail_after_swap(source,destination):
                original(source,destination)
                if Path(destination)==root:raise OSError('injected lost completion after atomic commit')
            with patch.object(restore.os,'replace',side_effect=fail_after_swap):
                with self.assertRaises(OSError):restore.restore(payload,raw)
            self.assertEqual((root/'nested/state.db').read_bytes(),b'consistent checkpoint')
            inode=(root/'nested/state.db').stat().st_ino
            restore.restore(payload,raw)
            self.assertEqual((root/'nested/state.db').stat().st_ino,inode)
            (root/'nested/state.db').write_bytes(b'new accepted state')
            with self.assertRaises(ValueError):restore.restore(payload,raw)
            self.assertEqual((root/'nested/state.db').read_bytes(),b'new accepted state')

    def test_partial_staging_resumes_while_original_target_stays_empty(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)/'volume';root.mkdir();raw=self.archive();sha=hashlib.sha256(raw).hexdigest();payload={'path':str(root),'uid':os.getuid(),'sha256':sha}
            stage=root.parent/('.'+root.name+'.restore-'+sha);stage.mkdir()
            restore.atomic_json(stage/'intent.json',payload)
            (stage/'data/nested').mkdir(parents=True);(stage/'data/nested/state.db').write_bytes(b'partial')
            self.assertEqual(list(root.iterdir()),[])
            restore.restore(payload,raw)
            self.assertEqual((root/'nested/state.db').read_bytes(),b'consistent checkpoint')

    def test_nonempty_unmarked_target_and_unknown_staging_contents_are_refused(self):
        for kind in('target','staging'):
            with tempfile.TemporaryDirectory()as folder:
                root=Path(folder)/'volume';root.mkdir();raw=self.archive();sha=hashlib.sha256(raw).hexdigest();payload={'path':str(root),'uid':os.getuid(),'sha256':sha}
                if kind=='target':(root/'accepted.db').write_bytes(b'keep')
                else:
                    stage=root.parent/('.'+root.name+'.restore-'+sha);stage.mkdir();restore.atomic_json(stage/'intent.json',payload)
                    (stage/'data').mkdir();(stage/'data/unknown.db').write_bytes(b'keep')
                with self.assertRaises(ValueError):restore.restore(payload,raw)

if __name__=='__main__':unittest.main()
