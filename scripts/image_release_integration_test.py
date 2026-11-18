"""Registry receipts and build context privacy, without publishing any image."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import image_release_lib as images


class ImageReleaseChecks(unittest.TestCase):
    def test_publish_uses_filtered_tracked_context_and_registry_confirmed_digest(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)/"repo";root.mkdir();(root/"Dockerfile").write_text("FROM scratch\n");(root/"app.go").write_text("package main\n")
            (root/".env").write_text("private-do-not-copy");(root/"untracked-private").write_text("private")
            output=Path(folder)/"result";digest="sha256:"+"a"*64
            def fake_run(args,**kw):
                if args[:2]==["docker","build"]:
                    context=Path(args[-1]);self.assertFalse((context/".env").exists());self.assertFalse((context/"untracked-private").exists());self.assertTrue((context/"app.go").exists())
                    return subprocess.CompletedProcess(args,0)
                if args[:2]==["docker","push"]:return subprocess.CompletedProcess(args,0,("digest: "+digest+" size: 123\n").encode(),b"")
                return subprocess.CompletedProcess(args,0,json.dumps(["registry.invalid/project/app@"+digest]).encode(),b"")
            planned={"version":1,"revision":"revision","source_sha256":"hash","platform":"linux/amd64","external_images":{},"builds":[{"key":"app","context":".","dockerfile":"Dockerfile","target":None,"tag":"registry.invalid/project/app:revision-hash"}]}
            with patch.object(images,"plan",return_value=(planned,[Path("Dockerfile"),Path("app.go")])),patch.object(images,"REQUIRED",{"app"}):
                report=images.publish({},output,root=root,run=fake_run)
            self.assertTrue(report["passed"]);self.assertFalse((output/"context").exists())
            self.assertEqual(json.loads((output/"images.json").read_text())["app"],"registry.invalid/project/app@"+digest)
            with self.assertRaisesRegex(ValueError,"preserve"):
                with patch.object(images,"plan",return_value=(planned,[])):images.publish({},output,root=root,run=fake_run)

    def test_registry_failure_preserves_logs_without_emitting_success_map(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder);(root/"Dockerfile").write_text("FROM scratch\n")
            planned={"platform":"linux/amd64","external_images":{},"builds":[{"key":"app","context":".","dockerfile":"Dockerfile","target":None,"tag":"registry.invalid/app:tag"}]}
            def fail(args,**kw):return subprocess.CompletedProcess(args,0 if args[1]=="build"else 1,b"",b"denied")
            with patch.object(images,"plan",return_value=(planned,[Path("Dockerfile")])):
                with self.assertRaisesRegex(ValueError,"push failed"):images.publish({},root/"out",root=root,run=fail)
            self.assertFalse((root/"out/images.json").exists());self.assertFalse(json.loads((root/"out/result.json").read_text())["passed"])


if __name__=="__main__":unittest.main()
