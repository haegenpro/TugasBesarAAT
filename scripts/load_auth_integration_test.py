"""Owned load sessions remain private and are revoked even after a failed run."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from load_auth_lib import token_inventory
from source_validation_lib import http_json


class LoadAuthChecks(unittest.TestCase):
    def test_private_inventory_contains_access_only_and_logout_runs_after_failure(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder);source=root/"clients.json"
            source.write_text(json.dumps({role+"_CLIENT_"+field:"private-"+role+field for role in("FIELD","MEDIA")for field in("ID","SECRET")}));source.chmod(0o600)
            calls=[]
            def api(url,token="",payload=None):
                calls.append((url,payload))
                if url.endswith("logout"):return {}
                role="field"if "FIELD"in payload["client_id"]else "media"
                return {"role":role,"access_token":"access-"+role,"refresh_token":"refresh-private-"+role,"expires_in":900}
            with patch("load_auth_lib.http_json",side_effect=api):
                with self.assertRaisesRegex(RuntimeError,"fault"):
                    with token_inventory("http://127.0.0.1:18080",source,root/"tokens.json",150)as tokens:
                        self.assertNotIn("refresh-private",tokens.read_text());self.assertEqual(tokens.stat().st_mode&0o777,0o600)
                        raise RuntimeError("fault during workload")
            self.assertEqual(sum(url.endswith("logout")for url,_ in calls),2)

    def test_http_no_content_logout_is_accepted(self):
        class Response:
            status=204
            def read(self,*args):return b""
            def __enter__(self):return self
            def __exit__(self,*args):pass
        with patch("source_validation_lib.build_opener")as opener:
            opener.return_value.open.return_value=Response()
            self.assertEqual(http_json("http://127.0.0.1/auth/logout",payload={"refresh_token":"private"}),{})


if __name__=="__main__":unittest.main()
