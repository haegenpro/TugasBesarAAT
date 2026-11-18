"""Private short-lived load sessions created from supplied client credentials."""
from contextlib import contextmanager
import json
from pathlib import Path

from source_validation_lib import http_json
from storage_lib import require,write_json

ROOT=Path(__file__).resolve().parent.parent


def credentials(path):
    path=Path(path);require(path.is_file()and not path.is_symlink()and path.stat().st_mode&0o077==0,"private client-credentials file required")
    value=json.loads(path.read_text())
    require(all(isinstance(value.get(role+"_CLIENT_"+field),str)and value[role+"_CLIENT_"+field]for role in("FIELD","MEDIA")for field in("ID","SECRET")),"Field and Media client credentials required")
    return value


@contextmanager
def token_inventory(base_url,credential_path,output,seconds):
    values=credentials(credential_path);output=Path(output);require(not output.exists(),"load token output exists; preserve private session evidence")
    resolved=output.resolve()
    require(not resolved.is_relative_to(ROOT)or resolved.is_relative_to(ROOT/"evidence"),"private load tokens must stay outside the repository or under ignored evidence/")
    sessions=[];tokens=[]
    try:
        for role in("field","media"):
            response=http_json(base_url.rstrip("/")+"/auth/login",payload={"client_id":values[role.upper()+"_CLIENT_ID"],"client_secret":values[role.upper()+"_CLIENT_SECRET"]})
            require(response.get("role")==role and isinstance(response.get("access_token"),str)and response["access_token"]and isinstance(response.get("refresh_token"),str)and response["refresh_token"],"Auth did not issue the requested role session")
            sessions.append(response["refresh_token"])
            require(type(response.get("expires_in"))is int and response["expires_in"]>=seconds+120,"issued access token TTL cannot cover the load window; supply longer TTL configuration")
            tokens.append({"role":role,"access_token":response["access_token"]})
        write_json(output,tokens)
        yield output
    finally:
        errors=[]
        for refresh in sessions:
            try:http_json(base_url.rstrip("/")+"/auth/logout",payload={"refresh_token":refresh})
            except (ValueError,OSError):errors.append("logout unavailable")
        require(not errors,"load session cleanup failed; sessions remain private and expire normally")
