#!/usr/bin/env bash
# Shared helpers for the Bash demo scripts. Requires curl and Python 3.

DEMO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

demo_env_value() {
  python3 - "$1" "$2" <<'PY'
import re
import sys
from pathlib import Path

path, wanted = Path(sys.argv[1]), sys.argv[2]
if not path.is_file():
    sys.exit(f"Environment file not found: {path}")
found = None
for number, line in enumerate(path.read_text().splitlines(), 1):
    if not line.strip() or line.lstrip().startswith('#'):
        continue
    entry = re.fullmatch(r'\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)', line)
    if not entry:
        sys.exit(f"Invalid environment entry at {path}:{number}")
    key, value = entry.group(1), entry.group(2).strip()
    if key != wanted:
        continue
    if found is not None:
        sys.exit(f"Duplicate variable {wanted} in {path}")
    if value.startswith("'"):
        quoted = re.fullmatch(r"'((?:\\'|[^'])*)'\s*(?:#.*)?", value)
        if not quoted:
            sys.exit(f"Invalid quoted variable {wanted} in {path}")
        value = quoted.group(1).replace("\\'", "'")
    elif value.startswith('"'):
        quoted = re.fullmatch(r'"((?:\\.|[^"\\])*)"\s*(?:#.*)?', value)
        if not quoted:
            sys.exit(f"Invalid quoted variable {wanted} in {path}")
        value = re.sub(r'\\([\\"nrt$])', lambda match: {'n': '\n', 'r': '\r', 't': '\t'}.get(match.group(1), match.group(1)), quoted.group(1))
    else:
        value = re.sub(r'\s+#.*$', '', value).rstrip()
    if '\n' in value or '\r' in value:
        sys.exit(f"Variable {wanted} in {path} must fit on one line")
    found = value
if found is None or found == '':
    sys.exit(f"{wanted} is missing or empty in {path}")
print(found)
PY
}

demo_json_field() {
  python3 -c 'import json, sys
value = json.load(sys.stdin)
for part in sys.argv[1].split("."):
    value = value[int(part)] if isinstance(value, list) else value[part]
if not isinstance(value, (str, int, float)):
    raise SystemExit("JSON field is not a scalar")
print(value)' "$1"
}

demo_json_pair() {
  printf '%s\0%s' "$1" "$2" | python3 -c 'import json, sys
first, second = sys.stdin.buffer.read().decode().split("\0", 1)
print(json.dumps({sys.argv[1]: first, sys.argv[2]: second}))' "$3" "$4"
}

demo_json_single() {
  printf '%s' "$1" | python3 -c 'import json, sys
print(json.dumps({sys.argv[1]: sys.stdin.read()}))' "$2"
}

demo_request() {
  local method="$1" url="$2"
  shift 2
  DEMO_STATUS="$(curl --silent --show-error --connect-timeout 3 --max-time 12 \
    --request "$method" --output "$DEMO_RESPONSE_FILE" --write-out '%{http_code}' \
    "$@" "$url")" || DEMO_STATUS=000
}

demo_require_status() {
  local expected="$1" label="$2"
  if [[ "$DEMO_STATUS" != "$expected" ]]; then
    printf 'FAIL: %s: expected HTTP %s, got %s\n' "$label" "$expected" "$DEMO_STATUS" >&2
    exit 1
  fi
}
