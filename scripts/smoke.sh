#!/usr/bin/env bash
# Bash twin of smoke.ps1. Requires curl and Python 3.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/demo-common.sh"

RESTART_AUTH=0
WAIT_FOR_EXPIRY=0
TEST_OUTAGE=0
usage() {
  echo 'Usage: scripts/smoke.sh [--restart-auth] [--wait-for-expiry] [--test-outage]'
}
for arg in "$@"; do
  case "$arg" in
    --restart-auth) RESTART_AUTH=1 ;;
    --wait-for-expiry) WAIT_FOR_EXPIRY=1 ;;
    --test-outage) TEST_OUTAGE=1 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; printf 'Unknown option: %s\n' "$arg" >&2; exit 2 ;;
  esac
done

API_URL="${API_URL:-http://localhost:8080}"
BMKG_URL="${BMKG_URL:-http://localhost:8081}"
PVMBG_URL="${PVMBG_URL:-http://localhost:8082}"
API_URL="${API_URL%/}"
BMKG_URL="${BMKG_URL%/}"
PVMBG_URL="${PVMBG_URL%/}"
DEMO_RESPONSE_FILE="$(mktemp)"
OUTAGE_ENABLED=0
cleanup() {
  local result=$?
  trap - EXIT
  if (( OUTAGE_ENABLED )); then
    demo_request POST "$PVMBG_URL/admin/outage" \
      -H "Authorization: Bearer $PVMBG_ADMIN_TOKEN" \
      -H 'Content-Type: application/json' --data '{"enabled":false}'
    if [[ "$DEMO_STATUS" != 200 ]]; then
      printf 'WARNING: could not clear PVMBG outage (HTTP %s)\n' "$DEMO_STATUS" >&2
      result=1
    fi
  fi
  rm -f "$DEMO_RESPONSE_FILE"
  exit "$result"
}
trap cleanup EXIT

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
AUTH_ENV="$DEMO_ROOT/auth-service/.env"
BMKG_KEY="$(demo_env_value "$DEMO_ROOT/bmkg-mock/.env" BMKG_API_KEY)"
PVMBG_TOKEN="$(demo_env_value "$DEMO_ROOT/pvmbg-mock/.env" PVMBG_TOKEN)"
if (( TEST_OUTAGE )); then
  PVMBG_ADMIN_TOKEN="$(demo_env_value "$DEMO_ROOT/pvmbg-mock/.env" PVMBG_ADMIN_TOKEN)"
fi

login() {
  local prefix="$1" client_id client_secret body
  client_id="$(demo_env_value "$AUTH_ENV" "${prefix}_CLIENT_ID")"
  client_secret="$(demo_env_value "$AUTH_ENV" "${prefix}_CLIENT_SECRET")"
  body="$(demo_json_pair "$client_id" "$client_secret" client_id client_secret)"
  demo_request POST "$API_URL/auth/login" -H 'Content-Type: application/json' --data-binary "$body"
  demo_require_status 200 "$prefix login"
}
hazards() {
  local token="$1" path="$2"
  demo_request GET "$API_URL$path" -H "Authorization: Bearer $token"
  demo_require_status 200 "$path"
}
expect_status() {
  local token="$1" path="$2" expected="$3"
  demo_request GET "$API_URL$path" -H "Authorization: Bearer $token"
  demo_require_status "$expected" "$path"
}

login MEDIA
MEDIA_ACCESS="$(demo_json_field access_token < "$DEMO_RESPONSE_FILE")"
[[ "$(demo_json_field role < "$DEMO_RESPONSE_FILE")" == media ]] || fail 'Wrong Media role'
login FIELD
FIELD_ACCESS="$(demo_json_field access_token < "$DEMO_RESPONSE_FILE")"
FIELD_REFRESH="$(demo_json_field refresh_token < "$DEMO_RESPONSE_FILE")"
FIELD_EXPIRES="$(demo_json_field expires_in < "$DEMO_RESPONSE_FILE")"
[[ "$(demo_json_field role < "$DEMO_RESPONSE_FILE")" == field ]] || fail 'Wrong Field role'
login INTERNAL
INTERNAL_ACCESS="$(demo_json_field access_token < "$DEMO_RESPONSE_FILE")"
[[ "$(demo_json_field role < "$DEMO_RESPONSE_FILE")" == internal ]] || fail 'Wrong Internal role'

demo_request GET "$PVMBG_URL/volcanic-reports" -H "Authorization: Bearer $BMKG_KEY"
[[ "$DEMO_STATUS" == 401 || "$DEMO_STATUS" == 403 ]] || fail "BMKG key was accepted by PVMBG (HTTP $DEMO_STATUS)"
demo_request GET "$BMKG_URL/seismic-events" -H "X-BMKG-Key: $PVMBG_TOKEN"
[[ "$DEMO_STATUS" == 401 || "$DEMO_STATUS" == 403 ]] || fail "PVMBG token was accepted by BMKG (HTTP $DEMO_STATUS)"

found=0
for (( attempt=0; attempt<20; attempt++ )); do
  hazards "$FIELD_ACCESS" /hazards
  count="$(demo_json_field count < "$DEMO_RESPONSE_FILE")"
  if (( count > 0 )); then found=1; break; fi
  sleep 1
done
(( found )) || fail 'No canonical hazards became available'
python3 -c 'import json, sys
item = json.load(sys.stdin)["data"][0]
sys.exit(0 if "attributes" in item else 1)' < "$DEMO_RESPONSE_FILE" || fail 'Field response has no raw attributes'

hazards "$MEDIA_ACCESS" /hazards
MEDIA_ID="$(demo_json_field data.0.hazard_id < "$DEMO_RESPONSE_FILE")"
python3 -c 'import json, sys
item = json.load(sys.stdin)["data"][0]
sys.exit(0 if not {"source_ref_id", "latitude", "longitude", "attributes"}.intersection(item) else 1)' \
  < "$DEMO_RESPONSE_FILE" || fail 'Media list leaked a raw field'
expect_status "$MEDIA_ACCESS" '/hazards?view=raw' 403
hazards "$MEDIA_ACCESS" "/hazards/$MEDIA_ID"
python3 -c 'import json, sys
item = json.load(sys.stdin)["data"]
sys.exit(0 if "attributes" not in item else 1)' < "$DEMO_RESPONSE_FILE" || fail 'Media detail leaked attributes'
hazards "$INTERNAL_ACCESS" /hazards
python3 -c 'import json, sys
item = json.load(sys.stdin)["data"][0]
sys.exit(0 if "attributes" in item else 1)' < "$DEMO_RESPONSE_FILE" || fail 'Internal response has no raw attributes'

if (( TEST_OUTAGE )); then
  demo_request POST "$PVMBG_URL/admin/outage" \
    -H "Authorization: Bearer $PVMBG_ADMIN_TOKEN" \
    -H 'Content-Type: application/json' --data '{"enabled":true}'
  demo_require_status 200 'enable PVMBG outage'
  OUTAGE_ENABLED=1
  stale=0
  for (( attempt=0; attempt<10; attempt++ )); do
    sleep 1
    hazards "$MEDIA_ACCESS" '/hazards?hazard_type=VOLCANIC'
    if [[ "$(demo_json_field source_status.PVMBG.state < "$DEMO_RESPONSE_FILE")" == stale ]]; then
      stale=1
      break
    fi
  done
  (( stale )) || fail 'PVMBG outage was not surfaced to Media'
  python3 -c 'import json, sys
data = json.load(sys.stdin)["data"]
sys.exit(0 if not data or "attributes" not in data[0] else 1)' \
    < "$DEMO_RESPONSE_FILE" || fail 'Outage response leaked raw attributes to Media'
  demo_request POST "$PVMBG_URL/admin/outage" \
    -H "Authorization: Bearer $PVMBG_ADMIN_TOKEN" \
    -H 'Content-Type: application/json' --data '{"enabled":false}'
  demo_require_status 200 'clear PVMBG outage'
  OUTAGE_ENABLED=0
fi

if (( RESTART_AUTH )); then
  docker compose -f "$DEMO_ROOT/docker-compose.yml" restart auth-service >/dev/null || fail 'Auth container restart failed'
  ready=0
  for (( attempt=0; attempt<20; attempt++ )); do
    demo_request GET "$API_URL/hazards" -H "Authorization: Bearer $FIELD_ACCESS"
    if [[ "$DEMO_STATUS" == 200 ]]; then ready=1; break; fi
    sleep 1
  done
  (( ready )) || fail 'Session did not survive Auth restart'
fi

OLD_ACCESS="$FIELD_ACCESS"
if (( WAIT_FOR_EXPIRY )); then
  sleep "$(( FIELD_EXPIRES + 1 ))"
  expect_status "$OLD_ACCESS" /hazards 401
fi
refresh_body="$(demo_json_single "$FIELD_REFRESH" refresh_token)"
demo_request POST "$API_URL/auth/refresh" -H 'Content-Type: application/json' --data-binary "$refresh_body"
demo_require_status 200 'Field token refresh'
NEW_ACCESS="$(demo_json_field access_token < "$DEMO_RESPONSE_FILE")"
expect_status "$OLD_ACCESS" /hazards 401
hazards "$NEW_ACCESS" /hazards

printf 'PASS: institution credential isolation, Media redaction, raw denial, Field/Internal raw access, token rotation, restart=%s, natural_expiry=%s, outage=%s\n' \
  "$RESTART_AUTH" "$WAIT_FOR_EXPIRY" "$TEST_OUTAGE"
