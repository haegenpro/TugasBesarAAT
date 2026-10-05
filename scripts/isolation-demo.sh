#!/usr/bin/env bash
# P3.1: prove institution credentials are isolated.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/demo-common.sh"

BMKG_URL="${BMKG_URL:-http://localhost:8081}"
PVMBG_URL="${PVMBG_URL:-http://localhost:8082}"
BMKG_KEY="$(demo_env_value "$DEMO_ROOT/bmkg-mock/.env" BMKG_API_KEY)"
PVMBG_TOKEN="$(demo_env_value "$DEMO_ROOT/pvmbg-mock/.env" PVMBG_TOKEN)"
DEMO_RESPONSE_FILE="$(mktemp)"
trap 'rm -f "$DEMO_RESPONSE_FILE"' EXIT
failed=0

probe() {
  local label="$1" method="$2" url="$3" expected="$4"
  shift 4
  demo_request "$method" "$url" "$@"
  if [[ ",$expected," == *",$DEMO_STATUS,"* ]]; then
    printf '%-62s -> HTTP %s  PASS\n' "$label" "$DEMO_STATUS"
  else
    printf '%-62s -> HTTP %s  FAIL (expected %s)\n' "$label" "$DEMO_STATUS" "$expected"
    failed=1
  fi
}

echo '--- Positive controls (own credential works) ---'
probe 'BMKG key  -> BMKG  /seismic-events' GET "$BMKG_URL/seismic-events" 200 -H "X-BMKG-Key: $BMKG_KEY"
probe 'PVMBG tok -> PVMBG /volcanic-reports' GET "$PVMBG_URL/volcanic-reports" 200 -H "Authorization: Bearer $PVMBG_TOKEN"
echo '--- Cross-institution (must be 401/403) ---'
probe 'BMKG key  -> PVMBG /volcanic-reports' GET "$PVMBG_URL/volcanic-reports" 401,403 -H "Authorization: Bearer $BMKG_KEY"
probe 'PVMBG tok -> BMKG  /seismic-events' GET "$BMKG_URL/seismic-events" 401,403 -H "X-BMKG-Key: $PVMBG_TOKEN"
probe 'BMKG key  -> PVMBG /admin/outage' POST "$PVMBG_URL/admin/outage" 401,403 -H "Authorization: Bearer $BMKG_KEY"
echo '--- Source token vs admin token on PVMBG (must be 401/403) ---'
probe 'PVMBG data token -> PVMBG /admin/schema-version' POST "$PVMBG_URL/admin/schema-version" 401,403 -H "Authorization: Bearer $PVMBG_TOKEN"
echo '--- No credential (must be 401/403) ---'
probe 'none -> BMKG' GET "$BMKG_URL/seismic-events" 401,403
probe 'none -> PVMBG' GET "$PVMBG_URL/volcanic-reports" 401,403

if (( failed )); then
  echo 'RESULT: FAIL'
  exit 1
fi
echo 'RESULT: PASS'
