#!/usr/bin/env bash
# Linux/macOS/Git Bash twin of scripts/load-test.ps1: runs scripts/load-test.js with k6 in Docker.
# Reads the Field client credentials from auth-service/.env.
# Usage: BASE_URL=... scripts/load-test.sh sustained|mixed VUS DURATION
#   BASE_URL=http://host.docker.internal:18080 scripts/load-test.sh sustained 60 70s
#   BASE_URL=http://host.docker.internal:18080 scripts/load-test.sh mixed 50 65s         # BMKG-only vs PVMBG path
# For "mixed", first set PVMBG's delay to 3 s. The mock reads its delay from pvmbg-mock/.env
# (Compose env_file), not from your shell, so `PVMBG_MIN_DELAY_MS=3000 docker compose ...` does nothing:
#   1. In pvmbg-mock/.env set PVMBG_MIN_DELAY_MS=3000 and PVMBG_MAX_DELAY_MS=3000
#   2. docker compose up -d --force-recreate pvmbg-mock
# (README, Demo D). Restore the original values and recreate the container afterwards.
set -euo pipefail

MODE="${1:?Usage: BASE_URL=... scripts/load-test.sh sustained|mixed VUS DURATION}"
VUS="${2:?VUS is required}"
DURATION="${3:?DURATION is required}"
: "${BASE_URL:?BASE_URL is required (for example http://host.docker.internal:18080)}"
[[ "$VUS" =~ ^[1-9][0-9]*$ ]] || { echo 'VUS must be a positive integer' >&2; exit 2; }
case "$MODE" in sustained|mixed) ;; *) echo "mode must be sustained or mixed" >&2; exit 2 ;; esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"

# envget FILE KEY: value of KEY=... without surrounding quotes, CR, or trailing comment-free whitespace
envget() {
  grep -E "^[[:space:]]*$2[[:space:]]*=" "$1" | head -1 | cut -d= -f2- | tr -d '\r' \
    | sed -e "s/^[[:space:]]*//" -e "s/[[:space:]]*$//" -e "s/^['\"]//" -e "s/['\"]$//"
}

CLIENT_ID="$(envget "$ROOT/auth-service/.env" FIELD_CLIENT_ID)"
CLIENT_SECRET="$(envget "$ROOT/auth-service/.env" FIELD_CLIENT_SECRET)"
[ -n "$CLIENT_ID" ] && [ -n "$CLIENT_SECRET" ] || { echo "FIELD_CLIENT_ID/SECRET not found in auth-service/.env" >&2; exit 1; }

if [ "$MODE" = mixed ]; then
  MIN="$(envget "$ROOT/pvmbg-mock/.env" PVMBG_MIN_DELAY_MS)"
  MAX="$(envget "$ROOT/pvmbg-mock/.env" PVMBG_MAX_DELAY_MS)"
  if [ "$MIN" != 3000 ] || [ "$MAX" != 3000 ]; then
    echo "WARNING: pvmbg-mock/.env has PVMBG delay ${MIN}..${MAX} ms, not 3000. The mixed run only proves P2.1 with a 3 s delay (README, Demo D)." >&2
  fi
fi

# MSYS_NO_PATHCONV stops Git Bash from rewriting the -v path; --add-host makes host.docker.internal work on Linux.
MSYS_NO_PATHCONV=1 docker run --rm -i \
  --add-host=host.docker.internal:host-gateway \
  -v "$SCRIPT_DIR:/scripts:ro" \
  -e BASE_URL="$BASE_URL" -e MODE="$MODE" -e VUS="$VUS" -e DURATION="$DURATION" \
  -e CLIENT_ID="$CLIENT_ID" -e CLIENT_SECRET="$CLIENT_SECRET" \
  grafana/k6 run /scripts/load-test.js
