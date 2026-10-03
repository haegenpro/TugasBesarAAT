# Runs scripts/load-test.js with k6 in Docker. Reads the Field client credentials from auth-service/.env.
# Usage: scripts/load-test.ps1 [-Mode sustained|mixed] [-Vus 60] [-Duration 70s]
# For -Mode mixed, first set PVMBG delay to 3s:
#   docker compose stop pvmbg-mock
#   $env:PVMBG_MIN_DELAY_MS=3000; $env:PVMBG_MAX_DELAY_MS=3000; docker compose up -d pvmbg-mock
param([ValidateSet('sustained','mixed')][string]$Mode = 'sustained', [int]$Vus = 60, [string]$Duration = '70s')
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$env_ = @{}
foreach ($l in Get-Content (Join-Path $root 'auth-service/.env')) {
    if ($l -match '^\s*([A-Z_]+)\s*=\s*(.*?)\s*$' -and -not $l.TrimStart().StartsWith('#')) { $env_[$Matches[1]] = $Matches[2].Trim("'",'"') }
}
docker run --rm -i `
    -v "${PSScriptRoot}:/scripts:ro" `
    -e MODE=$Mode -e VUS=$Vus -e DURATION=$Duration `
    -e CLIENT_ID=$($env_['FIELD_CLIENT_ID']) -e CLIENT_SECRET=$($env_['FIELD_CLIENT_SECRET']) `
    grafana/k6 run /scripts/load-test.js
