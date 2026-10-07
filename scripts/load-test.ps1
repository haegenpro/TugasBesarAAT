# Runs scripts/load-test.js with k6 in Docker. Reads the Field client credentials from auth-service/.env.
# Usage: scripts/load-test.ps1 -Mode sustained|mixed -Vus 60 -Duration 70s -BaseUrl http://host.docker.internal:18080
# For -Mode mixed, first set PVMBG's delay to 3 s. The mock reads its delay from pvmbg-mock/.env
# (Compose env_file), not from your shell, so $env:PVMBG_MIN_DELAY_MS=... has no effect. Instead:
#   1. In pvmbg-mock/.env set PVMBG_MIN_DELAY_MS=3000 and PVMBG_MAX_DELAY_MS=3000
#   2. docker compose up -d --force-recreate pvmbg-mock
# (README, Demo D). Restore the original values and recreate the container afterwards.
# Linux/macOS: use scripts/load-test.sh instead.
param(
    [ValidateSet('sustained','mixed')][string]$Mode,
    [int]$Vus,
    [string]$Duration,
    [string]$BaseUrl
)
$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($Mode)) { throw '-Mode is required' }
if ($Vus -lt 1) { throw '-Vus must be a positive integer' }
if ([string]::IsNullOrWhiteSpace($Duration)) { throw '-Duration is required' }
if ([string]::IsNullOrWhiteSpace($BaseUrl)) { throw '-BaseUrl is required' }
$root = Split-Path -Parent $PSScriptRoot
function Read-Env($p) {
    $h = @{}
    foreach ($l in Get-Content $p) {
        if ($l -match '^\s*([A-Z_]+)\s*=\s*(.*?)\s*$' -and -not $l.TrimStart().StartsWith('#')) { $h[$Matches[1]] = $Matches[2].Trim("'",'"') }
    }
    $h
}
$env_ = Read-Env (Join-Path $root 'auth-service/.env')
if ([string]::IsNullOrWhiteSpace($env_['FIELD_CLIENT_ID']) -or [string]::IsNullOrWhiteSpace($env_['FIELD_CLIENT_SECRET'])) {
    throw 'FIELD_CLIENT_ID and FIELD_CLIENT_SECRET are required in auth-service/.env'
}
if ($Mode -eq 'mixed') {
    $pv = Read-Env (Join-Path $root 'pvmbg-mock/.env')
    if ($pv['PVMBG_MIN_DELAY_MS'] -ne '3000' -or $pv['PVMBG_MAX_DELAY_MS'] -ne '3000') {
        Write-Warning "pvmbg-mock/.env has PVMBG delay $($pv['PVMBG_MIN_DELAY_MS'])..$($pv['PVMBG_MAX_DELAY_MS']) ms, not 3000. The mixed run only proves P2.1 with a 3 s delay (README, Demo D)."
    }
}
docker run --rm -i `
    -v "${PSScriptRoot}:/scripts:ro" `
    -e BASE_URL=$BaseUrl -e MODE=$Mode -e VUS=$Vus -e DURATION=$Duration `
    -e CLIENT_ID=$($env_['FIELD_CLIENT_ID']) -e CLIENT_SECRET=$($env_['FIELD_CLIENT_SECRET']) `
    grafana/k6 run /scripts/load-test.js
