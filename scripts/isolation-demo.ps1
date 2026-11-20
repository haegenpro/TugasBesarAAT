# Visible demo for P3.1: each institution's credential must be rejected by the other institution.
param(
    [string]$BmkgUrl,
    [string]$PvmbgUrl
)
$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($BmkgUrl)) { throw "-BmkgUrl is required" }
if ([string]::IsNullOrWhiteSpace($PvmbgUrl)) { throw "-PvmbgUrl is required" }

$BmkgUrl = $BmkgUrl.TrimEnd('/')
$PvmbgUrl = $PvmbgUrl.TrimEnd('/')
$root = Split-Path -Parent $PSScriptRoot
function Read-Env($p) { $h = @{}; foreach ($l in Get-Content $p) { if ($l -match '^\s*([A-Z_]+)\s*=\s*(.*?)\s*$' -and -not $l.TrimStart().StartsWith('#')) { $h[$Matches[1]] = $Matches[2].Trim("'",'"') } }; $h }
$bmkg = (Read-Env (Join-Path $root 'bmkg-mock/.env'))['BMKG_API_KEY']
$pvmbg = Read-Env (Join-Path $root 'pvmbg-mock/.env')

function Probe($label, $method, $uri, $headers, $expect) {
    try {
        $r = Invoke-WebRequest -Method $method -Uri $uri -Headers $headers -UseBasicParsing; $code = [int]$r.StatusCode
    } catch { $code = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { -1 } }
    $ok = $expect -contains $code
    '{0,-62} -> HTTP {1}  {2}' -f $label, $code, $(if ($ok) { 'PASS' } else { 'FAIL' })
    if (-not $ok) { $script:failed = $true }
}

Write-Host '--- Positive controls (own credential works) ---'
Probe 'BMKG key  -> BMKG  /seismic-events'      GET "$BmkgUrl/seismic-events" @{ 'X-BMKG-Key' = $bmkg } 200
Probe 'PVMBG tok -> PVMBG /volcanic-reports'    GET "$PvmbgUrl/volcanic-reports" @{ Authorization = "Bearer $($pvmbg['PVMBG_TOKEN'])" } 200
Write-Host '--- Cross-institution (must be 401/403) ---'
Probe 'BMKG key  -> PVMBG /volcanic-reports'    GET "$PvmbgUrl/volcanic-reports" @{ Authorization = "Bearer $bmkg" } 401,403
Probe 'PVMBG tok -> BMKG  /seismic-events'      GET "$BmkgUrl/seismic-events" @{ 'X-BMKG-Key' = $pvmbg['PVMBG_TOKEN'] } 401,403
Probe 'BMKG key  -> PVMBG /admin/outage'        POST "$PvmbgUrl/admin/outage" @{ Authorization = "Bearer $bmkg" } 401,403
Write-Host '--- Source token vs admin token on PVMBG (must be 401/403) ---'
Probe 'PVMBG data token -> PVMBG /admin/schema-version' POST "$PvmbgUrl/admin/schema-version" @{ Authorization = "Bearer $($pvmbg['PVMBG_TOKEN'])" } 401,403
Write-Host '--- No credential (must be 401/403) ---'
Probe 'none -> BMKG'  GET "$BmkgUrl/seismic-events" @{} 401,403
Probe 'none -> PVMBG' GET "$PvmbgUrl/volcanic-reports" @{} 401,403
if ($script:failed) { Write-Host 'RESULT: FAIL'; exit 1 } else { Write-Host 'RESULT: PASS' }
