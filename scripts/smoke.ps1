param([switch]$RestartAuth, [switch]$WaitForExpiry, [switch]$TestOutage)

$ErrorActionPreference = 'Stop'
$settings = @{}
Get-Content -LiteralPath '.env' | ForEach-Object {
    if ($_ -match '^([A-Za-z_][A-Za-z0-9_]*)=(.*)$') {
        $settings[$matches[1]] = $matches[2]
    }
}

function Login([string]$prefix) {
    $body = @{
        client_id = $settings["${prefix}_CLIENT_ID"]
        client_secret = $settings["${prefix}_CLIENT_SECRET"]
    } | ConvertTo-Json -Compress
    return Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/auth/login' -ContentType 'application/json' -Body $body
}

function Hazards([string]$token, [string]$path) {
    return Invoke-RestMethod -Method Get -Uri "http://localhost:8080$path" -Headers @{ Authorization = "Bearer $token" }
}

function ExpectStatus([string]$token, [string]$path, [int]$expected) {
    try {
        Invoke-WebRequest -Method Get -Uri "http://localhost:8080$path" -Headers @{ Authorization = "Bearer $token" } -UseBasicParsing | Out-Null
        throw "Expected HTTP $expected for $path"
    } catch {
        if (-not $_.Exception.Response -or [int]$_.Exception.Response.StatusCode -ne $expected) {
            throw
        }
    }
}

$media = Login 'MEDIA'
$field = Login 'FIELD'
$internal = Login 'INTERNAL'
if ($media.role -ne 'media' -or $field.role -ne 'field' -or $internal.role -ne 'internal') {
    throw 'Wrong server-assigned roles'
}

foreach ($case in @(
    @{ uri = 'http://localhost:8082/volcanic-reports'; headers = @{ Authorization = "Bearer $($settings['BMKG_API_KEY'])" } },
    @{ uri = 'http://localhost:8081/seismic-events'; headers = @{ 'X-BMKG-Key' = $settings['PVMBG_TOKEN'] } }
)) {
    try {
        Invoke-WebRequest -Method Get -Uri $case.uri -Headers $case.headers -UseBasicParsing | Out-Null
        throw "Cross-institution credential was accepted by $($case.uri)"
    } catch {
        if (-not $_.Exception.Response -or [int]$_.Exception.Response.StatusCode -notin @(401, 403)) {
            throw
        }
    }
}

$fieldData = $null
for ($attempt = 0; $attempt -lt 20; $attempt++) {
    $fieldData = Hazards $field.access_token '/hazards'
    if ($fieldData.count -gt 0) { break }
    Start-Sleep -Seconds 1
}
if ($fieldData.count -eq 0) { throw 'No canonical hazards became available' }
if (-not $fieldData.data[0].PSObject.Properties['attributes']) { throw 'Field response has no raw attributes' }

$mediaData = Hazards $media.access_token '/hazards'
$mediaFields = $mediaData.data[0].PSObject.Properties.Name
foreach ($fieldName in @('source_ref_id', 'latitude', 'longitude', 'attributes')) {
    if ($mediaFields -contains $fieldName) { throw "Media leaked $fieldName" }
}
ExpectStatus $media.access_token '/hazards?view=raw' 403

$mediaID = $mediaData.data[0].hazard_id
$mediaDetail = Hazards $media.access_token "/hazards/$mediaID"
if ($mediaDetail.data.PSObject.Properties['attributes']) { throw 'Media detail leaked attributes' }
$internalData = Hazards $internal.access_token '/hazards'
if (-not $internalData.data[0].PSObject.Properties['attributes']) { throw 'Internal response has no raw attributes' }

if ($TestOutage) {
    $adminHeaders = @{ Authorization = "Bearer $($settings['PVMBG_ADMIN_TOKEN'])" }
    $null = Invoke-RestMethod -Method Post -Uri 'http://localhost:8082/admin/outage' -Headers $adminHeaders
    try {
        $stale = $false
        for ($attempt = 0; $attempt -lt 10; $attempt++) {
            Start-Sleep -Seconds 1
            $volcanic = Hazards $media.access_token '/hazards?hazard_type=VOLCANIC'
            if ($volcanic.source_status.PVMBG.state -eq 'stale') {
                $stale = $true
                break
            }
        }
        if (-not $stale) { throw 'PVMBG outage was not surfaced to Media' }
        if ($volcanic.data.count -gt 0 -and $volcanic.data[0].PSObject.Properties['attributes']) {
            throw 'Outage response leaked raw attributes to Media'
        }
    } finally {
        $null = Invoke-RestMethod -Method Post -Uri 'http://localhost:8082/admin/outage' -Headers $adminHeaders
    }
}

if ($RestartAuth) {
    docker compose restart auth-service | Out-Null
    $ready = $false
    for ($attempt = 0; $attempt -lt 20; $attempt++) {
        try {
            $null = Hazards $field.access_token '/hazards'
            $ready = $true
            break
        } catch {
            Start-Sleep -Seconds 1
        }
    }
    if (-not $ready) { throw 'Session did not survive Auth restart' }
}

$oldAccess = $field.access_token
if ($WaitForExpiry) {
    Start-Sleep -Seconds ([int]$field.expires_in + 1)
    ExpectStatus $oldAccess '/hazards' 401
}
$refreshBody = @{ refresh_token = $field.refresh_token } | ConvertTo-Json -Compress
$newTokens = Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/auth/refresh' -ContentType 'application/json' -Body $refreshBody
ExpectStatus $oldAccess '/hazards' 401
$null = Hazards $newTokens.access_token '/hazards'

Write-Output "PASS: institution credential isolation, Media redaction, raw denial, Field/Internal raw access, token rotation, restart=$RestartAuth, natural_expiry=$WaitForExpiry, outage=$TestOutage"
