# Creates demo configuration from the combined template without replacing existing files.
param([string]$OutputRoot = (Split-Path -Parent $PSScriptRoot))
$ErrorActionPreference = 'Stop'
$template = Join-Path (Split-Path -Parent $PSScriptRoot) '.env.example'
$values = @{}
$registry = @{}
$ports = @{}
$section = 'root'
foreach ($line in Get-Content -LiteralPath $template) {
    if ($line -match '^# --- (.+) ---$') { $section = $Matches[1] }
    if ($line -match '^([A-Z_]+)=(.*)$') {
        $key = $Matches[1]; $value = $Matches[2]
        if ($section.StartsWith('registry/.env')) { $registry[$key] = $value }
        else { $values[$key] = $value }
        if ($section.StartsWith('root .env')) { $ports[$key] = $value }
    }
}
$streams = 'NATS_STREAM NATS_SUBJECT NATS_DLQ_STREAM NATS_LIFECYCLE_STREAM NATS_LIFECYCLE_SUBJECT'
$files = [ordered]@{
    'bmkg-mock/.env' = 'BMKG_API_KEY BMKG_DELAY_MS BMKG_EVENT_INTERVAL_MS SOURCE_ARCHIVE_PATH'
    'pvmbg-mock/.env' = 'PVMBG_TOKEN PVMBG_ADMIN_TOKEN PVMBG_MIN_DELAY_MS PVMBG_MAX_DELAY_MS PVMBG_REPORT_INTERVAL_MS SOURCE_ARCHIVE_PATH'
    'aggregator/.env' = "BMKG_API_KEY PVMBG_TOKEN POLL_INTERVAL DATABASE_URL BMKG_FRESHNESS_SEC PVMBG_FRESHNESS_SEC SOURCE_RETRY_MAX_SEC $streams"
    'store/.env' = 'POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD'
    'broker/.env' = "$streams NATS_STREAM_MAX_AGE NATS_DLQ_SUBJECT_PREFIX NATS_DLQ_MAX_AGE NATS_LIFECYCLE_MAX_AGE"
    'auth-service/.env' = 'AUTH_DB_PATH AUTH_INTERNAL_KEY ACCESS_TOKEN_TTL_SEC REFRESH_TOKEN_TTL_SEC MEDIA_CLIENT_ID MEDIA_CLIENT_SECRET FIELD_CLIENT_ID FIELD_CLIENT_SECRET INTERNAL_CLIENT_ID INTERNAL_CLIENT_SECRET'
    'client-api/.env' = 'AUTH_INTERNAL_KEY CLIENT_API_MAX_INFLIGHT CLIENT_API_MAX_INFLIGHT_PER_CLIENT'
    'client-tui/.env' = 'MEDIA_CLIENT_ID FIELD_CLIENT_ID INTERNAL_CLIENT_ID'
    'consumer-dashboard/.env' = "CONSUMER_ROLE CONSUMER_DB_PATH NATS_DURABLE NATS_DLQ_SUBJECT $streams"
    'consumer-notifier/.env' = "CONSUMER_ROLE CONSUMER_DB_PATH NATS_DURABLE NATS_DLQ_SUBJECT $streams"
    'monitoring/grafana/.env' = 'GF_SECURITY_ADMIN_USER GF_SECURITY_ADMIN_PASSWORD DLQ_STREAM_NAME'
    'monitoring/prometheus/.env' = 'METRICS_RETENTION_DAYS'
    'monitoring/loki/.env' = 'LOKI_GRPC_PORT LOG_RETENTION_DAYS'
}
function Write-EnvFile($relative, $settings) {
    $path = Join-Path $OutputRoot $relative
    if (Test-Path -LiteralPath $path) { Write-Host "Kept existing $relative"; return }
    New-Item -ItemType Directory -Force (Split-Path -Parent $path) | Out-Null
    $lines = @('# Demo values from .env.example; edit before deployment.')
    foreach ($key in ($settings.Keys | Sort-Object)) { $lines += "$key=$($settings[$key])" }
    [IO.File]::WriteAllLines($path, $lines, (New-Object Text.UTF8Encoding($false)))
    Write-Host "Created $relative"
}
Write-EnvFile '.env' $ports
Write-EnvFile 'registry/.env' $registry
foreach ($file in $files.Keys) {
    $settings = @{}
    foreach ($key in $files[$file].Split(' ')) {
        if (-not $values.ContainsKey($key)) { throw "Template missing $key for $file" }
        $settings[$key] = $values[$key]
    }
    if ($file -eq 'consumer-notifier/.env') {
        $settings.CONSUMER_ROLE = 'notifier'
        $settings.CONSUMER_DB_PATH = '/data/notifier.db'
        $settings.NATS_DURABLE = 'notifier-v1'
        $settings.NATS_DLQ_SUBJECT = 'hazard.dlq.notifier'
    }
    Write-EnvFile $file $settings
}
Write-Host 'Configuration ready. Existing files were preserved; verify shared credentials if mixing existing and generated files.'
