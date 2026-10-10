# M1 Test & Demo Runbook

Ordered, copy-paste test sequence for every M1 acceptance criterion
(`docs/IF4031 - Tugas Besar - M1.md`). Run the phases **top to bottom**: later
phases depend on state left by earlier ones, and the load tests come last
because they change PVMBG's configured delay.

Every step has:

- **Command**: a Bash variant (Linux / macOS / Git Bash) and a PowerShell variant (Windows).
- **Desired state**: what you must see. Anything else is a failure.
- **Criterion**: the spec item the step proves (e.g. `P2.3` = Problem 2, criterion 3).

| Phase | Covers | Mutates state? |
| --- | --- | --- |
| 0. Setup | helpers, credentials | no |
| 1. Boot & baseline | Ketentuan 3, 7; P1.1 | wipes volumes (optional) |
| 2. Schema evolution | P1.2 to P1.4 | PVMBG schema toggle -> v2 |
| 3. Trust & auth | P3.1 to P3.4 | creates sessions |
| 4. Pub/sub fan-out | P5.1 to P5.4 | stops/starts notifier, adds a 3rd consumer |
| 5. Microservices & storage | P4.1 to P4.3 | rebuilds one container |
| 6. Concurrency & availability | P2.1 to P2.3 | edits `pvmbg-mock/.env`, toggles outage |
| 7. Reset | - | restores config |

Evidence: **every numbered step is captured to `evidence/<step>.txt` as UTF-8** by running
`step <id>` first (see 0.3). Some steps also write named files (`1.3-volcanic-before.json`,
`*-started-before/after.txt`, ...) that later steps compare against. Add `evidence/` to
`.gitignore` (log excerpts; never print login JSON, it contains tokens). Appendix C lists
every file a complete run must produce.

Screenshot-ready checks also use `node scripts/evidence-probes.mjs` (Node.js 18+).
Each command prints its measured values and `PASS`/`FAIL`, saves a UTF-8 text
summary plus JSON snapshots in `evidence/`, and exits nonzero on failure.
These commands run only when explicitly invoked; `--help` is offline.
Use `--project <name>` if the system uses a named Compose project, and use the
same project for all other Compose commands. URLs come from the root `.env`.
Keep failed runs as evidence; desired values and examples are not measured results.

---

## Phase 0: Setup

### 0.1 Prerequisites

| Tool | Needed for | Linux/macOS | Windows |
| --- | --- | --- | --- |
| Docker + Compose v2 | everything | yes | Docker Desktop |
| `curl` | HTTP probes (Bash variant) | preinstalled | `curl.exe` ships with Win 10/11 (Git Bash also has it) |
| `jq` **or** `python3` | JSON parsing (Bash variant only) | `apt install jq` / `brew install jq`; python3 usually present | **not needed in PowerShell** (uses `ConvertFrom-Json`). In Git Bash: `winget install jqlang.jq`, or fall back to Python |
| k6 | load test | **not installed**: runs as `grafana/k6` container | same |
| nats CLI | broker inspection | **not installed**: runs as `natsio/nats-box` container | same |

Run everything from the repository root. The 14 per-service `.env` files (including three monitoring files) must
exist (see README, "Per-service environment files"). The root `.env` must
contain the complete deployment port plan plus `GRAFANA_BIND_ADDRESS`; see
[Port configuration](Port%20Configuration.md). Broker ports are internal only; use
`docker compose exec` or the backend-network NATS helper below for operator
inspection. No additional Compose file is needed. All service settings in `.env.example`
are required; there are no configuration fallbacks. Startup is independent of
external service availability. Consumer container health is liveness only;
processing proofs use `/ready` and record/backlog checks.

### 0.2 Helpers (paste once per terminal)

**Bash** (works with jq *or* python3; the helper auto-selects):

```bash
mkdir -p evidence

# ---- read secrets from the gitignored .env files (never printed) ----
envget(){ grep -E "^$2=" "$1" | head -1 | cut -d= -f2- | tr -d '\r' \
          | sed -e "s/^['\"]//" -e "s/['\"][[:space:]]*$//"; }
BMKG_KEY=$(envget bmkg-mock/.env BMKG_API_KEY)
PVMBG_TOKEN=$(envget pvmbg-mock/.env PVMBG_TOKEN)
PVMBG_ADMIN=$(envget pvmbg-mock/.env PVMBG_ADMIN_TOKEN)
for r in MEDIA FIELD INTERNAL; do
  eval "${r}_ID=\$(envget auth-service/.env ${r}_CLIENT_ID)"
  eval "${r}_SECRET=\$(envget auth-service/.env ${r}_CLIENT_SECRET)"
done
API="http://localhost:$(envget .env CLIENT_API_HOST_PORT)"
BMKG="http://localhost:$(envget .env BMKG_HOST_PORT)"
PVMBG="http://localhost:$(envget .env PVMBG_HOST_PORT)"
K6_API="http://host.docker.internal:$(envget .env CLIENT_API_HOST_PORT)"
# NET is resolved when `nats` runs (the network only exists once the stack is up; see the nats helper)

# ---- JSON helpers: jget <path>, jkeys <path>, jlen <path>, jfind <path> <field> <value> ----
# path is dotted, numbers index arrays, "." is the root. e.g.  jget data.0.hazard_id
if command -v jq >/dev/null 2>&1; then
  _P='getpath($p|split(".")|map(select(.!=""))|map(tonumber? // .))'
  jget(){  jq -r --arg p "$1" "$_P"; }
  jkeys(){ jq -r --arg p "$1" "$_P|keys[]"; }
  jlen(){  jq -r --arg p "$1" "$_P|length"; }
  jfind(){ jq --arg p "$1" --arg f "$2" --arg v "$3" "$_P|map(select(.[\$f]==\$v))"; }
else
  PY=$(command -v python3 || command -v python)   # Windows Git Bash: usually `python`
  _PYSRC='import sys,json
m,p=sys.argv[1],sys.argv[2]
d=json.load(sys.stdin)
for k in [x for x in p.split(".") if x]: d=d[int(k)] if k.isdigit() else d[k]
if m=="get": print(d if isinstance(d,str) else json.dumps(d,indent=2))
elif m=="keys": print("\n".join(sorted(d)))
elif m=="len": print(len(d))
elif m=="find": print(json.dumps([x for x in d if str(x.get(sys.argv[3]))==sys.argv[4]],indent=2))'
  jget(){  "$PY" -c "$_PYSRC" get "$1"; }
  jkeys(){ "$PY" -c "$_PYSRC" keys "$1"; }
  jlen(){  "$PY" -c "$_PYSRC" len "$1"; }
  jfind(){ "$PY" -c "$_PYSRC" find "$1" "$2" "$3"; }
fi

# ---- HTTP / stack helpers ----
code(){ curl -s -o /dev/null -w '%{http_code}' "$@"; }                       # status code only
login(){ curl -s -X POST "$API/auth/login" -H 'Content-Type: application/json' \
         ${3:+-H "X-Correlation-ID: $3"} -d "{\"client_id\":\"$1\",\"client_secret\":\"$2\"}"; }
hz(){ curl -s -H "Authorization: Bearer $1" "$API$2"; }                      # hz <token> </path>
agg(){ docker compose exec -T aggregator sh -c 'wget -qO- "http://localhost:${AGGREGATOR_PORT:?}$1"' sh "$1"; }   # internal API (not published)
rec(){ docker compose exec -T "$1" sh -c 'wget -qO- "http://localhost:${CONSUMER_PORT:?}/records"'; } # rec <service>
sql(){ echo "$1" | docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'; }
nats(){ local NET BROKER_URL; NET=$(docker network ls --format '{{.Name}}' | grep '_backend$' | head -1); [ -n "$NET" ] || { echo "no *_backend network: is the stack up? (Phase 1.1)" >&2; return 1; }
      BROKER_URL=$(docker compose exec -T broker printenv NATS_URL) || return
      docker run --rm --network "$NET" natsio/nats-box nats -s "$BROKER_URL" "$@"; }
started(){ docker inspect -f '{{.Name}} {{.State.StartedAt}}' $(docker compose ps -q); }  # container start times
# Both mocks emit a new event every 10 s on their own, so tests follow one specific event instead of counting.
latest(){ agg '/internal/hazards' | jget data.0.hazard_id; }                  # newest hazard in the Aggregator
wait_has(){ for i in $(seq 1 30); do rec "$1" | grep -q "$2" && { echo "$1 has $2"; return 0; }; sleep 2; done; echo "$1 MISSING $2"; return 1; }   # wait_has <service> <hazard_id>
```

**PowerShell** (Windows PowerShell 5.1 or PowerShell 7; no external tools):

```powershell
New-Item -ItemType Directory -Force evidence | Out-Null

# ---- read secrets from the gitignored .env files (never printed) ----
function Get-EnvVal($f,$k){ ((Get-Content $f | Where-Object { $_ -match "^$k=" } | Select-Object -First 1) -replace "^$k=",'').Trim().Trim("'",'"') }
$BmkgKey     = Get-EnvVal bmkg-mock/.env  BMKG_API_KEY
$PvmbgToken  = Get-EnvVal pvmbg-mock/.env PVMBG_TOKEN
$PvmbgAdmin  = Get-EnvVal pvmbg-mock/.env PVMBG_ADMIN_TOKEN
$Creds = @{}
foreach ($r in 'MEDIA','FIELD','INTERNAL') {
  $Creds[$r] = @{ id = Get-EnvVal auth-service/.env "${r}_CLIENT_ID"; secret = Get-EnvVal auth-service/.env "${r}_CLIENT_SECRET" }
}
$API = "http://localhost:$(Get-EnvVal .env CLIENT_API_HOST_PORT)"
$BmkgUrl = "http://localhost:$(Get-EnvVal .env BMKG_HOST_PORT)"
$PvmbgUrl = "http://localhost:$(Get-EnvVal .env PVMBG_HOST_PORT)"
$K6_API = "http://host.docker.internal:$(Get-EnvVal .env CLIENT_API_HOST_PORT)"
$AdminH = @{ Authorization = "Bearer $PvmbgAdmin" }

# ---- HTTP / stack helpers ----
function Code { param($Uri,[string]$Method='GET',$Headers=@{},$Body=$null)       # status code only
  $p=@{UseBasicParsing=$true;Method=$Method;Uri=$Uri;Headers=$Headers}
  if($Body){$p.Body=$Body;$p.ContentType='application/json'}
  try{[int](Invoke-WebRequest @p).StatusCode}catch{if($_.Exception.Response){[int]$_.Exception.Response.StatusCode}else{-1}} }
function Login($id,$sec,$cid=$null){ $h=@{}; if($cid){$h['X-Correlation-ID']=$cid}
  Invoke-RestMethod -Method Post -Uri "$API/auth/login" -ContentType 'application/json' -Headers $h `
    -Body (@{client_id=$id;client_secret=$sec}|ConvertTo-Json -Compress) }
function Hz($tok,$path,$cid=$null){ $h=@{Authorization="Bearer $tok"}; if($cid){$h['X-Correlation-ID']=$cid}
  Invoke-RestMethod -Uri "$API$path" -Headers $h }
function Agg($path){ docker compose exec -T aggregator sh -c 'wget -qO- "http://localhost:${AGGREGATOR_PORT:?}$1"' sh $path | Out-String | ConvertFrom-Json }
function Rec($svc){ docker compose exec -T $svc sh -c 'wget -qO- "http://localhost:${CONSUMER_PORT:?}/records"' | Out-String | ConvertFrom-Json }
function Sql($q){ $q | docker compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB' }
function Nats { $net = docker network ls --format '{{.Name}}' | Where-Object { $_ -like '*_backend' } | Select-Object -First 1   # looked up per call: the network only exists once the stack is up
  if (-not $net) { Write-Error 'no *_backend network: is the stack up? (Phase 1.1)'; return }
  $brokerUrl = (docker compose exec -T broker printenv NATS_URL | Out-String).Trim()
  if ($LASTEXITCODE -ne 0) { Write-Error 'cannot read broker NATS_URL'; return }
  docker run --rm --network $net natsio/nats-box nats -s $brokerUrl @args }
function Started { docker inspect -f '{{.Name}} {{.State.StartedAt}}' (docker compose ps -q) }
# Both mocks emit a new event every 10 s on their own, so tests follow one specific event instead of counting.
function Latest { (Agg '/internal/hazards').data[0].hazard_id }             # newest hazard in the Aggregator
function Wait-Has($svc,$id){ for($i=0;$i -lt 30;$i++){                # Wait-Has <service> <hazard_id>
  if((Rec $svc).data.hazard_id -contains $id){ "$svc has $id"; return }; Start-Sleep 2 }; "$svc MISSING $id" }
```

**Desired state:** no output, no errors. Quick self-check:

```bash
echo "bmkg key length: ${#BMKG_KEY}"; echo "$FIELD_ID"          # non-empty; never echo secrets
echo '{"a":{"b":[1,2]}}' | jget a.b.1                            # => 2
```
```powershell
$BmkgKey.Length; $Creds.FIELD.id                                 # non-empty
```

### 0.3 Evidence capture (paste once per terminal, after 0.2)

Windows PowerShell's `Tee-Object` and `>` write **UTF-16**, which breaks `grep`/diff and
turns box-drawing tables into garbage. It also prints a red `NativeCommandError` for
docker's progress output (stderr) when you use `2>&1`; the containers are fine, but it
pollutes the evidence. The helpers below fix both and make `step <id>` capture a whole step.

**Bash**:

```bash
exec 3>&1 4>&2                                                   # remember the real terminal
step(){ exec 1>&3 2>&4; sleep 0.3; exec > >(tee "evidence/$1.txt") 2>&1; echo "### step $1  $(date -Is)"; }
endstep(){ exec 1>&3 2>&4; }                                     # stop capturing

# ASCII-only consumer summary (replaces the box-drawing `nats consumer report` table, which
# turns into "Γöé" garbage when it passes through a non-UTF-8 Windows console)
consreport(){
  S=$(nats stream info HAZARDS --json)
  echo "stream HAZARDS: messages=$(echo "$S" | jget state.messages) subjects=$(echo "$S" | jget config.subjects | tr -d '[:space:]')"
  for c in ${@:-dashboard-v1 notifier-v1 dashboard-demo-v1}; do
    J=$(nats consumer info HAZARDS "$c" --json 2>/dev/null) || continue          # skip durables that don't exist (yet)
    echo "$c unprocessed=$(echo "$J" | jget num_pending) ack_pending=$(echo "$J" | jget num_ack_pending) redelivered=$(echo "$J" | jget num_redelivered) ack_floor=$(echo "$J" | jget ack_floor.stream_seq)"
  done; }
```

**PowerShell**:

```powershell
function Tee8($f){ begin{ $p="evidence/$f"; '' | Out-File $p -Encoding utf8 -NoNewline }       # UTF-8 tee
                   process{ $_ | Out-File $p -Append -Encoding utf8; $_ } }
function Step($id){ try{Stop-Transcript | Out-Null}catch{}; Start-Transcript -Path "evidence/$id.txt" -Force | Out-Null; "### step $id" }
function EndStep { try{Stop-Transcript | Out-Null}catch{} }

# Make the console and native-command pipes UTF-8 (default is the OEM code page, which mangles
# any non-ASCII output, e.g. nats tables, into "Γöé"), then use an ASCII-only consumer summary.
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false); $OutputEncoding = [Console]::OutputEncoding
function Consreport {
  $S = Nats stream info HAZARDS --json | Out-String | ConvertFrom-Json
  "stream HAZARDS: messages=$($S.state.messages) subjects=$($S.config.subjects -join ',')"
  foreach ($c in 'dashboard-v1','notifier-v1','dashboard-demo-v1') {
    $j = Nats consumer info HAZARDS $c --json 2>$null | Out-String
    if ($LASTEXITCODE -ne 0 -or -not $j.Trim()) { continue }                         # durable doesn't exist (yet)
    $j = $j | ConvertFrom-Json
    "$c unprocessed=$($j.num_pending) ack_pending=$($j.num_ack_pending) redelivered=$($j.num_redelivered) ack_floor=$($j.ack_floor.stream_seq)" } }
```

`Step` uses a PowerShell transcript (UTF-8, records native-command output). If a transcript
file comes out empty, wrap the step with `Tee8` instead. Run `endstep` / `EndStep` when
finished; every `Step` call closes the previous capture.

**Desired state:** after `step 1.1` (or `Step 1.1`) the file `evidence/1.1.txt` exists, opens
as readable text in any editor, and the header line `### step 1.1` is the first line.

---

## Optional monitoring-assisted workflow (Windows and Unix)

Use the commands below to trigger scenarios and assert exact results. Use Grafana
for repeated observation instead of repeatedly running Docker log filters or NATS
reports. Saved command outputs remain the acceptance evidence; dashboard captures
add context. All Bash commands support Linux, macOS, and Git Bash; PowerShell
commands support Windows PowerShell 5.1 and PowerShell 7.

After Phase 1.1, enable the five monitoring services if you want dashboard
observation during testing. This command works in Bash and PowerShell:

```bash
docker compose --profile monitoring up -d --build
```

Without this profile, use the API, SQL, Docker logs, and k6 assertions directly.
Record whether monitoring is enabled when comparing load-test results.
To stop only monitoring, run
`docker compose --profile monitoring stop grafana prometheus loki alloy nats-exporter`.
Omitting the profile from a later `up` does not stop containers already running.

Once monitoring is running, open Grafana using your root `.env` settings:

```bash
GRAFANA_ADDR=$(envget .env GRAFANA_BIND_ADDRESS)
GRAFANA_PORT=$(envget .env GRAFANA_HOST_PORT)
case "$GRAFANA_ADDR" in 0.0.0.0) GRAFANA_ADDR=127.0.0.1 ;; esac
printf 'Open http://%s:%s in your browser\n' "$GRAFANA_ADDR" "$GRAFANA_PORT"
```
```powershell
$grafanaAddress = Get-EnvVal .env GRAFANA_BIND_ADDRESS
$grafanaPort = Get-EnvVal .env GRAFANA_HOST_PORT
if ($grafanaAddress -eq '0.0.0.0') { $grafanaAddress = '127.0.0.1' }
Start-Process "http://${grafanaAddress}:$grafanaPort"
```

These commands assume the documented IPv4 bind configuration. Sign in with the
credentials in `monitoring/grafana/.env`; do not print or save the password. Open
**Monitoring → AAT Health, DLQ and Correlation Logs**, select all relevant services,
set the time range to the test window, and enable a 5–10 second dashboard refresh.
An existing Grafana volume retains its administrator password even if `.env` changes.

Before relying on a panel, check **Monitoring collection** and **NATS metrics
collection**. Missing collection is **No data**, not a successful test. Allow at
least two collection intervals (about 30 seconds) after startup or a transition.
Probes and metric scrapes run every 15 seconds, so they can miss short outages.
Alerts evaluate every 30 seconds; endpoint failures require one minute before
firing. Notifications are muted; inspect alert state in Grafana.

| Runbook step | Observe in Grafana | Assertions that remain required |
| --- | --- | --- |
| 1.2 | Liveness, Readiness, Endpoint availability history | Direct endpoint status codes |
| 1.4 | Logs matching correlation ID | Generate one known ID; verify Client API, Auth, Aggregator and latency fields |
| 4.1–4.4 | Consumer pending messages, pending acknowledgements, redeliveries; readiness | Stream subjects/durable identities, the same hazard ID in both consumers, missed-event catch-up, SQL outbox check |
| 4.5 | Logs for the optional demo consumer, when collected | Third consumer receives the same event; producer start time unchanged (demo consumer has no permanent probe target) |
| 5.2 | Endpoint availability history and consumer recovery | Keep the two-second authenticated business probe, image IDs, and container start-time comparison |
| 6.1–6.2b | Availability, outbound failures, request-rejection logs | Keep k6: concurrency, duration, throughput, p50/p95/p99, error rate, and controlled 429 checks |
| 6.3 | Outbound failure logs and recovery timeline | Outage toggle, direct 503 versus health 200, cached response 200 with stale metadata, recovery, and no restart |

For 1.4 and 4.2, copy the printed correlation ID into the dashboard's exact
correlation filter. Select the contributing services and inspect the matching
JSON fields. The existing log commands provide a text export/fallback; during
exploration there is no need to rerun them continuously.

For 4.3–4.4, keep the dashboard open across stop/start: notifier pending messages
should rise and then drain while the dashboard consumer continues. Broker backlog
draining alone does not prove business delivery: keep `wait_has` / `Wait-Has` for
the specific missed event. Pending acknowledgements and redelivery counts add
diagnostic context; they are not required to remain zero throughout recovery.

For 6.3, PVMBG `/health` can remain green during an intentional data outage.
Check the business response's `source_status` and `stale_since` plus warning logs.
Synthetic probe duration and **Logged outbound latency p95** are not end-to-end
business request latency and cannot replace k6 thresholds.

Schema mapping, role redaction, token expiry/refresh, JSONB storage, network
isolation, and lifecycle revision/order assertions remain API/SQL/Go tests.
Monitoring does not inspect those contracts or instrument application throughput,
outbox state, database health, or distributed traces.

For supplemental screenshots, save `evidence/1.2-health.png`,
`1.4-correlation.png`, `4.3-backlog-during.png`, `4.4-backlog-after.png`,
`5.2-availability.png`, and `6.3-outage-recovery.png` as applicable. Include the
visible time window, service/durable labels, and correlation ID where relevant.
Avoid credentials or tokens. Record the scenario times in the corresponding step
transcript. Screenshots do not replace Appendix C's exact record and response
checks. See [Monitoring](../monitoring/README.md) and
[Monitoring verification](../monitoring/VERIFICATION.md) for collector-loss and
DLQ-specific checks. A retained DLQ count is an archive count; a decrease does not
prove successful recovery.

---

## Phase 1: Clean boot & baseline (Ketentuan 3 and 7, P1.1)

### 1.1 Start the whole system with one command

> Capture: run `step 1.1` first. Output is saved to `evidence/1.1.txt`.

> Only run `down -v` if you want a pristine database (it **deletes all volumes**:
> canonical store, auth sessions, consumer SQLite, registry). Skip it to keep data.

```bash
docker compose --profile monitoring --profile consumer-demo down -v --remove-orphans
docker compose up -d --build 2>&1 | tee evidence/1.1-up.txt
until ! docker compose ps --format '{{.Health}}' | grep -qv '^healthy$'; do sleep 3; done
docker compose ps --format 'table {{.Service}}\t{{.Status}}' | tee evidence/1.1-ps.txt
```
```powershell
docker compose --profile monitoring --profile consumer-demo down -v --remove-orphans
docker compose up -d --build 2>&1 | ForEach-Object { "$_" } | Tee8 1.1-up.txt
do { Start-Sleep 3; $bad = docker compose ps --format '{{.Health}}' | Where-Object { $_ -ne 'healthy' } } while ($bad)
docker compose ps --format 'table {{.Service}}\t{{.Status}}' | Tee8 1.1-ps.txt
```

**Desired state:** exactly 11 default-profile containers, all `healthy`: `broker, postgres,
registry-db, schema-registry, bmkg-mock, pvmbg-mock, aggregator, auth-service,
client-api, consumer-dashboard, consumer-notifier`. Enabling the optional
`monitoring` profile adds `grafana, prometheus, loki, alloy, nats-exporter`, for
16 containers. Other optional profile containers are not included in these counts. The one command is
`docker compose up -d --build`. **Criterion:** Ketentuan 3.

### 1.2 Health endpoints

> Capture: run `step 1.2` first. Output is saved to `evidence/1.2.txt`.

```bash
for u in $BMKG/health $PVMBG/health $API/health $API/ready; do echo "$u -> $(code $u)"; done
```
```powershell
foreach ($u in "$BmkgUrl/health","$PvmbgUrl/health","$API/health","$API/ready") { "$u -> $(Code $u)" }
```

**Desired state:** all four `200`. `/ready` is `200` only when Auth and Aggregator
are reachable. **Criterion:** Ketentuan 7.

### 1.3 P1.1: normal aggregation on the *old* PVMBG schema

> Capture: run `step 1.3` first. Output is saved to `evidence/1.3.txt`.

Save a snapshot of the volcanic data **before** any schema change; Phase 2 and
Phase 5 compare against it.

```bash
sleep 8                                                    # a few poll cycles
S1=$(curl -s -H "X-BMKG-Key: $BMKG_KEY" $BMKG/seismic-events | jlen .)
V1=$(curl -s -H "Authorization: Bearer $PVMBG_TOKEN" $PVMBG/volcanic-reports | jlen .)
A1=$(agg '/internal/hazards' | jget count); echo "seismic=$S1 volcanic=$V1 aggregator=$A1"   # >= 20, >= 20, >= 40
curl -s -H "Authorization: Bearer $PVMBG_TOKEN" $PVMBG/volcanic-reports | jget 0         # no confidence_level
agg '/internal/hazards?hazard_type=VOLCANIC' | tee evidence/1.3-volcanic-before.json | jget data.0
agg '/internal/hazards?hazard_type=SEISMIC'  | jget data.0
sleep 25                                                   # each mock adds one record every 10 s
S2=$(curl -s -H "X-BMKG-Key: $BMKG_KEY" $BMKG/seismic-events | jlen .)
V2=$(curl -s -H "Authorization: Bearer $PVMBG_TOKEN" $PVMBG/volcanic-reports | jlen .)
A2=$(agg '/internal/hazards' | jget count); echo "seismic=$S2 volcanic=$V2 aggregator=$A2"   # each higher than before
```
```powershell
Start-Sleep 8
function Counts { $s = (Invoke-RestMethod -Uri "$BmkgUrl/seismic-events" -Headers @{'X-BMKG-Key'=$BmkgKey}).Count
  $p = (Invoke-RestMethod -Uri "$PvmbgUrl/volcanic-reports" -Headers @{Authorization="Bearer $PvmbgToken"}).Count
  "seismic=$s volcanic=$p aggregator=$((Agg '/internal/hazards').count)" }
Counts                                                                                                      # >= 20, >= 20, >= 40
$v = Invoke-RestMethod -Uri "$PvmbgUrl/volcanic-reports" -Headers @{Authorization="Bearer $PvmbgToken"}
$v[0] | ConvertTo-Json                                                                                      # no confidence_level
$vb = Agg '/internal/hazards?hazard_type=VOLCANIC'; $vb | ConvertTo-Json -Depth 8 | Set-Content evidence/1.3-volcanic-before.json -Encoding utf8
$vb.data[0] | ConvertTo-Json -Depth 5
(Agg '/internal/hazards?hazard_type=SEISMIC').data[0] | ConvertTo-Json -Depth 5
Start-Sleep 25                                                                                              # each mock adds one record every 10 s
Counts                                                                                                      # each higher than before
```

**Desired state:**
- BMKG returns **at least 20** seismic events and PVMBG **at least 20** reports (20 seed records plus one new record every 10 s), all **without** `confidence_level`.
- Aggregator count is **at least 40**. The exact numbers depend on how long the stack has been up.
- The second reading, 25 s later, is **higher for all three**: each mock produced at least two new records and the Aggregator ingested them. This is the "1 new event per 10 s per institution" rule from the spec.
- Each hazard has canonical top-level keys `hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude, occurred_at, ingested_at, attributes`.
- A seismic row: `source=BMKG`, `area_name` = region, `attributes` holds `magnitude, depth_km, potential_tsunami` (plus warning fields when one exists); severity follows the tsunami/magnitude rule.
- A volcanic row: `source=PVMBG`, `area_name` is a volcano name (from `volcano_reference`), `severity` matches `alert_level`, `attributes` holds `eruption_count_24h, ash_column_height_m` and **no** `confidence_level`.

**Criterion:** P1.1.

For source-to-canonical proof, run this after the baseline is ingested (same
command in Bash and PowerShell):

```text
node scripts/evidence-probes.mjs mapping --name 1.3-mapping
```

The screenshot must show matched seismic/volcanic counts and a PASS for
source fields, severity, timestamps and volcano references. The JSON includes
each original source record, related warning/reference, and matching canonical
row. Exact magnitude boundaries need the existing mapping tests as well:

```bash
(cd aggregator && go test -v ./internal/mapping) 2>&1 | tee evidence/1.3-boundaries.txt
```
```powershell
Push-Location aggregator
go test -v ./internal/mapping 2>&1 | ForEach-Object { "$_" } | Tee-Object ../evidence/1.3-boundaries.txt
Pop-Location
```

Capture `ExactlyFive`, `ExactlySixFive`, and warning-precedence test results.

### 1.4 Correlation ID flows across services (Ketentuan 7)

> Capture: run `step 1.4` first. Output is saved to `evidence/1.4.txt`.

```bash
CID=demo-corr-$(date +%s)
T=$(login "$FIELD_ID" "$FIELD_SECRET" "$CID" | jget access_token)
curl -s -o /dev/null -H "Authorization: Bearer $T" -H "X-Correlation-ID: $CID" "$API/hazards"
docker compose logs client-api auth-service aggregator 2>&1 | grep "$CID" | tee evidence/1.4-correlation.txt
```
```powershell
$cid = "demo-corr-$([int](Get-Date -UFormat %s))"
$t = (Login $Creds.FIELD.id $Creds.FIELD.secret $cid).access_token
$null = Hz $t '/hazards' $cid
docker compose logs client-api auth-service aggregator 2>&1 | Select-String $cid | Tee8 1.4-correlation.txt
```

**Desired state:** JSON log lines containing the **same** `correlation_id` from
at least `client-api` and `auth-service` (the login call), and `client-api` +
`aggregator` (the read), including outbound-call latency fields. **Criterion:** Ketentuan 7.

Also run `node scripts/evidence-probes.mjs health --name 1.2-all-services` and
`node scripts/evidence-probes.mjs trace --name 1.4-ingest-delivery` after fresh
events reach both consumers. Capture all container health commands/states and
shared correlation IDs for each source, Aggregator, and both consumers, with
outbound target and `latency_ms`. Postgres uses a database readiness probe;
the TUI is a CLI client rather than an HTTP service. Keep the client-request
trace above too: polling and client reads are separate request flows.

---

## Phase 2: PVMBG schema evolution (P1.2 to P1.4)

Starting state: PVMBG on the old schema (nothing toggled yet). If you re-run this
phase, first run Phase 1 again (the toggle is in-memory).

### 2.1 Record process start times (to prove "no restart")

> Capture: run `step 2.1` first. Output is saved to `evidence/2.1.txt`.

```bash
started | tee evidence/2.1-started-before.txt
```
```powershell
Started | Tee8 2.1-started-before.txt
```

**Desired state:** one line per container with its `StartedAt`.

### 2.2 Trigger the schema change while the system runs

> Capture: run `step 2.2` first. Output is saved to `evidence/2.2.txt`.

```bash
curl -s -X POST -H "Authorization: Bearer $PVMBG_ADMIN" $PVMBG/admin/schema-version
curl -s -H "Authorization: Bearer $PVMBG_TOKEN" $PVMBG/volcanic-reports | jkeys 0 | grep -c confidence_level   # => 1
```
```powershell
Invoke-RestMethod -Method Post -Uri "$PvmbgUrl/admin/schema-version" -Headers $AdminH | ConvertTo-Json -Compress
$v = Invoke-RestMethod -Uri "$PvmbgUrl/volcanic-reports" -Headers @{Authorization="Bearer $PvmbgToken"}
$v[0].PSObject.Properties.Name -contains 'confidence_level'                                                  # => True
```

**Reader prerequisite:** start with `PVMBG_READER_SCHEMA=/schemas/volcanic-v1.avsc`
(or `schemas/volcanic-v1.avsc` for native execution), and set the dedicated
`AGGREGATOR_ADMIN_TOKEN` in `aggregator/.env`. The source toggle does not change the
reader. Wait for a v2 report to be ingested; it must initially lack confidence in
`attributes`. Then activate the discovered reader from either shell:

```bash
docker compose exec -T aggregator sh -c 'wget -qO- --post-data="" --header="Authorization: Bearer ${AGGREGATOR_ADMIN_TOKEN:?}" "http://localhost:${AGGREGATOR_PORT:?}/admin/schema-version"'
# => {"reader_version":"v2"}; before discovery: HTTP 409, no change
```

Wait for a newly generated source report after activation. Previously dropped
fields are not backfilled automatically. Repeat the command to return to reader
v1 independently of the PVMBG writer. For the complete automated sequence with
before/after records and no-restart evidence, use `node scripts/evidence-probes.mjs schema`.

**Desired state:** POST answers `{"schema_version":"v2","confidence_level_visible":true}`;
PVMBG now emits `confidence_level`; its next data response contains a new report
with ID `rep-schema-<timestamp>-<activation>`. **Criterion:** P1.2.

### 2.3 Aggregator still produces valid HazardEvents, with the new field policy

> Capture: run `step 2.3` first. Output is saved to `evidence/2.3.txt`.

```bash
sleep 8
# Discover the generated ID; it is deliberately different on every activation.
NEW_REPORT=$(curl -s -H "Authorization: Bearer $PVMBG_TOKEN" $PVMBG/volcanic-reports | node -e 'let s="";process.stdin.on("data",d=>s+=d);process.stdin.on("end",()=>{const r=JSON.parse(s).filter(x=>typeof x.confidence_level==="number").sort((a,b)=>b.reported_at.localeCompare(a.reported_at));if(!r.length)process.exit(1);console.log(r[0].report_id)})')
echo "new report: $NEW_REPORT"
agg '/internal/hazards?hazard_type=VOLCANIC' | jfind data source_ref_id "$NEW_REPORT" | tee evidence/2.3-new-report.json
test "$(cat evidence/2.3-new-report.json | jlen .)" -gt 0 || { echo 'FAIL: schema report not ingested; retry after polling'; false; }
agg '/internal/hazards?hazard_type=VOLCANIC' | jget count
```
```powershell
Start-Sleep 8
$source = Invoke-RestMethod -Uri "$PvmbgUrl/volcanic-reports" -Headers @{Authorization="Bearer $PvmbgToken"}
$newReport = $source | Where-Object { $null -ne $_.confidence_level } | Sort-Object reported_at -Descending | Select-Object -First 1
if (-not $newReport) { throw 'No new schema report was returned' }
"new report: $($newReport.report_id)"
$va = Agg '/internal/hazards?hazard_type=VOLCANIC'
$mapped = $va.data | Where-Object source_ref_id -eq $newReport.report_id
if (-not $mapped) { throw 'Schema report not yet ingested; retry after the configured polling interval' }
$mapped | ConvertTo-Json -Depth 6 | Tee8 2.3-new-report.json
$va.count
```

**Desired state:**
- The newly generated report after reader activation exists as a canonical row with `attributes.confidence_level` matching its source value.
- Its top-level keys are the **same canonical set** as in 1.3 (no new top-level column, so the field went to `attributes`).
- Count is higher than the pre-toggle count. Do not expect exactly one more: the toggle adds a schema report, and the mock keeps generating a report every 10 s.
- Reports generated after the toggle (`rep-<boot>-<n>`) also carry `attributes.confidence_level`.
- Earlier rows keep their identity; schema activation does not proactively rewrite historic records. Records returned again within the polling overlap or replayed under v2 may acquire accepted attributes. New rows with confidence coexist with old rows without it.

**Declared policy (state it in the report):** fields absent from the selected
reader are **dropped**. Source fields accepted by both schemas without a canonical
destination are **passed through to `attributes`**. Reader-only defaults are omitted. **Criterion:** P1.3, P1.4.

For an asserted sequence, use the following **instead of** manual steps 2.1–2.4,
while PVMBG is still on v1. Do not toggle manually and then run this command:

```text
node scripts/evidence-probes.mjs schema --name 2-schema
```

It saves `schema-before.json`, `schema-after.json`, and `2.3-new-report.json`;
discovers the actual new report ID; verifies the canonical fields and new
attribute; compares container IDs/start times/restart counts; and compares
database column catalogs across the toggle. Capture its PASS/FAIL summary.

### 2.4 No crash, no restart, no manual code change

> Capture: run `step 2.4` first. Output is saved to `evidence/2.4.txt`.

```bash
started > evidence/2.4-started-after.txt
diff evidence/2.1-started-before.txt evidence/2.4-started-after.txt && echo "NO RESTART"
docker compose logs aggregator --since 3m 2>&1 | grep -ci 'contract violation'      # => 0
docker compose ps --format '{{.Service}} {{.Health}}' | grep -v healthy || echo "ALL HEALTHY"
```
```powershell
Started | Set-Content evidence/2.4-started-after.txt -Encoding utf8
if (-not (Compare-Object (Get-Content evidence/2.1-started-before.txt) (Get-Content evidence/2.4-started-after.txt))) { 'NO RESTART' }
(docker compose logs aggregator --since 3m 2>&1 | Select-String 'contract violation').Count               # => 0
docker compose ps --format '{{.Service}} {{.Health}}' | Where-Object { $_ -notmatch 'healthy' }          # => (nothing)
```

**Desired state:** `NO RESTART` (all `StartedAt` identical), `0` contract
violations, nothing unhealthy. **Criterion:** P1.2, P1.3.

---

## Phase 3: Trust boundaries & auth (P3.1 to P3.4)

### 3.1 Credentials are isolated per institution (P3.1)

> Capture: run `step 3.1` first. Output is saved to `evidence/3.1.txt`.

Automated, one PASS/FAIL line per probe (Linux, macOS, or Git Bash):

```bash
BMKG_URL="$BMKG" PVMBG_URL="$PVMBG" scripts/isolation-demo.sh | tee 3.1-isolation.txt
```

On Windows, use PowerShell:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/isolation-demo.ps1 -BmkgUrl $BmkgUrl -PvmbgUrl $PvmbgUrl | Tee8 3.1-isolation.txt
```

Manual equivalent:

```bash
echo "own BMKG key  -> BMKG  : $(code -H "X-BMKG-Key: $BMKG_KEY" $BMKG/seismic-events)"                       # 200
echo "own PVMBG tok -> PVMBG : $(code -H "Authorization: Bearer $PVMBG_TOKEN" $PVMBG/volcanic-reports)"       # 200
echo "BMKG key  -> PVMBG     : $(code -H "Authorization: Bearer $BMKG_KEY" $PVMBG/volcanic-reports)"          # 401/403
echo "PVMBG tok -> BMKG      : $(code -H "X-BMKG-Key: $PVMBG_TOKEN" $BMKG/seismic-events)"                    # 401/403
echo "BMKG key  -> PVMBG adm : $(code -X POST -H "Authorization: Bearer $BMKG_KEY" $PVMBG/admin/outage)"     # 401/403
echo "PVMBG data tok -> adm  : $(code -X POST -H "Authorization: Bearer $PVMBG_TOKEN" $PVMBG/admin/outage)"  # 401/403
echo "no credential BMKG     : $(code $BMKG/seismic-events)"                                                  # 401
echo "no credential PVMBG    : $(code $PVMBG/volcanic-reports)"                                               # 401
```
```powershell
"own BMKG key  -> BMKG  : $(Code "$BmkgUrl/seismic-events" 'GET' @{'X-BMKG-Key'=$BmkgKey})"                                   # 200
"own PVMBG tok -> PVMBG : $(Code "$PvmbgUrl/volcanic-reports" 'GET' @{Authorization="Bearer $PvmbgToken"})"                   # 200
"BMKG key  -> PVMBG     : $(Code "$PvmbgUrl/volcanic-reports" 'GET' @{Authorization="Bearer $BmkgKey"})"                      # 401/403
"PVMBG tok -> BMKG      : $(Code "$BmkgUrl/seismic-events" 'GET' @{'X-BMKG-Key'=$PvmbgToken})"                                # 401/403
"BMKG key  -> PVMBG adm : $(Code "$PvmbgUrl/admin/outage" 'POST' @{Authorization="Bearer $BmkgKey"})"                         # 401/403
"PVMBG data tok -> adm  : $(Code "$PvmbgUrl/admin/outage" 'POST' @{Authorization="Bearer $PvmbgToken"})"                      # 401/403
"no credential BMKG     : $(Code "$BmkgUrl/seismic-events")"                                                                  # 401
"no credential PVMBG    : $(Code "$PvmbgUrl/volcanic-reports")"                                                               # 401
```

**Desired state:** own credentials `200`; every cross-institution, admin-with-data-token, and
no-credential probe `401` or `403`; script ends `RESULT: PASS`. **Criterion:** P3.1.

### 3.2 Three genuinely different client identities

> Capture: run `step 3.2` first. Output is saved to `evidence/3.2.txt`.

```bash
MEDIA_J=$(login "$MEDIA_ID" "$MEDIA_SECRET");  FIELD_J=$(login "$FIELD_ID" "$FIELD_SECRET");  INT_J=$(login "$INTERNAL_ID" "$INTERNAL_SECRET")
echo "$MEDIA_J" | jget role;  echo "$FIELD_J" | jget role;  echo "$INT_J" | jget role      # media / field / internal
MEDIA_AT=$(echo "$MEDIA_J" | jget access_token); FIELD_AT=$(echo "$FIELD_J" | jget access_token); INT_AT=$(echo "$INT_J" | jget access_token)
echo "media id + field secret: $(code -X POST $API/auth/login -H 'Content-Type: application/json' -d "{\"client_id\":\"$MEDIA_ID\",\"client_secret\":\"$FIELD_SECRET\"}")"   # 401
```
```powershell
$Media = Login $Creds.MEDIA.id $Creds.MEDIA.secret; $Field = Login $Creds.FIELD.id $Creds.FIELD.secret; $Internal = Login $Creds.INTERNAL.id $Creds.INTERNAL.secret
$Media.role; $Field.role; $Internal.role                                                  # media / field / internal
"media id + field secret: $(Code "$API/auth/login" 'POST' @{} (@{client_id=$Creds.MEDIA.id;client_secret=$Creds.FIELD.secret}|ConvertTo-Json -Compress))"   # 401
```

**Desired state:** roles `media`, `field`, `internal` assigned **by the server**;
mixing one identity's ID with another's secret returns `401`. **Criterion:** P3 requirement 1.

### 3.3 Media sees only "Ringkasan" fields; raw is refused server-side (P3.2)

> Capture: run `step 3.3` first. Output is saved to `evidence/3.3.txt`.

```bash
echo "media keys:";    hz "$MEDIA_AT" '/hazards' | jkeys data.0
echo "field keys:";    hz "$FIELD_AT" '/hazards' | jkeys data.0
echo "internal keys:"; hz "$INT_AT"   '/hazards' | jkeys data.0
echo "media ?view=raw         : $(code -H "Authorization: Bearer $MEDIA_AT" "$API/hazards?view=raw")"                     # 403
HID=$(hz "$MEDIA_AT" '/hazards' | jget data.0.hazard_id)
echo "media detail            : $(code -H "Authorization: Bearer $MEDIA_AT" "$API/hazards/$HID")"                           # 200
echo "media detail ?view=raw  : $(code -H "Authorization: Bearer $MEDIA_AT" "$API/hazards/$HID?view=raw")"                  # 403
echo "no token                : $(code "$API/hazards")"                                                                      # 401
echo "media -> internal path  : $(code -H "Authorization: Bearer $MEDIA_AT" "$API/internal/hazards")"                        # must NOT be 200
hz "$MEDIA_AT" "/hazards/$HID" | grep -ciE 'latitude|longitude|attributes|source_ref_id'                                    # => 0
```
```powershell
'media keys:';    (Hz $Media.access_token '/hazards').data[0].PSObject.Properties.Name
'field keys:';    (Hz $Field.access_token '/hazards').data[0].PSObject.Properties.Name
'internal keys:'; (Hz $Internal.access_token '/hazards').data[0].PSObject.Properties.Name
$mh = @{Authorization="Bearer $($Media.access_token)"}
"media ?view=raw        : $(Code "$API/hazards?view=raw" 'GET' $mh)"                                         # 403
$hid = (Hz $Media.access_token '/hazards').data[0].hazard_id
"media detail           : $(Code "$API/hazards/$hid" 'GET' $mh)"                                             # 200
"media detail ?view=raw : $(Code "$API/hazards/$hid`?view=raw" 'GET' $mh)"                                   # 403
"no token               : $(Code "$API/hazards")"                                                            # 401
"media -> internal path : $(Code "$API/internal/hazards" 'GET' $mh)"                                         # must NOT be 200
(Invoke-WebRequest -UseBasicParsing -Uri "$API/hazards/$hid" -Headers $mh).Content | Select-String 'latitude|longitude|attributes|source_ref_id'   # => (nothing)
```

**Desired state:**
- **media keys** = exactly `hazard_id, source, hazard_type, severity, area_name, occurred_at, ingested_at`.
- **field / internal keys** additionally include `source_ref_id, latitude, longitude, attributes`.
- `?view=raw` as Media returns `403` on both list and detail. It is rejected, not hidden.
- No token returns `401`; the Aggregator's `/internal/...` path is not reachable through the public API.

**Criterion:** P3.2.

### 3.4 Token expiry and refresh without manual re-login (P3.3)

> Capture: run `step 3.4` first. Output is saved to `evidence/3.4.txt`.

This step takes about 65 seconds on purpose (configured TTL 60 s). Run it as one block.

```bash
L=$(login "$FIELD_ID" "$FIELD_SECRET")
AT=$(echo "$L" | jget access_token); RT=$(echo "$L" | jget refresh_token); TTL=$(echo "$L" | jget expires_in)
echo "t=0   read with fresh token : $(code -H "Authorization: Bearer $AT" $API/hazards)"            # 200
sleep $((TTL+1))
echo "t=TTL read with expired tok : $(code -H "Authorization: Bearer $AT" $API/hazards)"            # 401
R=$(curl -s -X POST $API/auth/refresh -H 'Content-Type: application/json' -d "{\"refresh_token\":\"$RT\"}")
AT2=$(echo "$R" | jget access_token); RT2=$(echo "$R" | jget refresh_token)
echo "new access token          : $(code -H "Authorization: Bearer $AT2" $API/hazards)"            # 200
echo "old access after refresh  : $(code -H "Authorization: Bearer $AT"  $API/hazards)"            # 401
echo "old refresh token reuse   : $(code -X POST $API/auth/refresh -H 'Content-Type: application/json' -d "{\"refresh_token\":\"$RT\"}")"   # 401/403
# Old token rejected immediately after refresh, even before natural expiry:
L=$(login "$FIELD_ID" "$FIELD_SECRET"); A=$(echo "$L" | jget access_token); B=$(echo "$L" | jget refresh_token)
curl -s -o /dev/null -X POST $API/auth/refresh -H 'Content-Type: application/json' -d "{\"refresh_token\":\"$B\"}"
echo "old access, no waiting    : $(code -H "Authorization: Bearer $A" $API/hazards)"              # 401
```
```powershell
$L = Login $Creds.FIELD.id $Creds.FIELD.secret
$h  = { param($t) @{Authorization="Bearer $t"} }
"t=0   read with fresh token : $(Code "$API/hazards" 'GET' (& $h $L.access_token))"               # 200
Start-Sleep ([int]$L.expires_in + 1)
"t=TTL read with expired tok : $(Code "$API/hazards" 'GET' (& $h $L.access_token))"               # 401
$R = Invoke-RestMethod -Method Post -Uri "$API/auth/refresh" -ContentType 'application/json' -Body (@{refresh_token=$L.refresh_token}|ConvertTo-Json -Compress)
"new access token          : $(Code "$API/hazards" 'GET' (& $h $R.access_token))"                 # 200
"old access after refresh  : $(Code "$API/hazards" 'GET' (& $h $L.access_token))"                 # 401
"old refresh token reuse   : $(Code "$API/auth/refresh" 'POST' @{} (@{refresh_token=$L.refresh_token}|ConvertTo-Json -Compress))"   # 401/403
$L2 = Login $Creds.FIELD.id $Creds.FIELD.secret
$null = Invoke-RestMethod -Method Post -Uri "$API/auth/refresh" -ContentType 'application/json' -Body (@{refresh_token=$L2.refresh_token}|ConvertTo-Json -Compress)
"old access, no waiting    : $(Code "$API/hazards" 'GET' (& $h $L2.access_token))"                # 401
```

**Desired state:** the exact sequence `200 -> 401 -> (refresh) 200 -> old 401`,
the reused refresh token is rejected (rotation), and an old access token is
dead right after a refresh without waiting for TTL. **Criterion:** P3.3.

Visual alternative for the demo: `docker compose run --rm --build client-tui`,
log in as Field, wait over 60 s, list hazards. The TUI refreshes by itself and
"Uji token lama" confirms the old token gets `401`.

Full automated variant (Linux, macOS, or Git Bash, ~70 s):

```bash
API_URL="$API" BMKG_URL="$BMKG" PVMBG_URL="$PVMBG" scripts/smoke.sh --restart-auth --wait-for-expiry --test-outage
```

On Windows, use PowerShell:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/smoke.ps1 -RestartAuth -WaitForExpiry -TestOutage -ApiUrl $API -BmkgUrl $BmkgUrl -PvmbgUrl $PvmbgUrl
```
Desired: final line `PASS: ...`. The outage option restores PVMBG afterward.

### 3.5 No hardcoded or committed credentials (P3.4)

> Capture: run `step 3.5` first. Output is saved to `evidence/3.5.txt`.

```bash
git ls-files | grep -E '(^|/)\.env$' || echo "no .env tracked"                 # => no .env tracked
test -f .env.example && echo ".env.example present"
for v in "$BMKG_KEY" "$PVMBG_TOKEN" "$PVMBG_ADMIN" "$MEDIA_SECRET" "$FIELD_SECRET" "$INTERNAL_SECRET"; do
  git grep -nF -e "$v" -- '*.go' '*.yml' '*.yaml' '*Dockerfile*' '*.sh' '*.js' | sed 's/^/HARDCODED: /'
  git log --oneline -S"$v" -- . ':!.env.example' ':!README.md' ':!docs' | sed 's/^/IN HISTORY: /'
done; echo "scan done"
```
```powershell
git ls-files | Select-String '(^|/)\.env$'                                            # expect no output
Test-Path .env.example                                                                  # => True
$secrets = $BmkgKey,$PvmbgToken,$PvmbgAdmin,$Creds.MEDIA.secret,$Creds.FIELD.secret,$Creds.INTERNAL.secret
foreach ($v in $secrets) {
  git grep -nF -e $v -- '*.go' '*.yml' '*.yaml' '*Dockerfile*' '*.sh' '*.js'
  git log --oneline "-S$v" -- . ':!.env.example' ':!README.md' ':!docs'
}; 'scan done'
```

**Desired state:** no tracked `.env`; `.env.example` present; the scan prints **no**
`HARDCODED:` / `IN HISTORY:` lines (matches only in `.env.example`/README placeholders are
acceptable and are excluded). If you used the example placeholder values as your real
secrets, matches in README are expected, but none may appear in code, compose, or Dockerfiles.
**Criterion:** P3.4.

For the broader scan, run `node scripts/evidence-probes.mjs secrets --name 3.5-secrets`.
This examines all tracked working files and reachable historical blobs for
current configured credentials, internal/signing keys, database passwords,
private-key markers, and potential literal secret assignments. Findings print
paths/object IDs with values redacted. It also rejects tracked real env files.
Review findings and any former secret values separately: exact-value and
heuristic scans are evidence within their stated scope, not a guarantee that
every possible former secret is absent. Do not save unredacted scanner output.

---

## Phase 4: Pub/sub fan-out (P5.1 to P5.4)

Starting state: system healthy, schema toggle currently ON (v2) from Phase 2.
Leave the toggle alone in this phase: turning it off would strip
`confidence_level` from stored rows and spoil the evidence for 5.3.

No step here triggers an event by hand. Both mocks emit one every 10 s, so
events keep flowing and **record counts are not comparable** between two
moments (`/records` also shows only the latest 100). Each step therefore
follows one specific `hazard_id`: `latest` / `Latest` reads the newest hazard
from the Aggregator, and `wait_has` / `Wait-Has` waits up to 60 s for that ID
to show up in a consumer.

### 4.1 Two independent durable subscribers on one stream (P5.1, P5.2)

> Capture: run `step 4.1` first. Output is saved to `evidence/4.1.txt`.

```bash
nats stream info HAZARDS --json | jget config.subjects          # ["hazard.events"]
consreport | tee evidence/4.1-consumers.txt
```
```powershell
(Nats stream info HAZARDS --json | Out-String | ConvertFrom-Json).config.subjects       # hazard.events
Consreport | Tee8 4.1-consumers.txt
```

**Desired state:** stream `HAZARDS` on subject `hazard.events`; two consumers
with **different durable names** (`dashboard-v1`, `notifier-v1`), both with
`Unprocessed/Pending` at 0 (a 1 can appear for a moment when an event has just arrived). The Aggregator publishes to the broker only; it never
references the consumers. **Criterion:** P5.1, P5.2.

### 4.2 One published event reaches both consumers

> Capture: run `step 4.2` first. Output is saved to `evidence/4.2.txt`.

```bash
H0=$(latest); sleep 12; H=$(latest); echo "previous=$H0 new=$H"            # different: a new event arrived on its own
wait_has consumer-dashboard "$H"; wait_has consumer-notifier "$H"
CID=$(rec consumer-dashboard | jfind data hazard_id "$H" | jget 0.correlation_id); echo "correlation_id=$CID"
docker compose logs aggregator 2>&1 | grep "$CID" | tail -3 | tee evidence/4.2-correlation.txt
```
```powershell
$h0 = Latest; Start-Sleep 12; $h = Latest; "previous=$h0 new=$h"            # different: a new event arrived on its own
Wait-Has consumer-dashboard $h; Wait-Has consumer-notifier $h
$cid = ((Rec consumer-dashboard).data | Where-Object hazard_id -eq $h).correlation_id; "correlation_id=$cid"
docker compose logs aggregator 2>&1 | Select-String $cid | Select-Object -Last 3 | Tee8 4.2-correlation.txt
```

**Desired state:** `new` differs from `previous`, so a fresh event was published
without any manual trigger; both consumers print `... has <hazard_id>` for that
**same** ID; the event's `correlation_id` from the consumer also appears in the
Aggregator log (its poll and publish lines). A `MISSING` line is a failure.
**Criterion:** P5.2.

### 4.3 Stop one consumer: producer and the other consumer carry on (P5.3)

> Capture: run `step 4.3` first. Output is saved to `evidence/4.3.txt`.

```bash
docker compose stop consumer-notifier
sleep 25; MISSED=$(latest); echo "published while notifier is down: $MISSED"   # several events arrive in 25 s
wait_has consumer-dashboard "$MISSED"                                      # dashboard not blocked
sql "SELECT count(*) AS outbox_pending FROM hazard_outbox;"                     # 0  (producer not stuck)
docker compose ps --format '{{.Service}} {{.Health}}' | grep -E 'aggregator|dashboard'
consreport | tee evidence/4.3-consumers-during.txt            # notifier-v1 Unprocessed > 0
docker compose logs aggregator --since 1m 2>&1 | grep -ci '"level":"error"'     # => 0
```
```powershell
docker compose stop consumer-notifier
Start-Sleep 25; $missed = Latest; "published while notifier is down: $missed"   # several events arrive in 25 s
Wait-Has consumer-dashboard $missed                                         # dashboard not blocked
Sql "SELECT count(*) AS outbox_pending FROM hazard_outbox;"                      # 0
docker compose ps --format '{{.Service}} {{.Health}}' | Select-String 'aggregator|dashboard'
Consreport | Tee8 4.3-consumers-during.txt      # notifier-v1 Unprocessed > 0
(docker compose logs aggregator --since 1m 2>&1 | Select-String '"level":"error"').Count   # => 0
```

**Desired state:** the dashboard has the event published during the stop;
`hazard_outbox` is empty, or shows 1 for a moment if an event was just inserted
(the Aggregator publishes without waiting for the dead consumer); Aggregator healthy
with zero error logs; `notifier-v1` shows `Unprocessed` above 0 and growing, because the
broker keeps those events for its durable cursor. Keep `MISSED` / `$missed` for 4.4.
**Criterion:** P5.3 (first half).

### 4.4 Restart it: backlog is delivered (no loss)

> Capture: run `step 4.4` first. Output is saved to `evidence/4.4.txt`.

```bash
docker compose start consumer-notifier
until docker compose exec -T consumer-notifier sh -c 'wget -q --spider "http://localhost:${CONSUMER_PORT:?}/ready"'; do sleep 2; done
wait_has consumer-notifier "$MISSED"                                       # the event it missed is delivered
consreport | tee evidence/4.4-consumers-after.txt            # notifier-v1 Unprocessed back to 0 (or 1)
```
```powershell
docker compose start consumer-notifier
do { docker compose exec -T consumer-notifier sh -c 'wget -q --spider "http://localhost:${CONSUMER_PORT:?}/ready"'; $readyExit = $LASTEXITCODE; if ($readyExit -ne 0) { Start-Sleep 2 } } until ($readyExit -eq 0)
Wait-Has consumer-notifier $missed                                          # the event it missed is delivered
Consreport | Tee8 4.4-consumers-after.txt
```

**Desired state:** notifier **catches up**: it now has the `hazard_id` that was
published while it was down, and its `Unprocessed` drops back to 0 (a 1 can appear
for a moment when a new event has just arrived). Events were **not lost** because JetStream retains messages
for `NATS_STREAM_MAX_AGE` (24 h) and the durable cursor resumes where it stopped.
Semantics to state in the report: **at-least-once**, so consumers dedupe by
`hazard_id` / stream sequence (the SQLite `records.hazard_id` primary key does this).
**Criterion:** P5.3 (second half), P5.5.

### 4.5 Add a third consumer without touching the producer (P5.4)

> Capture: run `step 4.5` first. Output is saved to `evidence/4.5.txt`.

```bash
started > evidence/4.5-started-before.txt
# Pre-flight: remove any demo container/volume left over from an earlier run (see the warning below)
docker compose --profile consumer-demo rm -sf consumer-dashboard-demo
docker volume rm $(docker volume ls -q -f name=dashboard-demo-data) 2>/dev/null || true
docker compose --profile consumer-demo up -d --force-recreate consumer-dashboard-demo
for i in $(seq 1 30); do                                    # bounded wait, max ~60 s
  st=not_ready
  if docker compose exec -T consumer-dashboard-demo sh -c 'wget -q --spider "http://localhost:${CONSUMER_PORT:?}/ready"'; then st=ready; break; fi
  sleep 2
done
[ "$st" = ready ] || { echo "demo consumer NOT ready ($st):"; docker compose --profile consumer-demo logs --tail 5 consumer-dashboard-demo; }
sleep 12; H=$(latest); echo "event published after the third consumer joined: $H"
for s in consumer-dashboard consumer-notifier consumer-dashboard-demo; do wait_has $s "$H"; done   # all three have it
echo "demo consumer records: $(rec consumer-dashboard-demo | jget count)"                                          # history replayed
started | grep -E 'aggregator' ; grep aggregator evidence/4.5-started-before.txt                                      # same StartedAt
consreport
```
```powershell
Started | Set-Content evidence/4.5-started-before.txt -Encoding utf8
# Pre-flight: remove any demo container/volume left over from an earlier run (see the warning below)
docker compose --profile consumer-demo rm -sf consumer-dashboard-demo
docker volume ls -q -f name=dashboard-demo-data | ForEach-Object { docker volume rm $_ }
docker compose --profile consumer-demo up -d --force-recreate consumer-dashboard-demo
for ($i = 0; $i -lt 30; $i++) {                              # bounded wait, max ~60 s
  $st = 'not_ready'; docker compose exec -T consumer-dashboard-demo sh -c 'wget -q --spider "http://localhost:${CONSUMER_PORT:?}/ready"'
  if ($LASTEXITCODE -eq 0) { $st = 'ready'; break }; Start-Sleep 2 }
if ($st -ne 'ready') { "demo consumer NOT ready ($st):"; docker compose --profile consumer-demo logs --tail 5 consumer-dashboard-demo }
Start-Sleep 12; $h = Latest; "event published after the third consumer joined: $h"
foreach ($s in 'consumer-dashboard','consumer-notifier','consumer-dashboard-demo') { Wait-Has $s $h }   # all three have it
"demo consumer records: $((Rec consumer-dashboard-demo).count)"                                              # history replayed
Started | Select-String aggregator; Select-String aggregator evidence/4.5-started-before.txt                    # same StartedAt
Consreport
```

> **Warning: leftover demo container / volume.** `consumer-dashboard-demo` belongs to the
> `consumer-demo` Compose profile, so a plain `docker compose down` / `down -v` does **not** remove it
> or its volume `dashboard-demo-data`. A leftover volume holds the SQLite file from the previous
> broker, so the new durable replays stream sequence 1 with different bytes and the consumer **halts**
> (`consumer_incompatible ... HAZARDS/1 was reused with different bytes`, `/ready` = 503, container
> still alive). A leftover container from a deleted network fails instead with `network ... not found`
> (`Exited (128)`). The pre-flight above removes both. Tear down with
> `docker compose --profile consumer-demo down -v --remove-orphans`.

**Desired state:** a third durable (`dashboard-demo-v1`) appears; all three consumers
print `... has <hazard_id>` for the **same** event published after it joined; the
Aggregator's `StartedAt` is unchanged and no code/config of the producer was edited.
The new consumer starts from the beginning of the stream, so it first replays the
retained history and its record count is far above 1. `wait_has` allows for that:
the new ID shows up once the replay has caught up, so do not compare the newest
row across consumers at one instant. **Criterion:** P5.4.

Optional cleanup of the demo consumer: `docker compose --profile consumer-demo stop consumer-dashboard-demo`.

---

## Phase 5: Independent services & flexible storage (P4.1 to P4.3)

### 5.1 One container per component (P4.1, part 1)

> Capture: run `step 5.1` first. Output is saved to `evidence/5.1.txt`.

```bash
docker compose ps --format 'table {{.Service}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}' | tee evidence/5.1-containers.txt
```
```powershell
docker compose ps --format 'table {{.Service}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}' | Tee8 5.1-containers.txt
```

**Desired state:** every component is its own container/image. Host-published ports are
only `18080` (client-api), `18081`, `18082`, and `127.0.0.1:13000` (Grafana) with the template settings. Broker and Schema Registry ports are internal only. Host ports must be set in the root `.env`.
`postgres`, `aggregator`, `auth-service`, and both consumers publish **no** host port.

### 5.2 Stop, rebuild, restart ONE container, nothing else restarts (P4.1, part 2)

> Capture: run `step 5.2` first. Output is saved to `evidence/5.2.txt`.

Use two terminals. First make a trivial edit (e.g. a comment in
`consumer-notifier/main.go`) so a **new image** is built.

**Terminal A: continuous probe of a path that does not need the notifier**

```bash
while true; do T=$(login "$FIELD_ID" "$FIELD_SECRET" | jget access_token)
  echo "$(date +%T) /hazards=$(code -H "Authorization: Bearer $T" $API/hazards) /health=$(code $API/health)"; sleep 2; done | tee evidence/5.2-probe.txt
```
```powershell
& { while ($true) { $t = (Login $Creds.FIELD.id $Creds.FIELD.secret).access_token
  "$(Get-Date -f HH:mm:ss) /hazards=$(Code "$API/hazards" 'GET' @{Authorization="Bearer $t"}) /health=$(Code "$API/health")"; Start-Sleep 2 } } | Tee8 5.2-probe.txt
```

**Terminal B: rebuild only the notifier**

```bash
started > evidence/5.2-started-before.txt; docker compose images consumer-notifier
docker compose stop consumer-notifier
docker compose build consumer-notifier
docker compose up -d --no-deps consumer-notifier
docker compose images consumer-notifier; started > evidence/5.2-started-after.txt
diff evidence/5.2-started-before.txt evidence/5.2-started-after.txt          # ONLY consumer-notifier differs
```
```powershell
Started | Set-Content evidence/5.2-started-before.txt -Encoding utf8; docker compose images consumer-notifier
docker compose stop consumer-notifier
docker compose build consumer-notifier
docker compose up -d --no-deps consumer-notifier
docker compose images consumer-notifier; Started | Set-Content evidence/5.2-started-after.txt -Encoding utf8
Compare-Object (Get-Content evidence/5.2-started-before.txt) (Get-Content evidence/5.2-started-after.txt)   # ONLY consumer-notifier
```

**Desired state:**
- Terminal A prints `200 / 200` on every line for the whole rebuild (no gap, no `000`/`5xx`).
- Image ID of `consumer-notifier` **changes**; only its `StartedAt` differs in the diff. All other containers keep theirs.
- After restart the notifier is healthy again and catches up (as in 4.4).

Stop Terminal A with Ctrl+C. **Criterion:** P4.1.

### 5.3 Dynamic attributes stored without a migration (P4.2)

> Capture: run `step 5.3` first. Output is saved to `evidence/5.3.txt`.

```bash
sql "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='hazard_events' ORDER BY ordinal_position;"
sql "SELECT source_ref_id, (attributes ? 'confidence_level') AS has_confidence, attributes->>'confidence_level' AS confidence FROM hazard_events WHERE hazard_type='VOLCANIC' ORDER BY ingested_at;" | tee evidence/5.3-side-by-side.txt
sql "SELECT hazard_type, (attributes ? 'confidence_level') AS has_confidence, count(*) FROM hazard_events GROUP BY 1,2 ORDER BY 1,2;"
git ls-files | grep -iE 'migrat' || echo "no migration files"
git log --oneline -- store/init.sql                                      # no commit/edit tied to the schema toggle
```
```powershell
Sql "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='hazard_events' ORDER BY ordinal_position;"
Sql "SELECT source_ref_id, (attributes ? 'confidence_level') AS has_confidence, attributes->>'confidence_level' AS confidence FROM hazard_events WHERE hazard_type='VOLCANIC' ORDER BY ingested_at;" | Tee8 5.3-side-by-side.txt
Sql "SELECT hazard_type, (attributes ? 'confidence_level') AS has_confidence, count(*) FROM hazard_events GROUP BY 1,2 ORDER BY 1,2;"
git ls-files | Select-String -Pattern 'migrat'; 'checked migrations'
git log --oneline -- store/init.sql
```

**Desired state:**
- Column list is the fixed set from `store/init.sql` and `attributes` is type `jsonb`. No `confidence_level` column was ever added.
- Rows **with** and **without** `confidence_level` coexist in one table; seismic rows never have it, and `rep-schema-<boot>-<activation>` rows have it.
- No migration files; `init.sql` untouched by the schema change.
- Both old and new rows are still readable through the API: `agg '/internal/hazards?hazard_type=VOLCANIC' | jget count` returns every row.

If every pre-existing volcanic row now shows the field, a saved snapshot alone
does not prove coexistence. Read an old hazard by its pre-toggle `hazard_id`
and the newly generated schema hazard from the current API/store. After the
asserted schema sequence above, run:

```text
node scripts/evidence-probes.mjs storage --name 5.3-storage
```

Capture both IDs, their current attributes, the JSONB check, and unchanged
column-catalog check. The JSON includes the original old snapshot alongside
both current API rows. Old records may acquire additive fields; their identity
and readability must still be preserved.
**Criterion:** P4.2.

### 5.4 Only the Aggregator can reach the Canonical Store (P4.3)

> Capture: run `step 5.4` first. Output is saved to `evidence/5.4.txt`.

```bash
docker network ls -q -f name=_store | xargs docker network inspect -f '{{range .Containers}}{{.Name}} {{end}}'    # aggregator + postgres ONLY
docker compose ps --format '{{.Service}}: {{.Ports}}' | grep postgres                                            # no host port
for s in client-api auth-service consumer-dashboard; do echo "== $s"; docker compose exec -T $s nslookup postgres 2>&1 | tail -2; done   # cannot resolve
echo "== aggregator"; docker compose exec -T aggregator nslookup postgres 2>&1 | tail -2                           # resolves
echo "client-api has no store route: $(code -H "Authorization: Bearer $FIELD_AT" $API/hazards)"                    # 200 via Aggregator API
```
```powershell
docker network ls -q -f name=_store | ForEach-Object { docker network inspect -f '{{range .Containers}}{{.Name}} {{end}}' $_ }    # aggregator + postgres ONLY
docker compose ps --format '{{.Service}}: {{.Ports}}' | Select-String postgres                                                    # no host port
foreach ($s in 'client-api','auth-service','consumer-dashboard') { "== $s"; docker compose exec -T $s nslookup postgres 2>&1 | Select-Object -Last 2 }   # cannot resolve
'== aggregator'; docker compose exec -T aggregator nslookup postgres 2>&1 | Select-Object -Last 2                                   # resolves
```

**Desired state:** the `store` network contains only `postgres` and `aggregator`;
Postgres publishes no host port; every other service **fails to resolve** `postgres`
while the Aggregator resolves it; downstream data still flows through the Aggregator's
API (reads return `200`). **Criterion:** P4.3, Ketentuan 2.

(`FIELD_AT` is from 3.2; re-login if it expired. In PowerShell use
`(Hz $Field.access_token '/hazards').count`.)

---

## Phase 6: Concurrency, availability, graceful degradation (P2.1 to P2.3)

This phase edits `pvmbg-mock/.env` and recreates `pvmbg-mock`, which **resets its in-memory
toggles** (schema back to v1, outage off). Phase 7 restores the original values.

### 6.0 Force PVMBG to a fixed 3 s delay

> Capture: run `step 6.0` first. Output is saved to `evidence/6.0.txt`.

```bash
ORIG_MIN=$(envget pvmbg-mock/.env PVMBG_MIN_DELAY_MS); ORIG_MAX=$(envget pvmbg-mock/.env PVMBG_MAX_DELAY_MS); echo "original: $ORIG_MIN..$ORIG_MAX"
sed -i.bak -e 's/^PVMBG_MIN_DELAY_MS=.*/PVMBG_MIN_DELAY_MS=3000/' -e 's/^PVMBG_MAX_DELAY_MS=.*/PVMBG_MAX_DELAY_MS=3000/' pvmbg-mock/.env && rm -f pvmbg-mock/.env.bak
docker compose up -d --force-recreate --no-deps pvmbg-mock
sleep 5
curl -s -o /dev/null -w 'PVMBG direct: %{time_total}s\n' -H "Authorization: Bearer $PVMBG_TOKEN" $PVMBG/volcanic-reports   # ~3.0
curl -s -o /dev/null -w 'BMKG  direct: %{time_total}s\n' -H "X-BMKG-Key: $BMKG_KEY" $BMKG/seismic-events                 # ~0.05-0.15
```
```powershell
$OrigMin = Get-EnvVal pvmbg-mock/.env PVMBG_MIN_DELAY_MS; $OrigMax = Get-EnvVal pvmbg-mock/.env PVMBG_MAX_DELAY_MS; "original: $OrigMin..$OrigMax"
(Get-Content pvmbg-mock/.env) -replace '^PVMBG_MIN_DELAY_MS=.*','PVMBG_MIN_DELAY_MS=3000' -replace '^PVMBG_MAX_DELAY_MS=.*','PVMBG_MAX_DELAY_MS=3000' | Set-Content pvmbg-mock/.env
docker compose up -d --force-recreate --no-deps pvmbg-mock
Start-Sleep 5
"PVMBG direct: {0:n2}s" -f (Measure-Command { Invoke-WebRequest -UseBasicParsing -Uri "$PvmbgUrl/volcanic-reports" -Headers @{Authorization="Bearer $PvmbgToken"} | Out-Null }).TotalSeconds
"BMKG  direct: {0:n2}s" -f (Measure-Command { Invoke-WebRequest -UseBasicParsing -Uri "$BmkgUrl/seismic-events" -Headers @{'X-BMKG-Key'=$BmkgKey} | Out-Null }).TotalSeconds
```

**Desired state:** PVMBG answers in about 3.0 s, BMKG in 0.05 to 0.15 s. Only `pvmbg-mock`
was recreated. (Edit the file by hand if you prefer; the mock reads its delay from the file, not the shell.)
**Keep `evidence/6.0.txt`**: it is the only proof that 6.1 ran against a 3 s PVMBG delay. If the
PVMBG line shows anything other than about 3 s, stop and fix it before running k6.

P2.1, P2.2, and P2.3 are independent acceptance scenarios. The assignment
requires ≥50 connections for ≥60 seconds only in P2.2. Section 6.1 below uses a
stricter mixed-load workload. To measure P2.1 separately, after verifying the
actual 3-second PVMBG delay in 6.0, run:

```bash
python3 scripts/p2-profile.py mixed --purpose isolation --project YOUR_PROJECT --vus 2 --duration 60s --output evidence/p2-isolation
```

One VU requests SEISMIC and the other VOLCANIC through Client API. The 60-second
window is chosen to collect percentile samples; it is not a P2.1 requirement.
Keep the successful SEISMIC p95 <300 ms and nonzero successful counts for both
types. Do not combine percentiles from Aggregator logs and Client API requests.
Run 6.2 separately; its acceptance gates have no latency ceiling. Run outage
recovery separately using section 6.3. See [Schema validation](Schema%20Validation.md)
for live/history type and constraint regression checks.

### 6.1 P2.1: BMKG-only stays fast while PVMBG is slow

Client API uses example capacities of 128 globally and 32 per verified
`client_id`. Both configuration variables are required; `0` disables the
respective cap. Global rejection occurs before Auth, whereas per-client
rejection occurs after Auth and before the Aggregator. All VUs below use
Field and share its budget across tokens, sessions, and hazard/notice routes.
429 is an expected controlled rejection, but its count must be reported.
Counts are per API instance; these are concurrency limits, not request-rate limits.

> Capture: run `step 6.1` first. Output is saved to `evidence/6.1.txt`.

**Quick manual look**: fire PVMBG-path requests and time a BMKG-only request in parallel:

```bash
T=$(login "$FIELD_ID" "$FIELD_SECRET" | jget access_token)
for i in 1 2 3 4 5; do curl -s -o /dev/null -H "Authorization: Bearer $T" "$API/hazards?hazard_type=VOLCANIC" & done
for i in 1 2 3 4 5; do curl -s -o /dev/null -w 'seismic %{time_total}s\n' -H "Authorization: Bearer $T" "$API/hazards?hazard_type=SEISMIC"; done; wait
```
```powershell
$ft = (Login $Creds.FIELD.id $Creds.FIELD.secret).access_token
$jobs = 1..5 | ForEach-Object { Start-Job { param($u,$t) Invoke-WebRequest -UseBasicParsing -Uri $u -Headers @{Authorization="Bearer $t"} | Out-Null } -ArgumentList "$API/hazards?hazard_type=VOLCANIC",$ft }
1..5 | ForEach-Object { "seismic {0:n3}s" -f (Measure-Command { Hz $ft '/hazards?hazard_type=SEISMIC' | Out-Null }).TotalSeconds }
$jobs | Wait-Job | Remove-Job
```

**Desired state:** every seismic request is far below 0.3 s. (Volcanic requests are
fast too: they read the Canonical Store, and the 3 s PVMBG delay only hurts the poller. That
is the decoupling the design relies on.)

**Authoritative isolation measurement (P2.1): two constant VUs for 65 s**, one
SEISMIC and one VOLCANIC. This measures repeated BMKG-only requests while the
volcanic request stream and the delayed PVMBG poller are active. It is a
concurrency/isolation check; P2.2's sustained load is measured separately below.

Use the current dataset, including 500+ entries when present. Before 6.1, record
the total and per-kind counts from the authenticated `/hazards` responses, the
response size, monitoring profile, and test time in `evidence/6.1.txt`. Record
counts again before 6.2 because ingestion continues. Do not wipe or shrink the
store between tests to obtain a passing latency. For this architecture both API
paths read the Canonical Store; retain 6.0's measured live 3 s source delay as
proof that PVMBG polling is slow during the test.

```bash
PURPOSE=isolation BASE_URL=$K6_API EVIDENCE_NAME=6.1-k6-isolation bash scripts/load-test.sh mixed 2 65s 2>&1 | tee evidence/6.1-k6-isolation.txt
K6_EXIT=${PIPESTATUS[0]}; echo "k6 exit code: $K6_EXIT"
```
```powershell
.\scripts\load-test.ps1 -Mode mixed -Purpose isolation -Vus 2 -Duration 65s -BaseUrl $K6_API -EvidenceName 6.1-k6-isolation 2>&1 | ForEach-Object { "$_" } | Tee8 6.1-k6-isolation.txt
"k6 exit code: $LASTEXITCODE"
```

**Desired state:** successful seismic p95 **< 300 ms**, successful responses from
both kinds, passing threshold lines and k6 exit code 0. The seismic-attempt p95
is also gated; successful-only timing prevents fast 429s from hiding slow reads.
Report the successful seismic sample count and p50/p95/p99, volcanic metrics,
throughput, errors and 429 count. A single timed request cannot establish p95.
The wrapper saves `evidence/6.1-k6-isolation.json`. **Criterion:** P2.1.

If this isolation run fails on the large dataset, retain it as a P2.1 failure
and investigate request processing/query/serialization cost. Reordering tests
or passing the sustained error-rate gate does not establish P2.1 compliance.
Negative timings invalidate timing evidence until investigated and rerun.

### 6.2 P2.2: one sustained mixed load, 50 VUs for 65 s

> Capture: run `step 6.2` first. Output is saved to `evidence/6.2.txt`.

Keep PVMBG's measured delay at 3 s and use the same growing dataset as 6.1.
Run one mixed workload: 25 VUs repeatedly request SEISMIC and 25 VOLCANIC.
There is no required escalation to 60 VUs or a second unfiltered `/hazards`
run. The unfiltered route may be tested additionally when relevant; report its
larger response payload separately.

Start this in **Terminal B** just before k6 in Terminal A:

```text
node scripts/evidence-probes.mjs runtime --seconds 80 --name 6.2-runtime
```

Run k6 in **Terminal A**:

```bash
PURPOSE=milestone BASE_URL=$K6_API EVIDENCE_NAME=6.2-k6-mixed bash scripts/load-test.sh mixed 50 65s 2>&1 | tee evidence/6.2-k6-mixed.txt
K6_EXIT=${PIPESTATUS[0]}; echo "k6 exit code: $K6_EXIT"
```
```powershell
.\scripts\load-test.ps1 -Mode mixed -Purpose milestone -Vus 50 -Duration 65s -BaseUrl $K6_API -EvidenceName 6.2-k6-mixed 2>&1 | ForEach-Object { "$_" } | Tee8 6.2-k6-mixed.txt
"k6 exit code: $LASTEXITCODE"
```

**Desired state:** ≥50 constant VUs for ≥60 s; report total HTTP throughput,
successful hazard throughput, p50/p95/p99 (including successful-only and
per-kind rows), unexpected error rates **<1%**, and the controlled 429 count
(including zero). P2.2 imposes no numerical latency limit: seismic p95 ≥300 ms
under this load is reported, but does not by itself fail P2.2. The script's
`Purpose=milestone` applies error-rate and successful-response gates, with no
300 ms gate. `Purpose=isolation` applies that gate in 6.1. **Criterion:** P2.2.

Screenshot the final `MILESTONE 1 LOAD TEST` block and runtime results; preserve
`evidence/6.2-k6-mixed.txt/json` and `6.2-runtime.txt/json`. Runtime evidence must
show health and stable container IDs/start times/restart counts throughout.
Final health alone cannot prove no crash. TCP counts include idle keep-alive
sockets and probes: report observed min/max overlapping the load window.
50 VUs alone does not establish ≥50 parallel connections; verify connection
evidence against that wording and retain any shortfall. Neither socket counts
nor VUs prove 50 simultaneous in-flight requests. The workload has 50 ms think
time, and all sessions share the Field identity's configured concurrency budget.

`unexpected_failure_rate` excludes 429 from numerator and denominator;
`http_req_failed` accepts 429 but retains them in its denominator. Separate
hazard/login failure rates are also gated below 1%. Always report 429s and
successful throughput so controlled rejection cannot obscure useful service.

### 6.2b Optional: demonstrate the controlled-rejection (429) path

> Capture: run `step 6.2b` first. Output is saved to `evidence/6.2b.txt`.

To capture global rejection, lower the global cap to 5 while leaving the
per-client cap at its example value of 32, overload it, and report the count.
Keep `-Purpose overload` in the PowerShell command (`PURPOSE=overload` in Bash):
the 30 s rejection test is separate from the milestone's minimum 60 s load test.
This optional test demonstrates rejection behavior; the spec does not require
producing 429s. A reported zero count in 6.2 is acceptable. Logs for this scenario use
`reason=max_in_flight`; do not infer the rejecting layer from status alone.

```bash
CAP_ORIG=$(envget client-api/.env CLIENT_API_MAX_INFLIGHT)
sed -i.bak 's/^CLIENT_API_MAX_INFLIGHT=.*/CLIENT_API_MAX_INFLIGHT=5/' client-api/.env && rm -f client-api/.env.bak
docker compose up -d --force-recreate --no-deps client-api; sleep 8
docker run --rm -i -v "$PWD/scripts:/scripts:ro" --add-host=host.docker.internal:host-gateway \
  -e BASE_URL=$K6_API -e MODE=sustained -e VUS=300 -e DURATION=30s -e CLIENT_ID="$FIELD_ID" -e CLIENT_SECRET="$FIELD_SECRET" \
  -e PURPOSE=overload grafana/k6 run /scripts/load-test.js 2>&1 | tee evidence/6.2b-k6.txt
K6_EXIT=${PIPESTATUS[0]}; echo "k6 exit code: $K6_EXIT"
echo "health under saturation: $(code $API/health)"                               # 200
docker compose logs client-api --since 2m 2>&1 | grep -c 'request rejected'       # > 0
# restore
sed -i.bak "s/^CLIENT_API_MAX_INFLIGHT=.*/CLIENT_API_MAX_INFLIGHT=$CAP_ORIG/" client-api/.env && rm -f client-api/.env.bak
docker compose up -d --force-recreate --no-deps client-api
```
```powershell
$CapOrig = Get-EnvVal client-api/.env CLIENT_API_MAX_INFLIGHT
(Get-Content client-api/.env) -replace '^CLIENT_API_MAX_INFLIGHT=.*','CLIENT_API_MAX_INFLIGHT=5' | Set-Content client-api/.env
docker compose up -d --force-recreate --no-deps client-api; Start-Sleep 8
.\scripts\load-test.ps1 -Mode sustained -Vus 300 -Duration 30s -BaseUrl $K6_API -Purpose overload -EvidenceName 6.2b-k6 2>&1 | ForEach-Object { "$_" } | Tee8 6.2b-k6.txt
"k6 exit code: $LASTEXITCODE"
"health under saturation: $(Code "$API/health")"                                  # 200
(docker compose logs client-api --since 2m 2>&1 | Select-String 'request rejected').Count     # > 0
(Get-Content client-api/.env) -replace '^CLIENT_API_MAX_INFLIGHT=.*',"CLIENT_API_MAX_INFLIGHT=$CapOrig" | Set-Content client-api/.env
docker compose up -d --force-recreate --no-deps client-api
```

**Desired state:** k6 prints `controlled_rejections_429` **greater than 0** (that line is now
always shown, even at 0); `http_req_failed` stays **< 1 %** (429 is an expected status in the
script) and `unexpected_errors` is `0`; `/health` answers `200` during saturation; the Client API
logs `request rejected`; no container crashes. The original cap is restored afterwards (check
with `envget client-api/.env CLIENT_API_MAX_INFLIGHT`). **Criterion:** P2.2 (controlled rejection reported).

Run the runtime sampler concurrently for this scenario too (`--seconds 40
--name 6.2b-runtime`), so `/health` is observed **during** saturation. A post-run
health command only proves health after load stopped. Overload mode requires
a nonzero 429 count; its 30 s run is not P2.2 sustained-load evidence.

To demonstrate the per-client path, instead leave the global cap at 128 and
temporarily set `CLIENT_API_MAX_INFLIGHT_PER_CLIENT=5` in `client-api/.env`.
Recreate only Client API with `docker compose up -d --force-recreate --no-deps client-api`
and run sustained load with 60 VUs for 70 s. Capture 429s with
`reason=max_in_flight_per_client`, verified `client_id`, `limit=5`, and
`correlation_id`. `/health` and `/ready` remain exempt. Restore the original
per-client setting and recreate Client API afterward. Automated tests also
hold client A's request while verifying that client B of the same role can
read normally and A's other token cannot bypass the cap; all four data routes
share the same budget.

### 6.3 P2.3: PVMBG outage, graceful degradation, recovery without restart

> Capture: run `step 6.3` first. Output is saved to `evidence/6.3.txt`.

For the full 10-minute evidence sequence, use this **instead of** the short
manual smoke sequence below (same command in both shells):

```text
node scripts/evidence-probes.mjs outage --seconds 600 --name 6.3-outage
```

The command starts with a warm/fresh cache, enables outage explicitly, verifies
direct source HTTP 503, and compares cached canonical IDs/content against the
pre-outage snapshot. Every ~10 s it prints seismic/volcanic HTTP statuses,
latencies, count, elapsed outage time and `stale_since`. It refreshes Field
tokens without printing credentials or tokens. It restores outage in `finally`,
then waits up to configured maximum retry delay + 45 s for fresh state **and**
a newly produced source report to reach the API, and checks container stability.
Capture its before/during/after JSON plus timeline and final PASS/FAIL. Do not
terminate the process forcibly. Ctrl+C requests restoration before exit; a
forced process kill cannot execute cleanup, so disable outage manually then.

The short sequence below is a smoke check only. Its 12 s waits do not establish
a prolonged outage or a bounded-backoff recovery. The asserted command above
supplies that evidence and avoids relying on tokens that expire mid-demo.

```bash
started > evidence/6.3-started-before.txt
T=$(login "$MEDIA_ID" "$MEDIA_SECRET" | jget access_token)
echo "before: $(hz "$T" '/hazards?hazard_type=VOLCANIC' | jget source_status.PVMBG.state)"                           # fresh
curl -s -X POST -H "Authorization: Bearer $PVMBG_ADMIN" -H 'Content-Type: application/json' -d '{"enabled":true}' $PVMBG/admin/outage     # {"outage":true}
echo "PVMBG data direct: $(code -H "Authorization: Bearer $PVMBG_TOKEN" $PVMBG/volcanic-reports)  health: $(code $PVMBG/health)"   # 503 / 200
sleep 12                                                                         # let the poller notice (<= poll interval + 3s delay)
curl -s -w '\nHTTP %{http_code} in %{time_total}s\n' -H "Authorization: Bearer $T" "$API/hazards?hazard_type=VOLCANIC" | tee evidence/6.3-volcanic-during.json | tail -3
curl -s -w '\nHTTP %{http_code} in %{time_total}s\n' -H "Authorization: Bearer $T" "$API/hazards?hazard_type=SEISMIC"  | tail -2
hz "$T" '/hazards?hazard_type=VOLCANIC' | jget source_status
docker compose logs aggregator --since 1m 2>&1 | grep '"level":"warn"' | grep -i pvmbg | head -3
docker compose logs aggregator --since 1m 2>&1 | grep -c '"level":"error"'      # => 0 (handled degradation is warn, not error)
# recover
curl -s -X POST -H "Authorization: Bearer $PVMBG_ADMIN" -H 'Content-Type: application/json' -d '{"enabled":false}' $PVMBG/admin/outage   # {"outage":false}
# Wait for a successful poll; retries can back off to 60 s in the example config.
RECOVERY_DEADLINE=$((SECONDS + 90))
PVMBG_STATE=unknown
while [ "$SECONDS" -lt "$RECOVERY_DEADLINE" ]; do
  T=$(login "$MEDIA_ID" "$MEDIA_SECRET" | jget access_token)
  PVMBG_STATE=$(hz "$T" '/hazards?hazard_type=VOLCANIC' | jget source_status.PVMBG.state)
  echo "PVMBG recovery: $PVMBG_STATE"
  [ "$PVMBG_STATE" = fresh ] && break
  sleep 3
done
echo "after: $PVMBG_STATE"
if [ "$PVMBG_STATE" = fresh ]; then
  echo "seismic after: $(code -H "Authorization: Bearer $T" "$API/hazards?hazard_type=SEISMIC")"
  started > evidence/6.3-started-after.txt; diff evidence/6.3-started-before.txt evidence/6.3-started-after.txt && echo "NO RESTART"
else
  echo "FAIL: PVMBG did not recover to fresh within 90 s ($PVMBG_STATE)." >&2
  false
fi
```
```powershell
Started | Set-Content evidence/6.3-started-before.txt -Encoding utf8
$mt = (Login $Creds.MEDIA.id $Creds.MEDIA.secret).access_token
"before: $((Hz $mt '/hazards?hazard_type=VOLCANIC').source_status.PVMBG.state)"                                       # fresh
Invoke-RestMethod -Method Post -Uri "$PvmbgUrl/admin/outage" -Headers $AdminH -ContentType 'application/json' -Body '{"enabled":true}' | ConvertTo-Json -Compress   # {"outage":true}
"PVMBG data direct: $(Code "$PvmbgUrl/volcanic-reports" 'GET' @{Authorization="Bearer $PvmbgToken"})  health: $(Code "$PvmbgUrl/health")"   # 503 / 200
Start-Sleep 12
$mh = @{Authorization="Bearer $mt"}
$sw = [Diagnostics.Stopwatch]::StartNew()
$during = Invoke-WebRequest -UseBasicParsing -Uri "$API/hazards?hazard_type=VOLCANIC" -Headers $mh
"volcanic: HTTP $($during.StatusCode) in $($sw.ElapsedMilliseconds) ms"            # 200, well under 1 s
$during.Content | Out-File evidence/6.3-volcanic-during.json -Encoding utf8     # keep the full body (stale_since, rows)
($during.Content | ConvertFrom-Json).source_status | ConvertTo-Json -Depth 4
$sw.Restart(); $seis = Invoke-WebRequest -UseBasicParsing -Uri "$API/hazards?hazard_type=SEISMIC" -Headers $mh
"seismic:  HTTP $($seis.StatusCode) in $($sw.ElapsedMilliseconds) ms"
($seis.Content | ConvertFrom-Json).source_status | ConvertTo-Json -Depth 4
docker compose logs aggregator --since 1m 2>&1 | Select-String '"level":"warn"' | Select-String -SimpleMatch -Pattern 'pvmbg' | Select-Object -First 3
(docker compose logs aggregator --since 1m 2>&1 | Select-String '"level":"error"').Count                              # => 0
Invoke-RestMethod -Method Post -Uri "$PvmbgUrl/admin/outage" -Headers $AdminH -ContentType 'application/json' -Body '{"enabled":false}' | ConvertTo-Json -Compress   # {"outage":false}
# Wait for a successful poll; retries can back off to 60 s in the example config.
$RecoveryDeadline = (Get-Date).AddSeconds(90)
$PvmbgState = 'unknown'
do {
  $mt = (Login $Creds.MEDIA.id $Creds.MEDIA.secret).access_token
  $PvmbgState = (Hz $mt '/hazards?hazard_type=VOLCANIC').source_status.PVMBG.state
  "PVMBG recovery: $PvmbgState"
  if ($PvmbgState -eq 'fresh') { break }
  Start-Sleep 3
} while ((Get-Date) -lt $RecoveryDeadline)
"after: $PvmbgState"
if ($PvmbgState -ne 'fresh') { throw "PVMBG did not recover to fresh within 90 s ($PvmbgState)." }
$mh = @{Authorization="Bearer $mt"}
"seismic after: $(Code "$API/hazards?hazard_type=SEISMIC" 'GET' $mh)"            # 200
Started | Set-Content evidence/6.3-started-after.txt -Encoding utf8
if (-not (Compare-Object (Get-Content evidence/6.3-started-before.txt) (Get-Content evidence/6.3-started-after.txt))) { 'NO RESTART' }
```

**Desired state:**

| Moment | Seismic request | Volcanic request |
| --- | --- | --- |
| Before outage | `200`, normal | `200`, `source_status.PVMBG.state = fresh` |
| During outage | `200`, normal latency | **`200`** (not 500, not a hang) with last-known rows, `source_status.PVMBG.state = stale` and a `stale_since` timestamp; answered in well under the client timeout |
| After outage off | `200` | back to `state = fresh` and newly ingested records within configured maximum retry delay plus response/processing time |

Recovery checks run every 3 s for up to 90 s with the example configuration,
allowing for the 60 s retry cap and the 3 s PVMBG response delay. Each check
obtains a new Media token so the 60 s access-token lifetime does not interrupt
the wait. If PVMBG remains stale/unavailable, the recovery step fails explicitly.
Capture `after: fresh`; `NO RESTART` alone does not establish source recovery.

Also: PVMBG direct data endpoint `503` while `/health` stays `200`; Aggregator logs `warn` (never `error`/500)
for the PVMBG failures; **no container restarted**. If volcanic rows were absent (cold start), an explicit
unavailable status is also acceptable. **Criterion:** P2.3.

If either path returns a timeout or unexpected status, retain the failed
measurement. Capture source outage/recovery warning logs separately; uppercase
`WARN`/`ERROR` is the current slog level spelling, so lowercase-only log filters
can silently miss evidence.

Optional: repeat 6.2 *during* the outage (enable outage, run sustained k6, disable) to show error rate stays **< 1 %** under degradation.

---

## Phase 7: Reset / teardown

Restore PVMBG's original delay (and clear in-memory toggles):

```bash
sed -i.bak -e "s/^PVMBG_MIN_DELAY_MS=.*/PVMBG_MIN_DELAY_MS=$ORIG_MIN/" -e "s/^PVMBG_MAX_DELAY_MS=.*/PVMBG_MAX_DELAY_MS=$ORIG_MAX/" pvmbg-mock/.env && rm -f pvmbg-mock/.env.bak
docker compose up -d --force-recreate --no-deps pvmbg-mock
docker compose --profile consumer-demo stop consumer-dashboard-demo
```
```powershell
(Get-Content pvmbg-mock/.env) -replace '^PVMBG_MIN_DELAY_MS=.*',"PVMBG_MIN_DELAY_MS=$OrigMin" -replace '^PVMBG_MAX_DELAY_MS=.*',"PVMBG_MAX_DELAY_MS=$OrigMax" | Set-Content pvmbg-mock/.env
docker compose up -d --force-recreate --no-deps pvmbg-mock
docker compose --profile consumer-demo stop consumer-dashboard-demo
```

Stop the system (keeps data) or wipe everything:

```bash
docker compose --profile monitoring --profile consumer-demo down        # keep volumes
docker compose --profile monitoring --profile consumer-demo down -v     # also delete all volumes (fresh start next time)
```

---

## Appendix A: criterion -> step traceability

| Spec criterion | Step(s) | Key evidence file |
| --- | --- | --- |
| Ketentuan 3 (one-command start) | 1.1 | `1.1.txt` |
| Ketentuan 7 (health, correlation ID, latency log) | 1.2, 1.4 | `1.4.txt` |
| P1.1 mapping, old schema | 1.3 | `1.3-volcanic-before.json` |
| P1.2 toggle without restart | 2.2, 2.4 | `2.1-started-before.txt`, `2.4-started-after.txt` |
| P1.3 valid HazardEvent after change | 2.3, 2.4 | `2.3-new-report.json` |
| P1.4 unknown-field policy (pass to `attributes`) | 2.3 | `2.3-new-report.json` |
| P2.1 BMKG p95 < 300 ms at 3 s PVMBG | 6.0, 6.1 | `6.0.txt`, `6.1.txt`, `6.1-k6-isolation.json` |
| P2.2 ≥ 50 conns, ≥ 60 s, p50/p95/p99, errors < 1 % | 6.2 | `6.2.txt`, `6.2-k6-mixed.json`, `6.2-runtime.txt/json` |
| P2.3 outage -> stale/unavailable, recover | 6.3 | `6.3.txt`, `6.3-volcanic-during.json` |
| P3.1 cross-institution rejection | 3.1 | `3.1-isolation.txt` |
| P3.2 Media redaction, raw -> 403 | 3.2, 3.3 | `3.2.txt`, `3.3.txt` |
| P3.3 expiry + refresh + old token rejected | 3.4 | `3.4.txt` |
| P3.4 no hardcoded secrets, `.env.example` | 3.5 | `3.5.txt` |
| P4.1 independent container rebuild | 5.1, 5.2 | `5.2.txt`, `5.2-started-*.txt` |
| P4.2 JSONB, no migration, old+new side by side | 5.3 | `5.3-side-by-side.txt` |
| P4.3 store only reachable by Aggregator | 5.4 | `5.4.txt` |
| P5.1 broker publish | 4.1 | `4.1.txt` |
| P5.2 two independent consumers | 4.1, 4.2 | `4.2.txt` |
| P5.3 consumer down, others unaffected, catch-up | 4.3, 4.4 | `4.3.txt`, `4.4.txt` |
| P5.4 new consumer, producer untouched | 4.5 | `4.5.txt` |

## Appendix B: troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `jget` prints nothing / `command not found: python3` | neither jq nor python3 on PATH | install jq, or set `PY=python` (Windows), or use the PowerShell variant |
| Git Bash: `docker run -v` fails with weird path | MSYS path conversion | prefix the command with `MSYS_NO_PATHCONV=1` |
| `k6` cannot reach `host.docker.internal` (Linux) | no host alias | add `--add-host=host.docker.internal:host-gateway` |
| `network natsio/nats-box not found` (or `no *_backend network`) when running `nats` / `Nats` | the helpers were pasted before the stack was up, so the old `$NET` was empty and `--network` swallowed the image name | re-paste the current 0.2 helpers (the network is now looked up on every call); make sure `docker compose ps` shows the stack up and `docker network ls` lists `*_backend` |
| All `/hazards` calls return `401` mid-run | access token TTL is 60 s | re-login (`login`/`Login`) |
| `wait_has` / `Wait-Has` prints `MISSING` | the consumer is stopped, halted, or still replaying a long history | check `docker compose ps` and the consumer's `/ready`; for a fresh third consumer, run the same `wait_has` again |
| `latest` returns the same ID twice in 4.2 | no new event was ingested in the 12 s window | check `docker compose logs bmkg-mock pvmbg-mock \| grep generated` and that `*_INTERVAL_MS` is at most 10000 |
| Consumer record counts look stuck at 100 | `/records` returns only the latest 100 rows | follow a specific `hazard_id` with `wait_has`, do not compare counts |
| `Code` returns `-1` in PowerShell | connection refused / timeout, not an HTTP error | check `docker compose ps` |
| Phase 6 numbers look odd after Phase 2 | `--force-recreate pvmbg-mock` reset toggles | expected; schema back to v1, outage off |
| Evidence file is unreadable / `grep` finds nothing / tables show `Γöé` | PowerShell `Tee-Object`, `>` and `Set-Content` write UTF-16 / ANSI | use `Tee8` and `Step` (0.3); for existing files: `iconv -f UTF-16 -t UTF-8 file` or `Get-Content file \| Set-Content -Encoding utf8 new` |
| Red `NativeCommandError` / `RemoteException` in `1.1.txt` | Windows PowerShell 5.1 turns docker's stderr progress lines into error records when `2>&1` is used | harmless (check `docker compose ps`); the runbook's `ForEach-Object { "$_" }` wrapper removes it; PowerShell 7 does not do this |
| Transcript file from `Step` is empty | host does not record native-command output | wrap the step's commands in `Tee8 <file>` instead, or run in Windows Terminal with PowerShell 7 |
| k6 `min=-...ns` for `http_req_duration` | Docker VM clock drift (WSL2) | cosmetic; see 6.1 notes. `wsl --shutdown` + restart Docker, then rerun |
| `controlled_rejections_429` missing from the k6 summary | old script version (counter only shown once incremented) | use the current `scripts/load-test.js`; it now declares a threshold so the line always prints |
| 4.x output is full of `Γöé`, `ΓöÇ`, `Γò¡` (garbled box characters) | `nats consumer report` prints UTF-8 box-drawing characters; the Windows console decodes them with the OEM code page, and `Tee8`/`Step` then saves the already-garbled text | use `consreport` / `Consreport` (plain ASCII, built from `--json`); in PowerShell run the UTF-8 line from 0.3 (`[Console]::OutputEncoding = ...`) once per terminal |
| 4.5 fails to become ready; `/ready` returns 503 although the container is alive | leftover `dashboard-demo-data` volume from an earlier run: the new durable replays stream sequence 1 with different bytes, the consumer halts (`consumer_incompatible`, `/ready` 503) and never becomes ready | `docker compose --profile consumer-demo rm -sf consumer-dashboard-demo`, `docker volume rm $(docker volume ls -q -f name=dashboard-demo-data)`, then `up -d --force-recreate`. The 4.5 pre-flight does this and its wait is now bounded (60 s) and prints the logs. Tear down with `--profile consumer-demo down -v --remove-orphans`. |

## Appendix C: evidence checklist (a complete run produces all of these)

In addition to the manual captures below, keep these asserted outputs. Screenshot
their final results and measured values; JSON supplies the detailed proof.

| Gap covered | Command/output stem | Screenshot must show |
| --- | --- | --- |
| Source mapping | `mapping --name 1.3-mapping` | matched source counts, mapping PASS, boundary Go test results |
| All-service health | `health --name 1.2-all-services` | each container/service state and health command |
| Ingest/delivery correlation | `trace --name 1.4-ingest-delivery` | shared IDs, source/consumer names, outbound target and latency |
| Live schema evolution | `schema --name 2-schema` on v1 | discovered new ID, valid attributes, unchanged containers and DB columns |
| Storage coexistence | `storage --name 5.3-storage` after asserted schema | old/new IDs both readable, JSONB, unchanged columns |
| Broader secret review | `secrets --name 3.5-secrets` | tracked-env check, scan scope, historical blob count, redacted findings |
| Load/crash/connection evidence | `runtime --name 6.2-runtime` concurrently with sustained k6 | timestamped TCP/health samples, stable containers, min/max |
| Prolonged outage/recovery | `outage --seconds 600 --name 6.3-outage` | preserved cache, HTTP/latency timeline, stale_since, new recovery record, no restart |

Invoke these as `node scripts/evidence-probes.mjs <command>`. Existing files
marked missing below are a checklist from a prior run, not a claim about the
results of the next run. The k6 wrappers save named JSON summaries; direct
`docker run` variants below still print the same screenshot-ready summary,
but use the wrappers if you also need the JSON file.

Tick each file off before the demo. "Must show" is what a reviewer will look for. Rows marked
**(missing on the first run)** are the gaps found when the first evidence set was reviewed.

| Step | File(s) | Must show |
| --- | --- | --- |
| 1.1 | `1.1.txt` | 11 default-profile containers `healthy`; 16 with optional monitoring |
| 1.3 | `1.3.txt`, `1.3-volcanic-before.json` | counts at least 20/20/40 and higher on the second reading 25 s later; no `confidence_level` before the toggle; **seismic sample row too** |
| 1.4 | `1.4.txt` | one `correlation_id` in client-api, auth-service **and** aggregator, with `latency_ms` |
| 2.x | `2.1-started-before.txt`, `2.3-new-report.json`, `2.4-started-after.txt`, `2.2.txt`, `2.3.txt`, `2.4.txt` | toggle returns `v2`; new row has the field in `attributes`; `NO RESTART`; 0 contract violations |
| 3.1 | `3.1.txt` | `RESULT: PASS` |
| 3.2 | `3.2.txt` **(missing)** | roles `media`/`field`/`internal`; mixed id+secret -> `401`. Do not print the login JSON (tokens). |
| 3.3 | `3.3.txt` **(missing)** | Media key list = 7 summary fields; Field/Internal also have `source_ref_id, latitude, longitude, attributes`; `?view=raw` -> `403`; no token -> `401` |
| 3.4 | `3.4.txt` **(missing)** | `200 -> 401 -> refresh 200 -> old 401`, reused refresh token rejected |
| 3.5 | `3.5.txt` **(missing)** | no tracked `.env`; `.env.example` present; no `HARDCODED:` / `IN HISTORY:` lines |
| 4.1 | `4.1.txt`, `4.1-consumers.txt` | two durables, unprocessed 0 (or briefly 1); **plain-ASCII `consreport` lines, no `Γöé`** |
| 4.2 | `4.2.txt` **(missing)** | `new` differs from `previous`; both consumers print `has <hazard_id>` for the same ID; its `correlation_id` is in the aggregator log |
| 4.3 | `4.3.txt` **(missing)** | notifier stopped; dashboard has the event published during the stop; `outbox_pending = 0`; aggregator healthy, 0 errors; notifier Unprocessed above 0 |
| 4.4 | `4.4.txt` | notifier prints `has <hazard_id>` for the event it missed; Unprocessed back to 0 (or briefly 1) (plain-ASCII `consreport` output) |
| 4.5 | `4.5.txt`, `4.5-started-before.txt` **(per-consumer id comparison was missing)** | all three consumers print `has <hazard_id>` for the same new event; aggregator `StartedAt` unchanged |
| 5.1 | `5.1.txt` | one image per component; only 18080/18081/18082 and host-only Grafana 13000 published; broker and Registry ports internal with the template settings |
| 5.2 | `5.2.txt`, `5.2-started-before.txt`, `5.2-started-after.txt`, Terminal-A probe log (`5.2-probe.txt`) **(all missing)** | image ID of the notifier changes; only its `StartedAt` differs; probe prints `200 / 200` throughout. Capture Terminal A with `... \| tee evidence/5.2-probe.txt` / `\| Tee8 5.2-probe.txt`. |
| 5.3 | `5.3.txt`, `5.3-side-by-side.txt` | `attributes` is `jsonb`; rows with and without `confidence_level`; no migration files |
| 5.4 | `5.4.txt` **(missing)** | `store` network = postgres + aggregator only; others cannot resolve `postgres`; no host port |
| 6.0 | `6.0.txt` **(missing)** | PVMBG about 3.0 s, BMKG 0.05 to 0.15 s |
| 6.1 | `6.1.txt`, `6.1-k6-isolation.txt/json` | 2 VUs, successful seismic p95 < 300 ms, samples from both kinds, dataset counts/size, exit code 0 |
| 6.2 | `6.2.txt` | ≥ 50 VUs, ≥ 60 s, throughput, p50/p95/p99, `http_req_failed` < 1 %, no unhealthy container |
| 6.2b (optional) | `6.2b.txt` if run | `controlled_rejections_429` > 0, error rate < 1 %, `/health` 200, `request rejected` logs |
| 6.3 | `6.3.txt`, `6.3-volcanic-during.json`, `6.3-started-*.txt` | volcanic **HTTP 200** during outage with `state=stale` + `stale_since` (not 5xx, not a hang); seismic 200; `warn` not `error` logs; back to `fresh`; `NO RESTART` |

## Reporting lifecycle verification

See [Reporting Lifecycle](Reporting%20Lifecycle.md) for the updated contracts.
Run the Go suites in each affected service. Store integration tests need a
**dedicated** `TEST_DATABASE_URL` with `store/init.sql` initialized; do not point
these tests at a live demo database because tests advance replay cursors.

The added tests cover immediate persistence while warning HTTP is blocked,
unknown-versus-empty assessment, late warnings with stable hazard IDs, correction
outbox work, warning-before-seismic ordering, cursor rollback, generic volcanic
mapping notices, source snapshot ordering, Media redaction in notice history,
source live eviction/archive restart, history auth/outage behavior, and consumer
correction-before-initial ordering with duplicate notification suppression.

For an actual outage, enable `/admin/outage`, inspect `/hazards` source/record
metadata, and check PVMBG `/history` also returns 503. Leave the outage enabled
10–20 minutes for the M1 demo; restore it and allow up to the configured maximum
retry interval plus source response time for recovery. Age cutoff does not stop
replay. For a faster archive-eviction demonstration, shorten the source generation
interval and keep Aggregator offline until more than 100 observations are generated;
restart the source and Aggregator, and verify archived observations catch up.

Inspect consumer `/records?view=notices`, `history`, and `notifications` alongside
its canonical `/records`. Stop one consumer while both streams continue, then
restart it within stream retention and verify higher revisions remain applied.
Run the existing mixed k6 test with PVMBG delay fixed at 3 seconds and at least
50 connections for 60 seconds. Keep verification credentials in gitignored env
files; never save tokens in evidence logs.
