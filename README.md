# TugasBesarAAT — IF4031 M1

This repository contains the BMKG and PVMBG mocks, BNPB Aggregator,
Aggregator-owned Canonical Store, Client API, Auth Service, and an interactive
downstream client. Broker consumers are still separate work.

## Services

| Service | Directory | Port | Purpose |
| --- | --- | --- | --- |
| BMKG mock | [`bmkg-mock/`](bmkg-mock/) | 8081 | Simulated seismic/tsunami source |
| PVMBG mock | [`pvmbg-mock/`](pvmbg-mock/) | 8082 | Simulated volcanic-activity source |
| Aggregator | [`aggregator/`](aggregator/) | internal (8080) | Polls both mocks, maps to canonical `HazardEvent`, owns the Store, publishes to NATS, serves the internal read API |
| Canonical Store | [`store/`](store/) | internal (5432) | PostgreSQL 16, accessed only by the Aggregator |
| Auth Service | [`auth-service/`](auth-service/) | internal (8083) | Owns persistent sessions, issues and rotates tokens |
| Client API | [`client-api/`](client-api/) | 8080 | Authenticated downstream reads and server-side field projection |
| Client TUI | [`client-tui/`](client-tui/) | interactive | One terminal app for Media, Field, and Internal identities |

Each Go service has its own `go.mod` (no shared workspace module) and its own
multi-stage Dockerfile (`golang:1.27.1-alpine3.24` builder → `alpine:3.24`
runtime). Every Go module requires Go 1.27.1.

## Broker choice: NATS JetStream, not RabbitMQ

The original assignment text mentions RabbitMQ; the team's technical
decision (see `docs/Technical Stack Specifications.md` and
`docs/Contracts.md`) supersedes that with **NATS JetStream**. The broker image
extends official `nats:2.15.0-alpine3.22`, mounts `broker/nats.conf` read-only,
and initializes its stream whenever it starts. JetStream storage persists in a
volume. New `HazardEvent`s and pending publications are saved in one Postgres
transaction. A dispatcher publishes each pending event to stream `HAZARDS` on
subject `hazard.events` and removes it from the outbox only after JetStream
acknowledges it.

## Repository layout

```text
bmkg-mock/        # Go module, :8081
pvmbg-mock/       # Go module, :8082
aggregator/       # Go module, :8080 (internal read API)
store/            # init.sql + Store README (Postgres, Aggregator-owned)
auth-service/     # Go module, :8083 (internal token service + SQLite)
client-api/       # Go module, :8080 (public API)
client-tui/       # Go module, interactive terminal client
broker/           # NATS JetStream + stream initializer
scripts/          # API smoke test
docker-compose.yml
.env.example
docs/             # Pre-existing team contract docs (wire format, stack, deployment)
```

## Quick start: Docker Compose

Create the eight service env files manually using the root `.env.example`
and the per-service table below. Copy only the variables required by each
service, including shared variables listed in the template. Then run from
the repository root:

```powershell
docker compose up -d --build
```

Each service can start independently. The Aggregator serves `/health` while
Postgres or NATS is unavailable. It retries its connections, begins polling
when Postgres and the tables from `store/init.sql` are ready, and queues events
in the outbox until NATS is ready.
`/ready` reports store and broker availability. The broker health check uses
`NATS_URL` from `broker/.env` and passes only after the stream is initialized
and reachable. The optional `broker-init` service can reapply the stream
configuration with `docker compose --profile tools run --rm broker-init`.
Changes to `broker/nats.conf` take effect after recreating the broker with
`docker compose up -d --force-recreate broker`; they do not require rebuilding
its image.
Postgres runs `init.sql` only for a new data volume. A local volume created
before `hazard_outbox` was added needs to be recreated or initialized manually.

Once everything is healthy:

```bash
curl http://localhost:8081/health   # {"status":"ok","service":"bmkg-mock"}
curl http://localhost:8082/health   # {"status":"ok","service":"pvmbg-mock"}
curl http://localhost:8080/health   # client-api
curl http://localhost:8080/ready    # auth and aggregator readiness
```

### Per-service environment files

The only environment template is `.env.example` in the root. Create the
following gitignored files manually, each containing only the variables
needed by that service:

| File | Used by | Configuration |
| --- | --- | --- |
| `bmkg-mock/.env` | BMKG mock | Port, API key, delay |
| `pvmbg-mock/.env` | PVMBG mock | Port, source/admin tokens, delays |
| `broker/.env` | Broker and optional broker-init tool | NATS URL, stream, subject, retention |
| `store/.env` | PostgreSQL | Database name, user, password |
| `aggregator/.env` | Aggregator | Source URLs/credentials, database URL, NATS publication, polling |
| `auth-service/.env` | Auth Service | Three client credentials, internal key, TTLs, SQLite path |
| `client-api/.env` | Client API | Port, Auth/Aggregator URLs, internal key |
| `client-tui/.env` | Interactive TUI | API URL and public client IDs; no login secrets |

Compose injects these values through `env_file`; it does not mount the files.
Build contexts exclude local env files with `.dockerignore`. The root `.env`
is not needed at runtime. Example secrets are demonstration placeholders;
configure real values in the service files.

Env values must fit on one line. Use single quotes to preserve literal
characters such as `$` and `#`, for example `AUTH_INTERNAL_KEY='your-key'`.

Keep shared values consistent when editing files: BMKG's key is also in
Aggregator; PVMBG's source token is also in Aggregator; the internal Auth key
is in Auth and Client API; client IDs are in Auth and TUI; NATS URL, stream,
and subject are in Broker and Aggregator. Set Aggregator's `DATABASE_URL`
from the PostgreSQL credentials, with URI-encoded user/password/name when
they contain special characters. If those credentials change later, update
`store/.env` and Aggregator's URL
together. Changing PostgreSQL credentials also requires updating the existing
database; recreating a container does not alter users in an initialized volume.

Docker host mappings stay at Client API `8080`, BMKG `8081`, PVMBG `8082`,
and NATS `4222`/`8222`. Keep `CLIENT_API_PORT=8080`, `BMKG_PORT=8081`,
and `PVMBG_PORT=8082` to match these mappings.
For other port changes, keep service URLs, health checks, and Compose mappings
consistent. Apply env changes with `docker compose up -d --force-recreate`
and restart the one-off TUI. Building images is only needed for code changes.

The Auth Service owns its SQLite database in the `auth-data` volume.
Restarting the Auth container does not remove valid sessions.

| Variable | Purpose | Default |
| --- | --- | --- |
| `MEDIA_CLIENT_ID`, `MEDIA_CLIENT_SECRET` | Media identity | Required |
| `FIELD_CLIENT_ID`, `FIELD_CLIENT_SECRET` | Field identity | Required |
| `INTERNAL_CLIENT_ID`, `INTERNAL_CLIENT_SECRET` | Internal identity | Required |
| `AUTH_INTERNAL_KEY` | Auth-to-Client-API trust key | Required |
| `ACCESS_TOKEN_TTL_SEC` | Access token lifetime | `60` |
| `REFRESH_TOKEN_TTL_SEC` | Refresh token lifetime | `86400` |
| `AUTH_DB_PATH` | Auth-owned SQLite file inside its container | `/data/auth.db` |
| `AUTH_PORT` | Auth container HTTP port | `8083` |
| `CLIENT_API_PORT` | Client API container HTTP port; fixed host mapping | `8080` |
| `AGGREGATOR_PORT` | Internal Aggregator HTTP port | `8080` |
| `AUTH_BASE_URL`, `AGGREGATOR_BASE_URL` | Internal upstream URLs for standalone Client API runs | Compose service URLs |
| `BNPB_API_URL` | TUI destination URL | `http://localhost:8080` on host; Compose service URL in Docker |

### Client API and TUI

| Method | Endpoint | Purpose |
| --- | --- | --- |
| POST | `/auth/login` | JSON `client_id`, `client_secret`; returns bearer access and refresh tokens, TTL, and server-assigned role |
| POST | `/auth/refresh` | JSON `refresh_token`; rotates both tokens and revokes the old access token immediately |
| POST | `/auth/logout` | JSON `refresh_token`; revokes the session |
| GET | `/hazards` | Bearer token; optional `since`, `hazard_type`, `severity`, `area_name`, `view=summary\|raw` |
| GET | `/hazards/{id}` | Bearer token; optional `view=summary\|raw` |
| GET | `/health`, `/ready` | Process health and dependency readiness |

Media receives only `hazard_id`, `source`, `hazard_type`, `severity`,
`area_name`, `occurred_at`, and `ingested_at` inside each hazard. A Media
request with `view=raw` returns `403`. Field and Internal receive the full
canonical event by default. `source_status` is response metadata, separate
from the hazard fields, so even Media can see when PVMBG data is stale.

```bash
docker compose up -d --build
docker compose run --rm --build client-tui
```

Run the TUI command in three terminals to test three identities concurrently.
The TUI reads public client IDs from Compose and prompts for each secret.
Choose a role, list or filter hazards, open a detail, or request raw data.
For the refresh demo, log in as Field, wait more than the default 60 seconds,
then list hazards. The TUI refreshes after the expired token gets `401`,
retries the read, and its “Uji token lama” action checks that the old token
gets `401`. The same TUI can run on the host with
`BNPB_API_URL=http://localhost:8080`.

Client API and Auth Service emit JSON logs with `correlation_id`; Client API
forwards `X-Correlation-ID` to Auth and Aggregator and logs outbound latency.
The database network contains only PostgreSQL and Aggregator. Aggregator's
internal port is not published to the host.

On Windows, `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/smoke.ps1 -RestartAuth -WaitForExpiry -TestOutage`
checks institution credential isolation, three client identities, Media redaction, token rotation, natural expiry,
PVMBG outage status, and session persistence after an Auth restart. It reads demo credentials from
the corresponding service env files and prints only the pass/fail result.

## Standalone (non-Docker) run

Each service reads process environment variables. Go services do not load
their `.env` files automatically when run outside Docker; export the required
values before starting them and use `localhost` URLs for services on the host.
The Aggregator can start without Postgres or NATS, and recovers when they
appear. Starting the broker initializes its JetStream stream.

```bash
# 1. Canonical Store
docker run -d --name pg -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=hazards \
  -v $(pwd)/store/init.sql:/docker-entrypoint-initdb.d/init.sql postgres:16-alpine3.24

# 2. NATS JetStream, including its stream initializer
# Prepare broker/.env manually from the root template
docker compose up -d broker

# 3. BMKG mock
cd bmkg-mock && BMKG_API_KEY=devkey go run ./cmd/server &

# 4. PVMBG mock
cd pvmbg-mock && PVMBG_TOKEN=devtoken PVMBG_ADMIN_TOKEN=devadmin go run ./cmd/server &

# 5. Aggregator
cd aggregator && \
  BMKG_API_KEY=devkey PVMBG_TOKEN=devtoken \
  DATABASE_URL=postgres://postgres:postgres@localhost:5432/hazards?sslmode=disable \
  NATS_URL=nats://localhost:4222 \
  go run ./cmd/server
```

## Running tests

```bash
cd bmkg-mock && go test ./...
cd pvmbg-mock && go test ./...
cd aggregator && go test ./...          # store integration tests skip without TEST_DATABASE_URL
```

To also run the Aggregator's Postgres-backed store tests:

```bash
docker run -d --name pg-test -p 5433:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=hazards \
  -v $(pwd)/store/init.sql:/docker-entrypoint-initdb.d/init.sql postgres:16-alpine3.24
cd aggregator && TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5433/hazards?sslmode=disable" go test ./internal/store/...
```

---

## BMKG mock (`bmkg-mock/`)

Simulated BMKG source: seismic events and tsunami warnings.

- **Port:** 8081
- **Auth:** header `X-BMKG-Key: <BMKG_API_KEY>` on data endpoints. Missing → `401`, wrong key → `403`.
- **Delay:** fixed simulated delay per request, `BMKG_DELAY_MS` (default `100`, spec range 50–150ms).
- **Seed data:** 20 deterministic seismic events (magnitudes spanning <5.0, 5.0–6.5, ≥6.5, with/without tsunami potential) and related tsunami warnings across Waspada/Siaga/Awas, generated at boot from a fixed epoch — same data every run.

### Endpoints

| Method | Path | Auth | Notes |
| --- | --- | --- | --- |
| GET | `/seismic-events?since=<ISO8601>` | `X-BMKG-Key` | `since` optional; missing/invalid `since` returns the full current dataset |
| GET | `/tsunami-warnings?since=<ISO8601>` | `X-BMKG-Key` | same `since` semantics |
| GET | `/health` | none | `{"status":"ok","service":"bmkg-mock"}` |

### Sample requests

```bash
curl -H "X-BMKG-Key: devkey" http://localhost:8081/seismic-events | jq '.[0]'
curl -H "X-BMKG-Key: devkey" "http://localhost:8081/seismic-events?since=2026-09-01T00:00:00Z"
curl -H "X-BMKG-Key: devkey" http://localhost:8081/tsunami-warnings | jq '.[0]'
```

Expected shape (array of objects, snake_case, UTC millisecond timestamps):

```json
{
  "event_id": "evt-00001",
  "magnitude": 3.2,
  "depth_km": 10,
  "epicenter_lat": -6.0,
  "epicenter_lon": 104.5,
  "region_name": "Kab. Cianjur",
  "occurred_at": "2026-08-01T00:00:00.000Z",
  "potential_tsunami": false
}
```

---

## PVMBG mock (`pvmbg-mock/`)

Simulated PVMBG source: volcanic activity reports, with two demo-critical
admin toggles.

- **Port:** 8082
- **Auth:** `Authorization: Bearer <PVMBG_TOKEN>` on `/volcanic-reports` — a **different mechanism** than BMKG's header key, per spec. Admin endpoints require `Authorization: Bearer <PVMBG_ADMIN_TOKEN>` (a separate credential).
- **Delay:** random delay per request in `[PVMBG_MIN_DELAY_MS, PVMBG_MAX_DELAY_MS]` (default 500–3000ms).
- **Seed data:** 20 deterministic volcanic reports across all four alert levels (Normal/Waspada/Siaga/Awas), spanning 10 seeded Indonesian volcanoes. Every third record already carries a `confidence_level` value in storage — demonstrating that old/new record shapes coexist independently of the wire-visibility toggle below.

### Endpoints

| Method | Path | Auth | Notes |
| --- | --- | --- | --- |
| GET | `/volcanic-reports?since=<ISO8601>` | Bearer `PVMBG_TOKEN` | `confidence_level` present only when the schema toggle is on |
| POST | `/admin/schema-version` | Bearer `PVMBG_ADMIN_TOKEN` | Toggles `confidence_level` visibility, in memory, no restart |
| POST | `/admin/outage` | Bearer `PVMBG_ADMIN_TOKEN` | Toggles outage mode, in memory, no restart |
| GET | `/health` | none | Always `200`, even during outage — outage is a business-logic state, not a process failure |

### Schema-version toggle (exact commands)

```bash
# Before toggle: confidence_level is absent
curl -H "Authorization: Bearer devtoken" http://localhost:8082/volcanic-reports | jq '.[0]'

# Toggle it on
curl -X POST -H "Authorization: Bearer devadmin" http://localhost:8082/admin/schema-version
# => {"schema_version":"v2","confidence_level_visible":true}

# After toggle: confidence_level now appears
curl -H "Authorization: Bearer devtoken" http://localhost:8082/volcanic-reports | jq '.[0]'

# Toggle back off
curl -X POST -H "Authorization: Bearer devadmin" http://localhost:8082/admin/schema-version
# => {"schema_version":"v1","confidence_level_visible":false}
```

### Outage toggle (exact commands)

```bash
# Enable outage
curl -X POST -H "Authorization: Bearer devadmin" http://localhost:8082/admin/outage
# => {"outage":true}

# Data endpoint now fails fast (503), health stays healthy
curl -i -H "Authorization: Bearer devtoken" http://localhost:8082/volcanic-reports   # 503
curl -i http://localhost:8082/health                                                 # 200

# Clear outage
curl -X POST -H "Authorization: Bearer devadmin" http://localhost:8082/admin/outage
# => {"outage":false}
```

`/admin/outage` also accepts an explicit body `{"enabled": true}` /
`{"enabled": false}` instead of toggling, if you want deterministic
before/after state in a script.

---

## Aggregator (`aggregator/`)

The BNPB Aggregator/Gateway: polls BMKG and PVMBG concurrently, maps both
into the canonical `HazardEvent`, owns the Canonical Store exclusively,
deduplicates, publishes new events to NATS JetStream, and serves the
internal read API the future Client-API will call.

### Polling

Two independent goroutines, each with its own `time.Ticker` at
`POLL_INTERVAL` seconds (default 3; spec range 2–5). There is no shared lock
around both sources' polling — a slow or failed PVMBG call never delays or
blocks the BMKG poller, and vice versa. Each cycle re-fetches the full
current dataset from each mock (no `since` bookkeeping needed at this data
volume — correctness relies on the Store's unique constraint for dedup, not
on tracking a last-poll timestamp).

### Mapping

`internal/mapping` implements:
- **Seismic → HazardEvent**: `source=BMKG`, `hazard_type=SEISMIC`, area/lat/lon from the event, severity per the priority rules below, `attributes` = `magnitude`, `depth_km`, `potential_tsunami`, plus the matching tsunami warning's fields (`warning_id`, `related_event_id`, `threat_level`, `affected_zones`, `estimated_arrival`) when one exists.
- **Volcanic → HazardEvent**: `source=PVMBG`, `hazard_type=VOLCANIC`, area/lat/lon resolved via the `volcano_reference` table (never hardcoded in mapping code), severity from the alert-level mapping, `attributes` = `eruption_count_24h`, `ash_column_height_m`, plus **any other field PVMBG's response carries** (including `confidence_level` once emitted) — decoded generically so a new field never causes a decode failure or gets silently dropped.

**Severity rules for seismic events** (priority order):
1. Any related tsunami warning at `Awas` → `AWAS`, overriding magnitude.
2. Otherwise, the highest related warning's level: Waspada→WASPADA, Siaga→SIAGA, Awas→AWAS.
3. No warning at all → by magnitude: `<5.0`→NORMAL, `5.0–6.49`→WASPADA, `≥6.5`→SIAGA.

### Deduplication

Keyed on `(source, source_ref_id)`, never on `hazard_id`. The Store's unique
index is the final correctness guard. A new row and its outbox record commit
together. A record re-seen on a later poll cycle updates its canonical row
without adding a second outbox record.

### Canonical Store

See [`store/README.md`](store/README.md). The Aggregator is the *only*
caller of `*pgxpool.Pool`; all access goes through `internal/store`'s
repository (`SaveDedup`, `GetByID`, `Query`, `LastKnownVolcanic`).

### Schema evolution

`confidence_level` is never a Postgres column — it flows into the
`attributes` JSONB column through the generic PVMBG decode path described
above, so PVMBG can start emitting it (via its schema-version toggle) with
zero Aggregator redeploy and zero migration. Old rows (no `confidence_level`)
and new rows (with it) live in the same table, same column, and both are
queryable/readable at once.

### Outage behavior

When PVMBG polling fails (timeout, 503, connection error), the Aggregator:
- logs at **`warn`**, not `error` — this is a handled degradation, not a bug;
- keeps BMKG polling completely unaffected (separate goroutine, no shared state on that path);
- serves the last-known volcanic data already in the Store for `/internal/hazards` reads, enriching each volcanic event's `attributes` at **response time** with `"stale": true` and `"stale_since": "<ISO8601 UTC>"` — the stored row itself is never mutated for this;
- never returns `500` because PVMBG is down; if there is no last-known volcanic data yet (e.g. first boot before any successful poll), volcanic events are simply absent from the result rather than fabricated.

### Concurrency design (critical requirement)

Internal API reads hit Postgres directly — they never wait on a polling
cycle. Combined with the two independent poller goroutines above, this
makes it structurally impossible for a slow PVMBG call to slow down a
BMKG-only `/internal/hazards?hazard_type=SEISMIC` read. `internal/poller`'s
`TestConcurrency_PVMBGSlow_BMKGStaysFast` test proves this with a 3-second
PVMBG `httptest` server running concurrently with 20 BMKG poll cycles,
asserting p95 < 300ms.

### NATS publishing

Every newly inserted `HazardEvent` is queued in Postgres and published to
subject `hazard.events` on the durable JetStream stream `HAZARDS`
(`NATS_STREAM`/`NATS_SUBJECT`, both configurable), wrapped in the envelope:

```json
{
  "envelope_version": 1,
  "published_at": "2026-09-24T12:34:56.123Z",
  "correlation_id": "...",
  "payload": { "...": "canonical HazardEvent" }
}
```

The dispatcher retries pending rows until JetStream acknowledges the write.
It then removes the row from the outbox. A crash between acknowledgement and
removal can publish a duplicate; consumers must deduplicate by `hazard_id`.

### Internal read API

| Method | Path | Query params | Returns |
| --- | --- | --- | --- |
| GET | `/internal/hazards` | `since`, `hazard_type`, `severity`, `area_name` (all optional; filtering happens in SQL, not in memory) | `{"data": [...], "count": N, "as_of": "<ISO8601>", "source_status": {...}}` |
| GET | `/internal/hazards/{id}` | — | A single `HazardEvent`, or `404` |
| GET | `/health` | — | `{"status":"ok","service":"aggregator"}` |
| GET | `/ready` | — | Store and broker availability; `503` when the store is unavailable |

```bash
docker compose exec -T aggregator wget -qO- 'http://localhost:8080/internal/hazards' | jq '.count'
docker compose exec -T aggregator wget -qO- 'http://localhost:8080/internal/hazards?hazard_type=VOLCANIC&severity=SIAGA'
docker compose exec -T aggregator wget -qO- 'http://localhost:8080/internal/hazards/<hazard-id>'
```

---

## Demo scenarios

All three commands below assume `docker compose up --build` is running.

### Demo A — normal aggregation

```bash
docker compose up --build
sleep 8   # let a few poll cycles happen

curl -H "X-BMKG-Key: changeme-bmkg-key" http://localhost:8081/seismic-events | jq 'length'
curl -H "Authorization: Bearer changeme-pvmbg-token" http://localhost:8082/volcanic-reports | jq 'length'
docker compose exec -T aggregator wget -qO- 'http://localhost:8080/internal/hazards' | jq '.count, .data[0]'
```

You should see 20 seismic records from BMKG, 20 volcanic reports from
PVMBG, and both reflected as canonical `HazardEvent`s from the Aggregator.
NATS receipt can be observed via the container logs
(`docker compose logs aggregator | grep "outbound call"` shows the publish
lines with `target_service":"nats"`), or by running a throwaway subscriber:

```bash
docker run --rm --network tugasbesaraat_backend natsio/nats-box:latest \
  nats sub -s nats://broker:4222 hazard.events
```

### Demo B — PVMBG schema evolution

```bash
curl -H "Authorization: Bearer changeme-pvmbg-token" http://localhost:8082/volcanic-reports | jq '.[0] | has("confidence_level")'
# => false

curl -X POST -H "Authorization: Bearer changeme-pvmbg-admin-token" http://localhost:8082/admin/schema-version
curl -H "Authorization: Bearer changeme-pvmbg-token" http://localhost:8082/volcanic-reports | jq '.[0] | has("confidence_level")'
# => true

sleep 5   # let the Aggregator's next poll cycle pick up the new shape

docker compose exec -T aggregator wget -qO- 'http://localhost:8080/internal/hazards?hazard_type=VOLCANIC' | \
  jq '.data[] | select(.attributes | has("confidence_level")) | .attributes.confidence_level'
```

No restart of any container is needed at any point in this sequence.
Records ingested before the toggle remain readable via
`/internal/hazards/{id}` without `confidence_level` in `attributes`.

### Demo C — PVMBG outage

```bash
curl -X POST -H "Authorization: Bearer changeme-pvmbg-admin-token" http://localhost:8082/admin/outage

curl -i http://localhost:8082/health        # still 200
curl -i http://localhost:8080/health        # still 200
curl -H "X-BMKG-Key: changeme-bmkg-key" http://localhost:8081/seismic-events | jq 'length'   # unaffected

sleep 5   # let the Aggregator notice PVMBG is down
docker compose exec -T aggregator wget -qO- 'http://localhost:8080/internal/hazards?hazard_type=VOLCANIC' | jq '.data[0].attributes.stale, .data[0].attributes.stale_since, .source_status.PVMBG'

docker compose logs aggregator | grep '"level":"warn"' | grep pvmbg

curl -X POST -H "Authorization: Bearer changeme-pvmbg-admin-token" http://localhost:8082/admin/outage
sleep 5
docker compose exec -T aggregator wget -qO- 'http://localhost:8080/internal/hazards?hazard_type=VOLCANIC' | jq '.data[0].attributes | has("stale")'
# => false, once the next successful poll clears the stale flag
```

### Demo D — BMKG stays fast while PVMBG is slow

```bash
# Force PVMBG's random delay to a fixed ~3s window
docker compose stop pvmbg-mock
PVMBG_MIN_DELAY_MS=3000 PVMBG_MAX_DELAY_MS=3000 docker compose up -d pvmbg-mock

for i in $(seq 1 20); do
  /usr/bin/time -f "%e s" docker compose exec -T aggregator wget -qO /dev/null 'http://localhost:8080/internal/hazards?hazard_type=SEISMIC'
done
```

Or use the Aggregator's own proof: `cd aggregator && go test ./internal/poller/... -run TestConcurrency -v`
runs exactly this scenario against `httptest` servers (BMKG instant, PVMBG
3s) and asserts p95 < 300ms for the BMKG-only path — no faked numbers, the
test actually measures it.

---

## Implementation notes / assumptions

- **`since` semantics on both mocks**: an omitted or unparseable `since`
  returns the full current dataset, per the spec's "handle missing `since`
  sensibly" guidance.
- **PVMBG admin auth**: reuses the same Bearer scheme as the data endpoint
  but with a distinct `PVMBG_ADMIN_TOKEN`, since the spec asks for an
  "environment-configured admin credential" without prescribing a mechanism.
- **Outage detection window**: the Aggregator only learns PVMBG is down on
  its next poll cycle (≤ `POLL_INTERVAL` seconds), not instantly on the
  admin toggle — this is inherent to a polling architecture and is why the
  demo scripts above include a short `sleep`.
- **Volcano reference data**: implemented as the `volcano_reference` Postgres
  table (the spec's preferred option), seeded with 10 Indonesian volcanoes
  matching PVMBG's mock `volcano_id` values exactly.
- **Runtime CA certificates**: the mock and Aggregator Dockerfiles copy
  `/etc/ssl/certs/ca-certificates.crt` from the `golang:1.27.1-alpine3.24` builder
  stage into the `alpine:3.24` runtime stage instead of running
  `apk add ca-certificates` there, so the runtime image needs no extra
  network access at build time and still ends up with a working CA bundle.
- **Dedup vs. schema evolution**: `SaveDedup` upserts on
  `(source, source_ref_id)` — a genuinely new record inserts and queues an
  outbox event; a record seen again on a later poll cycle
  updates its mutable columns (severity, area, coordinates, `attributes`)
  in place but keeps its original `hazard_id` and is never re-published.
  This is what lets PVMBG's schema-version toggle reach already-ingested
  rows on the very next poll cycle with no restart, matching Demo B.
