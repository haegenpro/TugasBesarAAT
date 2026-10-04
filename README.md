# TugasBesarAAT — IF4031 M1

This repository contains the BMKG and PVMBG mocks, BNPB Aggregator,
Aggregator-owned Canonical Store, Client API, Auth Service, and an interactive
downstream client, and two durable broker consumers.

## Services

| Service | Directory | Port | Purpose |
| --- | --- | --- | --- |
| BMKG mock | [`bmkg-mock/`](bmkg-mock/) | 8081 | Simulated seismic/tsunami source |
| PVMBG mock | [`pvmbg-mock/`](pvmbg-mock/) | 8082 | Simulated volcanic-activity source |
| Aggregator | [`aggregator/`](aggregator/) | internal (8080) | Polls both mocks, maps to canonical `HazardEvent`, owns the Store, publishes to NATS, serves the internal read API |
| Canonical Store | [`store/`](store/) | internal (5432) | PostgreSQL 16, accessed only by the Aggregator |
| Schema Registry | [`registry/`](registry/) | 8084 | Apicurio Registry with its own persistent PostgreSQL database |
| Auth Service | [`auth-service/`](auth-service/) | internal (8083) | Owns persistent sessions, issues and rotates tokens |
| Client API | [`client-api/`](client-api/) | 8080 | Authenticated downstream reads and server-side field projection |
| Client TUI | [`client-tui/`](client-tui/) | interactive | One terminal app for Media, Field, and Internal identities |
| Dashboard consumer | [`consumer-dashboard/`](consumer-dashboard/) | internal (8091) | Durable JetStream subscription and SQLite hazard projection |
| Notifier consumer | [`consumer-notifier/`](consumer-notifier/) | internal (8092) | Durable JetStream subscription and simulated notification ledger |

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
The broker also maintains a separate `HAZARDS_DLQ` stream. Each consumer has
its own durable cursor and publishes undecodable messages to its own DLQ
subject after the failure rules below. The broker stores and delivers bytes;
schema lookup and validation happen in each consumer after delivery.

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
registry/         # Apicurio Registry + separate persistent database
consumer-dashboard/ # durable dashboard projection
consumer-notifier/  # durable simulated notifications
scripts/          # smoke test, isolation demo, k6 load test
docker-compose.yml
.env.example
docs/             # Pre-existing team contract docs (wire format, stack, deployment)
```

## Quick start: Docker Compose

Create the eleven service env files manually using the root `.env.example`
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
| `bmkg-mock/.env` | BMKG mock | Port, API key, delay, schema registry URL |
| `pvmbg-mock/.env` | PVMBG mock | Port, source/admin tokens, delays, schema registry URL |
| `broker/.env` | Broker and optional broker-init tool | NATS URL, stream, subject, retention |
| `store/.env` | PostgreSQL | Database name, user, password |
| `registry/.env` | Schema Registry and its database | Separate database credentials and Apicurio storage settings |
| `aggregator/.env` | Aggregator | Source URLs/credentials, database URL, NATS publication, schema registry URL, polling |
| `auth-service/.env` | Auth Service | Three client credentials, internal key, TTLs, SQLite path |
| `client-api/.env` | Client API | Port, Auth/Aggregator URLs, internal key |
| `client-tui/.env` | Interactive TUI | API URL and public client IDs; no login secrets |
| `consumer-dashboard/.env` | Dashboard consumer | Role, durable name, SQLite path, broker and registry URLs, DLQ subject |
| `consumer-notifier/.env` | Notifier consumer | Role, durable name, SQLite path, broker and registry URLs, DLQ subject |

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
| `CLIENT_API_MAX_INFLIGHT` | Max concurrent Client API requests; excess gets `429` (`0` disables) | `128` |
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

Concurrency model: Go serves each request on its own goroutine, and the Client
API caps in-flight requests at `CLIENT_API_MAX_INFLIGHT`. A request over the cap
is rejected immediately with `429` and `Retry-After: 1`, without queueing and
before any call to Auth, so overload cannot pile up upstream work. Each upstream
call is bounded by a 5 s client timeout. `/health` and `/ready` are exempt and
stay answerable under saturation. Rejections are logged as `request rejected`.

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
- **Live data:** one new seismic event every `BMKG_EVENT_INTERVAL_MS` (default `10000`, allowed 1–10000; lower it to speed up a demo), with a tsunami warning when the event has tsunami potential. IDs look like `evt-<boot-unix>-<n>`, so they stay unique across restarts. The mock keeps the 20 seed events plus the latest 100 generated ones.

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
- **Seed data:** 20 deterministic volcanic reports across all four alert levels (Normal/Waspada/Siaga/Awas), spanning 10 seeded Indonesian volcanoes. Some source reports have a `confidence_level` value, hidden until the toggle is activated.
- **Live data:** one new volcanic report every `PVMBG_REPORT_INTERVAL_MS` (default `10000`, allowed 1–10000; lower it to speed up a demo), with IDs like `rep-<boot-unix>-<n>`. Generation continues during an outage, so the Aggregator catches up when the outage ends. The mock keeps the seed reports plus the latest 100 generated ones.

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
- **Seismic → HazardEvent**: `source=BMKG`, `hazard_type=SEISMIC`, area/lat/lon from the event, severity per the priority rules below, `attributes` = `magnitude`, `depth_km`, `potential_tsunami`, plus the matching tsunami warning's fields (`warning_id`, `related_event_id`, `threat_level`, `affected_zones`, `estimated_arrival`) when one exists. Further source fields without a canonical top-level mapping also enter `attributes`.
- **Volcanic → HazardEvent**: `source=PVMBG`, `hazard_type=VOLCANIC`, area/lat/lon resolved via the `volcano_reference` table (never hardcoded in mapping code), severity from the alert-level mapping, `attributes` = integer `eruption_count_24h`, `ash_column_height_m`, and `confidence_level` when present. Further source fields without a canonical top-level mapping also enter `attributes`; an unknown volcano ID is rejected rather than given fabricated coordinates.

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

The BMKG and PVMBG REST endpoints return JSON by default. Requests with
`Accept: application/avro` receive Confluent-framed Avro bytes (magic byte,
four-byte schema ID, Avro datum). The Aggregator uses that representation for
both sources. Schemas are registered in Apicurio under
`bmkg.seismic-events-value`, `bmkg.tsunami-warnings-value`, and
`pvmbg.volcanic-reports-value`; each subject uses `FULL_TRANSITIVE`
compatibility. The PVMBG admin toggle registers its additive v2 schema on
first activation, then exposes `confidence_level` in both JSON and Avro. Each
activation also adds one newly identified report carrying the field, so the
Aggregator's source-ID deduplication keeps existing hazard IDs while updating
their attributes and stores a new canonical row from the new report. The new
field remains present in PVMBG's response and is mapped into canonical
`attributes` when present.
Unknown schema IDs and unregistered schema versions fail closed. A process can
continue using schema IDs and codecs it has already cached while the registry
is unavailable. A cold registry outage returns `503` for Avro source reads and
the first PVMBG schema activation; JSON source reads and health checks remain
available. The Aggregator keeps serving stored data and retries polling.

The Aggregator's declared unmapped-field policy is **pass through to
`attributes`** for BMKG and PVMBG fields without a canonical top-level
destination. `confidence_level` follows this same rule; the Aggregator has no
dedicated field or branch for it. PVMBG v2 is decoded without a restart. The Canonical
Store still uses JSONB for attributes and needs no migration when the source
schema changes. The new source report created by the toggle gets its own
canonical row beside earlier reports; both remain readable.
If a source removes or renames a field required by the mapping, the Aggregator
rejects that poll and logs a contract violation with the correlation ID. It
does not save a partial or zero-valued `HazardEvent`. The Client API similarly
rejects an Aggregator JSON response missing required canonical fields and
returns `503` rather than serving an incomplete hazard. The TUI reports an
invalid response if a required field in its summary list disappears.

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

Every newly inserted `HazardEvent` is queued as JSON in Postgres and published
as Avro to the configured JetStream `NATS_STREAM` and `NATS_SUBJECT`. The
example `.env` uses `HAZARDS` and `hazard.events`; neither value has a code
default. The subject's registry name is `<NATS_SUBJECT>-value`. The Avro record
has the following logical envelope:

```json
{
  "envelope_version": 1,
  "published_at": "2026-09-24T12:34:56.123Z",
  "correlation_id": "...",
  "payload": { "...": "canonical HazardEvent" }
}
```

The broker payload uses the same five-byte schema-ID frame as source Avro.
`payload.attributes` is an Avro `map<string, bytes>`; each value is a JSON
encoded attribute value, preserving the mixed types used by the Canonical
Store and Client API. Consumers must resolve the schema ID and decode Avro
before interpreting the envelope. The Aggregator registers the broker schema
when it first needs to publish, so a cold registry outage leaves the service
running and the JSON outbox pending. The Aggregator-to-Client API contract
remains JSON.

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

## Durable consumers and DLQ

Both consumers subscribe to the same `HAZARDS` stream and `hazard.events`
subject, but use independent durable names. The dashboard records the full
decoded HazardEvent in its own SQLite volume; the notifier records one local
notification summary per hazard. Each SQLite transaction commits the projection
and its `(stream, sequence, payload digest)` processing identity together.
Redelivery after an ack failure sees the committed identity and only retries a
confirmed ack. `hazard_id` also remains unique, and a second event with changed
content for the same hazard halts the consumer until an explicit versioning
contract exists. Each consumer acknowledges only after its SQLite write succeeds.
Their `/health`, `/ready`, and `/records` endpoints are reachable only on the
Compose backend network. `/records` returns the latest 100 processed records
for demo inspection.

Each consumer reads the five-byte schema-ID frame, retrieves the writer schema
from Apicurio on a cache miss, and saves validated writer schemas by registry
URL and ID in SQLite. Cached schemas survive consumer restarts and registry
outages. JSON-encoded attribute values are decoded individually. A registry
timeout or 5xx response causes a 30-second fetch cooldown and delayed broker
redelivery; already cached schemas remain usable after the cooldown. A 404
starts a persisted five-minute retry window with capped backoff and jitter.
Ten distinct unknown-schema messages within one minute halt the consumer and
set `/ready` to 503, preventing a registry misconfiguration from mass-DLQing
valid events. Corrupt frames/Avro bytes go to the DLQ immediately. An
additional ten-message-per-minute corrupt-payload guard halts a widespread
producer or registry mismatch before it fills the DLQ. An unsupported envelope
or consumer-model incompatibility halts and alerts through the error log and
readiness endpoint. SQLite busy/locked errors retry; disk, I/O, and
corruption errors halt; a message-specific constraint error retries three
times, then goes to the DLQ with `reason_code=sqlite_constraint`. Ten distinct
constraint failures within a minute halt instead. The DLQ record includes
original bytes (base64 in JSON), stream sequence, consumer, reason code,
attempt count, and time; replay is manual.

The consumer fetches one message at a time. Delayed NAKs are held by JetStream,
so later valid events can proceed while fewer than 64 messages are pending.
Processing is bounded to 25 seconds under a 60-second ack wait. JetStream
`MaxDeliver=-1` allows the application policy to finish; a finite broker limit
could stop delivery before the timed retry or DLQ publication. DLQ publication
is confirmed before the original message is confirmed as acked. Its deterministic
message ID reduces duplicate DLQ publications during JetStream's deduplication
window; downstream DLQ tooling should use `(consumer, original_stream,
stream_sequence)` as its permanent deduplication key.

On a halt, `/ready` returns 503 and a reason, and Compose marks the container
unhealthy. The halt persists across restarts. Diagnose the registry, consumer
code, or SQLite volume, then stop the consumer, remove its `halt_reason` row
from the local `consumer_control` SQLite table, and start it again. Do this
before the stream retention window expires. The `/health` endpoint remains a
liveness check. This consumer treats hazard events as immutable; no per-hazard
ordering is required for its insert-only projection. A future update event
must include a version and reject stale writes before enabling updates.

The main stream retains messages for `NATS_STREAM_MAX_AGE` (example: 24h), so
an offline durable consumer catches up only while the messages remain there.
The separate DLQ stream retains failures for `NATS_DLQ_MAX_AGE` (example: 168h).
Set `NATS_DLQ_STREAM`, `NATS_DLQ_SUBJECT_PREFIX`, and `NATS_DLQ_MAX_AGE`
in `broker/.env`; configure `NATS_DLQ_STREAM` and a distinct
`NATS_DLQ_SUBJECT` in each consumer env file. The optional third consumer
uses the dashboard image, a new durable name, and a separate SQLite volume.

```bash
docker compose exec -T consumer-dashboard wget -qO- http://localhost:8091/records
docker compose exec -T consumer-notifier wget -qO- http://localhost:8092/records
docker compose stop consumer-notifier
# Both mocks emit a new event every 10 s; wait ~30 s and the dashboard keeps receiving them.
docker compose start consumer-notifier
docker compose --profile consumer-demo up -d consumer-dashboard-demo
docker compose exec -T consumer-dashboard-demo wget -qO- http://localhost:8093/records
```

The two main consumers and the optional third one all read the existing
publisher output; adding the third requires no Aggregator change. Both new
Go modules can be checked with `go test ./...` from their directories.
To demonstrate that a bad schema ID does not hold up a later valid event,
publish a deliberately unknown ID from the broker module, then wait for the
mocks' next generated events:

```bash
cd broker
NATS_URL=nats://localhost:4222 NATS_SUBJECT=hazard.events go run ./cmd/publish-invalid
cd ..
docker compose logs consumer-dashboard consumer-notifier | grep 'message quarantined'
curl -s 'http://localhost:8222/jsz?streams=true' | jq '[.account_details[].stream_detail[] | {name, messages: .state.messages}]'
cd broker
NATS_URL=nats://localhost:4222 NATS_DLQ_STREAM=HAZARDS_DLQ go run ./cmd/inspect-dlq hazard.dlq.dashboard
NATS_URL=nats://localhost:4222 NATS_DLQ_STREAM=HAZARDS_DLQ go run ./cmd/inspect-dlq hazard.dlq.notifier
cd ..
```

The unknown ID is retried for at least five minutes before quarantine; a
registry outage pauses fetching and does not count as another 404. The later
valid event can be processed while that retry waits. Each of the two consumers
places its own copy in `HAZARDS_DLQ`. Do not use the fixture against
a shared or production broker.

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

You should see at least 20 seismic records from BMKG and 20 volcanic reports
from PVMBG, both reflected as canonical `HazardEvent`s from the Aggregator.
Each mock adds one record every 10 seconds, so the counts keep growing;
`docker compose logs aggregator | grep polled` shows `new_hazards` above zero.
NATS publish acknowledgements can be observed via the container logs
(`docker compose logs aggregator | grep "outbound call"` shows the publish
lines with `target_service":"nats"`). A throwaway subscriber receives binary
Avro bytes, which need the registry schema to decode:

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
  jq '.data[] | select(.source_ref_id == "rep-00001" or .source_ref_id == "rep-schema-00001") | {source_ref_id, attributes}'
```

No restart of any container is needed at any point in this sequence.
The two canonical rows have different source IDs. After the toggle, the
Aggregator updates previously ingested reports that now include
`confidence_level` and stores the newly identified report with that attribute.

To trigger a registry failure during the demo, stop it with
`docker compose stop schema-registry`. A source process that has already
registered its current schema can still answer Avro requests from its cache.
Restart `pvmbg-mock` while the registry is stopped to show a cold cache:
its JSON endpoint and `/health` still answer, while an Avro request returns
`503` and the first schema toggle cannot activate. Start the registry again
with `docker compose start schema-registry`; the next Avro request can register
its schema and the toggle can proceed.

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

The mock reads its delay from `pvmbg-mock/.env`, not from the shell, so
prefixing `docker compose` with `PVMBG_MIN_DELAY_MS=...` has no effect. Edit
the file and recreate the container:

```bash
# In pvmbg-mock/.env set PVMBG_MIN_DELAY_MS=3000 and PVMBG_MAX_DELAY_MS=3000, then:
docker compose up -d --force-recreate pvmbg-mock

for i in $(seq 1 20); do
  /usr/bin/time -f "%e s" docker compose exec -T aggregator wget -qO /dev/null 'http://localhost:8080/internal/hazards?hazard_type=SEISMIC'
done
```

That loop is sequential. For the concurrent BMKG-only versus PVMBG-path
comparison, run the k6 mixed mode from Demo F while the delay is 3s. Restore
the original delay values and recreate the container afterwards.

Or use the Aggregator's own proof: `cd aggregator && go test ./internal/poller/... -run TestConcurrency -v`
runs exactly this scenario against `httptest` servers (BMKG instant, PVMBG
3s) and asserts p95 < 300ms for the BMKG-only path — no faked numbers, the
test actually measures it.

### Demo E — institution credential isolation

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/isolation-demo.ps1
```

Prints one pass/fail line per probe: each institution's own credential gets
`200`; the BMKG key on PVMBG (data and admin), the PVMBG token on BMKG, the
PVMBG data token on its admin endpoint all get `403`; no credential gets
`401`. Reads credentials from the service env files and never prints them.

### Demo F — load test (k6 in Docker)

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/load-test.ps1 -Mode sustained   # 60 VUs, 70s on /hazards
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/load-test.ps1 -Mode mixed -Vus 50 -Duration 65s
```

`sustained` reports throughput, p50/p95/p99, error rate, and the count of
`429` responses (`controlled_rejections_429`). At the default cap of 128 and 60
VUs expect none; raise `-Vus` (for example 300) or lower
`CLIENT_API_MAX_INFLIGHT` to see controlled rejections.
`mixed` splits the VUs between `hazard_type=SEISMIC` and `VOLCANIC` and fails
unless the SEISMIC p95 is under 300 ms; run it with PVMBG's delay at 3s (see
Demo D). Each VU logs in as Field and logs in again every 45 s, because the
access token lives 60 s. Volcanic reads come from the Canonical Store, so this
shows the request path is decoupled from the slow PVMBG poller. It requires Docker and uses
`host.docker.internal` to reach the Client API on port 8080.

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
  The schema-version toggle creates a fresh source report ID, so Demo B
  stores a new row alongside pre-toggle rows without changing their IDs.
