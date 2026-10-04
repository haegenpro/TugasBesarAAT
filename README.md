# TugasBesarAAT, IF4031 M1

This repository holds everything for Milestone 1: the BMKG and PVMBG mocks,
the BNPB Aggregator and its Canonical Store, the Client API, the Auth Service,
an interactive downstream client, and two durable broker consumers.

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

Every Go service has its own `go.mod`, so there is no shared workspace module,
and its own multi-stage Dockerfile that builds on `golang:1.27.1-alpine3.24`
and runs on `alpine:3.24`. All Go modules require Go 1.27.1.

## Broker choice: NATS JetStream, not RabbitMQ

The assignment text mentions RabbitMQ as an example. We went with
**NATS JetStream** instead, and the reasoning is in
`docs/Technical Stack Specifications.md` and `docs/Contracts.md`.

Here is how the broker is set up:

- The image extends the official `nats:2.15.0-alpine3.22`, mounts
  `broker/nats.conf` read-only, and initializes its stream every time it
  starts. JetStream storage lives in a volume.
- The Aggregator saves each new `HazardEvent` and its pending publication in
  one Postgres transaction. A dispatcher then publishes the event to stream
  `HAZARDS` on subject `hazard.events`, and removes it from the outbox only
  after JetStream acknowledges it.
- A separate `HAZARDS_DLQ` stream holds failures. Each consumer has its own
  durable cursor and sends messages it cannot decode to its own DLQ subject,
  following the rules described further down.
- The broker only stores and delivers bytes. Schema lookup and validation
  happen in each consumer after delivery.

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
docs/             # Team contract docs (wire format, stack, deployment)
```

## Quick start: Docker Compose

First, create the eleven service env files by hand. Use the root
`.env.example` together with the tables below, and copy into each file only
the variables that service needs, including the shared ones the template
points out. Then, from the repository root, run:

```powershell
docker compose up -d --build
```

A few things are worth knowing about startup:

- Every service can start on its own. The Aggregator answers `/health` even
  while Postgres or NATS is down. It keeps retrying, starts polling once
  Postgres and the tables from `store/init.sql` are ready, and queues events
  in the outbox until NATS is back. Its `/ready` endpoint reports store and
  broker availability.
- The broker health check uses `NATS_URL` from `broker/.env` and passes only
  once the stream is initialized and reachable. If you need to reapply the
  stream configuration, run
  `docker compose --profile tools run --rm broker-init`.
- Changes to `broker/nats.conf` take effect after you recreate the broker
  with `docker compose up -d --force-recreate broker`. You do not need to
  rebuild its image.
- Postgres runs `init.sql` only on a brand new data volume. If your local
  volume was created before `hazard_outbox` existed, recreate it or apply the
  schema manually.

Once everything is healthy, these should all respond:

```bash
curl http://localhost:8081/health   # {"status":"ok","service":"bmkg-mock"}
curl http://localhost:8082/health   # {"status":"ok","service":"pvmbg-mock"}
curl http://localhost:8080/health   # client-api
curl http://localhost:8080/ready    # auth and aggregator readiness
```

### Per-service environment files

The root `.env.example` is the only template. Create the following
gitignored files yourself, each with just the variables its service needs:

| File | Used by | Configuration |
| --- | --- | --- |
| `bmkg-mock/.env` | BMKG mock | Port, API key, delay, event interval, schema registry URL |
| `pvmbg-mock/.env` | PVMBG mock | Port, source/admin tokens, delays, report interval, schema registry URL |
| `broker/.env` | Broker and optional broker-init tool | NATS URL, streams, subjects, retention |
| `store/.env` | PostgreSQL | Database name, user, password |
| `registry/.env` | Schema Registry and its database | Separate database credentials and Apicurio storage settings |
| `aggregator/.env` | Aggregator | Source URLs/credentials, database URL, NATS publication, schema registry URL, polling |
| `auth-service/.env` | Auth Service | Three client credentials, internal key, TTLs, SQLite path |
| `client-api/.env` | Client API | Port, Auth/Aggregator URLs, internal key |
| `client-tui/.env` | Interactive TUI | API URL and public client IDs, no login secrets |
| `consumer-dashboard/.env` | Dashboard consumer | Role, durable name, SQLite path, broker and registry URLs, DLQ subject |
| `consumer-notifier/.env` | Notifier consumer | Role, durable name, SQLite path, broker and registry URLs, DLQ subject |

Some notes on how these files are used:

- Compose passes the values in through `env_file`. It does not mount the
  files, and `.dockerignore` keeps them out of the build context. A root
  `.env` is not needed at runtime.
- The secrets in `.env.example` are placeholders for demos. Put your real
  values in the service files.
- Each value has to fit on one line. Use single quotes when the value
  contains characters such as `$` or `#`, for example
  `AUTH_INTERNAL_KEY='your-key'`.

Several values appear in more than one file and must match:

- The BMKG key and the PVMBG source token are in their mock and in the
  Aggregator.
- The internal Auth key is in Auth and in the Client API.
- The client IDs are in Auth and in the TUI.
- The NATS URL, stream, and subject are in the Broker, the Aggregator, and
  both consumers.
- The Aggregator's `DATABASE_URL` is built from the credentials in
  `store/.env`. URI-encode the user, password, or database name if they
  contain special characters.

If you change the PostgreSQL credentials later, update `store/.env` and the
Aggregator's URL together, and update the existing database as well.
Recreating the container does not change users in a volume that is already
initialized.

On ports: the host mappings are fixed at `8080` for the Client API, `8081`
for BMKG, `8082` for PVMBG, and `4222`/`8222` for NATS. Keep
`CLIENT_API_PORT=8080`, `BMKG_PORT=8081`, and `PVMBG_PORT=8082` so they line
up. If you change any other port, keep the service URLs, health checks, and
Compose mappings consistent with it.

To apply an env change, run `docker compose up -d --force-recreate` and
restart the TUI if you have one open. You only need to rebuild images when
the code changes.

The Auth Service keeps its SQLite database in the `auth-data` volume, so
restarting the Auth container does not log anyone out.

### Configuration variables

"Required" means the service refuses to start without the variable. Please
set every `*_PORT` variable even where a default exists, because the Compose
health checks read the port from the same env file. Example values for all
of them are in `.env.example`.

**`bmkg-mock/.env`**

| Variable | Purpose | Default |
| --- | --- | --- |
| `BMKG_PORT` | HTTP port, fixed host mapping | `8081` |
| `BMKG_API_KEY` | Key expected in `X-BMKG-Key` | Required |
| `BMKG_DELAY_MS` | Fixed response delay (spec range 50 to 150) | `100` |
| `BMKG_EVENT_INTERVAL_MS` | Interval between generated seismic events, 1 to 10000 | `10000` |
| `SCHEMA_REGISTRY_URL` | Registry used for Avro responses | Compose registry URL |

**`pvmbg-mock/.env`**

| Variable | Purpose | Default |
| --- | --- | --- |
| `PVMBG_PORT` | HTTP port, fixed host mapping | `8082` |
| `PVMBG_TOKEN` | Bearer token for `/volcanic-reports` | Required |
| `PVMBG_ADMIN_TOKEN` | Bearer token for `/admin/*`, must differ from `PVMBG_TOKEN` | Required |
| `PVMBG_MIN_DELAY_MS`, `PVMBG_MAX_DELAY_MS` | Random response delay range | `500`, `3000` |
| `PVMBG_REPORT_INTERVAL_MS` | Interval between generated volcanic reports, 1 to 10000 | `10000` |
| `SCHEMA_REGISTRY_URL` | Registry used for Avro responses | Compose registry URL |

**`aggregator/.env`**

| Variable | Purpose | Default |
| --- | --- | --- |
| `AGGREGATOR_PORT` | Internal HTTP port | `8080` |
| `POLL_INTERVAL` | Polling interval in seconds (spec suggests 2 to 5) | `3` |
| `BMKG_BASE_URL`, `PVMBG_BASE_URL` | Source URLs, use the Compose service URLs from `.env.example` | `http://localhost:8081`, `http://localhost:8082` |
| `BMKG_API_KEY`, `PVMBG_TOKEN` | Source credentials, must match the mocks and differ from each other | Required |
| `DATABASE_URL` | Canonical Store connection, must match `store/.env` | Required |
| `NATS_URL` | Broker URL | `nats://localhost:4222` |
| `NATS_STREAM`, `NATS_SUBJECT` | JetStream stream and subject to publish to | Required |
| `SCHEMA_REGISTRY_URL` | Registry for source and broker schemas | Required |

**`store/.env`** and **`registry/.env`**

| Variable | Purpose | Default |
| --- | --- | --- |
| `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | Database credentials, each file has its own values | Required |
| `APICURIO_STORAGE_KIND`, `APICURIO_STORAGE_SQL_KIND` | Registry storage backend (`registry/.env` only) | Required |
| `APICURIO_DATASOURCE_URL`, `APICURIO_DATASOURCE_USERNAME`, `APICURIO_DATASOURCE_PASSWORD` | Registry database connection, must match the `POSTGRES_*` values in `registry/.env` | Required |

**`broker/.env`**

| Variable | Purpose | Default |
| --- | --- | --- |
| `NATS_URL` | URL the stream initializer and health check connect to | Required |
| `NATS_STREAM`, `NATS_SUBJECT` | Main stream and its subject | Required |
| `NATS_STREAM_MAX_AGE` | Main stream retention, for example `24h` | Required |
| `NATS_DLQ_STREAM`, `NATS_DLQ_SUBJECT_PREFIX` | Dead-letter stream and subject prefix | Required |
| `NATS_DLQ_MAX_AGE` | Dead-letter retention, for example `168h` | Required |

**`auth-service/.env`**

| Variable | Purpose | Default |
| --- | --- | --- |
| `MEDIA_CLIENT_ID`, `MEDIA_CLIENT_SECRET` | Media identity | Required |
| `FIELD_CLIENT_ID`, `FIELD_CLIENT_SECRET` | Field identity | Required |
| `INTERNAL_CLIENT_ID`, `INTERNAL_CLIENT_SECRET` | Internal identity | Required |
| `AUTH_INTERNAL_KEY` | Auth-to-Client-API trust key | Required |
| `ACCESS_TOKEN_TTL_SEC` | Access token lifetime | `60` |
| `REFRESH_TOKEN_TTL_SEC` | Refresh token lifetime, must exceed the access TTL | `86400` |
| `AUTH_DB_PATH` | Auth-owned SQLite file inside its container | `/data/auth.db` |
| `AUTH_PORT` | Auth container HTTP port | `8083` |

**`client-api/.env`** and **`client-tui/.env`**

| Variable | Purpose | Default |
| --- | --- | --- |
| `CLIENT_API_PORT` | Client API container HTTP port, fixed host mapping | `8080` |
| `AUTH_INTERNAL_KEY` | Same key as in `auth-service/.env` | Required |
| `AUTH_BASE_URL`, `AGGREGATOR_BASE_URL` | Internal upstream URLs | Compose service URLs |
| `CLIENT_API_MAX_INFLIGHT` | Max concurrent Client API requests, excess gets `429` (`0` disables) | `128` |
| `BNPB_API_URL` | TUI destination URL | `http://localhost:8080` |
| `MEDIA_CLIENT_ID`, `FIELD_CLIENT_ID`, `INTERNAL_CLIENT_ID` | Public client IDs shown by the TUI, which prompts for one if unset | None |

**`consumer-dashboard/.env`** and **`consumer-notifier/.env`** (all required)

| Variable | Purpose | Dashboard | Notifier |
| --- | --- | --- | --- |
| `CONSUMER_ROLE` | Projection type | `dashboard` | `notifier` |
| `CONSUMER_PORT` | Internal HTTP port | `8091` | `8092` |
| `CONSUMER_DB_PATH` | Consumer-owned SQLite file | `/data/dashboard.db` | `/data/notifier.db` |
| `NATS_DURABLE` | Durable consumer name | `dashboard-v1` | `notifier-v1` |
| `NATS_DLQ_SUBJECT` | This consumer's dead-letter subject | `hazard.dlq.dashboard` | `hazard.dlq.notifier` |
| `NATS_URL`, `NATS_STREAM`, `NATS_SUBJECT`, `NATS_DLQ_STREAM` | Same values as `broker/.env` | | |
| `SCHEMA_REGISTRY_URL` | Registry for decoding events | | |

### Client API and TUI

| Method | Endpoint | Purpose |
| --- | --- | --- |
| POST | `/auth/login` | JSON `client_id`, `client_secret`. Returns bearer access and refresh tokens, TTL, and the server-assigned role |
| POST | `/auth/refresh` | JSON `refresh_token`. Rotates both tokens and revokes the old access token immediately |
| POST | `/auth/logout` | JSON `refresh_token`. Revokes the session |
| GET | `/hazards` | Bearer token, optional `since`, `hazard_type`, `severity`, `area_name`, `view=summary\|raw` |
| GET | `/hazards/{id}` | Bearer token, optional `view=summary\|raw` |
| GET | `/health`, `/ready` | Process health and dependency readiness |

What each client sees:

- **Media** gets only `hazard_id`, `source`, `hazard_type`, `severity`,
  `area_name`, `occurred_at`, and `ingested_at` for each hazard. If Media
  asks for `view=raw`, the server answers `403`.
- **Field** and **Internal** get the full canonical event by default.
- `source_status` is response metadata that sits outside the hazard objects,
  so even Media can tell when PVMBG data is stale.

To try it out, start the stack and open the TUI:

```bash
docker compose up -d --build
docker compose run --rm --build client-tui
```

Run the TUI command in three terminals if you want to test the three
identities side by side. The TUI reads the public client IDs from Compose
and asks you for the secret. From there you can pick a role, list or filter
hazards, open a detail, or request raw data. You can also run the same TUI
on the host with `BNPB_API_URL=http://localhost:8080`.

For the refresh demo, log in as Field, wait a little over the default 60
seconds, then list hazards. The expired token gets `401`, the TUI refreshes
on its own and retries the read, and you do not have to log in again. After
that, the "Uji token lama" menu item confirms that the old token now gets
`401`.

About concurrency: Go serves each request on its own goroutine, and the
Client API caps in-flight requests at `CLIENT_API_MAX_INFLIGHT`. A request
over the cap is rejected right away with `429` and `Retry-After: 1`. It is
not queued and it never reaches Auth, so an overload cannot pile work up on
the upstream services. Each upstream call has a 5 second timeout. `/health`
and `/ready` are exempt from the cap, so they keep answering under load.
Rejections show up in the log as `request rejected`.

About logging and isolation: the Client API and the Auth Service write JSON
logs that carry `correlation_id`. The Client API forwards `X-Correlation-ID`
to Auth and to the Aggregator, and logs the latency of every outbound call.
The database network contains only PostgreSQL and the Aggregator, and the
Aggregator's port is not published to the host.

On Windows there is a smoke test that covers most of this in one go:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/smoke.ps1 -RestartAuth -WaitForExpiry -TestOutage
```

It checks institution credential isolation, the three client identities,
Media redaction, token rotation, natural expiry, PVMBG outage status, and
session persistence across an Auth restart. It reads the demo credentials
from the service env files and prints only a pass or fail line.

## Standalone (non-Docker) run

Each service reads its configuration from process environment variables. The
Go services do not load `.env` files on their own outside Docker, so export
the values you need before starting them, and use `localhost` URLs for
anything running on the host. The Aggregator can start without Postgres or
NATS and recovers once they appear. Starting the broker also initializes its
JetStream stream.

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

If you also want the Aggregator's Postgres-backed store tests to run:

```bash
docker run -d --name pg-test -p 5433:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=hazards \
  -v $(pwd)/store/init.sql:/docker-entrypoint-initdb.d/init.sql postgres:16-alpine3.24
cd aggregator && TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5433/hazards?sslmode=disable" go test ./internal/store/...
```

---

## BMKG mock (`bmkg-mock/`)

This is the simulated BMKG source. It serves seismic events and tsunami
warnings.

- **Port:** 8081
- **Auth:** send `X-BMKG-Key: <BMKG_API_KEY>` on the data endpoints. A
  missing key gets `401` and a wrong key gets `403`.
- **Delay:** a fixed delay per request, set by `BMKG_DELAY_MS` (default
  `100`, and the spec asks for 50 to 150 ms).
- **Seed data:** 20 deterministic seismic events, built at boot from a fixed
  epoch so every run starts with the same data. They cover magnitudes below
  5.0, between 5.0 and 6.5, and 6.5 or above, with and without tsunami
  potential, plus related tsunami warnings at Waspada, Siaga, and Awas.
- **Live data:** one new seismic event every `BMKG_EVENT_INTERVAL_MS`
  (default `10000`, allowed 1 to 10000). Lower it if you want a faster demo.
  An event with tsunami potential also gets a tsunami warning. The IDs look
  like `evt-<boot-unix>-<n>`, which keeps them unique across restarts. The
  mock keeps the 20 seed events plus the latest 100 generated ones.

### Endpoints

| Method | Path | Auth | Notes |
| --- | --- | --- | --- |
| GET | `/seismic-events?since=<ISO8601>` | `X-BMKG-Key` | `since` is optional. Without it, or with an invalid value, you get the full current dataset |
| GET | `/tsunami-warnings?since=<ISO8601>` | `X-BMKG-Key` | Same `since` behavior |
| GET | `/health` | none | `{"status":"ok","service":"bmkg-mock"}` |

### Sample requests

```bash
curl -H "X-BMKG-Key: devkey" http://localhost:8081/seismic-events | jq '.[0]'
curl -H "X-BMKG-Key: devkey" "http://localhost:8081/seismic-events?since=2026-09-01T00:00:00Z"
curl -H "X-BMKG-Key: devkey" http://localhost:8081/tsunami-warnings | jq '.[0]'
```

The response is an array of objects with snake_case fields and UTC
timestamps in milliseconds:

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

This is the simulated PVMBG source. It serves volcanic activity reports and
has two admin toggles that the demos rely on.

- **Port:** 8082
- **Auth:** send `Authorization: Bearer <PVMBG_TOKEN>` on
  `/volcanic-reports`. This is deliberately a **different mechanism** from
  BMKG's header key, as the spec requires. The admin endpoints need
  `Authorization: Bearer <PVMBG_ADMIN_TOKEN>`, which is a separate credential.
- **Delay:** a random delay per request between `PVMBG_MIN_DELAY_MS` and
  `PVMBG_MAX_DELAY_MS` (default 500 to 3000 ms).
- **Seed data:** 20 deterministic volcanic reports across all four alert
  levels (Normal, Waspada, Siaga, Awas) and 10 Indonesian volcanoes. Some of
  them already have a `confidence_level` value, which stays hidden until the
  schema toggle is on.
- **Live data:** one new volcanic report every `PVMBG_REPORT_INTERVAL_MS`
  (default `10000`, allowed 1 to 10000). Lower it if you want a faster demo.
  The IDs look like `rep-<boot-unix>-<n>`. Generation keeps going during an
  outage, so the Aggregator catches up once the outage ends. The mock keeps
  the seed reports plus the latest 100 generated ones.

### Endpoints

| Method | Path | Auth | Notes |
| --- | --- | --- | --- |
| GET | `/volcanic-reports?since=<ISO8601>` | Bearer `PVMBG_TOKEN` | `confidence_level` appears only while the schema toggle is on |
| POST | `/admin/schema-version` | Bearer `PVMBG_ADMIN_TOKEN` | Toggles `confidence_level` visibility in memory, no restart |
| POST | `/admin/outage` | Bearer `PVMBG_ADMIN_TOKEN` | Toggles outage mode in memory, no restart |
| GET | `/health` | none | Always `200`, even during an outage, because the outage is simulated and the process itself is fine |

### Schema-version toggle

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

### Outage toggle

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

If you would rather set the state explicitly than toggle it, for example in
a script, `/admin/outage` also accepts a body of `{"enabled": true}` or
`{"enabled": false}`.

---

## Aggregator (`aggregator/`)

This is the BNPB gateway. It polls BMKG and PVMBG concurrently, maps both
into the canonical `HazardEvent`, owns the Canonical Store, deduplicates,
publishes new events to NATS JetStream, and serves the internal read API
that the Client API calls.

### Polling

Each source has its own goroutine with its own `time.Ticker`, firing every
`POLL_INTERVAL` seconds (default 3, and the spec suggests 2 to 5). The two
pollers share no lock, so a slow or failing PVMBG call never delays the BMKG
poller, and the other way around.

Each cycle fetches the full current dataset from the mock. We do not track a
last-poll timestamp, because at this data volume it is simpler and safer to
let the Store's unique constraint handle duplicates.

### Mapping

`internal/mapping` does the conversion:

- **Seismic to HazardEvent:** `source=BMKG`, `hazard_type=SEISMIC`, and the
  area and coordinates come from the event. Severity follows the rules
  below. `attributes` holds `magnitude`, `depth_km`, and `potential_tsunami`,
  plus the related tsunami warning's fields (`warning_id`,
  `related_event_id`, `threat_level`, `affected_zones`, `estimated_arrival`)
  when there is one.
- **Volcanic to HazardEvent:** `source=PVMBG`, `hazard_type=VOLCANIC`, and
  the area and coordinates come from the `volcano_reference` table, never
  from values hardcoded in the mapping. Severity maps directly from the
  alert level. `attributes` holds `eruption_count_24h`,
  `ash_column_height_m`, and `confidence_level` when it is present. A report
  with an unknown volcano ID is rejected, because we would rather skip it
  than invent coordinates.

In both cases, any other source field that has no canonical top-level home
also goes into `attributes`.

**Severity rules for seismic events**, in priority order:

1. If any related tsunami warning is `Awas`, the severity is `AWAS`,
   whatever the magnitude.
2. Otherwise it follows the highest related warning: Waspada gives
   `WASPADA`, Siaga gives `SIAGA`.
3. With no warning at all it follows the magnitude: below 5.0 is `NORMAL`,
   5.0 up to but not including 6.5 is `WASPADA`, and 6.5 or above is `SIAGA`.

### Deduplication

Records are keyed on `(source, source_ref_id)`, never on `hazard_id`, and
the Store's unique index is the final guard. A new row and its outbox record
commit together. When a record shows up again on a later poll, its canonical
row is updated and no second outbox record is added.

### Canonical Store

The details are in [`store/README.md`](store/README.md). The Aggregator is
the only service that talks to this database, and inside the Aggregator all
access goes through the repository in `internal/store` (`SaveDedup`,
`GetByID`, `Query`, `LastKnownVolcanic`).

### Schema evolution

The mocks return JSON by default. A request with `Accept: application/avro`
gets Confluent-framed Avro instead, which is a magic byte, a four-byte
schema ID, and the Avro datum. The Aggregator uses the Avro form for both
sources.

The schemas are registered in Apicurio under `bmkg.seismic-events-value`,
`bmkg.tsunami-warnings-value`, and `pvmbg.volcanic-reports-value`, and each
subject uses `FULL_TRANSITIVE` compatibility.

When you turn on the PVMBG schema toggle for the first time, the mock
registers its additive v2 schema and starts exposing `confidence_level` in
both JSON and Avro. Each activation also adds one new report that carries
the field. On the Aggregator side, existing hazards keep their IDs and get
their attributes updated, and the new report becomes a new canonical row.
None of this needs a restart.

Our policy for fields the Aggregator does not know is **pass them through
to `attributes`**. This applies to any BMKG or PVMBG field without a
canonical top-level destination. `confidence_level` simply follows that
rule, and the Aggregator has no dedicated field or branch for it. The Store
keeps attributes in JSONB, so a source schema change needs no migration.

What we support and what we do not:

- **Added fields** are supported, as described above.
- **Removed or renamed required fields** are not. The Aggregator rejects
  that poll and logs a contract violation with the correlation ID. It never
  saves a partial or zero-valued `HazardEvent`.
- The Client API applies the same idea one level up. If an Aggregator
  response is missing a required canonical field, it returns `503` instead
  of serving an incomplete hazard. The TUI likewise reports an invalid
  response if a required field disappears from its list.
- **Unknown schema IDs and unregistered versions** are rejected.

If the registry goes down, a process keeps working with the schema IDs and
codecs it has already cached. With a cold cache, Avro source reads and the
first PVMBG schema activation return `503`, while JSON reads and health
checks keep working. The Aggregator keeps serving stored data and retries
polling in the meantime.

### Outage behavior

When a PVMBG poll fails, whether from a timeout, a 503, or a connection
error, the Aggregator does the following:

- It logs at **`warn`** and not `error`, because this is a degradation we
  handle on purpose.
- It keeps polling BMKG as usual. The two pollers are separate goroutines
  and share no state on that path.
- It keeps serving the last volcanic data from the Store on
  `/internal/hazards`. Each volcanic event gets `"stale": true` and
  `"stale_since": "<ISO8601 UTC>"` added to its `attributes` when the
  response is built. The stored row is never changed for this.
- It never returns `500` just because PVMBG is down. If there is no volcanic
  data yet, for example right after first boot, volcanic events are simply
  missing from the result. Nothing is made up.

### Concurrency design

Reads on the internal API go straight to Postgres and never wait for a
polling cycle. Together with the two independent pollers, this means a slow
PVMBG call has no way to slow down a BMKG-only read such as
`/internal/hazards?hazard_type=SEISMIC`.

There is a test for exactly this.
`TestConcurrency_PVMBGSlow_BMKGStaysFast` in `internal/poller` runs a PVMBG
`httptest` server with a 3 second delay next to 20 BMKG poll cycles, and
asserts that the BMKG p95 stays under 300 ms.

### NATS publishing

Every newly inserted `HazardEvent` is queued as JSON in Postgres, then
published as Avro to the JetStream stream and subject set by `NATS_STREAM`
and `NATS_SUBJECT`. The example env uses `HAZARDS` and `hazard.events`.
Neither has a default in the code. The schema is registered under
`<NATS_SUBJECT>-value`, and the Avro record has this logical shape:

```json
{
  "envelope_version": 1,
  "published_at": "2026-09-24T12:34:56.123Z",
  "correlation_id": "...",
  "payload": { "...": "canonical HazardEvent" }
}
```

A few details about the payload:

- It uses the same five-byte schema-ID frame as the source Avro.
- `payload.attributes` is an Avro `map<string, bytes>`, where each value is
  one JSON-encoded attribute. That keeps the mixed types used by the Store
  and the Client API intact.
- Consumers have to resolve the schema ID and decode the Avro before they
  can read the envelope.
- The Aggregator registers the broker schema the first time it needs to
  publish. If the registry is down at that point, the service keeps running
  and the outbox simply stays pending.
- The contract between the Aggregator and the Client API is still JSON.

The dispatcher retries pending rows until JetStream acknowledges the write,
then removes the row from the outbox. A crash between those two steps can
publish the same event twice, which is why consumers deduplicate by
`hazard_id`.

### Internal read API

| Method | Path | Query params | Returns |
| --- | --- | --- | --- |
| GET | `/internal/hazards` | `since`, `hazard_type`, `severity`, `area_name`, all optional and filtered in SQL | `{"data": [...], "count": N, "as_of": "<ISO8601>", "source_status": {...}}` |
| GET | `/internal/hazards/{id}` | none | A single `HazardEvent`, or `404` |
| GET | `/health` | none | `{"status":"ok","service":"aggregator"}` |
| GET | `/ready` | none | Store and broker availability, `503` when the store is unavailable |

```bash
docker compose exec -T aggregator wget -qO- 'http://localhost:8080/internal/hazards' | jq '.count'
docker compose exec -T aggregator wget -qO- 'http://localhost:8080/internal/hazards?hazard_type=VOLCANIC&severity=SIAGA'
docker compose exec -T aggregator wget -qO- 'http://localhost:8080/internal/hazards/<hazard-id>'
```

---

## Durable consumers and DLQ

Both consumers subscribe to the same `HAZARDS` stream and `hazard.events`
subject, each under its own durable name. The dashboard stores the full
decoded `HazardEvent` in its own SQLite volume, and the notifier stores one
notification summary per hazard. Their `/health`, `/ready`, and `/records`
endpoints are reachable only on the Compose backend network. `/records`
returns the latest 100 processed records so you can inspect them in a demo.

### Delivery and idempotency

Delivery is at-least-once, so a message can arrive more than once and the
consumers are built to handle that:

- Each SQLite transaction commits the projection together with the
  message's identity, which is `(stream, sequence, payload digest)`.
- A consumer acknowledges a message only after that write succeeds.
- If a message is redelivered after a failed ack, the consumer sees the
  identity it already committed and just retries the ack.
- `hazard_id` stays unique as well. If a second event arrives for the same
  hazard with different content, the consumer halts, because there is no
  versioning contract for updates yet.

The consumer fetches one message at a time. JetStream holds delayed NAKs, so
later valid events keep flowing as long as fewer than 64 messages are
pending. Processing is limited to 25 seconds under a 60 second ack wait.

### Schema handling

Each consumer reads the five-byte schema-ID frame and fetches the writer
schema from Apicurio when it is not cached. Validated schemas are saved in
SQLite by registry URL and ID, so they survive consumer restarts and
registry outages. The JSON-encoded attribute values are decoded one by one.

### When something goes wrong

| Situation | What the consumer does |
| --- | --- |
| Registry timeout or 5xx | Pauses fetching for 30 seconds and asks the broker to redeliver later. Cached schemas keep working |
| Schema ID not found (404) | Retries for five minutes with capped backoff and jitter, then sends the message to the DLQ. The retry window survives restarts |
| Ten different unknown-schema messages within a minute | Halts and sets `/ready` to 503, so a registry misconfiguration cannot push valid events into the DLQ in bulk |
| Corrupt frame or Avro bytes | Sends the message to the DLQ right away |
| Ten corrupt messages within a minute | Halts, so a broad producer or registry mismatch cannot fill the DLQ |
| Unsupported envelope or incompatible model | Halts and reports it in the error log and on `/ready` |
| SQLite busy or locked | Retries |
| SQLite disk, I/O, or corruption error | Halts |
| Constraint error on one message | Retries three times, then sends it to the DLQ with `reason_code=sqlite_constraint` |
| Ten different constraint failures within a minute | Halts |

A DLQ record contains the original bytes (base64 in JSON), the stream
sequence, the consumer, a reason code, the attempt count, and the time.
Replay is manual.

Two more points on the DLQ. JetStream is configured with `MaxDeliver=-1`,
because a finite limit could stop delivery before our own retry or DLQ step
finishes. Also, the DLQ publication is confirmed before the original message
is acknowledged. Its message ID is deterministic, which cuts down duplicate
DLQ entries within JetStream's deduplication window. For a permanent
deduplication key, DLQ tooling should use
`(consumer, original_stream, stream_sequence)`.

### Recovering from a halt

When a consumer halts, `/ready` returns 503 with a reason and Compose marks
the container unhealthy. `/health` still answers, since the process is
alive. The halt survives restarts, so it has to be cleared by hand:

1. Find the cause in the registry, the consumer code, or the SQLite volume.
2. Stop the consumer.
3. Delete the `halt_reason` row from its `consumer_control` SQLite table.
4. Start the consumer again.

Please do this before the stream retention window runs out.

The consumers treat hazard events as immutable, so their insert-only
projection needs no per-hazard ordering. If update events are added later,
they will need a version and a check that rejects stale writes.

### Retention

The main stream keeps messages for `NATS_STREAM_MAX_AGE` (24h in the
example), so a consumer that was offline can catch up only while its
messages are still there. The DLQ stream keeps failures for
`NATS_DLQ_MAX_AGE` (168h in the example). `NATS_DLQ_STREAM`,
`NATS_DLQ_SUBJECT_PREFIX`, and `NATS_DLQ_MAX_AGE` go in `broker/.env`, while
each consumer env file needs `NATS_DLQ_STREAM` and its own
`NATS_DLQ_SUBJECT`.

### Trying it out

```bash
docker compose exec -T consumer-dashboard wget -qO- http://localhost:8091/records
docker compose exec -T consumer-notifier wget -qO- http://localhost:8092/records
docker compose stop consumer-notifier
# Both mocks emit a new event every 10 s, wait ~30 s and the dashboard keeps receiving them.
docker compose start consumer-notifier
docker compose --profile consumer-demo up -d consumer-dashboard-demo
docker compose exec -T consumer-dashboard-demo wget -qO- http://localhost:8093/records
```

While the notifier is stopped, the Aggregator keeps publishing and the
dashboard keeps receiving. Once the notifier is back, it catches up on what
it missed. The third consumer reuses the dashboard image with a new durable
name and a separate SQLite volume, and it reads the same stream without any
change to the Aggregator. You can check both consumer modules with
`go test ./...` from their directories.

To show that one bad schema ID does not block later valid events, publish a
message with a deliberately unknown ID from the broker module, then wait for
the mocks to generate their next events:

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

The unknown ID is retried for at least five minutes before it is
quarantined, and a registry outage during that time only pauses fetching
without counting as another 404. Later valid events are processed while the
retry waits. Each of the two consumers puts its own copy in `HAZARDS_DLQ`.
Please do not run this fixture against a shared or production broker.

## Demo scenarios

All demos assume the stack is already running (`docker compose up -d --build`).
The commands use the placeholder credentials from `.env.example`, so swap in
your own.

| Problem | Criterion | Where |
| --- | --- | --- |
| P1 | Mapping for both sources on the initial schema | Demo A |
| P1 | Schema change at runtime, unknown-field policy | Demo B |
| P2 | BMKG-only p95 under 300 ms with PVMBG at 3 s | Demo D, Demo F (`mixed`) |
| P2 | Sustained load, controlled `429` | Demo F (`sustained`) |
| P2 | PVMBG outage and recovery | Demo C |
| P3 | Institution credentials rejected across institutions | Demo E |
| P3 | Media gets summary fields only, raw request gets `403` | [Client API and TUI](#client-api-and-tui), `scripts/smoke.ps1` |
| P3 | Natural token expiry, refresh, old token rejected | [Client API and TUI](#client-api-and-tui), `scripts/smoke.ps1 -WaitForExpiry` |
| P4 | One container rebuilt without touching the others | Demo G |
| P4 | Old and new record shapes side by side, Store reachable only by the Aggregator | Demo H |
| P5 | Two consumers, one stopped and catching up, a third added | [Durable consumers and DLQ](#durable-consumers-and-dlq) |

### Demo A: normal aggregation

```bash
sleep 8   # let a few poll cycles happen

curl -H "X-BMKG-Key: changeme-bmkg-key" http://localhost:8081/seismic-events | jq 'length'
curl -H "Authorization: Bearer changeme-pvmbg-token" http://localhost:8082/volcanic-reports | jq 'length'
docker compose exec -T aggregator wget -qO- 'http://localhost:8080/internal/hazards' | jq '.count, .data[0]'
```

You should see at least 20 seismic records from BMKG and at least 20
volcanic reports from PVMBG, all of them available as canonical
`HazardEvent`s from the Aggregator. Each mock adds one record every 10
seconds, so the counts keep growing, and
`docker compose logs aggregator | grep polled` shows `new_hazards` above
zero.

To see the publish acknowledgements, run
`docker compose logs aggregator | grep "outbound call"` and look for the
lines with `"target_service":"nats"`. You can also attach a throwaway
subscriber. It receives binary Avro, which needs the registry schema to
decode:

```bash
docker run --rm --network tugasbesaraat_backend natsio/nats-box:latest \
  nats sub -s nats://broker:4222 hazard.events
```

### Demo B: PVMBG schema evolution

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

Nothing is restarted anywhere in this sequence. The two canonical rows have
different source IDs. After the toggle, the Aggregator updates the reports
it already had that now include `confidence_level`, and stores the new
report with that attribute.

If you also want to show a registry failure, stop the registry with
`docker compose stop schema-registry`. A source that has already registered
its current schema keeps answering Avro requests from its cache. To show a
cold cache, restart `pvmbg-mock` while the registry is stopped. Its JSON
endpoint and `/health` still answer, an Avro request returns `503`, and the
first schema toggle cannot activate. Bring the registry back with
`docker compose start schema-registry`, and the next Avro request registers
its schema and the toggle works again.

### Demo C: PVMBG outage

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

### Demo D: BMKG stays fast while PVMBG is slow

The mock reads its delay from `pvmbg-mock/.env` and not from your shell, so
putting `PVMBG_MIN_DELAY_MS=...` in front of `docker compose` does nothing.
Edit the file and recreate the container instead:

```bash
# In pvmbg-mock/.env set PVMBG_MIN_DELAY_MS=3000 and PVMBG_MAX_DELAY_MS=3000, then:
docker compose up -d --force-recreate pvmbg-mock

for i in $(seq 1 20); do
  /usr/bin/time -f "%e s" docker compose exec -T aggregator wget -qO /dev/null 'http://localhost:8080/internal/hazards?hazard_type=SEISMIC'
done
```

That loop runs one request at a time. For the real comparison, with
BMKG-only and PVMBG requests running at the same moment, use the k6 `mixed`
mode from Demo F while the delay is still 3 seconds. When you are done, put
the original delay values back and recreate the container.

There is also a unit test that measures the same scenario:
`cd aggregator && go test ./internal/poller/... -run TestConcurrency -v`.
It runs against `httptest` servers, with BMKG answering instantly and PVMBG
taking 3 seconds, and asserts that the BMKG-only p95 stays under 300 ms.

### Demo E: institution credential isolation

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/isolation-demo.ps1
```

The script prints one pass or fail line per probe:

- Each institution's own credential gets `200`.
- The BMKG key on PVMBG (data and admin), the PVMBG token on BMKG, and the
  PVMBG data token on its own admin endpoint all get `403`.
- A request with no credential gets `401`.

It reads the credentials from the service env files and never prints them.

### Demo F: load test (k6 in Docker)

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/load-test.ps1 -Mode sustained   # 60 VUs, 70s on /hazards
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/load-test.ps1 -Mode mixed -Vus 50 -Duration 65s
```

`sustained` reports throughput, p50/p95/p99, error rate, and the number of
`429` responses (`controlled_rejections_429`). With the default cap of 128
and 60 VUs you should see none. To see controlled rejections, raise `-Vus`
(for example to 300) or lower `CLIENT_API_MAX_INFLIGHT`.

`mixed` splits the VUs between `hazard_type=SEISMIC` and `VOLCANIC`, and
fails unless the SEISMIC p95 is under 300 ms. Run it with PVMBG's delay at 3
seconds, as set up in Demo D. Volcanic reads come from the Canonical Store,
so this shows the request path does not depend on the slow PVMBG poller.

Each VU logs in as Field and logs in again every 45 seconds, since the
access token lives for 60. The script needs Docker and reaches the Client
API on port 8080 through `host.docker.internal`.

### Demo G: rebuild one service independently

We use PVMBG as the example here, and any other service works the same way.

```bash
docker compose ps   # note every uptime in the STATUS column

docker compose stop pvmbg-mock

# While it is down, requests that do not need it are still served.
TOKEN=$(curl -s -X POST http://localhost:8080/auth/login -H 'Content-Type: application/json' \
  -d '{"client_id":"field-demo","client_secret":"replace-with-field-secret"}' | jq -r .access_token)
curl -s -H "Authorization: Bearer $TOKEN" 'http://localhost:8080/hazards?hazard_type=SEISMIC' | jq '.count'
curl -s -H "Authorization: Bearer $TOKEN" 'http://localhost:8080/hazards?hazard_type=VOLCANIC' | jq '.count, .source_status'

docker compose build pvmbg-mock
docker compose up -d --no-deps pvmbg-mock

docker compose ps
```

In the last listing only `pvmbg-mock` has a fresh uptime, and every other
container kept running the whole time. While it was stopped, the seismic
count kept growing, and the volcanic request returned stored data with
`source_status.PVMBG.state` set to `stale` rather than an error. The
`--no-deps` flag is what keeps Compose from touching any other service.
Note that restarting `pvmbg-mock` resets its schema toggle to v1.

### Demo H: flexible storage and Store ownership

Run this after Demo B so that both record shapes exist. The query below runs
inside the Postgres container, as the operator. No service other than the
Aggregator can do this over the network.

```bash
docker compose exec -T postgres sh -c "psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -c \"SELECT attributes ? 'confidence_level' AS has_confidence, count(*) FROM hazard_events WHERE source = 'PVMBG' GROUP BY 1\""
```

You should get both an `f` row and a `t` row. Reports with and without
`confidence_level` sit in the same table and the same JSONB column, and no
migration ran between them. Both shapes are readable through the API too,
as Demo B shows.

Next, check who can actually reach the Store:

```bash
# Only postgres and aggregator are attached to the store network.
docker network inspect tugasbesaraat_store --format '{{range .Containers}}{{.Name}} {{end}}'

# Postgres publishes no host port.
docker compose port postgres 5432

# The hostname resolves from the Aggregator and from nowhere else.
docker compose exec -T aggregator nslookup postgres
docker compose exec -T client-api nslookup postgres
```

The last command fails. The Client API is not on the `store` network, so
its only way to hazard data is the Aggregator's `/internal/hazards` API.
The network name assumes the default Compose project name, `tugasbesaraat`.

---

## Implementation notes and assumptions

- **`since` on both mocks:** if `since` is missing or cannot be parsed, the
  mock returns the full current dataset. The spec does not say what to do in
  that case, so we chose the most forgiving behavior.
- **PVMBG admin auth:** the admin endpoints use the same Bearer scheme as
  the data endpoint, with a separate `PVMBG_ADMIN_TOKEN`. The spec does not
  prescribe how to protect them, and we did not want the data token to be
  able to trigger an outage.
- **Outage detection window:** the Aggregator finds out that PVMBG is down
  on its next poll, so within `POLL_INTERVAL` seconds and not at the instant
  you flip the toggle. That comes with polling, and it is why the demos
  include a short `sleep`.
- **Volcano reference data:** this is the `volcano_reference` Postgres
  table, a static reference owned by BNPB as the spec asks. It is seeded
  with 10 Indonesian volcanoes that match the mock's `volcano_id` values
  exactly.
- **Runtime CA certificates:** the mock and Aggregator Dockerfiles copy
  `/etc/ssl/certs/ca-certificates.crt` from the builder stage into the
  runtime stage, so we do not run `apk add ca-certificates` there. The
  runtime image needs no extra network access at build time and still has a
  working CA bundle.
- **Dedup and schema evolution:** `SaveDedup` upserts on
  `(source, source_ref_id)`. A new record is inserted and queued for
  publishing. A record seen again on a later poll has its mutable columns
  (severity, area, coordinates, `attributes`) updated in place, keeps its
  original `hazard_id`, and is never published again. The schema toggle
  creates a report with a fresh source ID, which is why Demo B ends up with
  a new row next to the older ones without changing their IDs.
