# Implemented technology stack — IF4031 M1

Verified against repository code and Dockerfiles on 2026-10-09. The assignment
requirements remain in [M1](IF4031%20-%20Tugas%20Besar%20-%20M1.md); this document
describes the implementation.

## Application services

| Component | Implementation |
| --- | --- |
| BMKG and PVMBG mocks | Go 1.27.1, Gin HTTP handlers, independent credentials and SQLite history archives |
| Aggregator | Go 1.27.1, independent polling/replay goroutines, canonical mapping, PostgreSQL storage and outbox dispatchers |
| Client API | Go 1.27.1 and Gin; server-side scope enforcement and summary projection; global and per-client concurrency limits |
| Auth Service | Go 1.27.1 and Gin; opaque access/refresh tokens and persistent SQLite sessions |
| Dashboard and Notifier consumers | Go 1.27.1, net/http health/readiness/records endpoints, durable JetStream subscriptions and SQLite projections |
| Client TUI | Go 1.27.1 terminal program; HTTP login, protected reads and automatic refresh |
| Broker initializer and tools | Go 1.27.1; stream configuration, read-only readiness checks, invalid-fixture publication and DLQ inspection |
| Monitoring launcher | Go 1.27.1; validates environment and renders configuration before starting each upstream program |

Each Go component has its own module. Source models, canonical models and wire
code are maintained within each service; services communicate over HTTP or NATS.

## Selection rationale

Go keeps application review and tooling consistent across services. Goroutines
support independent source polling and concurrent HTTP requests, while separate
modules and containers preserve independent builds and restarts. Multi-stage
builds keep the compiler out of runtime images.

PostgreSQL provides atomic canonical-state/outbox transactions, and JSONB holds
new source attributes without changing table columns. SQLite provides persistent
service-owned sessions, archives and projections without additional database
servers. JetStream provides durable fan-out, backlog and acknowledgements;
Apicurio supplies schema IDs and compatibility checks for Avro. Server-side opaque
token sessions support expiry, refresh rotation and logout through introspection.
k6 supplies the concurrent workload and latency measurements required by P2.

## Build and containers

Application Dockerfiles use `golang:1.27.1-alpine3.24` to build with
`CGO_ENABLED=0`, then copy binaries into `alpine:3.24`. Consumer SQLite uses
`modernc.org/sqlite`, which supports this build without a C compiler at runtime.
The Go toolchain stays in the build stage. Runtime image size depends on the
binary and base image; the repository does not promise a fixed size.

Mocks and Aggregator copy the CA certificate bundle from their build stage.
Each service has an independent container; database and monitoring containers
use the upstream images listed below. The monitoring Dockerfile has separate
targets for its five images.

## Storage and schema

| Component | Technology and ownership |
| --- | --- |
| Canonical Store | PostgreSQL 16, `postgres:16-alpine3.24`; only Aggregator joins its storage network |
| PostgreSQL driver | `github.com/jackc/pgx/v5`, native interface and connection pool |
| Dynamic source attributes | JSONB in `hazard_events.attributes`; `confidence_level` requires no column migration |
| Publication | Hazard/lifecycle outboxes written in the same transactions as their associated state; dispatchers wait for JetStream confirmation before deleting pending rows |
| Local application storage | SQLite owned by Auth, each mock and each consumer, with separate persistent volumes |
| Schema Registry | Apicurio Registry 3.3.3 with a separate PostgreSQL 16 database owned by Registry |
| Serialization | `github.com/linkedin/goavro/v2`; five-byte schema-ID frame followed by Avro data |
| Source schema evolution | `FULL_TRANSITIVE` compatibility; additive PVMBG v2 includes nullable `confidence_level` |
| Source replay | JSON history pages from source-owned SQLite archives; persistent PostgreSQL cursors |

Source endpoints return JSON by default. Aggregator requests Avro for current
observations and JSON for history. Unknown source fields without a canonical
destination pass through to `attributes`. See [wire format](HazardEvent%20Wire%20Format.md),
[contracts](Contracts.md), and [reporting lifecycle](Reporting%20Lifecycle.md).

## Broker and recovery

NATS JetStream uses `nats:2.15.0-alpine3.22` and
`github.com/nats-io/nats.go`. Hazard, lifecycle and DLQ streams use persistent
file storage. Durable subscriptions retain independent progress for each
consumer. Delivery is at least once: consumers commit projection changes and
deduplication state in SQLite before a broker-confirmed ACK. Lifecycle revisions
prevent older snapshots from overwriting newer projections.

Broker and Registry have internal Docker addresses and no published host ports.
They trust backend network membership. Downstream clients use Client API.
Consumer recovery uses a local Unix socket and the `consumer resume` command;
the socket directory has mode 0700 and the socket 0600. See
[consumer recovery](consumer-recovery.md).

## Authentication and configuration

Auth generates independent 32-byte access and refresh tokens using
`crypto/rand` and encodes them with URL-safe Base64. SHA-256 token hashes,
client identity, role and expirations are stored in SQLite. Client API validates
access tokens through authenticated HTTP `POST /internal/introspect`.
Refresh rotates both tokens in the existing session, invalidating the old pair.
Media has `hazards:summary`; Field and Internal have `hazards:raw`.
All three identities have read-only access to disaster data.

Go programs read process environment using the standard library. Compose
injects root deployment ports and matching URLs; gitignored per-service env files
hold credentials and service settings. Native Go execution requires explicitly
exported environment variables. The combined template is
[.env.example](../.env.example). See [port configuration](Port%20Configuration.md).

## Monitoring and logs

| Component | Pinned image | Role |
| --- | --- | --- |
| Grafana | `grafana/grafana:13.2.3` | Dashboards and alerts; PromQL queries to Prometheus, LogQL queries to Loki |
| Prometheus | `prom/prometheus:v3.15.0` | Metrics storage, scraping and remote-write receiver |
| Loki | `grafana/loki:3.7.8` | Log storage |
| Alloy | `grafana/alloy:v1.20.1` | Embedded Blackbox probes and Docker log collection |
| NATS Exporter | `natsio/prometheus-nats-exporter:0.20.2` | Broker monitoring API to Prometheus metrics |

Application logs use `log/slog` JSON handlers. Correlation IDs travel in HTTP
headers, Go contexts and broker envelopes. Aggregator traces PostgreSQL
connection acquisition, connections, preparation, queries, transaction commands
and pings, plus NATS connection, stream lookup/info and publication. SQL traces
record a fingerprint and safe error category/SQLSTATE; SQL text, arguments and
DSNs are excluded. Native upstream infrastructure logs retain their own formats.
See [deployment and logging](Deployment%20%26%20Logging%20Conventions.md) and
[monitoring](../monitoring/README.md).

## Verification tools

Go tests use `testing` and `github.com/stretchr/testify`. Race detection and
`go vet` check the application changes. Integration tests require the
documented `TEST_DATABASE_URL`, `TEST_NATS_URL` or `TEST_SCHEMA_REGISTRY_URL`.

P2 load testing uses k6 with `scripts/load-test.js` and Bash/PowerShell Docker
wrappers. Milestone runs require at least 50 virtual users and 60 seconds; the
runbook also captures actual TCP connections, source delay, health and restart
state. Summaries report throughput, p50/p95/p99, successful reads, controlled
429 rejections and unexpected failure rates. Smoke/isolation scripts and
`scripts/evidence-probes.mjs` exercise the remaining P1–P5 scenarios.

Run commands and evidence requirements are in the
[root README](../README.md) and [Test Runbook](Test%20Runbook.md).
