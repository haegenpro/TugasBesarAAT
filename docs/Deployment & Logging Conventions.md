# Deployment and logging conventions

Verified against the implemented stack on 2026-10-09.

## Deployment topology

[docker-compose.yml](../docker-compose.yml) is the executable deployment
definition. Listener and host ports are supplied through root `.env`.
The table shows template values; all internal ports remain configurable.

| Component | Internal port | Storage owner / exposure |
| --- | --- | --- |
| BMKG mock | 8081 | Source SQLite archive; authenticated data API published |
| PVMBG mock | 8082 | Source SQLite archive; separately authenticated data/admin API published |
| Aggregator | 8080 | Exclusive owner of canonical PostgreSQL; internal API |
| Canonical PostgreSQL | 5432 | Storage network shared only with Aggregator; no host port |
| Auth Service | 8083 | Session SQLite; internal token API |
| Client API | 8080 | Public downstream HTTP entry point; reads through Aggregator |
| Broker | 4222 / 8222 | NATS JetStream file storage and monitoring API; internal |
| Dashboard / Notifier | 8091 / 8092 | Independent SQLite projections; internal HTTP inspection |
| Schema Registry | 8080 / 9000 | Schema API and management readiness; internal |
| Registry PostgreSQL | 5432 | Separate database and network owned by Registry |
| Client TUI | none | Interactive client on the client network |
| Monitoring | configurable | Alloy, NATS Exporter, Prometheus, Loki and Grafana; Grafana has an explicit host bind address |

Each SQLite owner and storage server has its own volume. PostgreSQL and Registry
PostgreSQL share neither volume nor network. The client network contains Client
API and the TUI; application services use backend addresses. Broker and Registry
trust membership of the backend network. Host/Docker administrators can access
containers and their internal APIs.

Root `.env` owns shared deployment ports. Gitignored service env files hold
credentials and service settings. Compose injects matching listener ports,
dependency URLs and probes. The application programs use `os.Getenv` and do not
load env files themselves during native execution. Use the complete
[.env.example](../.env.example), [README](../README.md) and
[port configuration](Port%20Configuration.md) for configuration.

## Health and independent startup

There are no Compose `depends_on` gates. With valid configuration and writable
local storage, application listeners start while external dependencies retry in
the background. Auth health checks its session database; other application
`/health` endpoints principally indicate liveness. Supported `/ready` endpoints
report dependencies and consumer processing state.

PVMBG's simulated outage returns 503 from data/history endpoints while
`/health` remains 200. Consumer Compose health checks use liveness; a halted
consumer can stay healthy while `/ready` returns 503. Registry's supervisor
retries initialization if its database is absent; its API/readiness listener can
remain unreachable until initialization succeeds. Native infrastructure has
its own readiness checks. See [Registry](../registry/README.md).

## Correlation IDs

Application HTTP requests preserve `X-Correlation-ID` or generate a UUID when
the header is absent. Client API forwards it to Auth and Aggregator. Aggregator
also stores it in the request's Go context so PostgreSQL operations inherit the
same ID.

Each source polling/replay iteration has its own ID, shared by source HTTP,
schema access and associated storage work. Background maintenance and broker
setup generate IDs when no request exists. Pending outbox reads have a dispatcher
ID; once an event is loaded, publication and outbox completion use the event's
stored ID. Consumers preserve that ID through decoding, persistence, ACK and
DLQ entries; uncached schema lookups forward it as an HTTP header.

## Structured application logs

Application services use `log/slog` JSON handlers, with these operational fields:

| Field | Meaning |
| --- | --- |
| `service` | Application component |
| `correlation_id` | Request, polling, event or background operation ID |
| `level` | Severity; handlers differ in capitalization |
| `message` or `msg` | Operation description |
| `timestamp` or `time` | Handler timestamp |
| `latency_ms` | Duration of the recorded call |
| `target_service`, `target_endpoint` | Outbound dependency and operation |
| `status_code` / `success` | HTTP status / operation outcome |

Mocks and Aggregator normalize timestamp/message field names; other Go JSON
handlers retain standard `time`/`msg` fields. Startup logs may have no request
ID. Monitoring parses both application formats; upstream NATS, PostgreSQL,
Apicurio and monitoring programs retain their native logging formats.

## Outbound latency coverage

HTTP wrappers record calls to sources, Auth, Aggregator and Registry. NATS
wrappers record connection and stream setup as well as publication, fetches and
ACK operations in the relevant component. Aggregator's pgx tracer records
connection acquisition, physical connections, preparation, queries and
transaction SQL commands. Its store wrapper also records pings, which bypass
pgx query tracing.

Consumer fetch timing ends after the asynchronous batch closes and its final
error is available, before local projection processing and acknowledgement.
Empty long polls include their configured broker wait in the logged duration.

SQL logs identify the operation and a SHA-256 query fingerprint. They exclude
SQL text, bound values, DSNs and server error details. Errors use safe categories
and PostgreSQL SQLSTATE where available. Driver query timing ends when results
are consumed or closed; acquisition timing is recorded separately. Failed
acquisition and timeout paths are logged as well.

Handled source outages log warnings. Contract violations and consumer halts have
separate error records. Request latency and individual dependency latency answer
different questions; use k6 for P2's end-to-end p95 evidence.

## Monitoring and evidence

Alloy embeds Blackbox checks of configured health/readiness URLs and reads Docker
logs over the Docker Unix socket. Metrics go to Prometheus through remote write;
logs go to Loki. NATS Exporter reads broker monitoring HTTP endpoints without
consuming events. Grafana queries Prometheus with PromQL and Loki with LogQL.
Monitoring availability does not gate application processing.

Only Grafana, Prometheus and Loki require monitoring env files. Root ports and
Compose supply Alloy/Exporter settings. The launcher validates configuration
before starting each upstream program. See [monitoring](../monitoring/README.md)
and [Test Runbook](Test%20Runbook.md) for retention, permissions and evidence.

The runbook covers independent restart, live schema evolution, authentication,
redaction, outage recovery, sustained load and fan-out/backlog scenarios.
