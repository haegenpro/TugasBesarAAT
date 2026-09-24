# Canonical Tech Stack Specification — IF4031 M1

*As of 2026-09-24*

## Language & Runtime

| Item | Value |
| --- | --- |
| Language | Go, all services (mocks, Aggregator, Canonical Store access layer, Client-API, Auth, Broker publisher/consumers) |
| Go version | 1.27.1 |
| Module strategy | One `go.mod` per service (BMKG mock, PVMBG mock, Aggregator, Client-API, Auth, each consumer) — no shared workspace-level module. Shared types (`HazardEvent`, the broker envelope) are duplicated per service or vendored as a small internal package per service, not a shared external module, to keep each service independently buildable per the spec's container-independence requirement |

## Container Images

| Item | Value |
| --- | --- |
| Builder image | `golang:1.27-alpine3.24` |
| Runtime image | `alpine:3.24` |
| Build pattern | Multi-stage Dockerfile: build a static binary in the `golang:1.27-alpine3.24` stage (`CGO_ENABLED=0`), copy the binary alone into a fresh `alpine:3.24` stage — keeps runtime images small and avoids shipping the Go toolchain |
| Alpine packages needed at runtime | `ca-certificates` (for outbound TLS calls, if any) — install explicitly in the final stage since Alpine doesn't include it by default |

## HTTP Layer

| Item | Value |
| --- | --- |
| Framework | Gin (`github.com/gin-gonic/gin`) |
| Applies to | BMKG mock, PVMBG mock, Aggregator (internal read API), Client-API, Auth |
| Middleware | Gin's built-in `Recovery()` + a custom correlation-ID middleware (generate/forward `X-Correlation-ID`, per the deployment conventions doc) + a custom `slog`-based request logger (Gin's default logger is plain text, not the structured JSON format the logging spec requires — replace it, don't layer both) |
| Request binding/validation | Gin's built-in `ShouldBindJSON` against Go structs tagged per the wire format spec |

## Canonical Store

| Item | Value |
| --- | --- |
| Database | PostgreSQL 16 (`postgres:16-alpine` image) |
| Driver | `pgx` (v5), used via its native interface (`github.com/jackc/pgx/v5`) rather than wrapped in `database/sql`, for better JSONB and context-cancellation support |
| Schema management | A single `init.sql`, mounted into the Postgres container and run once at first boot (via the official image's `/docker-entrypoint-initdb.d/` convention) — **no migration tool**. This is sufficient because the fixed columns are set by the assignment spec and won't change during M1; the only field that "changes" (`confidence_level`) lives inside the `attributes` JSONB column and needs no schema change at all |
| Table shape (starting point) | Fixed, indexed columns for `hazard_id` (PK), `source`, `hazard_type`, `severity`, `area_name`, `occurred_at`, `ingested_at`; a `JSONB` column for `attributes` (and optionally the full raw payload, for easy whole-record reads) |

## Message Broker

| Item | Value |
| --- | --- |
| Broker | NATS with JetStream enabled |
| Image | `nats:2.10-alpine` with `-js` flag (or JetStream enabled via config file) |
| Go client | `github.com/nats-io/nats.go` |
| Delivery mode | JetStream durable consumer with explicit ack (`AckExplicit`) — matches the at-least-once requirement; **the delivery-semantics section of the cross-team contracts doc needs updating to JetStream's ack model** if this broker change is confirmed (see note above) |

## Auth

| Item | Value |
| --- | --- |
| JWT library | `github.com/golang-jwt/jwt/v5` |
| Token format | Signed JWT, HMAC (HS256) is sufficient for M1's scope — no need for asymmetric keys across a 3-service, single-team system |

## Shared Utilities

| Item | Value |
| --- | --- |
| UUID generation | `github.com/google/uuid` (`uuid.NewString()`) |
| Structured logging | Stdlib `log/slog` with `slog.NewJSONHandler`, per the deployment conventions doc's field set (`timestamp`, `service`, `correlation_id`, `level`, `message`, `latency_ms`) |
| Env/config loading | Stdlib `os.Getenv` in containers (env vars come from `docker compose`'s `env_file`); `github.com/joho/godotenv` only if running a service outside Docker during local development |
| HTTP client (outbound calls) | Stdlib `net/http.Client` with `context.WithTimeout` per call — no separate HTTP client library needed |

## Testing & Quality

| Item | Value |
| --- | --- |
| Unit testing | Stdlib `testing` package; `github.com/stretchr/testify` for assertions (`assert`/`require`) — reduces boilerplate without adding a heavy dependency |
| Linting | `golangci-lint` — **informal**: recommended for each person to run locally before pushing, not enforced via CI gate or required for merge |
| Load testing (P2's ≥50-connection/60s requirement) | Not yet pinned — recommend `vegeta` (Go-native, single static binary, scriptable, outputs p50/p95/p99 directly) to stay consistent with an all-Go stack, over `k6` (JS-based, more common but an extra runtime to install). Confirm with whoever runs P2's load test |

## Open items

- [ ] **Broker reversal** — confirm NATS+JetStream with Anggota 3 explicitly; update `cross-team-contracts.md`'s Contract 2 (broker choice, ack model, Go publish/consume code) if confirmed
- [ ] Load testing tool — `vegeta` suggested above, not yet confirmed
- [ ] NATS image tag/version — `nats:2.10-alpine` is a placeholder above, confirm the actual version available when setting up