# Deployment & Logging Conventions Spec

2026-09-20 · @Someone

## Overview

This document fixes the deployment topology and observability conventions the M1 spec requires (Ketentuan Implementasi #3 and #7) but leaves open: how the docker-compose skeleton is shaped, and how correlation IDs and structured logs work across services written in different languages. Agree on this once, in Phase 0, before splitting into individual problems — changing it later means touching every service.

**Applies to:** whoever writes each service's Dockerfile and app entrypoint, in any language. Examples given in Go and Python as the team's minimum baseline.

## docker-compose skeleton

| Service | Container port | Owner | Notes |
| --- | --- | --- | --- |
| `bmkg` | 8081 | Person A | mock, fixed contract |
| `pvmbg` | 8082 | Person A | mock, fixed contract |
| `aggregator` | internal only | Person A (P1), Person B (P2) | polls bmkg + pvmbg, publishes to broker |
| `canonical-store` | internal only | Person B | accessed only by `aggregator` (spec rule) |
| `client-api` | 8080 | Person B / Person C | client-facing, reads via `aggregator`'s API |
| `auth-service` | internal only | Person C | issues/validates tokens; own container so it can be restarted independently for P4's proof |
| `broker` | internal only | Person C | Kafka/RabbitMQ/NATS — placeholder now, pick concrete image once P5 starts |
| `consumer-dashboard`, `consumer-notifier` | internal only | Person C | ≥ 2 independent subscribers required by P5 |

```yaml
services:
  bmkg:
    build: ./services/bmkg
    ports: ["${BMKG_HOST_PORT:?Set BMKG_HOST_PORT in root .env or the shell}:8081"]
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/health"]
      interval: 5s
      retries: 5

  pvmbg:
    build: ./services/pvmbg
    ports: ["${PVMBG_HOST_PORT:?Set PVMBG_HOST_PORT in root .env or the shell}:8082"]
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8082/health"]
      interval: 5s
      retries: 5

  aggregator:
    build: ./services/aggregator
    env_file: .env

  canonical-store:
    image: mongo:7   # or postgres:16 if using JSONB — pick in P4

  client-api:
    build: ./services/client-api
    ports: ["${CLIENT_API_HOST_PORT:?Set CLIENT_API_HOST_PORT in root .env or the shell}:8080"]

  auth-service:
    build: ./services/auth-service

  broker:
    image: bitnami/kafka:latest   # placeholder — confirm choice before P5

  consumer-dashboard:
    build: ./services/consumer-dashboard

  consumer-notifier:
    build: ./services/consumer-notifier
```

Each service reaches others by **compose service name** (`http://pvmbg:8082`), never hardcoded IPs — this also means the same compose file works identically whether a service is written in Go or Python.

## Environment variables & .env.example

One `.env.example` at the repo root, prefixed by service so there's no ambiguity about ownership:

```env
# BMKG
BMKG_PORT=8081
BMKG_API_KEY=changeme-bmkg-key

# PVMBG
PVMBG_PORT=8082
PVMBG_TOKEN_SECRET=changeme-pvmbg-secret
PVMBG_DELAY_MIN_MS=500
PVMBG_DELAY_MAX_MS=3000

# Aggregator
AGGREGATOR_POLL_INTERVAL_SEC=3
AGGREGATOR_LOG_LEVEL=info

# Client API / Auth
CLIENT_API_PORT=8080
ACCESS_TOKEN_TTL_SEC=60
JWT_SIGNING_KEY=changeme-jwt-key

# Broker
BROKER_URL=kafka://broker:9092
```

Rules:

- Every variable a service reads must appear here with a placeholder value — spec requirement, and it's how graders (and teammates) know what to configure
- No real secrets committed, ever — only `.env.example` is committed, `.env` itself is gitignored
- Both Go (`os.Getenv`, or `godotenv` for local dev) and Python (`os.environ`, or `python-dotenv`) read this identically since it's plain `KEY=value` — no language-specific format needed

## Health checks and independent startup

Project HTTP services expose `GET /health` returning `200 OK` when their own listener and required local storage are working:

```json
{ "status": "ok", "service": "pvmbg" }
```

- Do not gate startup with `depends_on` or wait for external services before starting local listeners. A service with valid configuration starts on its own and retries unavailable dependencies in the background.
- `/health` checks local operation. `/ready` checks dependency availability and processing state where supported. Consumer Compose probes use `/health`; readiness can be 503 while their containers are healthy. PVMBG outage mode can leave `/health` at 200 while data endpoints return 503.
- Each service must survive being stopped/restarted alone without crashing its dependents. Test actual absence at startup and recovery after dependencies return.
- Registry uses the SQL-only supervisor described in `registry/README.md`. It stays running while Apicurio initializes or retries after an exit. Its API and management readiness probe can remain unreachable until its owned database appears; Compose reports readiness separately.

## Correlation ID propagation

**Header name:** `X-Correlation-ID` on every inbound and outbound HTTP call.

**Who generates it:** the `client-api` service generates a new UUIDv4 if an inbound request arrives without one; the `aggregator` generates its own per polling cycle (it has no inbound client request to inherit an ID from). Every downstream call made while handling that request/cycle must forward the same ID.

**Go (net/http middleware):**

```go
func CorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cid := r.Header.Get("X-Correlation-ID")
		if cid == "" {
			cid = uuid.NewString()
		}
		ctx := context.WithValue(r.Context(), correlationIDKey, cid)
		w.Header().Set("X-Correlation-ID", cid)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
```

**Python (FastAPI/Flask middleware):**

```python
import uuid
from contextvars import ContextVar

correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")

async def correlation_id_middleware(request, call_next):
    cid = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
    correlation_id.set(cid)
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = cid
    return response
```

When calling another service, always forward the current correlation ID as an outgoing header — this is what makes it possible to grep one correlation ID across all three services' logs for the report's evidence requirement.

## Structured logging format

One JSON object per log line, same field names regardless of language:

```json
{"timestamp":"2026-09-20T14:32:07.123Z","service":"aggregator","correlation_id":"a1b2c3d4-...","level":"info","message":"polled pvmbg","latency_ms":842}
```

| Field | Required | Notes |
| --- | --- | --- |
| `timestamp` | yes | ISO 8601 UTC, same format as HazardEvent fields |
| `service` | yes | matches the compose service name |
| `correlation_id` | yes when in a request/poll flow | empty string if genuinely none (e.g. startup logs) |
| `level` | yes | `debug` / `info` / `warn` / `error` |
| `message` | yes | human-readable, short |
| `latency_ms` | when logging an outbound call | see next section |

**Go:** stdlib `log/slog` with a JSON handler emits this shape natively:

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
logger.Info("polled pvmbg", "service", "aggregator", "correlation_id", cid, "latency_ms", elapsed.Milliseconds())
```

**Python:** `structlog` configured for JSON output:

```python
import structlog
log = structlog.get_logger()
log.info("polled pvmbg", service="aggregator", correlation_id=cid, latency_ms=elapsed_ms)
```

Either library can be swapped for plain `json.dumps(...)` over stdout if a dependency isn't wanted — the field names in the table are what matters, not the library.

## Outbound call latency logging

Spec requirement (Ketentuan Implementasi #7): every outgoing call's latency must be recorded. Concretely, wrap every cross-service HTTP/broker call so it logs one line per call, success or failure:

```json
{"timestamp":"...","service":"aggregator","correlation_id":"...","level":"info","message":"outbound call","target_service":"pvmbg","target_endpoint":"/volcanic-reports","status_code":200,"latency_ms":842}
```

- `target_service` and `target_endpoint` identify what was called
- `status_code` on HTTP calls; a broker publish/ack can log a boolean `success` instead
- Log this **even on timeout or error** — that's exactly the evidence P2's graceful-degradation criteria needs (showing PVMBG's slow/failed calls without the whole system blocking)
- Implement this once as a thin wrapper/decorator around your HTTP client (Go: wrap `http.Client.Do`; Python: wrap `requests`/`httpx` calls) rather than hand-adding a log line at every call site — it's easy to forget one call site otherwise, and a missing log line here is a specific, checkable line item in the rubric's implementation evidence.

## Log level conventions

| Level | Use for | Example |
| --- | --- | --- |
| `debug` | verbose internal detail, off by default | raw response body from a mock |
| `info` | normal operation | "polled bmkg, 3 new events" |
| `warn` | degraded but handled — the system did the right thing | PVMBG outage detected, serving stale Canonical Store data; token near expiry |
| `error` | genuinely unexpected — something the system could not handle | malformed JSON from a mock that isn't the declared schema-evolution case; unhandled exception |

The important line: **a PVMBG outage (P2) is a `warn`, not an `error`** — the whole point of P2 is that the system handles this gracefully, so logging it as `error` would misrepresent the very behavior being demonstrated. Reserve `error` for cases where graceful handling actually failed.

## Checklist before integration

- [ ] Every service builds from its own Dockerfile and starts via `docker compose up` with no manual steps
- [ ] Every service has a working `/health` endpoint distinguishing "process up" from business-logic outage state
- [ ] `.env.example` lists every variable any service reads, with a placeholder value
- [ ] `X-Correlation-ID` is generated at the entry point and forwarded on every downstream call — verified by grepping one ID across two services' logs
- [ ] All logs are single-line JSON with the field set in the Structured Logging table above
- [ ] Every outbound call logs `target_service`, `status_code`/`success`, and `latency_ms`, including on failure
- [ ] PVMBG outage and similar handled-degradation cases log at `warn`, not `error`
- [ ] No service can be stopped/restarted and take down an unrelated service alongside it

## Continuous monitoring

The implemented Compose stack adds Grafana, Prometheus, Loki, Alloy, and a NATS
exporter. The exporter reads NATS monitoring endpoints for JetStream stream and
consumer metrics, without consuming, acknowledging, or deleting messages. Alloy
embeds Blackbox Exporter to probe existing `/health` and supported `/ready`
endpoints and forwards Docker logs to Loki. Prometheus stores probe metrics;
Grafana provides dashboards and alert state. Application startup is independent
of monitoring, and consumers keep their liveness-based Compose probes.

Monitoring uses required per-service environment files and a startup validator
that reports every missing/empty/whitespace-only setting by name before starting
the upstream program. Root Grafana bind address and host port use Compose's
required-variable checks. See [Monitoring](../monitoring/README.md) for setup,
retention, collection permissions, and evidence capture.
