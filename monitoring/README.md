# Monitoring

All five use the optional `monitoring` Compose profile. A plain
`docker compose up -d --build` starts only the application services.

Five containers provide continuous endpoint history, broker metrics, and searchable logs:

| Service | Pinned upstream image | Responsibility |
| --- | --- | --- |
| Grafana | `grafana/grafana:13.2.3` | Dashboard, log search, alert state |
| Prometheus | `prom/prometheus:v3.15.0` | Metrics storage, independent collector scrapes |
| Loki | `grafana/loki:3.7.8` | Logs storage and retention |
| Alloy | `grafana/alloy:v1.20.1` | Embedded Blackbox probes and Docker log collection |
| NATS exporter | `natsio/prometheus-nats-exporter:0.20.2` | Read-only NATS/JetStream monitoring metrics |

The multi-stage Dockerfile builds only the small Go startup launcher, then copies
it into each official image. There is no OpenTelemetry SDK, separate Blackbox
container, or application instrumentation in this version.

## Required configuration

Configure the monitoring HTTP ports, `GRAFANA_BIND_ADDRESS`, and
`GRAFANA_HOST_PORT` in root `.env`; see [Port configuration](../docs/Port%20Configuration.md). Select an
explicit IP address: `127.0.0.1` restricts access to this host; `0.0.0.0` exposes
the selected port on all host IPv4 interfaces. Other machines use the host's
actual address, not `0.0.0.0`, to open Grafana. IPv6 bind addresses must use
Compose's bracketed IPv6 notation in the port mapping if you change that mapping.

Create these gitignored files manually from the corresponding sections of the
root `.env.example`; do not copy the combined template wholesale:

| File | Required variables |
| --- | --- |
| `monitoring/grafana/.env` | `GF_SECURITY_ADMIN_USER`, `GF_SECURITY_ADMIN_PASSWORD`, `DLQ_STREAM_NAME` |
| `monitoring/prometheus/.env` | `METRICS_RETENTION_DAYS` |
| `monitoring/loki/.env` | `LOG_RETENTION_DAYS`, `LOKI_GRPC_PORT` |

Alloy and NATS Exporter require no per-service env file. Compose generates all
service URLs, health probes, and scrape targets from root ports and supplies
Alloy's `COMPOSE_PROJECT_NAME`, including when `-p` selects a
project name. Docker log discovery filters to that project. Monitoring and
one-shot broker-init logs are excluded; application and database/broker logs
are included. Optional demo consumers can appear in logs but are not permanent
probe targets, so their absence does not raise availability alerts.

Every listed value is required. Missing environment files stop Compose. Missing,
empty, or whitespace-only service values cause one error listing all missing
names, without their values, and the launcher exits before starting the product.
Invalid ports, addresses, HTTP(S) URLs, retention values, and DLQ stream names
also stop startup. Dashboard stream names support letters, digits, underscores,
and hyphens. Set `DLQ_STREAM_NAME` to the broker's `NATS_DLQ_STREAM`.
Root host mappings use Compose's required-variable checks separately. There are
no environment fallback values. Examples are suggested values only.

Changing a root monitoring port updates its listener, health check, data source
URLs, and scrape/push destinations together after container recreation. Loki's
private gRPC port stays in its service env. Monitoring does not connect to
application databases or their private networks.

## Start and inspect

From the repository root, with all environment files configured:

```bash
docker compose --profile monitoring up -d --build
docker compose ps
```

To stop only monitoring and retain its data:

```bash
docker compose --profile monitoring stop grafana prometheus loki alloy nats-exporter
```

Running `up` without the profile does not stop existing monitoring containers.
To stop and remove the application and monitoring together, retaining volumes,
use `docker compose --profile monitoring down`.

Open the address and port you explicitly configured. Sign in with the configured
Grafana credentials, then open **Monitoring → AAT Health, DLQ and Correlation Logs**.
Grafana only creates the initial administrator from environment credentials when
its persistent database is new. To rotate an existing user's password, use
Grafana's user settings; changing the environment alone does not rotate it.

Grafana is the only published monitoring port. Prometheus, Loki, Alloy, and the
NATS exporter are reachable on the internal monitoring network. Alloy and the
exporter also join `backend` to probe endpoints and read broker statistics. The
exporter exposes `/metrics` internally on `NATS_EXPORTER_PORT`; it has no database or volume. Services do not depend on the monitoring components at
startup and continue operating when monitoring is stopped.

## Dashboard interpretation

- **Liveness** and **Readiness** are separate. A halted consumer can remain Docker
  healthy because Compose probes `/health`, while `/ready` reports `503`.
- Probe success means the endpoint returned `200`. An HTTP `503` yields
  `probe_success=0`, even if scraping the exporter succeeded (`up=1`). A network
  failure may produce HTTP status `0`.
- Current endpoint panels require a successful independent Alloy scrape and
  probe samples no older than 45 seconds. Collector loss produces **No data**,
  not a retained green status. Historical plots show samples/gaps; collector
  availability is shown independently.
- **HTTP probe duration** measures synthetic checks. **Logged outbound latency
  p95** comes from JSON `latency_ms` fields with `target_service`, in milliseconds.
  It is not end-to-end request latency. Missing logs are not converted to zero.
- Consumer `consumer_fetch` logs include waiting for the broker's complete batch
  response and final error, before local processing and ACK. An empty two-second
  long poll can therefore record about 2000 ms with `success=true`.
- Select services and a time window. Enter an exact correlation ID in the text
  field to view its matching JSON log entries across services. The general log
  panel also shows non-JSON infrastructure output.
- Correlation IDs remain log fields, not Loki stream labels or Prometheus labels.
  Log queries parse JSON when reading it. Existing correlation propagation stays
  unchanged; logs are not distributed trace spans.

Alloy probes every 15 seconds with a five-second probe timeout. Prometheus
independently scrapes Alloy, Loki, the NATS exporter, and itself every 15 seconds. Grafana evaluates
endpoint/collection alerts every 30 seconds and requires one minute of failure.
Missing data or query errors are alert conditions too. Alerts are visible in
Grafana; an all-day mute policy suppresses notifications. No external notification
destination is configured by this project. A muted loopback webhook supplies
the receiver required by Grafana; it is not used for notification delivery.
Replace that receiver and remove the mute policy only when you explicitly
configure notifications.

This version does not add application `/metrics`, throughput, outbox
instrumentation, database health metrics, or traces. Broker consumer metrics
show delivery backlog; HTTP endpoint health alone does not prove delivery advances.

## DLQ quarantine monitoring

The exporter reads NATS `/varz` and detailed `/jsz` statistics. Prometheus stores
`jetstream_stream_total_messages`, `jetstream_stream_total_bytes`, and consumer
`num_pending`, `num_ack_pending`, and `num_redelivered` metrics. These are broker
state measurements, not counts reconstructed from application logs. The exporter
never subscribes to the DLQ or changes delivery, acknowledgement, or retention.

The dashboard shows **retained DLQ messages**, bytes, archive history, and durable
consumer delivery metrics. The DLQ is a quarantine archive: expiration or deletion
can reduce its count without anyone recovering a failure. It is not an unresolved
work counter. Stream count and consumer plots are gated on successful exporter
scrapes; missing stream metrics remain **No data**, never synthetic zero.

Loki panels count observed `message quarantined` logs by consumer/reason and show
`DLQ publish or ack failed` operations. Each count can differ from retained broker
records: logs may be lost, a stored record may precede an acknowledgement failure,
and retries can generate several failure logs. Open log details for `reason_code`,
`attempts`, `original_stream`, `stream_sequence`, and `correlation_id`. Copy the ID
into the existing exact correlation filter to inspect the wider event flow.

Grafana evaluates three additional rules every 30 seconds: exporter scrape failure
lasting one minute, any observed quarantine in the last five minutes, and any
observed quarantine-operation failure in the last five minutes. No matching event
logs means no observed event, not proof of healthy collection. Loki query errors
alert; the existing independent collector alerts cover collection availability.
Notification delivery remains muted by the existing dashboard-only policy.

### Manual inspection and republishing

The examples below require Docker, `jq`, and `base64` on the host. Run them from
the repository root. The broker image includes the DLQ inspector; it uses the
container's configured broker connection. Broker host ports remain closed. These
commands require Docker/host operator access, independently of downstream client
identities. See the [broker access boundary](../README.md#broker-access-boundary).

```bash
docker compose exec -T broker inspect-dlq hazard.dlq.dashboard > /tmp/dlq-record.json
jq -r '.record.raw' /tmp/dlq-record.json | base64 --decode > /tmp/dlq-payload.bin
```

The inspector prints the latest retained record under that prefix. Its JSON
wrapper is **not** the application event. Inspect `reason_code`, fix the cause,
and confirm the original bytes are valid before republishing. Corrupt Avro must
be regenerated in the correct framed format; editing the DLQ JSON does not repair
the event. For a valid original payload, run the NATS CLI in a temporary container
on the same backend network as the broker. The network is discovered from the
current Compose broker, so the project name need not be hardcoded:

```bash
BROKER_NETWORK=$(docker inspect -f '{{range $name, $network := .NetworkSettings.Networks}}{{$name}}{{end}}' "$(docker compose ps -q broker)")
NATS_URL=$(docker compose exec -T broker printenv NATS_URL)
docker run --rm -i --network "$BROKER_NETWORK" natsio/nats-box:latest \
  nats --server "$NATS_URL" pub --jetstream --force-stdin --no-templates \
  "$(jq -r '.record.original_subject' /tmp/dlq-record.json)" < /tmp/dlq-payload.bin
```

These commands use the internal Compose broker address. Preserve the record's
correlation ID with `-H 'X-Correlation-ID:the-record-id'` when present.
Verify processing logs and resulting data: successful publishing alone does not
prove recovery. Republishing reaches both subscribers and gets a new stream
sequence, so transport deduplication cannot identify it as the old delivery.
Business-record checks may suppress duplicate effects; replaying changed payloads
under an existing hazard ID may instead halt an incompatible consumer. Lifecycle
events retain their own event-ID/revision rules. A halted consumer may additionally
need the local operator resume command described in
[Consumer recovery](../docs/consumer-recovery.md) after fixing the cause.

Republishing neither removes the DLQ record nor adds a recovery status. Records
expire according to explicitly configured `NATS_DLQ_MAX_AGE` (168h in the example).
This integration adds no replay command, automatic replay, or resolution tracking.

## Storage and Docker access

Both retention values are explicitly configured integer days; the selected
examples are 30 days. Prometheus expires old metrics; Loki's compactor performs
asynchronous retention deletion. Retention is an age limit, not a disk quota.
Disk use grows with log volume and metric series count. Loki 3.7 also throttles
writes when filesystem usage reaches its built-in 90% WAL threshold; `/ready`
alone does not prove logs are being accepted. Check Loki/Alloy logs and free disk
space if collection is throttled. Volumes survive ordinary
container restarts/recreation; removing volumes removes monitoring history.

Alloy persists Docker collection positions and its metrics remote-write WAL.
These help collection resume after restarts, but logs are not guaranteed lossless
through long collector/storage outages or Docker log rotation. Retention cannot
restore logs that were never collected.

Alloy reads `/var/run/docker.sock`. The read-only filesystem mount does **not**
restrict Docker API privileges: use this setup on your trusted local/demo Docker
host. The current socket path assumes Linux Docker or Docker Desktop's Linux
container environment. No other monitoring container receives the socket.

## Verification and evidence

Launcher checks (run from `monitoring/launcher`):

```bash
go test ./...
go vet ./...
```

To verify aggregate missing-value reporting without affecting running containers:

```bash
docker compose run --rm -e GF_SECURITY_ADMIN_USER= -e GF_SECURITY_ADMIN_PASSWORD= grafana
```

The container must exit nonzero and name both variables. A whitespace-only value
must fail too. Environment files are still mandatory for `compose run`.

Capture health panels and logs from an actual flow. For example, send a known ID
to Client API `/ready`, using your explicitly configured host port:

```bash
source scripts/demo-common.sh
API_URL="http://localhost:$(demo_env_value "$DEMO_ROOT/.env" CLIENT_API_HOST_PORT)"
curl -i -H 'X-Correlation-ID: monitoring-evidence-001' "$API_URL/ready"
```

Search that ID in Grafana across Client API, Auth, and Aggregator. Separately
capture Aggregator publication and consumer processing logs for one shared event
ID and the logged outbound latency. Probes generate their own IDs when none are
provided; they are independent of the evidence request.

In an isolated demo stack, stop one consumer and verify failed liveness/readiness
probes while the producer and other consumer continue. Restart it and verify
recovery. Stop Alloy to verify collection loss shows **No data** and alert state;
restart it and recreate a consumer to verify rediscovery. Verify persisted history
before/after restarting Loki and Prometheus. Stop monitoring to demonstrate that
application requests and event processing continue.

Before applying configuration changes, validate generated files with `promtool
check config`, `loki -verify-config=true`, and `alloy validate` inside their
containers. The generated configurations live in `/tmp/monitoring`; Grafana
provisioning lives under `/tmp/monitoring/provisioning`. Recreate affected
containers after changing environment or templates.
