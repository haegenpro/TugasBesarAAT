# R3 Ingestion and Delivery

This document covers what Reliable Ingestion & Delivery adds for M2 and how to operate and test it. The binding contracts remain:
- [Ingestion and Messaging](Ingestion-and-Messaging-Contract.md)
- [Validation and Replay](Validation-and-Replay-Contract.md)
- [Coastal Delivery and Validation](Coastal-Delivery-and-Validation.md)

## Components

| Component | Runs in | Lease | Input | Output |
| --- | --- | --- | --- | --- |
| BMKG acquisition | `ingestion` | `acquisition:BMKG` | BMKG mock | `BNPB_RAW_BMKG` |
| Field report acceptance | `ingestion` | none; idempotent ledger | client-api `POST /field-reports` | `BNPB_RAW_REPORTS` → `field_reports` projection |
| Satellite connector | `ingestion` | `acquisition:SATELIT` | Feed Satelit SSE | `BNPB_RAW_SATELLITE` → existing wildfire pipeline |
| Coastal acquisition | `coastal-processor` | `acquisition:SENSOR_PANTAI` | Sensor Pantai | `BNPB_RAW_COASTAL` |
| Coastal validation | `coastal-processor` | none; durable consumer `coastal-validation-v1` | `BNPB_RAW_COASTAL` | quarantine, or `BNPB_CRITICAL` warnings |
| Coastal canonical writer | `coastal-processor` | `canonical:SENSOR_PANTAI` | `coastal-canonical-v1` | `COASTAL` HazardEvents via `/internal/canonical/batches` |
| Warning recipient | `consumer-notifier` | none; durable consumer `tim-lapangan-v1` | `BNPB_CRITICAL` | receipt and inbox notification in `notifier-db` |
| PVMBG replay | `aggregator` | `acquisition:PVMBG` | parked `failed_ingestions` | canonical PVMBG events |

- `ingestion` and `coastal-processor` are the same `raw-ingestion` image with different `INGESTION_COMPONENTS`. Both use the ingestion-owned database.
- Each acquisition runs on exactly one replica at a time. Consumers and HTTP handlers run on every replica.
- A Sensor Pantai failure is isolated to the `coastal-processor` pods, its own breakers and its own rate limit.

## Configuration

### Ingestion (`raw-ingestion`)

| Variable | Default | Meaning |
| --- | --- | --- |
| `INGESTION_COMPONENTS` | `bmkg` | Comma-separated: `bmkg`, `field-reports`, `satellite`, `coastal`. |
| `RAW_STREAM_REPLICAS` | `3` | JetStream replicas for raw streams. Use `1` only for a single local broker. |
| `FIELD_REPORT_SERVICE_TOKEN` | — | Bearer token shared with client-api. Must differ from every other credential. |
| `FIELD_REPORT_NAMESPACE` | — | Stable namespace for field report RawInput identities. |
| `SATELLITE_FEED_URL`, `SATELLITE_FEED_TOKEN` | — | Feed Satelit base URL and group token (Secret). |
| `SATELLITE_NAMESPACE`, `SATELLITE_INITIAL_OFFSET` | — | Must equal the `SATELIT` entry of the Aggregator's `RAW_INPUT_SESSIONS`. |
| `SATELLITE_FROM_EXCLUSIVE` | `false` | Set to `true` if the feed's `from` excludes the given offset. |
| `SATELLITE_IDLE_TIMEOUT` | `30s` | Reconnect when no event or heartbeat arrives in this time. |
| `SATELLITE_FLUSH_EVERY`, `SATELLITE_FLUSH_AFTER` | `256`, `1s` | Progress is committed after this many events or this much time. |
| `SATELLITE_MAX_EVENT_BYTES` | `262144` | Maximum SSE event size, also enforced per line. |
| `PANTAI_BASE_URL`, `PANTAI_AUTH_TOKEN` | — | Sensor Pantai endpoint and token (Secret). |
| `COASTAL_NAMESPACE` | — | Stable namespace for coastal RawInput identities. |
| `COASTAL_WARNING_SCHEMA` | — | Path to `docs/fixtures/coastal-warning-v1.avsc`. |
| `AGGREGATOR_BASE_URL`, `CANONICAL_SERVICE_TOKEN` | — | Canonical adapter used by the coastal writer. |
| `COASTAL_POLL_INTERVAL` | `500ms` | Delay between pages once caught up. |
| `COASTAL_PAGE_SIZE` | `100` | Observations requested per page. |
| `COASTAL_REQUEST_TIMEOUT` | `1s` | Per-request deadline to the sensor. |
| `COASTAL_BREAKER_THRESHOLD` | `3` | Consecutive failures that open an endpoint breaker. |
| `COASTAL_BREAKER_COOLDOWN`, `COASTAL_BREAKER_MAX_COOLDOWN` | `2s`, `30s` | Open period; doubles after each failed probe. |
| `COASTAL_MAX_REQUESTS_PER_SECOND` | `8` | Shared ceiling for all sensor calls, including probes. |
| `COASTAL_STALE_AFTER` | `1m` | Data is stale when no new observation arrives, or the sensor clock runs ahead of the newest observation, by more than this. |
| `COASTAL_CLOCK_SKEW` | `2m` | An observation later than the sensor clock by more than this is a contract violation. |
| `COASTAL_STATION_TTL` | `5m` | Station metadata cache lifetime. |

### Other services

| Service | Variable | Meaning |
| --- | --- | --- |
| Aggregator | `PVMBG_ADAPTER_VERSION` | `1` (default) parks incompatible PVMBG writers. `2` reads the declared rename, type change and missing field. |
| Aggregator | `PVMBG_READER_SCHEMA` | Use `/schemas/volcanic-adapter-v2.avsc` together with adapter version 2. |
| Aggregator | `COASTAL_STATUS_URL` | `http://coastal-processor:8080`. Enables `SENSOR_PANTAI` in `source_status`. |
| client-api | `INGESTION_BASE_URL`, `FIELD_REPORT_SERVICE_TOKEN` | Enable `POST /field-reports` and `GET /field-reports/{receipt_id}`. |
| consumer-notifier | `WARNING_STREAM`, `WARNING_SUBJECT`, `WARNING_DURABLE` | `BNPB_CRITICAL`, `bnpb.warning.coastal`, `tim-lapangan-v1`. Notifier role only. |
| consumer-notifier | `WARNING_EXPECTATIONS_URL`, `COASTAL_SLO_TOKEN` | Base URL `http://coastal-processor:8080` and a dedicated read-only accounting token shared with coastal-processor. Required when warnings are enabled. |
| consumer-notifier | `WARNING_SLO_MAX_EXPECTATIONS` | `100000`: maximum identities evaluated in one SLO window; narrow `since` if exceeded. |
| Aggregator | `FAILED_INPUT_QUOTA_BYTES` | `268435456`: storage quota for complete oversized failed response bodies. Capture rolls back explicitly on quota/read failure and source progress stays unchanged. |
| consumer-notifier | `WARNING_BACKLOG_AFTER`, `WARNING_FRESH_QUOTA` | `1m`, `3`: a warning older than this is backlog; deliver up to 3 fresh warnings per backlog warning. |

### Kubernetes

Add an `r3` block to a copy of `deployment/role1/config.example.json`:

```json
"r3": {
  "field_reports_namespace": "field-reports-v1",
  "coastal_namespace": "coastal-v1",
  "satellite_feed_url": "https://satellite.naufarrel.dev",
  "pvmbg_adapter_version": 1,
  "pantai_secret": "pantai-runtime"
}
```

Setup steps:
1. Add `FIELD_REPORT_SERVICE_TOKEN`, `SATELLITE_FEED_TOKEN`, `CANONICAL_SERVICE_TOKEN` and a distinct `COASTAL_SLO_TOKEN` to the role1 runtime Secret. Add `FIELD_REPORT_SERVICE_TOKEN` to `client-api-runtime` and the same `COASTAL_SLO_TOKEN` to `notifier-runtime`.
2. Generic NetworkPolicy cannot name a host, so allow the satellite feed explicitly with `scripts/platform-isolation.py --extra-flows`, for example `{"app":"ingestion","cidr":"<feed address>/32","ports":[443]}`.
3. Run `raw-ingestion migrate --offline REPOSITORY_ROOT RUNTIME_ROLE` with the administrator migration credential to apply the ingestion R3 tables through `0004_coastal_accounting.sql`, including upgrades from `0002_r3.sql` or `0003_source_envelopes.sql`. Run the canonical offline migration command to apply `store/migrations/0013_failure_payloads.sql` to the Aggregator-owned database before starting the new image. Never edit existing migration files or checksums.

## Schema change policy (P3)

| Change from a source | Behaviour |
| --- | --- |
| Additional optional field, required mapping unchanged | Accepted; kept in `attributes`. |
| Field renamed (`alert_level` → `level`) | Parked as incompatible until an adapter declares the alias. |
| Type changed (`eruption_count_24h` int → string) | Parked until an adapter declares a validated conversion. |
| Required field removed (`ash_column_height_m`) | Parked; the value is never fabricated. Adapter v2 treats it as absent. |
| Truncated, deeply nested, invalid UTF-8 or oversized input | Preserved and parked; other sources keep flowing. |
| Registry, network or storage unavailable | `retrying` with capped, jittered backoff. |
| Sensor Pantai observation violating the basic contract | Quarantined with the reason; never becomes a HazardEvent. |

Failures appear in `GET /internal/audit/unprocessed` as `retrying` or `parked`. Recovered messages disappear from that list.

## Scenario triggers

The examples assume `$API`, `$PVMBG`, `$AGG` and `$INGESTION` base URLs, plus tokens for Tim Lapangan (`$FIELD`), Internal Ops (`$OPS`), audit (`$AUDIT`) and the PVMBG admin (`$PVMBG_ADMIN`).

### Incompatible PVMBG schema, adaptation and replay

```sh
curl -X POST "$PVMBG/admin/schema-version" -H "Authorization: Bearer $PVMBG_ADMIN" -d '{"mode":"rename"}'
curl "$AGG/internal/audit/unprocessed?source=PVMBG" -H "Authorization: Bearer $AUDIT"
```

1. Roll out the adapter by setting `"pvmbg_adapter_version": 2`, re-rendering, and applying. Alternatively, set the two Aggregator variables above and restart.
2. Replay the parked IDs as Internal Ops:

```sh
curl -X POST "$INGESTION/internal/ingestion/replays" -H "Authorization: Bearer $OPS" -d '{"request_id":"replay-1","source":"PVMBG","namespace":"legacy-m1","adapter_version":"pvmbg-adapter-v2","message_ids":["<id>"]}'
curl "$INGESTION/internal/ingestion/replays/<job_id>" -H "Authorization: Bearer $OPS"
```

3. Replay runs in the PVMBG poller under its acquisition lease, in `(received_at, message_id)` order, and commits through the idempotent PVMBG path. `GET /internal/audit/checksum?source=PVMBG` shows the result without duplicates.

Other modes:
- `type_change` and `missing_required` behave the same way as `rename`.
- An empty `POST /admin/schema-version` body keeps the original v1/v2 toggle.

### Field report that survives a receiver crash

```sh
curl -i -X POST "$API/field-reports" -H "Authorization: Bearer $FIELD" -H "Idempotency-Key: report-1" -H "Content-Type: application/json" \
  -d '{"report_type":"field_observation","occurred_at":"2026-11-16T03:00:00Z","latitude":-6.9,"longitude":107.6,"description":"banjir"}'
kubectl -n sistem-informasi-kebencanaan-bnpb delete pod -l app.kubernetes.io/name=ingestion --wait=false
curl "$API/field-reports/<receipt_id>" -H "Authorization: Bearer $FIELD"
```

Behaviour:
- Repeating the request with the same key and body returns the same receipt.
- Changing the body returns `409 identity_conflict`.
- 202 is sent only after the report is in the JetStream stream (R3, file storage). The status moves from `accepted` to `processed` exactly once after the pods return.

### Sensor Pantai failure

```sh
kubectl -n sistem-informasi-kebencanaan-bnpb scale statefulset -l app.kubernetes.io/name=sensor-pantai --replicas=0
curl "$API/hazards?hazard_type=COASTAL" -H "Authorization: Bearer $FIELD" -w '%{time_total}\n'
curl http://coastal-processor:8080/metrics | grep coastal_
```

- Client reads keep returning the last stored COASTAL events promptly. `source_status.SENSOR_PANTAI` becomes `stale`; it is `unavailable` before any successful check or when the status authority cannot be reached beyond the Aggregator cache lifetime.
- Every coastal replica reads the same persisted status. Acquisition updates are fenced, timestamps and expiry use the ingestion database clock, and a replacement leader restores the last-known state. Repeated old observations do not refresh the last-new-observation clock.
- `coastal_observations_breaker_state` reports 2 (open) and `coastal_observations_short_circuited_total` grows, while calls to the sensor stay bounded.
- Restoring the sensor closes the breaker after one successful half-open probe, with no manual step.

### Critical warnings and SLO

```sh
curl "http://consumer-notifier:8092/records?view=warnings"
curl "http://consumer-notifier:8092/warnings/slo?since=2026-11-20T00:00:00Z"
```

The SLO report reconciles paginated acquisition-side expected warning IDs with notifier-owned receipts, without sharing database credentials:
- `expected`, `delivered`, `pending`, `pending_overdue`, `rejected`, and `receipt_only` counts;
- `evaluated` includes delivered warnings, rejected critical observations, and pending warnings older than five seconds; still-in-flight younger warnings do not consume the budget yet;
- `on_time_ratio = on_time / evaluated` and `error_budget_used = (1 - on_time_ratio) / 0.05`;
- `p95_seconds` and maximum for delivered warnings, labeled `p95_scope=delivered_only`; `p95_complete=false` when unresolved or uncovered data remains;
- `p95_lower_bound_seconds` also includes the current age of pending warnings, so a fast delivered subset cannot hide a slow unresolved cohort;
- `coverage_since`, `coverage_complete`, conflicts and negative-clock anomalies.

Expected IDs commit **before raw publication**, so broker/validator/recipient outages remain in the denominator. The source ledger retains the first acquisition time across duplicate observations. If the expected-warning authority fails, the SLO endpoint returns 503; warning consumption continues independently. `COASTAL_SLO_TOKEN` grants only ledger reads, not payload inspection or replay.

Scope is `durably_acquired_critical_observations`. `all_acquired_critical_delivered` does not prove that unseen sensor events were acquired: reconcile separately against the sensor/reference identity set. Latency uses the ingestion and notifier database clocks and requires verified clock synchronization; negative durations are flagged and not counted as on-time. Historical receipt-only windows from before deployment remain explicitly uncovered.

### Satellite reconnect

```sh
kubectl -n sistem-informasi-kebencanaan-bnpb delete pod -l app.kubernetes.io/name=ingestion --wait=false
curl "$AGG/internal/satellite/history?namespace=<satellite namespace>&limit=1000" -H "Authorization: Bearer <history token>"
```

- Positions in the history stay contiguous and unique.
- `satellite_feed_lag_offsets` returns to near zero after the burst.
- `satellite_feed_reconnects_total`, `satellite_feed_gaps_total` and `satellite_feed_duplicates_total` show what happened.

## Metrics

| Prefix | Meaning |
| --- | --- |
| `field_reports_*` | Accepted, duplicate, pending, republished and processed reports. |
| `coastal_<endpoint>_breaker_state`, `coastal_<endpoint>_short_circuited_total`, `coastal_<endpoint>_request_seconds` | Breaker state (0 closed, 1 half-open, 2 open), rejected calls and latency per sensor endpoint. |
| `coastal_observations_*`, `coastal_warnings_*`, `coastal_canonical_*` | Acquisition, validation, quarantine, warning publication and canonical writes. |
| `coastal_observation_lag_seconds`, `coastal_source_stale` | Sensor clock minus newest observation; 0 fresh, 1 stale, 2 unavailable. |
| `satellite_feed_*`, `satellite_events_*` | Feed lag, reconnects, gaps, duplicates, published and quarantined events. |
| `coastal_warnings_delivered_total`, `coastal_warnings_on_time_total`, `coastal_warning_last_delivery_seconds` | Recipient-side delivery (consumer-notifier). |

## Verification

Go is built in Docker. Integration tests need a throwaway PostgreSQL on loopback:

For reproducible Windows/Linux local checks that create and remove their own test containers, run:

```sh
python scripts/test-r3-local.py
python scripts/test-r3-local.py --broker-only
```

The first command runs all Go modules, vet, focused PostgreSQL integration and R3 race checks. The second uses a pinned real JetStream broker for report acceptance, warning receipts and existing consumer/broker tests. JSON test events and skip counts are saved under `evidence/r3-review/<run-id>`. These single-node checks do not establish Kubernetes failover, zero critical loss or the live-feed throughput target. See [the review and remaining gaps](R3-Review-2026-11-16.md).

Satellite and coastal acquisition now persist each original envelope before publishing. Reacquisition reuses its original timestamp, correlation context and ownership provenance so canonical-history fingerprints remain identical after the broker deduplication window. Receipts are retired in the fenced cursor transaction after PubAck; they are not an indefinite duplicate copy of history.

Manual targeted checks:

```sh
docker run -d --name r3-pg -e POSTGRES_USER=lab -e POSTGRES_PASSWORD=lab postgres:16-alpine3.24
docker run --rm --network container:r3-pg -e M2_DATABASE_URL=postgres://lab:lab@127.0.0.1:5432/postgres?sslmode=disable \
  -v "$PWD:/src" -w /src/aggregator golang:1.27.1-alpine3.24 go test ./internal/fieldreport/ ./internal/coastal/ ./internal/satellite/ ./internal/ingestion/ ./internal/poller/ ./internal/clients/
docker run --rm --network container:r3-pg -e TEST_POSTGRES_URL=postgres://lab:lab@127.0.0.1:5432/postgres?sslmode=disable \
  -v "$PWD:/src" -w /src/consumer-notifier golang:1.27.1-alpine3.24 go test ./...
python3 -m unittest discover -s scripts -p '*_test.py'
```

### Open item: the live satellite feed

The satellite parser follows the documented interface and the wildfire processor's input contract, but has not yet been run against the live feed.

Once the group token is available:
1. Read `GET /v1/session` and the first events of `GET /v1/stream`.
2. Confirm the field names and that `from` is inclusive. If it is exclusive, set `SATELLITE_FROM_EXCLUSIVE=true`.
3. Add any field alias to `aliases` in `aggregator/internal/satellite/normalize.go`.
