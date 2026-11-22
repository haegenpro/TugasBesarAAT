# M2 configuration reference

Operator configuration is `.m2-local/config.json` for development and, for
example, `.m2-local/k3s.json` for the real deployment. Pass the latter with
`--config` on each helper. Ordinary settings become ConfigMaps and credentials
become owner-specific Secrets; applications do not read operator files directly.

Application containers load their Secret first and ConfigMap second. The
ConfigMap therefore overrides stale public tuning keys retained in an older
Secret. Explicit container `env` overrides both sources, so inspect the running
process when verifying a diagnostic override. Credentials remain in Secrets.

| JSON key | Meaning / default |
| --- | --- |
| `version`, `namespace` | Schema version 1; required assignment namespace |
| `profile`, `context` | `minikube` or `k3s`; explicit kubeconfig context |
| `nodes.a`, `nodes.b` | Actual distinct Kubernetes node names, not SSH aliases |
| `storage_root` | Dedicated normalized root, default `/var/lib/aat-m2` |
| `image_prefix`, `image_tag` | Application image prefix and immutable revision tag |
| `network.pod_cidr`, `network.service_cidr` | Match the actual cluster; must not overlap |
| `features.monitoring` | Enable browser observability, default true |
| `features.ai` | Enable optional B-side summarizer, default false |
| `satellite.url`, `satellite.namespace` | Source origin and immutable session/history identity |
| `satellite.initial_offset` | First expected original offset for a fresh session, default 0 |
| `satellite.lateness_seconds` | Development watermark allowance, default 1800; needs source evidence |
| `coastal.profile`, `coastal.speed` | Immutable vendor source profile/speed, default practice/60 |
| `limits.postgres_volume_gib` | Per PostgreSQL copy, default 2 GiB |
| `limits.broker_volume_gib` | Per broker volume, default 2 GiB |
| `limits.broker_message_mib` | Aggregate per-broker message ceiling, default 1024 MiB |
| `limits.quarantine_mib` | Demo failed-input budget, default 256 MiB per owning store |
| `external.satellite_token` | Private supplied feed token; blank disables acquisition |
| `external.audit_token` | Later-supplied Audit token; blank uses a generated development token |
| `external.openrouter_api_key`, `external.summary_model` | Both required when AI is enabled |

## Operational tuning

The optional sections below can be added to the same private JSON file. Omitted
sections and keys retain the current defaults, including for existing files.
The [example configuration](../../deployment/m2/config.example.json) lists every
key. Nested overrides merge with defaults; unknown keys inside these sections,
invalid types, and inconsistent capacities fail before deployment.

For example, merge this section into the private configuration to capture all
new root traces during a quiet demo:

```json
{
  "observability": {
    "trace_sample_ratio": 1.0
  }
}
```

Use `0.05` again for the normal default. Parent sampling decisions still propagate
across services. Metrics are unaffected by trace sampling. Queue saturation can
drop spans; these are telemetry buffers, not business-event queues.

| Key under `observability` | Default |
| --- | --- |
| `trace_sample_ratio` | `0.05`; finite number from 0 to 1 |
| `trace_queue_spans`, `trace_batch_spans` | `128`, `32`; batch must fit within queue |
| `trace_export_timeout_ms` | `2000`; applies to application HTTP exporters and batch processors |
| `ai_trace_export_timeout_ms` | `1000`; preserves the summarizer's existing timeout |
| `prometheus_scrape_seconds` | `15`; also sets rule evaluation interval |
| `prometheus_retention_hours`, `prometheus_retention_mib` | `6`, `256` |
| `jaeger_max_traces` | `1000`; bounded in-memory history, lost on Jaeger restart |
| `loki_retention_hours` | `24`; compaction/deletion is asynchronous, not immediate space reclamation |

Applications receive `OTEL_TRACES_SAMPLER_ARG`, `OTEL_BSP_MAX_QUEUE_SIZE`,
`OTEL_BSP_MAX_EXPORT_BATCH_SIZE`, and `OTEL_BSP_EXPORT_TIMEOUT` (milliseconds).
The monitoring YAML files are renderer templates; deploy their generated
ConfigMaps rather than passing the templates directly to the backends.

Database settings are per process, not a cluster-wide connection budget:

| Key under `database` | Default |
| --- | --- |
| `services.aggregator.max_open` | `6` |
| `services.auth-service.max_open`, `services.consumer-dashboard.max_open`, `services.consumer-notifier.max_open` | `8` each |
| `services.ingestion.max_open`, `services.wildfire-processor.max_open`, `services.coastal-processor.max_open`, `services.field-report-processor.max_open` | `4` each |
| `services.<name>.statement_timeout_ms` | `1500` for Aggregator/Auth/consumers; `5000` for ingestion/processors |
| `services.<name>.lock_timeout_ms` | `500` for Aggregator/Auth/consumers; `2000` for ingestion/processors |
| `ingestion_idle_transaction_timeout_ms` | `10000`; ingestion and its three processor types |
| `aggregator_field_read_connections` | `3`; additional pool in each Aggregator |
| `aggregator_read_strategy` | `coordinator`; optional `workers` reads current shard primaries directly |
| `aggregator_worker_read_connections` | `2`; allowed `1` or `2`, reserved from the general pool budget when worker reads are enabled |
| `registry_max_open` | `4`; registry JDBC pool |

Ingestion previously ignored the shared `DB_STATEMENT_TIMEOUT=1500ms` setting
and used a literal five seconds. Its new explicit default is five seconds,
preserving effective behavior while honoring overrides. Its lock timeout keeps
the owning PostgreSQL formation's existing two-second default. Account for all
replicas, Field reservations, and Citus connections when increasing pool sizes.

Worker reads retain the coordinator publication barrier and read current shard
placement before querying the two worker groups. Field reserves one worker
connection from its existing pool budget. With default budgets, six Aggregators
during a rollout use at most 54 application connections per member. General
capacity must leave at least one coordinator connection; Field needs at least
two total connections. Unreachable groups produce explicitly partial pages
without a continuation cursor. This option does not change the write path.
The canonical verification helper follows the configured strategy; set
`M2_TEST_WORKER_READS=true` or `false` to exercise the other strategy explicitly.

The registry pool uses `APICURIO_DATASOURCE_JDBC_MAX_SIZE`, with initial/minimum
sizes zero. Apicurio 3.3.3 maps these settings to its named PostgreSQL datasource;
unnamed `QUARKUS_DATASOURCE_JDBC_*` settings did not constrain that pool. The
[versioned upstream configuration](https://github.com/Apicurio/apicurio-registry/blob/3.3.3/app/src/main/resources/application.properties)
explains the mapping. A local runtime check after this correction returned 200
for all 240 concurrent-burst reads and observed four registry database sessions,
versus twenty before the correction. This verifies the configured connection
budget; it is not a demonstrated fix for the load-test scaling shortfall.

| Key under `admission` | Default |
| --- | --- |
| `client_max_inflight`, `client_per_client` | `64`, `16` |
| `client_field_reserved` | `16` |
| `client_auth_max_inflight`, `client_auth_field_reserved` | `32`, `8` |
| `auth_max_inflight`, `auth_field_reserved` | `32`, `8` |
| `auth_db_field_reserved` | `3`; included in the Auth database pool total |

Reservations must be positive and below their respective totals. The per-client
limit cannot exceed Client API capacity. Auth's DB reservation must be below
`database.services.auth-service.max_open`.

| Key under `http` | Default |
| --- | --- |
| `max_connections_per_host`, `max_idle_per_host`, `max_idle_connections` | `8`, `4`, `16`; shared instrumented HTTP clients |
| `field_max_connections_per_host` | `8`; Client API's separate Field transport |

These are per process and destination host, including connections waiting on
upstream responses. Idle connections per host must not exceed the connection
limit. Field transport retains trace propagation. Increasing HTTP concurrency
can move queues into Auth or database pools and make latency worse; validate
successful throughput, per-role outcomes, and resource use before adopting it.
These defaults preserve the earlier deployment values.

| Key under `sources` | Default |
| --- | --- |
| `satellite_idle_timeout_ms` | `20000`; resets on received stream bytes |
| `coastal_request_timeout_ms` | `1200` |
| `coastal_capture_timeout_ms` | `8000`; bounded tail capture after identifying a rejected response |
| `coastal_station_refresh_ms`, `coastal_poll_interval_ms` | `30000`, `250` |
| `coastal_requests_per_second` | `4`; shared by both endpoints, burst remains 1 |
| `coastal_breaker_failures` | `3` |
| `coastal_breaker_cooldown_ms`, `coastal_breaker_max_cooldown_ms` | `2000`, `15000`; initial cooldown cannot exceed maximum |

| Key under `runtime` | Default |
| --- | --- |
| `application_memory_mib`, `aggregator_memory_mib`, `mock_memory_mib` | `192`, `384`, `96` |
| `monitoring_memory_mib`, `grafana_memory_mib` | `192`, `128` |
| `go_processors` | `2` |

These set Go's soft memory target and processor count. Memory targets must stay
below their M2 container limits; Kubernetes requests/limits are unchanged. The
target does not bound total process RSS. Vendor services, PostgreSQL, Java and
Fluent Bit retain their existing runtime settings.

| Key under `ai` | Default |
| --- | --- |
| `summary_calls_per_day`, `chat_calls_per_day` | `45`, `150`; chat permits 0 |
| `refresh_interval_seconds`, `min_call_interval_seconds` | `1800`, `120` |
| `llm_timeout_seconds`, `chat_timeout_seconds` | `60`, `60` |
| `chat_concurrency` | `2`; permitted range 1..16 |
| `max_tokens`, `reasoning_tokens` | `4096`, `1024`; reasoning must leave at least half the answer budget |

AI settings take effect only when the optional service is enabled. These knobs
do not enable AI or supply credentials.

### Applying a change

The first release containing this feature requires rebuilt application images,
because the new exporter/source settings and ingestion timeout handling are code
changes. After deploying those images, tuning changes require configuration
rollouts, not image rebuilds:

```bash
python3 scripts/m2/manage.py --config .m2-local/config.json plan
python3 scripts/m2/manage.py --config .m2-local/config.json deploy
python3 scripts/m2/manage.py --config .m2-local/config.json ready
```

Use the intended private config path, including the bounded replay config when
acquisition should remain disabled. The cluster must already be running.
Configuration changes update pod templates and trigger the relevant rollouts;
no hot reload is promised. `deploy` applies the complete desired manifest and
restores configured replica counts. Keep capture of trace evidence separate from
changing Jaeger retention, because restarting its memory store clears history.

Spec-defined wildfire rules, durability, fencing, authorization, Kubernetes
memory limits, lease/failover timing, and PostgreSQL server internals remain fixed.

Private runtime credentials are generated once in `.m2-local/secrets.json`.
Deleting that file while retaining PostgreSQL data is not a password-rotation
procedure. Do not regenerate it just to fix a startup problem. Keep a secure copy
with the corresponding cluster identity. Use separate working directories/private
state when two independent clusters need different credentials.

### Application environment overrides

These settings are read at process startup. For a diagnostic rollout, set them
on the named Deployment as container environment variables. The operator JSON
has no corresponding keys. Defaults apply to manifests rendered from the
repository without these overrides.

| Deployment | Environment variable | Default and purpose |
| --- | --- | --- |
| `ingestion` | `SATELLITE_BATCH_RECORDS`, `SATELLITE_BATCH_BYTES`, `SATELLITE_BATCH_WAIT_MS` | `100` records, `2097152` bytes, `100` ms before flushing a batch |
| `ingestion` | `RAW_PUBLISH_BATCH`, `RAW_PUBLISH_RESERVED_BATCH` | `100` and `50` entries, respectively |
| `ingestion` | `RAW_PUBLISH_LANES` | `1` ordered publisher. Use `2` for a separate critical coastal and Field publisher |
| `ingestion` | `RAW_PUBLISH_CLAIM_LEASE`, `RAW_PUBLISH_ACK_TIMEOUT` | `30s` and `10s` |
| `wildfire-processor` | `WILDFIRE_BATCH_RECORDS`, `WILDFIRE_ACK_CONCURRENCY` | `25` records and acknowledgement concurrency equal to the configured batch size |
| `wildfire-processor`, `coastal-processor` | `CANONICAL_DELIVERY_BATCH`, `CANONICAL_DELIVERY_CLAIM_LEASE` | `50` entries and `60s` per claim |
| `wildfire-processor`, `coastal-processor` | `CANONICAL_SEQUENCE_MAX` | `50` ordered operations per owner call, up to `200`. `1` uses one operation per call |
| `aggregator` | `AGGREGATOR_WARMUP_TIMEOUT`, `AGGREGATOR_WARMUP_CONNS` | `10s` and half the general pool size rounded down, minimum `1` |
| `auth-service` | `AUTH_STORAGE_TIMEOUT`, `AUTH_REFRESH_GRACE` | `1.5s` storage deadline and `10s` to retry a refresh with the immediately previous token |

See [Ingestion-Performance.md](Ingestion-Performance.md) for batching and
measurement details.

## Owner boundaries and runtime limits

Numeric tuning values below describe the defaults unless explicitly fixed by M2.

- Aggregator alone receives `canonical_app` credentials; Citus replicas enforce
  that owner and the formation's role/HBA policy.
- Ingestion/processors alone receive `ingestion_app` credentials. Canonical writes
  use an authenticated owner HTTP API. Auth, registry, dashboard and notifier have
  their own database/user pairs in the shared services formation.
- Normal Aggregator replicas: **3**. Five is only the required temporary scale-up
  point in the `3 -> 5 -> 2` test. No HPA is installed.
- Aggregator general DB pool: 6 per process; separate Field read pool: 3. Ingestion
  and each processor: 4 per process. Auth/consumers: 8 total per process; Auth's
  reserved Field pool is included in its total. Registry JDBC maximum: 4.
- Typical Go target: 192 MiB and two Go processors. Aggregator target: 384 MiB.
  Kubernetes requests/caps follow the M2 role table; Go targets are lower than caps.
- Payload parsing limit: 4 MiB, JSON nesting limit 64. Field reports are smaller
  than 1 MiB including forwarding headroom. Bounded failed-response storage
  streams oversized evidence rather than loading its tail into memory.
- Lease TTL: 15 seconds, renewal every 5 seconds, using the owning database clock.
  PostgreSQL checks the epoch/expiry again at commit. Source holders include the
  pod and process-incarnation UUID for evidence.
- Coastal calls share 4 requests/second with burst 1 and 1.2-second deadlines;
  station metadata refreshes independently every 30 seconds. Breakers open after
  three failures, initially for 2 seconds, backing off to 15 seconds.
- Satellite reconnects from durable original progress. An idle connection is
  canceled after 20 seconds without received bytes, while ownership cancellation
  also stops the request.
- Field/general Client API and Auth admission are separate. Live introspection
  remains authoritative; signed hints only schedule capacity. AI uses the general
  allowance and an additional two-request cap.
- Trace sampling is parent-aware, 5%, with bounded 128-span queues and 32-span
  batches. Jaeger memory storage retains at most 1000 traces. Logs/metrics retain
  a short demo history; export evidence before cleanup/restart.

The exact generated environment is in `scripts/m2/apps.py`; PostgreSQL settings
are in `deployment/m2/storage/entrypoint.py`; monitoring settings are in
`scripts/m2/monitoring.py` and `deployment/m2/monitoring/`. Change runtime settings
through the reviewed renderer, rebuild when source changes, and use a new image
tag. Secret/config checksums trigger relevant rollout when their contents change.

## Ubuntu bootstrap environment

Copy `deployment/m2/node.env.example` privately to each VM. It documents
`M2_NODE_NAME`, `M2_NODE_IP`, `M2_VPN_IFACE`, `M2_K3S_VERSION`, `M2_POD_CIDR`,
`M2_SERVICE_CIDR`, `M2_JOIN_TOKEN_FILE`, and B's `M2_SERVER_URL`. Use the same
pinned K3s release/CIDRs on both nodes and distinct node names/IPs. The bootstrap
reads these values only on the intended VM; no filled node environment is committed.
