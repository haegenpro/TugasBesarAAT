# M2 configuration reference

Operator configuration is `.m2-local/config.json` for development and, for
example, `.m2-local/k3s.json` for the real deployment. Pass the latter with
`--config` on each helper. Ordinary settings become ConfigMaps and credentials
become owner-specific Secrets; applications do not read operator files directly.

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

Private runtime credentials are generated once in `.m2-local/secrets.json`.
Deleting that file while retaining PostgreSQL data is not a password-rotation
procedure. Do not regenerate it just to fix a startup problem. Keep a secure copy
with the corresponding cluster identity. Use separate working directories/private
state when two independent clusters need different credentials.

## Owner boundaries and runtime limits

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
