# Service-owner deployment integration

Implemented, unexecuted. This catalog renders owner applications under the same
namespace, role/resource, Secret, discovery, persistence and probe conventions.
It does not claim the owner's application HA, inbox/202 acknowledgement policy,
schema compatibility or full project acceptance is finished.

Provide actual built digests for every selected image in a private copy of
`images.example.json`. Null entries deliberately cannot be rendered/deployed.
The existing prescribed Sensor image pin is retained. Per-service `*-runtime`
Secrets supply the existing owner's complete required environment; nonsecret
catalog overrides set Kubernetes URLs, listeners and archive paths. Preserve
distinct credentials and database ownership when preparing those Secrets.

```sh
python3 scripts/platform-services.py --images evidence/images.json --output evidence/services
```

The output includes Kubernetes objects and `requirements.json` listing local
paths/UIDs, memory requests and external dependencies. No automatic apply or
disk creation occurs. Persistent members use a distinct node-affine Retain PV
and stable workload name; never mount a shared read-write archive between source
replicas. The deployment orchestrator prepares only these declared owned paths.
Readiness/liveness are separate. Jobs such as broker-init run once, with bounded
retry; the orchestrator waits for dependencies before applying them.

The broker has three independent local data members on A/B/C, stable headless
route discovery, a private client Service, explicit file storage and
`sync_interval: always`. The existing initializer is configured for three stream
replicas. This is a private-network starting configuration: authenticated NATS
accounts/TLS, stream placement/separation, capacity, source/recipient durability
and zero-loss fault proof remain joint ingestion/platform integration. It is not
a claim that one broker stream satisfies all raw/critical-path requirements or
the no-complete-dataset placement constraints. No acceptance flag is bypassed.

`registry-db`, `auth-db`, `dashboard-db` and `notifier-db` are explicit external
owner manifests/Services, with their own runtime/schema/replication contracts.
The orchestrator must receive these artifacts and readiness checks before
starting dependent applications. This catalog does not substitute a single
unreplicated PostgreSQL pod and call that HA. Owners add their resources via the
deployment bundle; no service may use the canonical database for its own state.

Source mocks and Sensor currently have one durable local member. Their actual
restart/session semantics and replica-safe persistence are owner integration
work, not solved by scaling a file-writing source to three copies. New
satellite/raw/coastal/processor applications are contributed by their owners as
additional catalog entries or reviewed bundle artifacts. Platform does not
replace their business logic. The full catalog exceeds the light bootstrap's
workload budget once database and monitoring requirements are included; nothing
has been started or measured for fit.

The NATS flush setting follows the [official durability documentation](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/README.md).
All manifest/render/runtime checks remain deferred.
