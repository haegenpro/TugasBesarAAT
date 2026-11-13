# Platform deployment contract and remaining work

Owner: Platform, Coordination & Reliability. The
[local QEMU/KVM bootstrap provider](../deployment/platform/README.md) now supplies
a working four-node k3s environment and pinned Tester Agent. Application/storage
deployment remains pending; [storage](../deployment/storage/README.md) and
[replicated Aggregator](../deployment/aggregator/README.md) now have unverified
implementation drafts. See [bootstrap validation](Platform-Bootstrap-Validation.md)
for the earlier infrastructure results only.

## Portable topology

Assume four separate-kernel Linux nodes. Initially these are QEMU VMs on Daniel's
laptop; the same node identities may later run on other hosts/cloud VMs. Node
addresses, CPU/RAM/disk allocation and image versions must be deployment inputs,
not assumptions inside application code. Check KVM acceleration and disk/RAM
capacity before provisioning. Record physical-host placement in the README.

Proposed Kubernetes layout: nodes A/B/C are k3s servers and workload nodes; D is
an agent/workload node. Host API access uses an authenticated-health-checking
HAProxy endpoint; initial guest registration uses the first private server and
then k3s discovers other server endpoints. Bootstrap version pins and host API
continuity are implemented. [Permanent first-server replacement](Platform-Replacement-Runbook.md)
has an unexecuted recovery workflow. The light local profile has 2 GiB per server and 1 GiB for the agent, 7 GiB
total. This cannot accommodate the full canonical database reservation below;
use the profile for infrastructure/coordination development.

Retain the storage owner's [four-group placement](Canonical-and-Storage-Contract.md#5-placement-on-four-kubernetes-nodes):

| Worker group | A | B | C | D |
| --- | --- | --- | --- | --- |
| G1 | absent | primary | replica | replica |
| G2 | replica | absent | primary | replica |
| G3 | replica | replica | absent | primary |
| G4 | primary | replica | replica | absent |

The twelve worker PostgreSQL instances require 6 GiB requests / 12 GiB combined
limits. A proposed three-instance coordinator group adds 1.5 GiB requests / 3 GiB
limits. Budget guest/kernel/Kubernetes overhead, application databases, broker,
coordination and observability separately. This is an unvalidated initial plan,
not evidence it fits the laptop. Changing placement/location is configuration;
changing copy counts or acknowledgement rules reopens the shared storage design.

Every store uses node-local data. The no-full-dataset constraint also applies to
replicas, WAL/staging, backups, processor history and audit materialization. A
replacement joins with empty local storage and rebuilds from surviving eligible
copies. Restore four-node placement before declaring three-copy protection
restored. Do not place all missing partitions on all three survivors.

## Service inventory and responsibilities

| Component | Component owner | Platform integration |
| --- | --- | --- |
| Aggregator/canonical APIs | Distributed State & Stream Processing | >=3 replicas across nodes, entry point, readiness, migration job, authority and HA configuration |
| Citus/Patroni canonical storage | Distributed State & Stream Processing + Platform | coordinator/worker replication, DCS, local volumes, promotion/routing, roles and recovery |
| Wildfire processor/history/audit/analytics | Distributed State & Stream Processing | placement, bounded resources, authority and expiry integration |
| Connectors, schema/failure/replay, coastal delivery | Reliable Ingestion & Delivery | source Secrets, durable broker/storage, independent capacity and service discovery |
| NATS JetStream and schema registry | Reliable Ingestion & Delivery + Platform | pinned images, replication/flush policy, capacity, readiness and owned volumes |
| Auth, Client API, consumer projections and their databases | Existing service owners | discovery, Secrets, storage isolation, priority/admission and rollout |
| Sensor Pantai and BMKG/PVMBG mocks | Relevant source owners | required pinned image, source storage, config, probes and Tester Agent discovery |
| Prometheus/Grafana/Loki/Alloy/Tempo/exporters | Platform | migrate existing assets, discovery/collection, persistence, dashboards and alerts |
| Tester Agent | Instructor supplied; Platform installs | unchanged supplied manifest/image and unobstructed required privileges |

Each component owner supplies its service manifest, instrumentation, tests and
documentation under platform conventions. Platform integrates and verifies the
complete inventory, including the owning persistence needed before 202/receipt
acknowledgements. Canonical replication does not protect unrelated databases.

## Deployment conventions to implement

- Namespace `sistem-informasi-kebencanaan-bnpb`; every application pod has the
  assignment's `role` label: aggregator, store, broker, consumer, api, mock,
  sensor or other. Use additional application/component labels for policies.
- Stable internal Services; public entry only through intended client interfaces.
  Audit access follows the supplied internal-token contract. Canonical/ownership
  routes accept only their dedicated service credential.
- Secrets for credentials, ConfigMaps for non-secret settings; preserve
  `.env.example` for local development. Do not commit generated kubeconfig,
  provisioning credentials or test tokens.
- Required per-instance memory requests/limits: Aggregator 256Mi/512Mi; store
  512Mi/1Gi; broker/stream processor 512Mi/1Gi; consumer/event processor
  128Mi/256Mi; Auth/Client API 128Mi/256Mi; mock 64Mi/128Mi; other 128Mi/256Mi.
  Confirm Sensor classification against instructor conventions before applying
  a budget. CPU allocation is configurable.
- Local PV/hostPath for `role=store`; explicit node affinity and independent data
  directories. Migrations run offline with administrator credentials; runtime
  uses restricted credentials and performs no startup DDL.
- Network policies plus database credentials enforce each service's storage
  ownership. Permit Citus/Patroni/DCS and monitoring traffic as required; do not
  obstruct the supplied Tester Agent.
- Readiness removes unhealthy replicas from ingress; liveness must not restart
  every service merely because a dependency is recovering. Bounded shutdown,
  traffic drain and rollout settings are jointly tested under load.
- Source-wide acquisition election and protected state mutations are integrated
  before activating multiple pollers. Canonical delivery leases alone do not
  supply these capabilities.
- Capacity/admission reserves Tim Lapangan progress when Media is rejected.
  Critical warnings remain independent of canonical snapshot/write barriers.

## Implementation sequence

The following sequence is the original implementation plan. Platform drafts for
these stages are now linked in the [integration handoff](Platform-Integration-Handoff.md).
Owner integration and the deferred acceptance evidence remain open.

1. Deliver [canonical authority contract and integration proof](Platform-Coordination-Contract.md).
2. Provision four configurable nodes, HA API entry and Tester Agent.
3. Implement one synchronous PostgreSQL group, then full Citus/Patroni formation;
   integrate durability adapters and distributed recovery with the storage owner.
   Author checks with the code and execute them in the later validation phase.
   Include bounded cleanup of the existing admission control session/advisory
   lock when an application process stays frozen, not only its data transaction.
4. Deploy source-to-canonical-to-read flow; integrate poller/processor ownership,
   all service-owned persistence and storage isolation.
5. Implement audit admission/expiry/cleanup and node-replacement recovery.
6. Extend existing monitoring, admission priority and reproducible load/fault
   evidence; validate full budget and adjust configuration/topology jointly.
7. Publish one-command deployment, topology diagram, runbook and acceptance
   evidence. Deployment remains incomplete until exercised, not just rendered.

References: [HA k3s servers](https://docs.k3s.io/datastore/ha-embedded),
[Patroni Citus integration](https://patroni.readthedocs.io/en/latest/citus.html),
[Patroni replication modes](https://patroni.readthedocs.io/en/latest/replication_modes.html).
