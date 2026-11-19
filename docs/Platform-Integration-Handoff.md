# Platform integration handoff

The pulled source/processing work is integrated into release preparation,
replicated service database rendering, monitoring and empty-disk recovery code.
This is not a completed physical deployment or HA acceptance result. No VM was
started during this integration. Focused local checks were authorized and run;
four-host load/fault validation remains deferred. Changes are committed by
subsection without pushing or including either assignment specification file.

The remaining image, maintenance and source/game-day scripts are now assembled
in the [input-driven workflow](Platform-Input-Driven-Workflow.md). Supply the
private input files and select its stages later. Declared machine allocation
remains an assumption; runtime capacity checks still apply. Real-node execution
can reveal integration issues and is not replaced by these local checks.

Current onboarding uses [manual VirtualBox guests on Wi-Fi](Platform-VirtualBox-WiFi-Setup.md)
on four physical Mint/Windows machines; the VPS stays outside the cluster.
The existing wired Linux/libvirt and Windows/VirtualBox 7.0.x providers do not
manage this manual setup. Provider/deployment integration is a remaining task,
in addition to the real-node validation listed below.

## Implemented integration

| Area | Result | Remaining input or execution |
| --- | --- | --- |
| Release assembly | [Release preparation](../deployment/platform/RELEASE.md): image publishing/digest capture, bootstrap/activation bundles and verified maintenance receipts | Supply registry/input files; execute bootstrap and maintenance on real nodes |
| Private configuration | [Configuration generator](Platform-Private-Configuration.md): consistent service keys, three storage PKIs/password sets with per-owner service database logins, restricted DSNs, integrated feature settings, client credentials and bundled 38-province reference | Instructor Audit token remains pending; confirm namespaces/offsets and bounded satellite egress |
| Four physical hosts | [Four-host setup](Platform-Four-Physical-Hosts.md): manual VirtualBox/Wi-Fi onboarding; existing wired Linux/libvirt and Windows/VirtualBox provider retained separately | Actual addresses/capacity, peer connectivity, provider/deployment integration, then measured physical validation |
| Wildfire batches | [Multi-entity integration](Platform-Multi-Entity-Integration.md): live Citus capability checks retain fencing, transaction recovery and WAL confirmation | Replicated replay and failure validation |
| Service databases | [Owned databases](Platform-Owned-Service-Databases.md): one shared HA formation with four separately owned databases, per-owner roles/credentials/pg_hba, verified isolation and explicit schema/Registry initialization | Live bootstrap, identity capture and failover checks |
| Empty-disk recovery | [Integrated replacement](Platform-Integrated-Node-Replacement.md): all PG formations, retained broker membership, UID-guarded holds, atomic source restore and durable phase resume | Actual source checkpoints, physical disk-loss/interrupted-phase runs and accepted-data reconciliation |
| Source checkpoints | [Stopped-writer capture](Platform-Source-Checkpoints.md): owned volumes, SQLite WAL snapshots, integrity checks and deployment-bound receipts | Choose/capture actual restore points; later source writes need protection and reconciliation |
| Broker recovery | [Account and recovery wiring](Platform-Broker-Recovery.md): APP/system credentials separated, same-name rebuild, stream/durable quorums and acknowledgement anchors | Apply prepared private Secrets; physical failure validation |
| Live satellite | [Bounded probe](Platform-Satellite-Probe.md) plus reference-based SQL reconciliation and measured 60x disconnect/catch-up runner | Supply independent reference data and execute against the real feed/deployment |
| Monitoring | [Observability](Platform-Observability-Handoff.md): owner metrics, every broker, all three database formations and warning coverage/error-budget signals | Runtime startup, reference/receipt reconciliation and clock accuracy |
| Field priority | [Overload handoff](Platform-Overload-Handoff.md): verified scheduling hints, reserved Auth/Client lanes, split Auth/read pools and protected Field routes; local saturation checks pass | Physical database CPU/I/O, locks/session limits, network/source saturation and full-system measurements |
| Combined validation | [Workflow](Platform-Input-Driven-Workflow.md): fresh private load sessions, generator samples, readiness checks, stable 1/3-replica comparison, combined scale/rollout/VM pause and source/recipient/Audit reconciliation | Reference dataset, different compatible rollout image and actual load/fault execution |

## Prepare the actual release

The private scaffold has been generated at
`evidence/platform-private/fourhost-release`, using the supplied satellite token.
Its 45 Secret objects, 24 leaf certificates, application DSNs and catalog key
wiring were checked without contacting a deployment. The instructor Audit token
remains pending. For this older scaffold, input completion now supplies the
[project province table](Province-Reference.md) without regenerating credentials.
The scaffold uses example four-host identities and is **not ready for activation**.
It predates the shared service database formation: it holds credentials for four
separate `*-db` formations and lacks the `service-db` per-owner password keys, so
the current release renderer reports its Secrets as incomplete. Because nothing
has been deployed with it, generate a fresh scaffold with `platform-configure.py`
instead of patching it. See [private configuration](Platform-Private-Configuration.md)
before editing or regenerating it. No credentials are committed.

Alternatively copy `deployment/platform/release.example.json` to a private directory. Use
`deployment/aggregator/config.integrated.example.json` and
`deployment/role1/config.integrated.example.json` for analytics, R3, adapter-v2
and audit-session settings. Ordinary examples remain development profiles.
Copy the existing service catalog as `services.json`. Supply actual digest maps,
bounded satellite HTTPS egress CIDRs and private Secrets.

Integrated source namespaces are proposed history identities. Confirm them
before first activation and never reuse them for different/reset histories.
A bounded satellite practice check returned IDs 0, 1 and 2 for `from=0`; the
integrated example starts at zero. The token stays private. The connector now
supplies an explicit User-Agent accepted by the observed feed. The bounded probe
is implemented and locally checked; actual live resume, burst, completeness and
recovery verification remain pending.

`deployment/role1/wildfire.integrated.example.json` now pins the adjacent
38-row `provinces.json` supplied by the project owner. Release preparation copies
it by default; explicit table overrides remain supported. The repository's
`(0,0)` fixture is only for tests. See [reference details](Province-Reference.md).
Course reference results for correctness comparison are a separate input.

```sh
# Later storage-only bootstrap, before application prerequisites are complete:
python3 scripts/platform-release.py --phase infrastructure \
  --inventory /private/four-hosts.json --config /private/release.json \
  --output /private/releases/BOOTSTRAP
# Full activation bundle, after supplying its external inputs and identities:
python3 scripts/platform-release.py --inventory /private/four-hosts.json \
  --config /private/release.json --output /private/releases/RUN
python3 scripts/platform-deploy.py plan --inventory /private/four-hosts.json \
  --bundle /private/releases/RUN/bundle.json
```

Storage-only preparation does not need the province table or application images.
It starts no applications and keeps `ready_for_activation` false. It still
requires a real storage image, all storage credentials and sufficient measured
capacity before applying. Prepare storage paths and Secrets, bootstrap databases, initialize service schemas
with owning applications stopped, and capture all original database system IDs.
Perform canonical/ingestion migrations and BMKG cutover with relevant writers and
publishers stopped. The maintenance runner records and verifies these operations;
the renderer resolves their blockers from bound receipts. External inputs are
completed separately. A flag or healthy pod is not durability evidence. Application
apply uses the exact reviewed bytes and stops without destructive rollback.

Application updates use immutable container images; VMs do not need Git pulls.
Align host-helper revisions on physical hosts when provider code changes.
Replacement needs all three plan/config pairs and explicit recovery policies for
every persistent workload. Unknown or unbacked source state blocks replacement.
The replacement engine supports `resume-node` for version 3 runs with the
original run ID, inventory, artifacts and checkpoints. Pending phases reconcile
lost provider/DCS/controller replies; completed restores do not overwrite later
source writes. Offline `status` exposes durable progress. Changed identities,
disk files, artifacts or primary timelines stop automatic continuation for
inspection. Physical fault execution and timeline ancestry reconciliation still
need validation. Holds/releases reject recreated or concurrently scaled controllers.

## Capacity and verification

The four service databases now share one replicated PostgreSQL formation (see
[owned databases](Platform-Owned-Service-Databases.md)), and the single-group
ingestion and shared formations are placed on A/B/D. The integrated examples
request **19,328 MiB (18.9 GiB) RAM across all four VMs** (formerly 25,856 MiB),
before guest/host overhead, rollout surge and recovery headroom. With the
measured allocatable memory of the current two 8 GB and two 4 GB VMs and a
512 MiB per-node reserve, the offline plan has no memory blocker; node C, a K3s
server, is the tightest at 3,072 of 3,403 MiB pinned. This is a scheduling
estimate, not measured runtime usage. Four 4 GiB VMs still fail the offline
check. Disk capacity is separate: databases/WAL, broker messages, images, VM
roots, quarantine and backups also need space. No CockroachDB migration has been
made.

| Focused check | Result |
| --- | --- |
| Complete offline Python regressions | 124 passed, including workflow, receipt/input, reference reconciliation, load/session/headroom, renderer/recovery and provider regressions; temporary loopback test included |
| Source observer SQL | Four real PostgreSQL checks passed: complete fixture, missing recipient, changed payload and wrong original member identity; no live source or replication claim |
| Maintenance wrapper | Go command/DSN encoding checks passed; simulated API workflow verifies ordered DDL/cutover, receipt refusal and full-release gate completion |
| Interrupted recovery checks | Eight phase boundaries, actual atomic restore filesystem checks, lost DCS/provider replies and changed-input/controller/disk refusal; no physical VM fault |
| Private configuration | 45 Secret objects; 24 leaf certificates verified; DSN passwords and required catalog keys match; external inputs remain absent |
| Ingestion bootstrap migration | 8 real PostgreSQL checks passed, no skips: fresh/Patroni initialization, repeat/checksum guard, foreign data/canonical identity/wrong group/empty identity refusal |
| Isolated Citus/payload checks | 12 passed, no skips: batch atomicity/retry/rollback, lost commit reply and observed payload |
| Isolated service database/Registry checks | All four schemas initialize/repeat; unsafe privileges, legacy schemas and checksum mismatch refused; real Registry bootstrap/runtime ready |
| Three real broker processes | 2 passed, no skips: empty-storage rebuild retains 48 accepted payloads and durable-consumer progress; privileged decommission is a separate operation |
| Auth/Field priority | Auth 4, Client API 16 and Aggregator 2 passed without skips; actual PostgreSQL general-pool saturation and live revocation included |
| Satellite tooling | Local SSE success/fault checks pass; all Aggregator packages compile; this does not replace a live probe |

Raw evidence remains ignored, including successful Citus run
`evidence/platform-integration/2e7581e9726c` and database run
`evidence/platform-databases/075aeb9759a5`, plus broker recovery run
`evidence/platform-broker-recovery/a7b4c298ac21`. Temporary resources were cleaned up;
the ingestion bootstrap run is `evidence/platform-ingestion-bootstrap/8896a22c5d7b`
and expanded priority run is `evidence/platform-priority/c945a862ff43`.
Latest source-observer SQL evidence is
`evidence/platform-source-validation/fef669e0574b`, with no cleanup pending.
Earlier failed attempts are preserved. These are local/protocol checks, not
replicated failover, physical capacity or zero-loss acceptance results.

Keep the declared allocation as an assumption for now. Later supply connection/
provider inventory, registry inputs and consistent source backups. Image digests
are produced by the publishing script. The instructor Audit token and independent
comparison data remain pending; they are needed before
full acceptance. No immediate machine-capacity assessment is requested.
Then prepare the four-host deployment and execute the
[acceptance matrix](Platform-Acceptance-Plan.md) and
[game-day runbook](Platform-Game-Day-Runbook.md). Reconcile original operation
identities, checksums and actual receipts. Keep unexecuted acceptance rows pending.
