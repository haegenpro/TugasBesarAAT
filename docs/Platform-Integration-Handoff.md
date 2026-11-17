# Platform integration handoff

The pulled source/processing work is integrated into release preparation,
replicated service database rendering, monitoring and empty-disk recovery code.
This is not a completed physical deployment or HA acceptance result. No VM was
started during this integration. Focused local checks were authorized and run;
four-host load/fault validation remains deferred. Changes are committed by
subsection without pushing or including either assignment specification file.

## Implemented integration

| Area | Result | Remaining input or execution |
| --- | --- | --- |
| Release assembly | [Release preparation](../deployment/platform/RELEASE.md): six database formations, applications, isolation, monitoring and explicit broker reservations | Actual image digests, Secrets, bootstrap identities, migrations and cutover |
| Four physical hosts | [Four-host setup](Platform-Four-Physical-Hosts.md): one VM per physical host, Linux/libvirt or Windows/VirtualBox | Actual OS mapping, measured resources, identities, LAN addresses and SSH prerequisites |
| Wildfire batches | [Multi-entity integration](Platform-Multi-Entity-Integration.md): live Citus capability checks retain fencing, transaction recovery and WAL confirmation | Replicated replay and failure validation |
| Service databases | [Owned databases](Platform-Owned-Service-Databases.md): four HA formations, restricted roles and explicit schema/Registry initialization | Live bootstrap, identity capture and failover checks |
| Empty-disk recovery | [Integrated replacement](Platform-Integrated-Node-Replacement.md): all declared PG formations, broker and explicit backup restores | Consistent source backups, provider disk-loss runs, interrupted-phase validation and accepted-data reconciliation |
| Monitoring | [Observability](Platform-Observability-Handoff.md): owner metrics, every broker, all six database formations and warning coverage/error-budget signals | Runtime startup, reference/receipt reconciliation and clock accuracy |
| Field priority | [Overload handoff](Platform-Overload-Handoff.md): authenticated HTTP check confirms Field progress under Media saturation | Shared Auth/database/network saturation and full-system measurements |

## Prepare the actual release

Copy `deployment/platform/release.example.json` to a private directory. Use
`deployment/aggregator/config.integrated.example.json` and
`deployment/role1/config.integrated.example.json` for analytics, R3, adapter-v2
and audit-session settings. Ordinary examples remain development profiles.
Copy the existing service catalog as `services.json`. Supply actual digest maps,
bounded satellite HTTPS egress CIDRs and private Secrets.

Integrated source namespaces are proposed history identities. Confirm them
before first activation and never reuse them for different/reset histories.
A bounded satellite practice check returned IDs 0, 1 and 2 for `from=0`; the
integrated example starts at zero. The token stays private. The connector now
supplies an explicit User-Agent accepted by the observed feed. Full resume,
burst, completeness and recovery verification remain pending.

`deployment/role1/wildfire.integrated.example.json` deliberately contains an
invalid province-hash placeholder. Supply an agreed real province centre table
and its exact SHA-256; the repository's `(0,0)` fixture is not deployment data.
Course reference results for correctness comparison are a separate input.

```sh
python3 scripts/platform-release.py --inventory /private/four-hosts.json \
  --config /private/release.json --output /private/releases/RUN
python3 scripts/platform-deploy.py plan --inventory /private/four-hosts.json \
  --bundle /private/releases/RUN/bundle.json
```

Prepare storage paths and Secrets, bootstrap databases, initialize service schemas
with owning applications stopped, and capture all original database system IDs.
Perform canonical/ingestion migrations and BMKG cutover with relevant writers and
publishers stopped. Resolve actual prerequisites before removing their unresolved
bundle entries. A flag or healthy pod is not durability evidence. Application
apply uses the exact reviewed bytes and stops without destructive rollback.

Application updates use immutable container images; VMs do not need Git pulls.
Align host-helper revisions on physical hosts when provider code changes.
Replacement needs all six plan/config pairs and explicit recovery policies for
every persistent workload. Unknown or unbacked source state blocks replacement.
The top-level replacement phase engine is not yet fully resumable after errors.

## Capacity and verification

The current integrated examples request **25,856 MiB (25.25 GiB) RAM across all
four VMs**, before guest/host overhead, rollout surge and recovery headroom.
Placement is uneven. The four service database formations account for 8.5 GiB.
Four 4 GiB VMs fail the offline check; four hosts with at least 8 GB each have
not been shown to fit. Disk capacity is separate: databases/WAL, broker messages,
images, VM roots, quarantine and backups also need space.

Consolidating the four service databases onto one replicated PostgreSQL group
has been proposed, with a rough 18.9 GiB total request estimate. It is **not
implemented or measured**. No CockroachDB migration has been made either.

| Focused check | Result |
| --- | --- |
| Offline integration checks | 10 passed: bundle/broker budgets, database release assembly, metrics and recovery coverage |
| Existing renderer checks | 16 passed |
| Inventory/provider regressions | 33 passed, including the temporary loopback HTTP test |
| Isolated Citus/payload checks | 12 passed, no skips: batch atomicity/retry/rollback, lost commit reply and observed payload |
| Isolated service database/Registry checks | All four schemas initialize/repeat; unsafe privileges, legacy schemas and checksum mismatch refused; real Registry bootstrap/runtime ready |
| Focused Go checks | Broker recovery quorum decisions, notifier metrics and authenticated Field priority passed |

Raw evidence remains ignored, including successful Citus run
`evidence/platform-integration/2e7581e9726c` and database run
`evidence/platform-databases/075aeb9759a5`. Temporary resources were cleaned up;
earlier failed attempts are preserved. These are local/protocol checks, not
replicated failover, physical capacity or zero-loss acceptance results.

Next supply exact per-host RAM/disk/OS/network details, the agreed real province
reference and comparison data, image digests and consistent source backups.
Then prepare the four-host deployment and execute the
[acceptance matrix](Platform-Acceptance-Plan.md) and
[game-day runbook](Platform-Game-Day-Runbook.md). Reconcile original operation
identities, checksums and actual receipts. Keep unexecuted acceptance rows pending.
