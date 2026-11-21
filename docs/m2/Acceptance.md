# M2 acceptance evidence checklist

This is a runnable rehearsal guide and an evidence ledger, not a completed test
report. Real-VM tests await the team's hosts. Course-feed, province-reference and
historical catalog agreement await the supplied artifacts/credentials. Local
integration tests use synthetic data and cannot substitute for those checks.

Record revision/image tag, topology, source session, start/end UTC, clock status,
generator host, offered/achieved load, configuration and before/during/after
checksums for every run. Keep tokens and raw sensitive reports out of public
artifacts. Retain original acknowledgments and receipts when checking zero loss.

## Implemented local checks

| Check | Command / evidence boundary |
| --- | --- |
| Placement, isolation, memory classes, volume ownership/reset guards | `python3 -m unittest discover -s scripts/m2 -p 'test_*.py' -v` |
| Native service suites | `python3 scripts/test-services.py --race --jobs 2` |
| Citus commit fences, expired owners, dedup, Audit, retractions, visibility recovery, complete quarantine | `python3 scripts/m2/verify.py --suite canonical` |
| Field lost response/broker outage, original source offsets, durable wildfire region/reference fixture | `python3 scripts/m2/verify.py --suite ingestion` |
| Independent Auth/Dashboard/Notifier database schemas | `python3 scripts/m2/verify.py --suite services` |
| Bounded feed resume probe | `aggregator/cmd/satellite-probe`, with real credentials later |
| Synthetic wildfire oracle | `aggregator/cmd/wildfire-reference replay`, fixture configuration and JSONL |

A skipped integration test is not a pass. Preserve the actual command output and
check that required database/broker settings were present. The normal unit suite
does not run destructive VM scenarios.

## P1: partitioning and recovery

- Import the authoritative earthquake catalog through the canonical service owner;
  do not insert into another service's private database. Record catalog count and
  checksum before failure. The importer/contract must match the supplied catalog.
- Check eight canonical shards span worker groups 1 and 2, each with a second
  physical copy. Verify primaries write on both physical nodes when healthy.
- Record `/internal/audit/checksum`, representative `/internal/audit/location`
  results, Patroni roles/timelines and NATS stream/consumer replica state.
- Run independent reads/writes while B powers off. Measure failure detection and
  takeover; retain every acknowledged write. Check partial/unavailable responses
  explicitly rather than accepting an empty result as complete.
- Restart B and verify replication/catch-up, both shard copies and restoration of
  worker 2's B primary. Compare acknowledged-input results and checksum.
- Rehearse the separate empty-B test only on the dedicated B agent VM:

```bash
sudo python3 deployment/m2/rebuild-b.py --hostname ACTUAL_B_HOSTNAME
sudo python3 deployment/m2/rebuild-b.py --hostname ACTUAL_B_HOSTNAME --execute
```

This script checks the hostname/agent role, stops B and its K3s containers, clears
only the six declared B application replica directories and restarts the agent.
It preserves B's K3s identity. It has not yet been tested on the real VMs. Keep A
running and observe rejoin from A; do not invoke the full application reset or
schema initialization to conceal lost data. A failure requires separate recovery
and is outside the accepted two-node availability promise.

## P2: leases, scaling and request continuity

- Compare upstream logs while Aggregator replicas scale 3 -> 5 -> 2. Permit retries
  after an ambiguous response while checking there is no simultaneous ordinary
  source owner and no duplicate final HazardEvent.
- Stop/freeze the current owner and measure takeover <=60 seconds. Inspect lease
  scope, pod/process incarnation and epoch. Unfreeze it and check stale commits
  are rejected. Database-expiry integration tests alone do not prove SIGSTOP or
  physical clock-skew behavior.
- Compare 1 versus 3 Aggregators on the same warmed dataset and generator; require
  at least 2x throughput, preserving the same operation mix and latency boundary.
- Run 10,000 VUs for >=120 seconds from the separate PC. Report both offered and
  achieved rates, timeouts, 429s and 5xxs. Keep unexpected errors below 1% according
  to the evaluator's boundary; never exclude overload responses to manufacture a pass.
- Roll Aggregator pods while requests continue; require no failed requests and
  graceful drain. Use a new image tag and Kubernetes rolling deployment. `up`
  intentionally stops writers for offline migrations and is not this demonstration.

## P3: incompatible payload and ordered replay

- Exercise compatible writer extras, malformed bytes and a breaking PVMBG schema.
  Check the M1 reader projection and whole original payload preservation.
- Open Client API `/ops` as Internal Ops. Inspect failure reason/status, download
  original bytes and compare SHA-256. Metadata-only Audit must not leak raw data.
- Activate the explicit approved shared reader/adapter version, select retained
  messages and replay. Verify entity ordering and stable canonical identity.
- Crash after canonical commit but before source/transport acknowledgment; retry
  the same identity and compare final state. Include oversized and truncated
  transport responses; a retained prefix must never be described as complete.

## P4: satellite timing and state

- Probe actual SSE IDs, session/reset behavior, inclusive/exclusive resume,
  reconnect overlap, update/retract payloads and virtual-clock behavior.
- Install the authoritative province artifact and record SHA-256. Confirm WGS84
  distance, spherical center, ties and fire identity agree with the course reference.
- Exercise low-confidence exclusion, singleton counts, transitive bridges,
  1 km/360-minute boundaries, late corrections, split/merge and immediate
  predecessor restoration. Inspect stable hazard UUIDs and retraction output.
- Run at 60x and demonstrate catch-up <=60 seconds against the declared source
  clock. Record received/prepared/applied original offsets separately, oldest
  pending age and durable-receipt-to-canonical p95. Never relabel output ordinals
  as original feed offsets.
- Kill/restart the processor during a correction and compare final state with an
  uninterrupted run. Check bounded working memory/backpressure under a dense region.

## P5: coastal failures and priority

- Exercise observations and station-metadata endpoint failures independently;
  confirm breaker opening, bounded half-open probes and the shared request limit.
- Verify stored hazard reads remain fast while the source fails, with visible
  stale/unavailable status. Successful repeats of old observations do not imply
  fresh measurements. Check virtual source age separately from wall-clock receipt age.
- Overload Media traffic while Field requests continue through live introspection
  and reserved admission/DB capacity. Include login and report submission, not just
  reads. Client-supplied role headers must not bypass authentication.
- Block canonical storage while valid critical warnings continue through their
  separate durable lane. Verify expected identities were persisted first and every
  eligible warning reaches the recipient; measure p95 <=5 seconds. Rejected invalid
  critical inputs and missing receipts must remain visible in reconciliation.
- Open Grafana metrics/logs/traces during the incident and explain it from measured
  evidence. Confirm all application replicas are scraped, correlation IDs match
  across handoffs, and telemetry stays within resource caps without repeated restarts.

## Resource and demo lifetime checks

Use cgroup/container working sets and host disk use during load, rollback,
rebuild and warm startup. Record peak memory per role, node totals, OOM/restarts,
WAL/message growth, filesystem free space and catch-up headroom. Test full-queue
backpressure without deleting pending work. Configured requests, PV sizes and
retention settings do not establish physical fit.

Save evidence before stopping. Ordinary stop/start preserves state. Explicit
application/observability resets are only for controlled demo cleanup outside
measured recovery windows.
