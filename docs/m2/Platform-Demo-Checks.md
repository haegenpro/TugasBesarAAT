# M2 demo checks on the existing four-laptop cluster

How to trigger and verify every multi-node M2 criterion on the manually installed
K3s cluster (inventory **v3**, one VirtualBox VM per laptop, Wi-Fi). Each command
keeps its own evidence and exits **0 passed, 1 failed, 2 blocked** (an input is
missing). A check never reports success from missing data.

These tools were written and tested offline with fakes (`demo_checks_test.py`,
`platform_lib/existing_actions_test.py`). They have **not** yet been run against
the live cluster. The application must answer authenticated hazard and Audit
reads first (see [startup repair](Platform-Application-Startup-Repair.md)).

## Common inputs

Run on the controller (Mint) with the v3 inventory from
[existing-cluster deployment](Platform-Existing-Cluster-Deployment.md). Tokens are
private files (`chmod 600`), never command arguments.

```bash
INV=/private/existing.json
AUDIT="--audit-url http://127.0.0.1:18081 --audit-token-file /private/audit-token"
CLIENT="--client-url http://127.0.0.1:18080 --field-token-file /private/field-token"
DB="--receipt /private/maintenance/receipt.json --owner-config /private/inputs/role1.json"
```

Use `kubectl port-forward` (or a reviewed NodePort) for the Aggregator (Audit API),
Client API, ingestion and PVMBG admin endpoints. Evidence is written under
`evidence/platform-demo/<check>-<run>/` (ignored by Git).

## VM actions and guest steps (operator-assisted)

The controller cannot reach the other laptops. When a check needs a VM action,
it prints the exact command for the owning laptop and then **observes** the
result itself (kubelet TCP reachability and the Kubernetes API); an action that
was not performed times out (`AAT_OPERATOR_TIMEOUT`, default 300 s) instead of
passing. On the laptop that hosts the VM:

```bash
python scripts/platform-vm-control.py pause   --vm "AAT - B"
python scripts/platform-vm-control.py resume  --vm "AAT - B"
python scripts/platform-vm-control.py freeze  --vm "AAT - B" --seconds 90   # always resumes
python scripts/platform-vm-control.py poweroff --vm "AAT - B"
python scripts/platform-vm-control.py start   --vm "AAT - B"
```

Add an optional `"vm_name"` to each inventory node so printed instructions name
the right VM. Guest steps (volume preparation, restores) are exported as
`step.py` + `payload.bin`; run them inside the VM with
`sudo python3 step.py < payload.bin`, then `touch` the printed `done` file.

## Criteria

Each criterion also has a named entry point in
[`scripts/m2/`](../../scripts/m2/README.md), for example
`python scripts/m2/p2-takeover.py …` instead of
`platform-demo-check.py takeover …`; arguments are passed through unchanged.

| Criterion | Command | Operator part |
| --- | --- | --- |
| P1.1–1.3 data spread, cross-location ordered reads | `platform-demo-check.py distribution --inventory $INV $AUDIT $CLIENT` | none |
| P1.2 load during a regional burst | run `distribution` again while the burst/load runs (load is sampled per node) | start the burst |
| P1.4 node lost with its data | `platform-demo-check.py tester --mode practice --scenario <name> --inventory $INV $AUDIT` | none (Tester Agent) |
| P1.5–1.6 node back with empty disk | `platform-node-replacement.py replace-node --inventory $INV --config replacement.json --node <node>` then `check-storage-recovery.py` | power off/on VM, run exported guest steps |
| P1.7 analytics dashboard marks partial data | open the dashboard during P1.4; `check-analytics-browser.py` | observe |
| P2.1 three Aggregators on different nodes | `kubectl get pods -o wide -l role=aggregator` | none |
| P2.2–2.3 single acquisition, takeover ≤60 s | `platform-demo-check.py takeover --scope acquisition:PVMBG --inventory $INV $AUDIT $DB` (BMKG: `--scope acquisition:BMKG`) | none |
| P2.4 clock shift | Tester Agent clock scenario, then `platform-demo-check.py fencing --since <start UTC> --scope … --inventory $INV $AUDIT $DB` | none |
| P2.5 frozen node | `platform-demo-check.py fencing --freeze-node <node> --freeze-seconds 90 --scope … --inventory $INV $AUDIT $DB` | `platform-vm-control.py pause`/`resume` when asked |
| P2.6–2.8 10,000 VU, 3→5→2, throughput, rollout | `platform-game-day.py run --scenario scale|baseline|rollout …` | none |
| P2/P5 combined with a VM pause | `platform-game-day.py run --scenario combined --node <node> …` | pause/resume when asked |
| P3.1–3.2 incompatible PVMBG schema parked | `platform-demo-check.py schema --mode rename --pvmbg-url … --pvmbg-admin-token-file … --inventory $INV $AUDIT` | none |
| P3.3 malformed messages | `platform-demo-check.py malformed --inventory $INV $CLIENT` | none |
| P3.4 replay after adaptation | roll out `pvmbg_adapter_version: 2`, then `platform-demo-check.py replay --namespace-id <ns> --ingestion-url … --ops-token-file … --inventory $INV $AUDIT` | adapter rollout |
| P3.5 failures visible only to Ops | `platform-demo-check.py ops-only --ingestion-url … --ops-token-file … --media-token-file … --inventory $INV $AUDIT` | none |
| P3.6 202 survives receiver crashes | `platform-demo-check.py field-reports --count 20 --inventory $INV $CLIENT` | none |
| P4.1–4.4, 4.6, 4.8 satellite | `platform-source-validation.py` / `platform-acceptance.py` (60× burst, reconnect, reference) | supply the reference |
| P4.5 processor crash | `platform-demo-check.py processor-crash --reference-file ref.json --inventory $INV $AUDIT $DB` | none |
| P5.1 critical coastal 0 lost, p95 ≤5 s | `platform-source-validation.py` | none |
| P5.4 unhealthy sensor | `platform-demo-check.py coastal-breaker --path "/hazards?source=SENSOR_PANTAI" --prometheus-url … --rate-query '<PromQL>' --inventory $INV $CLIENT` | trigger the sensor fault |
| P5.6 game day | `platform-demo-check.py tester --mode exam --scenario <name> --inventory $INV $AUDIT -- <key args>` | none during recovery |
| Bonus: real Wi-Fi cut | `platform-network.py run --inventory $INV --output DIR`, `platform-host-probe.py` on every laptop (one with `--disconnect-after 60 --disconnect-seconds 30`), copy logs to `DIR/hosts/<physical_host>.jsonl`, `platform-network.py reconcile` | run probes, copy logs |

List Tester Agent scenarios with `platform-demo-check.py tester --mode list --inventory $INV`.
The `tester` check only observes after the scenario: it snapshots checksums and
restarts before, samples recovery for `--recovery-seconds`, and stores every
Tester Agent pod's log.

## What the checks decide

- **distribution**: Audit listing complete and ordered by `ingested_at, hazard_id`;
  checksums equal a recomputation from the listing; every event has ≥2 nodes;
  no node holds every event; primaries on more than one node; optional Client API
  pages unique. Per-node store CPU/memory is recorded.
- **takeover**: crashes instances (`--grace-period=0`) until the lease holder
  changes (the holder ID is not a pod name), then requires a higher epoch and a
  new dispatch within 60 s. Upstream mock logs (`acquisition.request_id/epoch`)
  must show no repeated first request and no older epoch after a newer one; the
  Audit API must show no duplicate `source_ref_id`.
- **fencing**: the same upstream and duplicate checks over a time window, plus
  the number of stale commits rejected (`ownership expired before commit`).
- **field-reports**: one stable receipt per idempotency key across crashes and
  retries, and every accepted report reaches `processed`.
- **processor-crash**: the processor catches up (`applied_through ==
  received_through`), no duplicate fires, and the SATELIT count/checksum equals
  the supplied reference.
- **Wi-Fi bonus**: host logs prove the adapter went down and came back, the
  controller saw the node drop out and return Ready, other nodes stayed
  reachable, and inter-node RTT was measured. Network evidence alone stays
  *blocked* until application and demo evidence are attached.

## Known limits

- No built-in regional earthquake burst generator: run P1.2 during an existing
  load or burst.
- P2.4 relies on the Tester Agent clock scenario; P4.7 (live feed) and P5.7
  (explaining failures from dashboards) are demonstrations by the team.
- The exam satellite session (`POST /v1/exam/start`) is started by the team when
  recording starts.
- Existing-cluster replacement keeps the node's K3s identity and recreates only
  its owned application volumes empty (the originals are moved to
  `/var/lib/aat/replaced/<run>`). A fully reinstalled VM (new node UID) needs the
  manual K3s join and a recaptured inventory first.
