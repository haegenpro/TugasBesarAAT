# M2 evidence runbook

How to turn each M2 criterion into evidence for the report. Every command is run in a
visible terminal and captured as a real screenshot (about 100 columns, large font).
The scripts print a PASS/FAIL line or a verdict where one is possible; a verdict that
needs a human judgement (a dashboard, a log line) is marked **manual**. Nothing here
marks a criterion as passed by itself. If a run fails, report the failure.

Assumptions: commands run from the repository root on Linux/WSL, `.m2-local/config.json`
exists, and the cluster runs the tag recorded by `evidence_snapshot.py`.

## Profile switch

The profile is chosen once and every tool follows it.

```bash
python3 scripts/m2/manage.py init --profile minikube   # local rehearsal only (not valid for the demo)
python3 scripts/m2/manage.py init --profile k3s --context CTX --node-a NODE_A --node-b NODE_B
```

| Behaviour | `minikube` | `k3s` |
| --- | --- | --- |
| kubeconfig | private `.m2-local/kubeconfig` | the host's own kubeconfig |
| image delivery | `images.py load-minikube` | `images.py export`, then `images.py import-k3s` on **each** node |
| node resources in load runs | Docker container stats/limits | kubelet stats (`node_usage`) |
| node rebuild (P1-5) | not available | `deployment/m2/rebuild-b.py` |

## Common setup (once per rehearsal)

```bash
python3 scripts/m2/manage.py up
python3 scripts/m2/evidence_tester_agent.py install --execute      # pinned manifest, image untouched
python3 scripts/m2/evidence_tester_agent.py status                 # image digest, one pod per node, role labels
python3 scripts/m2/evidence_snapshot.py --output .m2-local/evidence-RUN   # revision, nodes, pods, requests/limits, checksum
python3 scripts/m2/manage.py plan                   # per-node memory requests/limits (Appendix B)
```

Client API token without printing the secret:

```bash
BASE=http://NODE_A_IP:NODEPORT
SECRET=$(python3 -c "import json;print(json.load(open('.m2-local/secrets.json'))['media'])")
TOKEN=$(curl -s -X POST $BASE/auth/login -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"media-demo\",\"client_secret\":\"$SECRET\"}" | python3 -c "import json,sys;print(json.load(sys.stdin)['access_token'])")
```

## Platform and observability

| Requirement | Evidence | Verdict |
| --- | --- | --- |
| Kubernetes, 2 nodes, namespace, role labels | `kubectl get nodes -o wide`; `kubectl -n sistem-informasi-kebencanaan-bnpb get pods -o wide -L role`; `evidence_tester_agent.py status` | script PASS lines + manual |
| requests/limits per class (Appendix B) | `manage.py plan`, `evidence_snapshot.py` `status.json` | manual comparison with the table |
| Tester Agent unmodified and running | `evidence_tester_agent.py status`; `evidence_tester_agent.py logs` | PASS/FAIL |
| Metrics dashboard per service | `access.py grafana`, dashboard "M2 operations" (17 panels) | manual |
| One trace across services | `access.py jaeger`; send `curl -H "X-Correlation-ID: demo-1" $BASE/hazards?limit=50`, find the trace | manual |
| Structured logs with correlation ID | Grafana Explore, Loki: `{app="client-api"} \|= "demo-1"` | manual |
| Audit API reachable with internal token | `evidence_audit.py checksum` | PASS/FAIL |

## Problem 1

| # | Produce | Verdict / limit |
| --- | --- | --- |
| 1 | `p1_catalog_import.py --input F --sha256 H --execute`, then `evidence_audit.py checksum --source BMKG`, then `p1_placement_load.py distribution --sample 300` | `PLACEMENT OK`: no unit holds everything, primaries on both nodes, every sampled event has a replica on the other node. Sampling is not exhaustive; the Citus placement line adds the shard view. |
| 2 | In terminal 1 `p1_placement_load.py nodeload --duration 300 --output .m2-local/nodeload.json`; in terminal 2 the catalog import, then `p1_placement_load.py spike --count 5000 --execute`; finish with `p1_placement_load.py distribution` | Per-node CPU/memory timeline and per-unit event counts. The report must discuss the skew shown; no script judges "good enough". The spike adds synthetic events to the dataset. |
| 3 | `curl -s -w '\n%{time_total}s\n' -H "Authorization: Bearer $TOKEN" "$BASE/hazards?limit=50"`, then repeat with `&cursor=<next_cursor>`; then `evidence_audit.py scan` | `scan` verifies ordering (`ingested_at`, `hazard_id`), no duplicates and recomputes the checksum. |
| 4 | `evidence_audit.py snapshot --ids --output .m2-local/before.json`; run `evidence_outage_probe.py --watch --duration 300 --output .m2-local/outage-4.json` in one terminal; in another `evidence_tester_agent.py practice <node-loss scenario>` (see `evidence_tester_agent.py list`); afterwards `evidence_audit.py snapshot --ids --output .m2-local/after.json`; `evidence_audit.py compare .m2-local/before.json .m2-local/after.json` | `CONTINUITY OK` (no 5xx, no hang, answers complete or explicitly partial) and `ZERO LOSS: yes`. Writes acknowledged during the outage are not in `before.json`; capture the acknowledged identities separately if the report claims them. |
| 5 | k3s only: `sudo python3 deployment/m2/rebuild-b.py --hostname HOST --execute` while `evidence_outage_probe.py --duration 300` runs; then `p1_placement_load.py distribution` | `CONTINUITY OK` plus placement showing node B holds data again. Not reproducible on Minikube. |
| 6 | After 4 and 5: `p1_placement_load.py distribution`, then repeat the single-node loss | `PLACEMENT OK` (replica on a different node for every sampled event) before the repeat. |
| 7 | Browser: `access.py client-api`, open `/analytics`; compare totals with `evidence_audit.py checksum`; repeat with node B stopped | **manual**: charts, severity and area tables, "incomplete" banner. No script checks the page. |

## Problem 2

| # | Produce | Verdict / limit |
| --- | --- | --- |
| 1 | `kubectl -n sistem-informasi-kebencanaan-bnpb get pods -l app=aggregator -o wide`; `evidence_outage_probe.py --watch` while `kubectl delete pod` removes one instance | Pods on different nodes; no errors during removal. |
| 2 | `evidence_audit.py scan` and `kubectl logs deploy/bmkg-mock` / `pvmbg-mock` for upstream request lines | `scan` proves no duplicate HazardEvent. Upstream single-request proof is **manual** from the mock logs. |
| 3 | Delete the polling owner, then `evidence_audit.py gaps --source BMKG --since <kill time> --max-gap 60` (same for PVMBG) | PASS when no ingestion gap exceeds 60 s and no duplicates. Valid because the mocks emit about one event per second. |
| 4 | `evidence_tester_agent.py practice <clock scenario>`; then `evidence_audit.py scan` | Duplicates absent. "Never two successful commits" is also covered by integration tests (`evidence_integration_tests.py --suite canonical`). |
| 5 | Freeze/suspend the owner's VM yourself (not possible from the cluster), unfreeze, then `evidence_audit.py scan` and `evidence_audit.py gaps` | Same checks; the freeze itself is **manual**. |
| 6 | `p2_load_test.py --output .m2-local/load-RUN --scale --audit-check` from the separate generator PC (`--base-url http://NODE_A_IP:NODEPORT`) | PASS/FAIL lines: error rate under 1%, at least 120 s continuously at 10,000 VUs, 3 → 5 → 2 applied, no duplicate HazardEvent. |
| 7 | `p2_throughput_compare.py --output .m2-local/throughput-RUN` | `P2-7 MET` / `NOT MET` with the measured ratio and whether the dataset stayed fixed. The best measured ratio so far is below 2.0; report it as measured. |
| 8 | `p2_load_test.py --output .m2-local/roll-RUN --roll-from <previous image> --strict-zero-failures` | `P2-8` PASS only with zero 5xx, zero transport failures and zero unexpected failures (controlled 429 with Retry-After excluded). |

## Problem 3

| # | Produce | Verdict / limit |
| --- | --- | --- |
| 1 | Policy: `docs/m1/Schema Validation.md` and the `/ops` schema-policy view; trigger the `breaking` mode on the PVMBG mock as described in [Replay-Demo.md](Replay-Demo.md) | **manual** comparison of declared policy and behaviour. |
| 2 | `evidence_audit.py unprocessed --show 15` | Counts by `retrying`/`parked` and retryable/not-retryable. |
| 3 | `evidence_integration_tests.py --suite ingestion` for oversize/truncated/deep nesting; `kubectl get pods` shows no restarts | Test output plus restart counters. Live malformed-input injection is **manual**. |
| 4 | Activate the reader on `/ops`, request replay; then `evidence_audit.py unprocessed` and `evidence_audit.py scan` | Failures resolved, no duplicates, ordering intact. |
| 5 | Inspect `/ops` as Internal Ops, then as Media/Field (expect 403) | **manual**. |
| 6 | `evidence_tester_agent.py practice <pod kill scenario>` while `p2_load_test.py --report-every 5` posts reports with `Idempotency-Key` | Reconciliation of 202 receipts against canonical reports is **manual**; no script reconciles acknowledgments. |

## Problem 4

| # | Produce | Verdict / limit |
| --- | --- | --- |
| 1 | `evidence_audit.py scan --source SATELIT`; sample one event with `curl -H "Authorization: Bearer $TOKEN" "$BASE/hazards?limit=1"` | Duplicate-free WILDFIRE events; multi-satellite grouping needs the reference. |
| 2 | Late-data counters: processor metrics/logs | **manual**; no counter appears on the dashboard. |
| 3 | Update/retract: integration tests (`evidence_integration_tests.py --suite ingestion`) and Audit before/after | test output + `evidence_audit.py compare`. |
| 4 | Dashboard panels "Satellite received position", "Canonical apply p95", "Canonical lag" during a 60× session | **manual**. The applications export only the received position for the satellite; applied/prepared offsets and oldest pending age are not exported, so lag has to be argued from these panels. |
| 5 | `evidence_tester_agent.py practice <processor kill scenario>`; interrupted vs uninterrupted: `evidence_audit.py snapshot --ids` on two clean runs, then `evidence_audit.py compare` | Requires two clean runs on separate namespaces; compare output is the verdict. |
| 6 | `p4_province_reference.py --file F --sha256 H --execute`; compare Audit events with the assistants' reference | **manual** difference report. |
| 7 | Live feed for several minutes: Grafana satellite panels, `evidence_audit.py scan --source SATELIT` | **manual** + scan. |
| 8 | Break the connection (`evidence_tester_agent.py practice <network scenario>` or `kubectl delete pod`); `evidence_audit.py scan --source SATELIT` | No duplicates, positions continue; probe results in `satellite-probe` output. |

## Problem 5

| # | Produce | Verdict / limit |
| --- | --- | --- |
| 1 | `evidence_tester_agent.py practice <coastal scenario>`; Grafana "Coastal warning p95"; `evidence_audit.py scan --source SENSOR_PANTAI` | p95 from the dashboard; zero-loss reconciliation of critical warnings is **manual** from the notifier ledger. |
| 2 | `evidence_audit.py unprocessed --source SENSOR_PANTAI` | Rejected inputs visible with reasons. |
| 3 | `evidence_outage_probe.py --duration 180` while the sensor is unhealthy | Latency criterion for the other sources; `CONTINUITY OK`. |
| 4 | Dashboard "Coastal breaker state", "Coastal fetch failures and short-circuits"; `curl -w '%{time_total}'` against the coastal read | Fast answers while the breaker is open; request counts from the panels. |
| 5 | `p2_load_test.py` mixed load; read the Field/Media rows in `summary.json` | Field outcomes versus Media rejections are in the summary tags; judgement is **manual**. |
| 6 | Run the exam combination without touching the cluster; record the timeline | **manual**; the scripts only observe (`evidence_outage_probe.py`, `evidence_audit.py`, `evidence_tester_agent.py logs`). |
| 7 | Explain the failure from Grafana, Loki and Jaeger | **manual**. |
| 8 | Reliability target from "Coastal warning p95" and "Error budget remaining" | **manual**. |

## What no script can establish

Visual dashboard content (P1-7, P4-4), the VM freeze (P2-5), the assistants' reference
comparison (P4-6), reconciliation of 202 receipts and critical warnings against
receipts (P3-6, P5-1), and any statement that the scenario was run without
intervention (P5-6). Capture these with screenshots or logs and write them up as manual
evidence. Results from Minikube or a colocated generator must be labelled as local
rehearsal, not as demo evidence.
