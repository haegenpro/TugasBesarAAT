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
| node rebuild (P1-5) | `deployment/m2/rebuild-b-minikube.py` | `deployment/m2/rebuild-b.py` |

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
| 5 | K3s: `sudo python3 deployment/m2/rebuild-b.py --hostname HOST --execute`; Minikube: `python3 deployment/m2/rebuild-b-minikube.py --execute` (same six B replicas, same wipe). Either while `evidence_outage_probe.py --duration 300` runs; then `p1_placement_load.py distribution` | `CONTINUITY OK` plus placement showing node B holds data again. |
| 6 | After 4 and 5: `p1_placement_load.py distribution`, then repeat the single-node loss | `PLACEMENT OK` (replica on a different node for every sampled event) before the repeat. |
| 7 | `p1_analytics_compare.py`; then in the browser `access.py client-api` and open `/analytics`; repeat both with node B stopped | Script: dashboard-versus-Audit table per type, severity and area with `NUMBERS MATCH` or `DIFFERENCES FOUND`, plus `partial`/`unavailable_units`. **manual**: the charts and the "incomplete" banner on the page. Result files are attached (`result.txt`, `result.json`). |

## Problem 2

| # | Produce | Verdict / limit |
| --- | --- | --- |
| 1 | `kubectl -n sistem-informasi-kebencanaan-bnpb get pods -l app=aggregator -o wide`; `evidence_outage_probe.py --watch` while `kubectl delete pod` removes one instance | Pods on different nodes; no errors during removal. |
| 2 | `evidence_audit.py scan` and `kubectl logs deploy/bmkg-mock` / `pvmbg-mock` for upstream request lines | `scan` proves no duplicate HazardEvent. Upstream single-request proof is **manual** from the mock logs. |
| 3 | Delete the polling owner, then `evidence_audit.py gaps --source BMKG --since <kill time> --max-gap 60` (same for PVMBG) | PASS when no ingestion gap exceeds 60 s and no duplicates. Valid because the mocks emit about one event per second. |
| 4 | `evidence_tester_agent.py practice <clock scenario>`; then `evidence_audit.py scan` | Duplicates absent. "Never two successful commits" is also covered by integration tests (`evidence_integration_tests.py --suite canonical`). |
| 5 | Freeze/suspend the owner's VM yourself (not possible from the cluster), unfreeze, then `evidence_audit.py scan` and `evidence_audit.py gaps` | Same checks; the freeze itself is **manual**. |
| 6 | `p2_load_test.py --output .m2-local/load-RUN --scale --audit-check` from the separate generator PC (`--base-url http://NODE_A_IP:NODEPORT`) | PASS/FAIL lines: error rate under 1%, at least 120 s continuously at 10,000 VUs, 3 → 5 → 2 applied, no duplicate HazardEvent. |
| 7 | `p2_throughput_compare.py --output .m2-local/throughput-RUN` (default: 10,000 VUs as in P2-6, `--target-rps auto`) | `P2-7 MET` / `NOT MET` with the measured ratio and whether the dataset stayed fixed, or `P2-7 INVALID` when the one-instance sample reached the cap. Both samples offer the same total requests per second, spread evenly over all VUs. `auto` first measures the saturated one-instance rate in a 30 s pilot and uses 3 x that as the cap for both samples; give `--target-rps N` to fix the cap. The cap must be above what one Aggregator serves, so a ratio of 3 x with the high sample at the cap is printed as a lower bound. The best measured ratio with the earlier 200-VU closed loop was below 2.0; report results as measured. |
| 8 | `p2_load_test.py --output .m2-local/roll-RUN --roll-from <previous image> --strict-zero-failures` | `P2-8` PASS only with zero 5xx, zero transport failures and zero unexpected failures (controlled 429 with Retry-After excluded). |

## Problem 3

| # | Produce | Verdict / limit |
| --- | --- | --- |
| 1 | `p3_schema_behavior.py --execute` (switches the PVMBG mock through `v1`, `compatible`, `breaking` and back; see also [Replay-Demo.md](Replay-Demo.md)) | **Results attached, no verdict**: declared policy, then per mode the PVMBG events added, new unprocessed messages by status/retryability and sample reasons. Compare them with `docs/m1/Schema Validation.md`. |
| 2 | `evidence_audit.py unprocessed --show 15` | Counts by `retrying`/`parked` and retryable/not-retryable. |
| 3 | `evidence_integration_tests.py --suite ingestion` for oversize/truncated/deep nesting; `kubectl get pods` shows no restarts | Test output plus restart counters. Live malformed-input injection is **manual**. |
| 4 | Activate the reader on `/ops`, request replay; then `evidence_audit.py unprocessed` and `evidence_audit.py scan` | Failures resolved, no duplicates, ordering intact. |
| 5 | `p3_ops_access_matrix.py` (status code per identity and endpoint, including forged role headers); also open `/ops` as Media/Field in the browser | `ONLY INTERNAL OPS CAN INSPECT FAILED MESSAGES` or lines naming each violation or inconclusive row. The browser view is **manual**. |
| 6 | `p3_field_report_ledger.py submit --count 500 --prefix demo1` while `evidence_tester_agent.py practice <pod kill scenario>` runs; then `p3_field_report_ledger.py reconcile --ledger <ledger path>` | PASS/FAIL: every 202 has a retrievable receipt, one receipt per key, no receipt shared, ingestion holds every acknowledged report, no duplicate processing result. Accepted-versus-processed counts and reports stored without a seen 202 are only reported. |

## Problem 4

| # | Produce | Verdict / limit |
| --- | --- | --- |
| 1 | `evidence_audit.py scan --source SATELIT`; sample one event with `curl -H "Authorization: Bearer $TOKEN" "$BASE/hazards?limit=1"` | Duplicate-free WILDFIRE events; multi-satellite grouping needs the reference. |
| 2 | `p4_ingestion_progress.py --minutes 10` (late mutations, excluded detections and singletons per source) | **Results attached, no verdict**: compare the late counter with the lateness bound you declared. |
| 3 | Update/retract: integration tests (`evidence_integration_tests.py --suite ingestion`) and Audit before/after | test output + `evidence_audit.py compare`. |
| 4 | `p4_ingestion_progress.py --minutes 15 --metrics` during a 60x session, plus the dashboard panels "Satellite received position" and "Canonical apply p95" | **Results attached, no verdict**: received/prepared/applied offsets, lag, peak lag and seconds from the peak back to zero (the 60-second target). The Prometheus panels show only the received position; applied and prepared offsets come from this tool (Internal Ops progress API). The apply-latency panel measures canonical apply time, not data-in to stored. |
| 5 | `evidence_tester_agent.py practice <processor kill scenario>`; interrupted vs uninterrupted: `evidence_audit.py snapshot --ids` on two clean runs, then `evidence_audit.py compare` | Requires two clean runs on separate namespaces; compare output is the verdict. |
| 6 | `p4_province_reference.py --file F --sha256 H --execute`; compare Audit events with the assistants' reference | **manual** difference report. |
| 7 | Live feed for several minutes: `p4_ingestion_progress.py --minutes 5 --metrics`, the Grafana satellite panels and `evidence_audit.py scan --source SATELIT` | **Results attached, no verdict** (offsets advancing live) plus the scan's duplicate check. |
| 8 | Break the connection (`evidence_tester_agent.py practice <network scenario>` or `kubectl delete pod`) while `p4_ingestion_progress.py --metrics` runs; then `evidence_audit.py scan --source SATELIT` | Scan: no duplicates. Progress timeline attached: offsets continue after the reconnect (reconnect counter included with `--metrics`). |

## Problem 5

| # | Produce | Verdict / limit |
| --- | --- | --- |
| 1 | `evidence_tester_agent.py practice <coastal scenario>`; then `p5_warning_slo_snapshot.py`; `evidence_audit.py scan --source SENSOR_PANTAI` | PASS/FAIL/NOT MEASURABLE for p95 ≤ 5 s and no overdue pending warning (judged only when the notifier reports a complete cohort and valid p95). Reconciling the sensor's own critical events with deliveries is **manual**. |
| 2 | `evidence_audit.py unprocessed --source SENSOR_PANTAI` | Rejected inputs visible with reasons. |
| 3 | `evidence_outage_probe.py --duration 180` while the sensor is unhealthy | Latency criterion for the other sources; `CONTINUITY OK`. |
| 4 | `p5_sensor_fault_probe.py --duration 240` while the sensor is unhealthy, plus the dashboard "Coastal breaker state" and "Coastal fetch failures and short-circuits" panels | **Results attached, no verdict**: client latency and per-source status timeline, breaker states, requests/s to the sensor, failures and short-circuits per second, ingestion `source_health`. Metrics come from one ingestion pod. |
| 5 | `p2_load_test.py` mixed load; read the Field/Media rows in `summary.json` | Field outcomes versus Media rejections are in the summary tags; judgement is **manual**. |
| 6 | Run the exam combination without touching the cluster; record the timeline | **manual**; the scripts only observe (`evidence_outage_probe.py`, `evidence_audit.py`, `evidence_tester_agent.py logs`). |
| 7 | Explain the failure from Grafana, Loki and Jaeger | **manual**. |
| 8 | `p5_warning_slo_snapshot.py --samples 20 --interval 30` across the game day, plus "Error budget remaining" | **Results attached, no verdict**: error-budget use, on-time ratio and counts over time. Defining the target is a report decision. |

## What no script can establish

The charts and banner on the analytics page (P1-7), the VM freeze (P2-5), the assistants'
reference comparison (P4-6), reconciliation of the sensor's own critical events with
deliveries (P5-1), and any statement that a scenario was run without intervention
(P5-6) or explained from the dashboards (P5-7). Capture these with screenshots or logs and
write them up as manual evidence. Observation tools print and save their results
(`result.txt`, `result.json` in a private `.m2-local/evidence-*` directory) so they can be
attached; they never decide for you. Results from Minikube or a colocated generator must be
labelled as local rehearsal, not as demo evidence.
