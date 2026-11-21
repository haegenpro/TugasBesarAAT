# scripts/m2

Tooling for the M2 deployment and its evidence. A file name tells which part of the
specification it serves:

| Prefix | Meaning |
| --- | --- |
| `p1_` … `p4_` | belongs to one problem (no `p5_` yet; Problem 5 is evidenced with the `evidence_` tools) |
| `evidence_` | reads the running system and produces evidence for several criteria |
| *(none)* | platform: deploy, lifecycle, images, observability, reset (Platform and Observability requirements) |

Every tool states its criteria on a `Criteria:` line in its docstring. How to turn each
criterion into report evidence is in [../../docs/m2/Evidence-Runbook.md](../../docs/m2/Evidence-Runbook.md).
Pick the cluster once with `manage.py init --profile minikube|k3s`; all tools follow it.

## Per problem

| Script | Criteria | Purpose |
| --- | --- | --- |
| `p1_catalog_import.py` | P1-1 | checksum-verified import of the historical earthquake catalog through the canonical owner |
| `p1_placement_load.py` | P1-1, P1-2, P1-5, P1-6 | `distribution` (primary/replica per node and unit), `nodeload` (CPU/memory per node), `spike` (one-region write burst) |
| `p2_load_test.py` + `p2_load_test.js` | P2-6, P2-8 (P5-5 from the summary) | 10,000-VU k6 run, 3 → 5 → 2 scaling, rolling image replacement, PASS/FAIL verdicts |
| `p2_load_credentials.py` | P2-6 | writes only the demo client credentials for the separate generator PC |
| `p2_throughput_compare.py` | P2-7 | one-versus-three Aggregator throughput on the same dataset |
| `p1_analytics_compare.py` | P1-7 | dashboard numbers versus a direct Audit API count per type, severity and area; verdict on the numbers only (the page itself is manual) |
| `p3_schema_mode.py` | P3-1 | switches the PVMBG mock between `v1`, `compatible` and `breaking` contracts |
| `p3_schema_behavior.py` | P3-1 | runs every contract and attaches what happened (events added, unprocessed by status) next to the declared policy; no verdict |
| `p3_ops_access_matrix.py` | P3-5 | HTTP status per identity and Internal Ops endpoint, with a verdict that only Internal Ops gets through |
| `p3_field_report_ledger.py` | P3-6 | `submit` records every 202 with its Idempotency-Key, `reconcile` checks receipts and database counts and prints the 202 latency (p50, p95, max) |
| `p3_quarantine_payload.py` | P3-2 | downloads retained original bytes through Internal Ops, recomputes SHA-256 against `X-Content-SHA256`, checks the Audit listing carries no raw field |
| `p3_replay_flow.py` | P3-4 | `--execute` activates the reader version, replays retained messages, repeats the request (must select nothing), compares Audit counts and duplicates |
| `p4_province_reference.py` | P4-1, P4-6 | validates and installs the checksum-pinned province reference |
| `p4_requeue_satellite_updates.py` | P4-3 | one-off migration for updates saved under the earlier retraction rule |
| `p4_ingestion_progress.py` | P4-2, P4-4, P4-7 (helps P4-8) | received/prepared/applied offsets, lag, watermark, late-data counters over time; no verdict |
| `p5_warning_slo_snapshot.py` | P5-1, P5-8 | critical-warning delivery measurements from the notifier; PASS/FAIL only when measurable |
| `p5_sensor_fault_probe.py` | P5-4 (supports P5-3) | client latency, source status, breaker state and request rate to the sensor while it is unhealthy; no verdict |

## Evidence tools (several criteria)

| Script | Criteria | Purpose |
| --- | --- | --- |
| `evidence_audit.py` | P1-1, P1-4, P1-6, P2-2…P2-6, P3-2, P3-4, P4-1, P4-3, P4-5, P4-8, P5-2 | Audit API: `checksum`, `scan`, `location`, `unprocessed`, `snapshot`, `compare` (zero loss), `gaps` (takeover) |
| `evidence_outage_probe.py` | P1-4, P1-5, P1-6, P2-1, P2-8, P5-3 | request continuity while a node or instance fails |
| `evidence_snapshot.py` | Platform, P1-4 | revision, nodes, pods, requests/limits and checksum snapshot |
| `evidence_tester_agent.py` | Tester Agent contract; P1-4…P1-6, P2-3…P2-5, P3-6, P4-5, P4-8, P5-6 | install, check, list, practice and read the Tester Agent log |
| `evidence_integration_tests.py` | supports P1-3, P1-4, P3-3, P3-4, P4-3, P5-1 | Go integration suites on synthetic data (not a pass by themselves) |

## Verdict or attached result

Tools that can judge a criterion mechanically print PASS/FAIL (or a one-line verdict) and exit
non-zero on failure. Tools marked "no verdict" only measure: they print the tables and save
`result.txt` and `result.json` in a private `.m2-local/evidence-<tool>-<time>/` directory
(override with `--output DIR`) so the results can be attached and judged by a person. Tokens
are never written. `evidence_report.py` and `evidence_metrics.py` are their shared helpers.

## Platform (no single criterion)

`manage.py` (init, deploy, up, ready, scale, stop/start), `render.py` and `apps.py`
(manifests), `monitoring.py` (Prometheus, Grafana, Jaeger, Loki, Fluent Bit), `lifecycle.py`,
`images.py`, `minikube.py` and `install_tools.py` (Minikube only), `reset.py`, `access.py`
(browser port-forwards), `tuning.py`, `common.py`, `acceptance_support.py`.

## Tests

```bash
python3 -m unittest discover -s scripts/m2 -p 'test_*.py' -v
```

`test_p2_load_test.py` needs Linux (it exercises the Docker-based generator logic).
