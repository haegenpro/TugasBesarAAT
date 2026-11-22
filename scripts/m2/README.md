# scripts/m2

Tooling for the M2 deployment and its evidence. A file name tells which part of the
specification it serves:

| Prefix | Meaning |
| --- | --- |
| `p1_` … `p5_` | belongs to one problem |
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
| `p2_throughput_compare.py` | P2-7 | one-versus-three Aggregator throughput on the same dataset, 10,000 VUs and one capped total request rate for both samples (`--target-rps N` or `auto` pilot); INVALID if one instance already meets the cap |
| `p1_analytics_compare.py` | P1-7 | dashboard numbers versus a direct Audit API count per type, severity and area; verdict on the numbers only (the page itself is manual) |
| `p3_schema_mode.py` | P3-1 | switches the PVMBG mock between `v1`, `compatible` and `breaking` contracts |
| `p3_schema_behavior.py` | P3-1 | `--execute` runs reader v1 with mock v1, compatible and breaking, then activates reader v2, replays and runs breaking again; PASS/FAIL per scenario against the declared design |
| `p3_ops_access_matrix.py` | P3-5 | HTTP status per identity and Internal Ops endpoint, with a verdict that only Internal Ops gets through |
| `p3_field_report_ledger.py` | P3-6 | `submit` records every 202 with its Idempotency-Key, `reconcile` checks receipts and database counts and prints the 202 latency (p50, p95, max) |
| `p3_quarantine_payload.py` | P3-2 | downloads retained original bytes through Internal Ops, recomputes SHA-256 against `X-Content-SHA256`, checks the Audit listing carries no raw field |
| `p3_replay_flow.py` | P3-4 | `--execute` activates the reader version, replays retained messages, repeats the request (must select nothing), compares Audit counts and duplicates |
| `p4_province_reference.py` | P4-1, P4-6 | validates and installs the checksum-pinned province reference |
| `p4_requeue_satellite_updates.py` | P4-3 | one-off migration for updates saved under the earlier retraction rule |
| `p4_processor_kill_check.py` | P4-5 | force-deletes the wildfire processor repeatedly during processing; PASS/FAIL for no regression, resumption, no lost or duplicated identity |
| `p4_reference_compare.py` | P4-6 | compares Audit WILDFIRE events with a reference file from the assistants (needs the file) |
| `p4_ingestion_progress.py` | P4-2, P4-4, P4-7 (helps P4-8) | received/prepared/applied offsets, lag, watermark, late-data counters over time; no verdict |
| `p4_feed_rate.py` | P4-4 sizing (helps P4-7) | polls the live feed's `/v1/session` (read-only) and reports offsets per second at the running speed (60x) and at 1x, average and busiest windows; writes the burst-test input; no verdict |
| `p4_fixture_feed.py` | P4-4 | controlled SSE feed in our event format (steady rate, then a burst of factor x that rate); started in the cluster by `p4_burst_cluster.py` |
| `p4_burst_cluster.py` | P4-4 | runs a 60× fixture burst for 10 s and reports recovery, end-to-end p50/p95/p99 and pair integrity. Removes fixture resources and ingestion overrides (`--restore` after a crash). Test rows remain in the stores |
| `baseline_capture.py` | performance baselines | records revision, images, limits, redacted config, dataset digest and generator CPU; scrapes pipeline metrics around a run; judges final progress against an expected file; `compare` prints two runs |
| `p5_warning_slo_snapshot.py` | P5-1, P5-2, P5-8 | delivery measurements over the whole coverage period plus reconciliation (expected = delivered + rejected + pending, rejected ids listed); PASS/FAIL only when measurable |
| `p5_sensor_fault_probe.py` | P5-3, P5-4 | with `--break-at/--restore-at` scales the sensor down and up itself and judges latency, errors, stale status, breaker opening, request surge and recovery; scrapes every ingestion pod |

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

`manage.py` (init, deploy, up, ready, scale, stop/start, satellite on/off/status), `render.py` and `apps.py`
(manifests), `monitoring.py` (Prometheus, Grafana, Jaeger, Loki, Fluent Bit), `lifecycle.py`,
`images.py`, `minikube.py` and `install_tools.py` (Minikube only), `reset.py`, `access.py`
(browser port-forwards), `tuning.py`, `common.py`, `acceptance_support.py`.

## Tests

```bash
python3 -m unittest discover -s scripts/m2 -p 'test_*.py' -v
```

`test_p2_load_test.py` needs Linux (it exercises the Docker-based generator logic).
