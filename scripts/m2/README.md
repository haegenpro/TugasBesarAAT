# Milestone 2 test entry points, by problem

One script per M2 criterion, named `<problem>-<topic>.py`. Each runs the
existing tool in [`scripts/`](../) with the right subcommand and defaults, and
passes every other argument through. Run from the repository root:

```bash
python scripts/m2/p2-takeover.py --help        # criterion, example, then the tool's own options
python scripts/m2/p2-takeover.py --inventory /private/existing.json --scope acquisition:PVMBG ...
```

Exit codes and evidence come from the forwarded tool (demo checks: 0 passed,
1 failed, 2 blocked; evidence under `evidence/`). Full commands and operator
steps: [M2 demo checks](../../docs/m2/Platform-Demo-Checks.md).

| Script | Criterion | What it proves | Runs |
| --- | --- | --- | --- |
| `p1-data-spread.py` | P1.1–1.3 | No node holds every event; load spread; ordered cross-location reads | `platform-demo-check.py distribution` |
| `p1-node-loss.py` | P1.4 | Node killed with its data (Tester Agent); nothing lost, requests still answered | `platform-demo-check.py tester --mode practice` |
| `p1-replace-node.py` | P1.5–1.6 | Node back with an empty disk without stopping reads/writes | `platform-node-replacement.py replace-node` |
| `p1-analytics-dashboard.py` | P1.7 | Analytics dashboard, incomplete-data marker | `check-analytics-browser.py` |
| `p2-takeover.py` | P2.2–2.3 | Leader crash, takeover ≤60 s, exactly once | `platform-demo-check.py takeover` |
| `p2-clock-shift.py` | P2.4 | Clock shift never yields two successful owners | `platform-demo-check.py fencing` |
| `p2-freeze-node.py` | P2.5 | Frozen leader stores nothing after waking | `platform-demo-check.py fencing --freeze-node` |
| `p2-load-scale.py` | P2.6 | 10,000 VU, instances 3 → 5 → 2, errors <1% | `platform-game-day.py run --scenario scale` |
| `p2-throughput.py` | P2.7 | 3 instances ≥2× throughput of 1 | `platform-game-day.py run --scenario baseline` |
| `p2-rollout.py` | P2.8 | Version change during load without failures | `platform-game-day.py run --scenario rollout` |
| `p3-schema-break.py` | P3.1–3.2 | Incompatible PVMBG change parked with reason/source/time | `platform-demo-check.py schema` |
| `p3-malformed.py` | P3.3 | Broken messages refused without crashes | `platform-demo-check.py malformed` |
| `p3-replay.py` | P3.4 | Replay after adapting; no duplicates | `platform-demo-check.py replay` |
| `p3-ops-only.py` | P3.5 | Failed messages visible only to Internal Ops | `platform-demo-check.py ops-only` |
| `p3-field-reports.py` | P3.6 | 202 survives receiver crashes; processed once | `platform-demo-check.py field-reports` |
| `p4-satellite-stream.py` | P4.1–4.4, 4.6, 4.8 | 60× burst catch-up, reconnect, reference comparison | `platform-source-validation.py run --scenario satellite-burst` |
| `p4-processor-crash.py` | P4.5 | Processor crash; final result equals the reference | `platform-demo-check.py processor-crash` |
| `p5-coastal-delivery.py` | P5.1 | Critical coastal events: 0 lost, p95 ≤5 s | `platform-source-validation.py run --scenario reconcile` |
| `p5-sensor-breaker.py` | P5.4 | Unhealthy sensor: no request surge, fast answers | `platform-demo-check.py coastal-breaker` |
| `p5-game-day.py` | P5.6 | Tester Agent exam scenario, recovery without intervention | `platform-demo-check.py tester --mode exam` |
| `bonus-wifi-cut.py` | Bonus | Real Wi-Fi cut between physical hosts | `platform-network.py run` |

Not scripted because they are demonstrations: P2.1 (`kubectl get pods -o wide
-l role=aggregator`), P4.7 (live feed during the demo) and P5.7 (explaining a
failure from the dashboards). Tools that run on a VM's host laptop stay in
`scripts/`: `platform-vm-control.py` and `platform-host-probe.py`. Deployment
and platform tools (`platform-cluster`, `-existing`, `-release`, `-deploy`,
`-storage`, `-databases`, `-maintenance`, …) are not tied to one problem and
also stay in `scripts/`.
