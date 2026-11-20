# Milestone 1 scripts (Docker Compose)

Run from the repository root. These tools target the M1 Docker Compose stack;
M2 Kubernetes tools stay in [`scripts/`](../).

| Script | Purpose |
| --- | --- |
| `setup-env.py` / `setup-env.ps1` | Generate consistent local `.env` files from `.env.example` |
| `smoke.sh` / `smoke.ps1` | End-to-end smoke test of the Compose stack |
| `isolation-demo.sh` / `isolation-demo.ps1` | P3.1: institution credentials are isolated (BMKG/PVMBG keys) |
| `load-test.js`, `load-test.sh` / `load-test.ps1` | M1 k6 load test (sustained, mixed) |
| `evidence-probes.mjs` | Evidence probes (mapping, schema, storage, health, trace, outage, secrets) |
| `m1-demo.mjs` | Operator interface for live M1 demos |
| `verify-preparation.py` | Verify a running disposable Compose project (restarts Auth/consumers unless `--read-only`) |
| `demo-common.sh` | Shared helpers sourced by the shell demos |

Tests: `node --test scripts/m1/*.test.mjs`. Procedures are in the
[Test Runbook](../../docs/m1/Test%20Runbook.md) and
[Panduan Demo M1](../../docs/m1/Panduan%20Demo%20M1.md).
