# M1 preparation verification — 9 November 2026

The preparation refactor was verified with fresh schemas and a disposable Compose project named `aat-m1-prep-check`. Auth, Dashboard and Notifier now own separate PostgreSQL databases; the source history archives remain SQLite. This is M1 preparation, not an implementation of M2.

## Results

| Check | Result |
| --- | --- |
| All ten Go modules with race detection | Pass |
| Real PostgreSQL Auth/consumer integration tests | Pass; isolated schemas and independent connection pools |
| Canonical store/replay/pagination integration tests | Pass against a dedicated initialized database |
| NATS/Registry gated integration tests | Pass for Aggregator, broker and both consumers, with race detection |
| `go vet` across all modules | Pass |
| JavaScript regression tests | 22 passed |
| Clean Compose build/start, including monitoring and optional subscriber | Pass |
| Institution credentials, Media projection/raw denial, Field/Internal access, refresh/logout | Pass |
| Session persistence after Auth restart | Pass |
| Concurrent refresh across independent Auth instances | Exactly one successful rotation |
| Concurrent consumer updates and duplicate delivery | Highest revision retained; inbox/event identities unique |
| Stale recovery request versus a new identical halt | New halt preserved; stale resume rejected atomically |
| Raw invalid UTF-8/truncated/oversized source responses | Original bytes/prefix preserved with failure classification |
| Pagination | Bounded pages, stable tie ordering, unique records, invalid/filter-changed cursors rejected |
| Runtime database privileges | No superuser, role/database creation or schema creation; maintenance database connections denied |
| Application metrics | Available from all seven HTTP services |
| Request trace | Client API → Auth/Aggregator → PostgreSQL in Tempo |
| Message trace | BMKG → Aggregator/outbox → Dashboard and Notifier in one stored trace |
| Source/canonical mapping | Pass |
| Runtime schema evolution | Pass with no container restart or database column change |
| Old/new dynamic attributes | Coexist in JSONB; current confidence matches the source |
| Independent consumer/replay | Dashboard continues while Notifier stops; Notifier catches up |
| Third subscriber | Own PostgreSQL store and durable consumer; same new hazard reaches all three |
| PVMBG outage | Ten-second outage; cached/stale reads preserved, new report recovered without restarts |
| Formatting, Python compilation and current documentation links | Pass |

## Sustained workload

The existing M1 k6 workload ran 50 virtual users for 60 seconds:

| Measurement | Value |
| --- | --- |
| Successful hazard reads | 38,153 |
| Successful hazard throughput | 635.41/s |
| Successful-read p50 / p95 / p99 | 21.30 / 56.03 / 79.27 ms |
| Controlled HTTP 429 rejections | 1,182 |
| Unexpected errors | 0 |
| Unexpected error rate | 0% |
| Load thresholds | Pass |

These are local development measurements, not M2's 10,000-VU scaling evidence. The short outage is a regression check, not the full ten-to-twenty-minute M1 demonstration.

## Evidence and reproduction

Local, gitignored artifacts are under `evidence/preparation-*`, including [k6 output](../evidence/preparation-load.log), [race tests](../evidence/preparation-go-race.log), [infrastructure tests](../evidence/preparation-infrastructure.log), [end-to-end checks](../evidence/preparation-end-to-end.log), and [outage recovery](../evidence/preparation-outage.log). The load thresholds passed and the text summary was captured; k6 could not write its optional JSON summary because its container user lacked permission on the evidence mount.

Use the [README](../README.md) and [runbook](Test%20Runbook.md) to reproduce. `scripts/test-services.py` accepts `--race` and `--vet`. Integration tests require the documented dedicated test URLs. `scripts/verify-preparation.py --project <project>` checks a running disposable stack and intentionally restarts Auth/consumers; `--read-only` skips those restarts.

Race suites were run separately from Docker image compilation. A contended run hit an intentional halt-write deadline; the final suites pass without concurrent compilation, and advisory locks are scoped by schema to isolate unrelated test schemas.

## Limits

No old persisted data is migrated. Canonical and supporting PostgreSQL instances remain unreplicated, and the broker remains R1. API worker switches do not implement leader election/fencing. Partial-read fields are groundwork; an unavailable single canonical store still returns 503. Failed-ingestion byte storage is groundwork, not the full M2 inspection/ordered replay workflow. No Kubernetes, sharding, satellite/coastal integration, Field write endpoint, or M2 Audit API is included.

If storage is unavailable when a consumer tries to persist a halt, it keeps a sticky in-memory halt and remains unready. That flag cannot be guaranteed across process loss until persistence succeeds; the triggering unacknowledged message remains eligible for redelivery. Replication and broader node-loss guarantees remain M2 work.
