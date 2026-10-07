# Monitoring verification — 2026-10-07

Verified with the current application code in an isolated Compose project,
`aat-monitoring-verify`, using disposable example credentials and separate volumes.
Grafana was explicitly published at `127.0.0.1:23000`; application ports differed
from the README examples. Existing unrelated containers were left running.

## Passed

- Launcher unit tests and Go static checks.
- All four actual wrapper images reject unset required settings; whitespace-only
  and invalid settings reject startup. Multiple missing names are reported together.
- Compose validation and native Prometheus, Loki, and Alloy configuration checks.
- All thirteen configured health/readiness targets returned successful probe samples.
- HTTP correlation ID found in Client API, Auth, and Aggregator logs.
- Event correlation shared by Aggregator and both consumers.
- Every dashboard PromQL and LogQL query executed successfully.
- Infrastructure logs collected; monitoring containers excluded.
- Stopped consumer detected; other consumer and Client API remain available.
- Consumer readiness recovers automatically.
- Alloy outage produces no current endpoint data and failed collector scrape.
- Alloy restart and consumer recreation resume discovery and collection.
- Earlier correlation logs and metrics survive Loki/Prometheus restarts.
- Grafana uses exactly the explicitly configured host binding.
- Dashboard-only notification policy provisioned.
- Sustained consumer failure reaches firing alert state.
- Client API and event delivery continue with all monitoring stopped.

## Evidence IDs

These IDs were found through Grafana's Loki data source during verification:

- HTTP request: `monitoring-evidence-1791367682771152262`
- Event flow: `059ce0a1-4010-472e-9fd8-80ef4918ed37`

## Conditions and limits

The host filesystem was nearly full. Loki 3.7's production WAL protection remained
unchanged; the isolated test container explicitly used
`-ingester.wal-disk-full-threshold=0.99` so log ingestion could be tested. This flag
is absent from the delivered Compose configuration. Free disk space before using
Loki on this host with its normal 90% threshold.

Thirty-day retention was verified in generated/native configuration, not by
waiting thirty days. Earlier log and metric samples survived actual storage
container restarts. No external notification delivery was configured or tested.

The isolated verification containers were removed after the checks; their volumes
were retained. Production `.env` files were not created. Follow the setup steps in
[Monitoring](README.md) and provide explicit values before starting your stack.

## DLQ monitoring extension

The DLQ extension uses the same isolated Compose project and explicit disposable
settings. The exporter is the pinned official `0.20.2` image plus the existing
startup launcher. No production environment files were created.

- Launcher tests cover all newly required variables, aggregate missing-name
  reporting, whitespace-only values, invalid monitoring URLs, and unsafe dashboard
  stream names. Go tests and static checks passed.
- Actual exporter images reject missing/whitespace/non-HTTP monitoring URLs.
  Actual Prometheus/Grafana images report their new required variables when unset.
- Native Prometheus configuration validation and Alloy validation passed.
- All updated dashboard queries executed successfully. Seven alert rules were
  provisioned; actual consumer quarantine and publication failures fired the
  two new log-based alerts with both consumer identities.
- A deliberately corrupt binary event produced exactly one retained record per
  consumer. Broker DLQ size matched Prometheus, and inspection confirmed exact
  original bytes, correlation ID, attempts, and failure reason.
- A temporary capacity limit on the isolated DLQ caused real publication failures.
  Consumers kept originals for retry and successfully quarantined them once the
  limit was removed.
- Exporter outage removed current DLQ/consumer metric results while the Client API,
  valid event processing, and quarantine continued. The exporter alert fired after
  its configured one-minute delay; restarting the exporter restored archive metrics.
- A temporary 20-second DLQ retention and 10-second broker deduplication window
  expired retained records, and Prometheus reported zero without any replay.
  Verification restored retention to 168h and the deduplication window to 2m.
- With all five monitoring services stopped, API readiness, valid event delivery,
  and quarantine still worked. Only the isolated test containers were removed;
  unrelated containers and persisted volumes were retained.

Temporary capacity and retention changes are verification fixtures only, not
changes to the delivered broker configuration. The existing production Loki WAL
protection and muted notification policy remain unchanged.

DLQ evidence correlation ID: `dlq-monitoring-1791369850757908173`. The inspected dashboard record
had `reason_code=corrupt_payload`, `attempts=1`, `original_stream=HAZARDS`,
`stream_sequence=243`, and raw bytes encoded as `Y29ycnVwdC1mcmFtZQD/`. The paired
notifier record shared the same correlation ID. The first corrupt publication
produced two retained entries; later publications raised the archive to six before
test-only expiration reduced it to zero. All dashboard queries and both
consumer-labeled log alerts were checked through Grafana's APIs.
