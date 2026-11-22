# Ingestion performance work: measurements and results

The pipeline batches satellite acquisition, ordered wildfire preparation, raw publication
and canonical delivery. The measurements below identify the stage, input and environment
for each recorded run.

## How it is measured

- Local, no cluster: `aggregator/internal/ingest/baseline_test.go` feeds a seeded satellite stream
  (detections, partial updates, retractions of earlier detections, malformed records, twenty
  separated clusters) through the real store and checks the durable fires against the reference
  model. Run these commands from `aggregator/` against a throwaway PostgreSQL:

      M2_BASELINE_EVENTS=1000 M2_BASELINE_OUT=out.json go test ./internal/ingest -run IngestionBaseline -v
      M2_BASELINE_EVENTS=5000 M2_BASELINE_BATCH=100 M2_BASELINE_ACQUIRE_ONLY=1 go test ./internal/ingest -run IngestionBaseline -v

  `M2_BASELINE_BATCH=1` is the batch-size-one comparison mode.
- On the cluster: `scripts/m2/baseline_capture.py capture` records revision, images, replicas, resource
  limits, redacted configuration, dataset digest and generator CPU, scrapes the pipeline metrics
  before, during and after a run, and judges final progress against an expected-result file.
  `compare` prints two captures side by side. Read scaling uses the same tool with `--kind read-scaling`.
- New metrics (all prefixed `app_ingest_`): `stage_seconds_*`, `commit_seconds[_stage]`,
  `pool_acquire_seconds` and cumulative pool counters, `lock_wait_seconds_*` (source progress, lease
  guard, entity advisory), `publish_seconds_*`, `ack_seconds`, `canonical_owner_call_seconds`,
  `canonical_sequence_calls_total`, `canonical_sequence_operations_total`,
  `acquire_to_commit_seconds`, `batches_total_*` / `batch_records_total_*` / `batch_bytes_total_*`,
  `backlog_*` and `backlog_oldest_age_seconds_*` for the raw outbox, unprepared inputs and canonical
  outbox, `ownership_failures_total_*` and `pg_sessions_waiting_syncrep`, `pg_replication_replay_lag_seconds`.

## Step 1: baseline (unchanged pipeline, one record per transaction)

Local, one process, 1000 events, PostgreSQL 16 in Docker (fsync off):

| stage | rate | note |
|---|---|---|
| acquisition | 324 records/s | one transaction and one commit per record |
| preparation | 53 records/s | 18.7 ms per record; commit is only 1.4 ms of it |

Preparation is dominated by work inside the transaction, not by commit count, so batching commits
alone will not fix it. Step 3 has to change what runs per record.
The result file is `docs/m2/perf/ingestion-local-baseline.json`.

## Step 2: batched raw satellite acquisition

`AppendBatch` writes a contiguous run (payloads, metadata, raw outbox entries, resolution of retained
response failures, received frontier) in one transaction. `Append` is now `AppendBatch` with one record.
The SSE reader and the writer are separated by a bounded buffer; batches close at a record count, a
payload byte limit or a wait time (`SATELLITE_BATCH_RECORDS`=100, `SATELLITE_BATCH_BYTES`=2 MiB,
`SATELLITE_BATCH_WAIT_MS`=100). The resume position moves only after commit.

Acquisition of 5000 records, local:

| batch size | fsync off | fsync on |
|---|---|---|
| 1 | 307/s | 230/s |
| 10 | 2351/s | 1948/s |
| 100 | 11116/s | 10070/s |
| 500 | 16077/s | 9921/s |

Default 100: beyond it the gain is small and a failed batch costs more to retry.

Tests (`satellite_session_test.go`): timer flush while the feed is silent, count and byte limits,
disconnect in the middle of a batch, resume from the durable position (second connection asks for the
durable offset, not the reader's), shutdown leaving only whole committed batches, gaps, duplicates,
overlapping retry, conflicting content, mid-write failure rolling the batch back, retained-failure
resolution.

Not yet measured on the cluster (replication wait is part of commit time there).

## Step 3: batched ordered preparation and attempt accounting

`fireBatcher` (wildfire lane) fetches up to `WILDFIRE_BATCH_RECORDS` (25) consecutive inputs, loads them with one
query, records their attempts, prepares them in source order inside one transaction with one savepoint per
input, commits once, and confirms only after the commit. The per-input state transitions are the same code
(`prepareFireIn`). The batch accumulates source progress in memory and flushes it before commit,
shortening the time it holds the progress-row lock. A fire's member rows are updated with one statement.

- Attempts: `NoteAttempts` is the batched form of `NoteAttempt` and keeps its meaning. It commits in its own
  short transaction before processing, so the count survives a rollback or crash and a recorded attempt does
  not mean the input finished.
- Failure isolation: an input that fails rolls back to its savepoint and is an ordering barrier. The inputs
  before it stay committed, it and the rest are redelivered in order, and later inputs are never attempted
  ahead of it.
- Bounded fallback: after a failure of the batch as a whole the next fetch is one input, then the size doubles
  back to the configured value, so one record cannot make the same large batch fail again and again.
  `WILDFIRE_BATCH_RECORDS=1` is the batch-size-one comparison mode.
- Tests: identical durable results (fires, member rows, canonical output, states, frontiers) for batch sizes
  0 (old path), 1, 2, 5 and 1000 on two recorded streams; failure isolation with attempt counts; no
  ownership means nothing changes; batch size fallback.

## Step 4: claimed raw publication

`publish.go` replaces the single-row publisher that held a database transaction open during the broker call.
A short transaction claims due entries (owner, lease epoch, expiry; one indexed read, ordered by priority,
received time, position), the broker is called with stable identities (`record id:replay generation`) without
any transaction, and a short fenced transaction deletes the acknowledged prefix and releases the rest. A claim
is also free as soon as the lease changed hands, because only the current holder can legitimately hold one.
An entry re-queued by a replay while its old generation is in flight keeps its place and is published again
under its new generation.

Knobs: `RAW_PUBLISH_BATCH` (100), `RAW_PUBLISH_RESERVED_BATCH` (50), `RAW_PUBLISH_CLAIM_LEASE` (30s),
`RAW_PUBLISH_ACK_TIMEOUT` (10s), `RAW_PUBLISH_LANES` (1 = one ordered loop; 2 = critical coastal and Field have
their own loop and never queue behind a bulk batch). `RAW_PUBLISH_BATCH=1` is the comparison mode.

Tests (local PostgreSQL and NATS): ordered, batched publication; critical and Field first and separate lane
ranges; crash after claiming; claim expiry; ownership change frees claims and fences the stalled owner while
the broker drops its repeat; replay during a claim; broker failure and partial acknowledgement (only the
prefix completes, the rest is released); a repeat beyond the broker duplicate window is a business no-op
(outcome equals processing once).

## Step 5: claimed canonical delivery

`deliver.go` claims entries in namespace and position order. It sends consecutive operations from one
namespace to Aggregator's authenticated `POST /internal/canonical/sequences`. The owner applies the
sequence in one transaction and checks predecessor order, operation identity and payload digest.
After a lost response or an unavailable owner, the processor retries with the same operation IDs and
payloads. It falls back to ordered `POST /internal/canonical/batches` calls on `409` to identify the
rejected operation, or on `404` for compatibility with an older owner.

After confirmation, one fenced transaction marks the confirmed inputs applied, deletes their outbox
entries and advances each affected source frontier. The worker releases unconfirmed claims for retry
and clears old claims when it takes ownership of the delivery loop. `CANONICAL_DELIVERY_BATCH` defaults
to 50, `CANONICAL_DELIVERY_CLAIM_LEASE` to 60s and `CANONICAL_SEQUENCE_MAX` to 50 (maximum 200).
Set `CANONICAL_SEQUENCE_MAX=1` to compare single-operation owner calls with the same claim batch size.

Tests cover ordered commits and contiguous applied progress, lost responses and idempotent retries,
single-operation fallback to isolate a rejected entry, older owners, and stale or expiring claims.
The sequence counters and `app_ingest_canonical_owner_call_seconds` show the number of operations
delivered together and the cost of each owner call.

## Step 6: confirmation and contention

Confirmations run after commit with bounded concurrency. `WILDFIRE_ACK_CONCURRENCY` defaults to the
configured `WILDFIRE_BATCH_RECORDS` (25 by default). Set it to `1` for serial acknowledgements. A failed
acknowledgement causes redelivery, which the idempotent state check handles without extra canonical
output. Use `app_ingest_lock_wait_seconds_*` and `app_ingest_pg_sessions_waiting_*` to inspect contention.

## Still to do

Collect comparable cluster measurements with replication enabled, then evaluate the failure and
recovery cases in the [acceptance checklist](Acceptance.md). The local baseline reports
single-process rates and the acquisition benchmark measures the raw append stage.

## P4-4 burst test on the cluster

The earlier practice run measured backlog from a feed that was already running at 60× with millions
of historical offsets. The controlled test below records a steady baseline before triggering the burst
and measures recovery from the time the burst ends.

1. Measure the live feed (read-only, about 30 minutes; the group token comes from the private
   configuration and is never printed):

       python3 scripts/m2/p4_feed_rate.py --minutes 30

   It reports offsets per second at the running speed and at 1x (average and the busiest 60, 300 and
   600 virtual seconds) and writes `recommendation` into its `result.json`.
   The normal rate is the busiest 1× rate over 60 virtual seconds, or the 1× average
   when the sample has no full 60-second window. Offsets include any heartbeats with
   an offset, so the recommendation is an upper bound on the detection rate.
2. Small rehearsal, then the real run:

       python3 scripts/m2/p4_burst_cluster.py --normal-rate 2 --seconds 3
       python3 scripts/m2/p4_burst_cluster.py --rate-from .m2-local/evidence-p4-feed-rate-<stamp>/result.json

   The tool installs `p4-fixture` (scripts/m2/p4_fixture_feed.py), seeds the province reference for a
   fresh namespace, points ingestion at the fixture, waits until other satellite namespaces have no
   backlog, warms up at the normal rate, bursts for 10 s at 60x, and judges received and applied back
   within the baseline lag no later than 60 s after the burst ends. It then reports per-event latency
   (fixture availability to raw_inputs.received_at to canonical_batches.applied_at) and checks that every
   pair became exactly one wildfire. The report and offset samples are stored in the run's evidence
   directory. The tool removes the temporary fixture resources and ingestion overrides. Fixture data
   stays in the stores under the test namespace. If the tool was killed, run
   `python3 scripts/m2/p4_burst_cluster.py --restore` with the same cluster configuration.

While it runs, the live feed acquisition is paused (ingestion reads the fixture) and resumes from its
stored progress afterwards. A live backlog that does not drain within `--idle-wait` makes the run
INVALID, because the test inputs would queue behind it in the shared stream and worker.
