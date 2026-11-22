# Ingestion performance work: measurements and results

Work is done in small steps. After each step the same fixed input is measured again and the
result is added here. PostgreSQL replication, JetStream persistence, the guardian policy and the
visibility barrier are unchanged.

## How it is measured

- Local, no cluster: `aggregator/internal/ingest/baseline_test.go` feeds a seeded satellite stream
  (detections, partial updates, retractions of earlier detections, malformed records, twenty
  separated clusters) through the real store and checks the durable fires against the reference
  model. Run it against a throwaway PostgreSQL:

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
(`prepareFireIn`); a fire's member rows are now updated with one statement.

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

`deliver.go` uses the same pattern. A short transaction claims the next entries in namespace and position
order; each is POSTed to the owner with its original operation identity and content, one after the other (the
existing owner API is unchanged); one fenced transaction then marks the confirmed inputs applied, deletes
their outbox entries and advances each source frontier once. The unconfirmed tail is released at once, and all
claims are released when a delivery loop starts. `CANONICAL_DELIVERY_BATCH` (50; 1 = comparison mode) and
`CANONICAL_DELIVERY_CLAIM_LEASE` (60s).

Tests (fake owner with predecessor checks and idempotency): ordered batches and a contiguous applied frontier;
owner commit followed by a lost response (re-sent unchanged, applied once); predecessor rejection completes
nothing; crash after the owner confirmed; stale claim fenced at completion; exclusive and expiring claims.

A multi-operation owner API is deliberately not added yet. Add it only if the cluster metrics show the owner
call (`ingest_canonical_owner_call_seconds`) dominating after local completion is batched.

## Step 6: confirmation and contention

Confirmations after the commit run with bounded concurrency (`WILDFIRE_ACK_CONCURRENCY`, default 1 = the previous
behavior). A confirmation that fails only causes redelivery; the idempotent state check makes it a no-op
(tested: lost confirmations give the same durable result and no extra canonical output). Lock contention is
visible as `ingest_lock_wait_seconds_*` and `ingest_pg_sessions_waiting_*`; source-progress and lease-guard
critical sections already hold only the statements that need them. No coordination split is made without
measurements. Critical coastal processing keeps its own lane and path.

## Still to do

Cluster measurements for steps 2 to 6 against the step 1 baseline, then the integrated acceptance run
(processor crashes, lease takeover, suspension and resumption, partitions, ambiguous acknowledgements, node
and disk loss) and the assignment gates. Defaults are chosen from those runs. Visibility-barrier redesign and
storage-durability tuning stay deferred.

## P4-4 burst test on the cluster

The earlier P4-4 run watched the practice feed, which runs at 60x all the time, while starting
millions of offsets behind. That measures how fast the pipeline reads history, not recovery after a
burst. The burst test below starts from an up-to-date pipeline instead.

1. Measure the live feed (read-only, about 30 minutes; the group token comes from the private
   configuration and is never printed):

       python3 scripts/m2/p4_feed_rate.py --minutes 30

   It reports offsets per second at the running speed and at 1x (average and the busiest 60, 300 and
   600 virtual seconds) and writes `recommendation` (normal = busiest 1x over 60 virtual seconds,
   burst = 60 x normal) into its `result.json`.
2. Small rehearsal, then the real run:

       python3 scripts/m2/p4_burst_cluster.py --normal-rate 2 --seconds 3
       python3 scripts/m2/p4_burst_cluster.py --rate-from .m2-local/evidence-p4-feed-rate-<stamp>/result.json

   The tool installs `p4-fixture` (scripts/m2/p4_fixture_feed.py), seeds the province reference for a
   fresh namespace, points ingestion at the fixture, waits until other satellite namespaces have no
   backlog, warms up at the normal rate, bursts for 10 s at 60x, and judges received and applied back
   within the baseline lag no later than 60 s after the burst ends. It then reports per-event latency
   (fixture availability to raw_inputs.received_at to canonical_batches.applied_at) and checks that every
   pair became exactly one wildfire. Every change is undone at the end; if the tool was killed, run
   `python3 scripts/m2/p4_burst_cluster.py --restore`.

While it runs, the live feed acquisition is paused (ingestion reads the fixture) and resumes from its
stored progress afterwards. A live backlog that does not drain within `--idle-wait` makes the run
INVALID, because the test inputs would queue behind it in the shared stream and worker.
