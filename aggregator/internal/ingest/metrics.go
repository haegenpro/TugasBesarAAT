package ingest

import (
	"aggregator/internal/platform"
	"context"
	"github.com/jackc/pgx/v5"
	"time"
)

// Every metric name below is a constant from a small fixed set, so cardinality
// stays bounded. Durations use platform.Observe (seconds, histogram buckets);
// counts and bytes are monotonic counters so a scraper derives averages as
// total/batches and rates from deltas.

// stage measures one pipeline stage: defer stage("name")().
func stage(name string) func() {
	started := time.Now()
	return func() { platform.Observe("ingest_stage_seconds_"+name, time.Since(started).Seconds()) }
}

// begin starts a transaction and records pool acquisition plus BEGIN time. The
// pool's cumulative acquire wait is exported separately by sampleMetrics.
func (s *Store) begin(ctx context.Context) (pgx.Tx, error) {
	started := time.Now()
	tx, e := s.Pool.Begin(ctx)
	platform.Observe("ingest_pool_acquire_seconds", time.Since(started).Seconds())
	return tx, e
}

// commit records COMMIT duration. With synchronous replication this includes the
// wait for the standby, so it is also the replication-wait signal per commit.
func commit(ctx context.Context, tx pgx.Tx, name string) error {
	started := time.Now()
	e := tx.Commit(ctx)
	d := time.Since(started).Seconds()
	platform.Observe("ingest_commit_seconds", d)
	platform.Observe("ingest_commit_seconds_"+name, d)
	if e != nil {
		platform.Add("ingest_commit_failures_total_"+name, 1)
	}
	return e
}

// lockWait records how long a row/advisory lock acquisition took.
func lockWait(name string, started time.Time) {
	platform.Observe("ingest_lock_wait_seconds_"+name, time.Since(started).Seconds())
}

// batchStats records one batch of work for a stage.
func batchStats(name string, records int, payloadBytes int) {
	platform.Add("ingest_batches_total_"+name, 1)
	platform.Add("ingest_batch_records_total_"+name, float64(records))
	platform.Add("ingest_batch_bytes_total_"+name, float64(payloadBytes))
}

// RunMetrics samples the pool until ctx ends, and with backlog also the database-wide backlog and
// PostgreSQL wait state. The database-wide numbers are the same from every process, so only the
// ingestion processes sample them; every worker querying them as well only loads the database.
func (s *Store) RunMetrics(ctx context.Context, backlog bool) {
	interval := platform.Duration("INGEST_METRICS_INTERVAL", 5*time.Second)
	for {
		s.samplePool()
		if backlog {
			s.sampleBacklog(ctx)
		}
		if !Sleep(ctx, interval) {
			return
		}
	}
}

func (s *Store) sampleMetrics(ctx context.Context) {
	s.samplePool()
	s.sampleBacklog(ctx)
}

func (s *Store) samplePool() {
	st := s.Pool.Stat()
	platform.Gauge("ingest_pool_acquire_total", float64(st.AcquireCount()))
	platform.Gauge("ingest_pool_acquire_wait_seconds_total", st.AcquireDuration().Seconds())
	platform.Gauge("ingest_pool_empty_acquire_total", float64(st.EmptyAcquireCount()))
	platform.Gauge("ingest_pool_canceled_acquire_total", float64(st.CanceledAcquireCount()))
	platform.Gauge("ingest_pool_acquired_conns", float64(st.AcquiredConns()))
	platform.Gauge("ingest_pool_max_conns", float64(st.MaxConns()))
}

func (s *Store) sampleBacklog(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	// Backlog: how many rows wait at each hand-off and how old the oldest is.
	for _, q := range []struct{ name, count, oldest string }{
		// The oldest entries are read through the claim and pending indexes (the outbox row carries
		// its input's received_at; within a source, received_at follows position), not by scanning.
		{"raw_outbox", `SELECT count(*) FROM raw_outbox`, `SELECT coalesce(extract(epoch FROM now()-min((SELECT o.received_at FROM raw_outbox o WHERE o.priority=p AND o.received_at IS NOT NULL ORDER BY o.received_at LIMIT 1))),0) FROM generate_series(0,3) p`},
		{"unprepared", `SELECT count(*) FROM raw_inputs WHERE state='pending'`, `SELECT coalesce(extract(epoch FROM now()-min((SELECT i.received_at FROM raw_inputs i WHERE i.source=p.source AND i.namespace=p.namespace AND i.state='pending' ORDER BY i.position LIMIT 1))),0) FROM source_progress p`},
		{"canonical_outbox", `SELECT count(*) FROM canonical_outbox`, `SELECT coalesce(extract(epoch FROM now()-min(created_at)),0) FROM canonical_outbox`},
	} {
		var n int64
		var age float64
		if s.Pool.QueryRow(ctx, q.count).Scan(&n) == nil {
			platform.Gauge("ingest_backlog_"+q.name, float64(n))
		}
		if s.Pool.QueryRow(ctx, q.oldest).Scan(&age) == nil {
			platform.Gauge("ingest_backlog_oldest_age_seconds_"+q.name, age)
		}
	}
	// PostgreSQL waits: sessions blocked on synchronous replication or on locks,
	// and the worst standby replay lag when this node is the primary.
	var syncRep, locks int64
	if s.Pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE wait_event='SyncRep'),count(*) FILTER (WHERE wait_event_type='Lock') FROM pg_stat_activity WHERE datname=current_database()`).Scan(&syncRep, &locks) == nil {
		platform.Gauge("ingest_pg_sessions_waiting_syncrep", float64(syncRep))
		platform.Gauge("ingest_pg_sessions_waiting_lock", float64(locks))
	}
	var lag float64
	if s.Pool.QueryRow(ctx, `SELECT coalesce(max(extract(epoch FROM replay_lag)),0) FROM pg_stat_replication`).Scan(&lag) == nil {
		platform.Gauge("ingest_pg_replication_replay_lag_seconds", lag)
	}
}
