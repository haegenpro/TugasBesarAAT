package store

import (
	"aggregator/internal/analytics"
	"aggregator/internal/canonical"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// Administrator-loaded, already-applied fixture, not a write-throughput claim.
func hardeningRows(t *testing.T, s *Store, n int) {
	t.Helper()
	ctx := context.Background()
	_, err := s.Pool.Exec(ctx, `INSERT INTO canonical_entities(route_key,hazard_id,source,source_ref_id,revision,active,ingested_at)
 SELECT canonical_route('BMKG','hardening-'||i),gen_random_uuid(),'BMKG','hardening-'||i,1,true,'2026-01-01 00:00:00.123456Z' FROM generate_series(1,$1::int) i`, n)
	require.NoError(t, err)
	_, err = s.Pool.Exec(ctx, `INSERT INTO hazard_events(route_key,hazard_id,source,source_ref_id,revision,active,ingested_at,hazard_type,severity,area_name,latitude,longitude,occurred_at,attributes)
 SELECT route_key,hazard_id,source,source_ref_id,revision,active,ingested_at,'SEISMIC','NORMAL',source_ref_id,0,0,'2026-01-01T00:00:00Z','{}' FROM canonical_entities`)
	require.NoError(t, err)
}

type hardeningTx struct {
	pgx.Tx
	conn   *pgx.Conn
	before bool
}

func (t *hardeningTx) Commit(ctx context.Context) error {
	if t.before {
		_ = t.Tx.Rollback(ctx)
		_ = t.conn.Close(ctx)
		return errors.New("connection lost before COMMIT")
	}
	err := t.Tx.Commit(ctx)
	_ = t.conn.Close(context.Background())
	return err
}
func (t *hardeningTx) Rollback(ctx context.Context) error {
	err := t.Tx.Rollback(ctx)
	_ = t.conn.Close(context.Background())
	return err
}

func TestM8HardeningLostReplies(t *testing.T) {
	for _, citus := range []bool{false, true} {
		for _, stage := range []string{"copy-page", "copy-final", "verification", "publication", "before-commit"} {
			t.Run(fmt.Sprintf("citus_%v/%s", citus, stage), func(t *testing.T) {
				s, dsn := m89Lab(t, citus)
				hardeningRows(t, s, 501)
				ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
				defer cancel()
				var once sync.Once
				var injected atomic.Bool
				s.auditTestBegin = func(call context.Context, got string) (pgx.Tx, error) {
					var target bool
					if got == stage || stage == "before-commit" && got == "copy-page" {
						once.Do(func() { target = true })
					}
					if !target {
						return s.Pool.Begin(call)
					}
					injected.Store(true)
					endpoint := dsn
					if stage != "before-commit" {
						proxy, e := newCommitProxy(ctx, dsn)
						if e != nil {
							return nil, e
						}
						t.Cleanup(func() { _ = proxy.Close() })
						endpoint = proxy.DSN
					}
					conn, e := pgx.Connect(call, endpoint)
					if e != nil {
						return nil, e
					}
					tx, e := conn.Begin(call)
					if e != nil {
						_ = conn.Close(call)
						return nil, e
					}
					return &hardeningTx{Tx: tx, conn: conn, before: stage == "before-commit"}, nil
				}
				stop, e := s.ConfigureSnapshots(ctx)
				require.NoError(t, e)
				defer stop()
				capture, e := s.StartSnapshot(ctx, nil)
				require.NoError(t, e)
				m2Eventually(t, 25*time.Second, func() bool {
					v, e := s.snapshotStatus(ctx, capture.ID)
					return e == nil && (v.Status == "ready" || v.Status == "aborted")
				})
				require.True(t, injected.Load(), "targeted destination transaction was not exercised")
				state, e := s.snapshotStatus(ctx, capture.ID)
				require.NoError(t, e)
				if stage == "before-commit" {
					require.Equal(t, "aborted", state.Status)
					return
				}
				require.Equal(t, "ready", state.Status, state.Reason)
				checksum, e := s.SnapshotChecksum(ctx, capture.ID, "")
				require.NoError(t, e)
				require.False(t, checksum.Partial)
				require.Equal(t, int64(501), checksum.Count)
				var count, distinct int
				require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*),count(DISTINCT hazard_id) FROM audit_snapshot_records WHERE capture_id=$1`, capture.ID).Scan(&count, &distinct))
				require.Equal(t, 501, count)
				require.Equal(t, count, distinct)
				var manifests int
				require.NoError(t, s.Pool.QueryRow(ctx, "SELECT count(*) FROM platform_audit_captures").Scan(&manifests))
				require.Equal(t, 1, manifests)
			})
		}
	}
}

type refusedCapture struct {
	snapshotLabDurability
	calls *atomic.Int64
}

func (d refusedCapture) ConfirmCapture(ctx context.Context, id, state string) error {
	d.calls.Add(1)
	if state == "ready" {
		return canonical.Unavailable()
	}
	return d.snapshotLabDurability.ConfirmCapture(ctx, id, state)
}
func TestM8HardeningReadyNeedsDurability(t *testing.T) {
	s, _ := m89Lab(t, false)
	hardeningRows(t, s, 1)
	ctx := context.Background()
	stop, e := s.ConfigureSnapshots(ctx)
	require.NoError(t, e)
	defer stop()
	v, e := s.StartSnapshot(ctx, nil)
	require.NoError(t, e)
	m2Eventually(t, 5*time.Second, func() bool { v, e = s.snapshotStatus(ctx, v.ID); return e == nil && v.Status == "ready" })
	var calls atomic.Int64
	s.Canonical.Durability = refusedCapture{snapshotLabDurability{s: s}, &calls}
	_, e = s.SnapshotStatus(ctx, v.ID)
	require.Error(t, e)
	require.Equal(t, int64(1), calls.Load())
}

func TestM9HardeningAreaAndResponseBudgets(t *testing.T) {
	s, _ := m89Lab(t, true)
	hardeningRows(t, s, 5000)
	ctx := context.Background()
	q := analytics.Query{From: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), Interval: "day"}
	r, e := s.AnalyticsSummary(ctx, q)
	require.NoError(t, e)
	require.True(t, r.DataComplete)
	require.Len(t, r.Areas, 5000)
	require.Equal(t, int64(5000), r.Total)
	b := m2Batch("area-5001", 1, 0)
	m2Apply(t, s, b)
	_, e = s.AnalyticsSummary(ctx, q)
	m2Error(t, e, "analytics_quota")
	_, e = s.Pool.Exec(ctx, `UPDATE hazard_events SET active=false WHERE source_ref_id='area-5001'`)
	require.NoError(t, e)
	_, e = s.Pool.Exec(ctx, `UPDATE hazard_events SET area_name=source_ref_id||repeat('<',100) WHERE active`)
	require.NoError(t, e)
	_, e = s.AnalyticsSummary(ctx, q)
	m2Error(t, e, "analytics_quota") // JSON escapes expand the final encoded response.
	_, e = s.Pool.Exec(ctx, `UPDATE hazard_events SET area_name=source_ref_id WHERE active`)
	require.NoError(t, e)
	r, e = s.AnalyticsSummary(ctx, q)
	require.NoError(t, e)
	require.Equal(t, int64(5000), r.Total)
}

func TestM9HardeningConcurrencyCancellationAndPools(t *testing.T) {
	s, _ := m89Lab(t, true)
	hardeningRows(t, s, 8)
	ctx := context.Background()
	q := analytics.Query{From: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), Interval: "day"}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.analytics.testAfterShard = func(call context.Context) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-call.Done():
			return call.Err()
		}
	}
	call, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { _, e := s.AnalyticsSummary(call, q); done <- e }()
	<-entered
	_, e := s.AnalyticsSummary(ctx, q)
	m2Error(t, e, "overloaded")
	m2Apply(t, s, m2Batch("writer-independent", 1, 0))
	cancel()
	require.Error(t, <-done)
	close(release)
	s.analytics.testAfterShard = nil
	for _, pool := range s.analytics.pools {
		require.Equal(t, int32(2), pool.Config().MaxConns)
		require.LessOrEqual(t, pool.Stat().TotalConns(), int32(2))
	}
	pool := s.analytics.pools[1]
	first, e := pool.Acquire(ctx)
	require.NoError(t, e)
	second, e := pool.Acquire(ctx)
	require.NoError(t, e)
	deadline, stop := context.WithTimeout(ctx, 40*time.Millisecond)
	defer stop()
	_, e = pool.Acquire(deadline)
	require.Error(t, e)
	first.Release()
	second.Release()
	r, e := s.AnalyticsSummary(ctx, q)
	require.NoError(t, e)
	require.Equal(t, int64(9), r.Total)
	stopSnapshots, e := s.ConfigureSnapshots(ctx)
	require.NoError(t, e)
	defer stopSnapshots()
	v, e := s.StartSnapshot(ctx, nil)
	require.NoError(t, e)
	m2Eventually(t, 5*time.Second, func() bool { v, e = s.snapshotStatus(ctx, v.ID); return e == nil && v.Status == "ready" })
	q.Snapshot = v.ID
	r, e = s.AnalyticsSummary(ctx, q)
	require.NoError(t, e)
	require.Equal(t, int64(9), r.Total)
}

func TestM9HardeningExactEncodedBoundary(t *testing.T) {
	r := analytics.Result{Areas: []analytics.Area{{Name: "", Count: 1}}}
	raw, e := json.Marshal(r)
	require.NoError(t, e)
	r.Areas[0].Name = strings.Repeat("x", analytics.MaxResponse-len(raw))
	require.NoError(t, analyticsResponseBudget(r))
	r.Areas[0].Name += "x"
	m2Error(t, analyticsResponseBudget(r), "analytics_quota")

}
