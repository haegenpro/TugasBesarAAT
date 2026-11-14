package store

import (
	"aggregator/internal/analytics"
	"aggregator/internal/audit"
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/migrations"
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Test-only single-copy provider: no production setting can select this adapter.
type snapshotLabDurability struct {
	labDurability
	s *Store
}

func (d snapshotLabDurability) ConfirmCapture(ctx context.Context, id, state string) error {
	var found bool
	e := d.s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM platform_audit_captures WHERE capture_id=$1 AND state=$2)`, id, state).Scan(&found)
	if e != nil || !found {
		return canonical.Unavailable()
	}
	return nil
}
func (d snapshotLabDurability) ConfirmRecord(ctx context.Context, q string, args ...any) error {
	var exists bool
	if e := d.s.Pool.QueryRow(ctx, q, args...).Scan(&exists); e != nil {
		return e
	}
	if !exists {
		return canonical.Unavailable()
	}
	return nil
}
func m89Lab(t *testing.T, citus bool) (*Store, string) {
	t.Helper()
	dsn, c := m2Database(t, citus)
	target := "postgres"
	if citus {
		target = "citus"
	}
	m2Schema(t, c, target, false)
	s := m2Store(t, dsn)
	m4ReadSetup(t, s, dsn, citus)
	s.Canonical.Durability = snapshotLabDurability{s: s}
	close, e := s.ConfigureAnalytics(context.Background())
	require.NoError(t, e)
	t.Cleanup(close)
	return s, dsn
}
func seedAnalytics(t *testing.T, s *Store) {
	t.Helper()
	start := time.Date(2026, 1, 1, 0, 0, 0, 123456000, time.UTC)
	for i, ref := range []string{"q1", "q2", "volcano", "wild", "coast", "outside"} {
		b := m2Batch(ref, int64(i+1), 0)
		m := &b.Mutations[0]
		m.Event.OccurredAt = start.Add(time.Duration(i) * time.Hour)
		m.Event.AreaName = "A"
		switch i {
		case 1:
			m.Event.Severity = domain.SeveritySiaga
			m.Event.Attributes = map[string]any{"potential_tsunami": true, "warning_id": "w1", "related_event_id": ref}
		case 2:
			m.Source = domain.SourcePVMBG
			m.Event.HazardType = domain.HazardVolcanic
			m.Event.Severity = domain.SeverityNormal
			m.Event.AreaName = "Å"
		case 3:
			m.Source = domain.SourceSatelit
			m.Event.HazardType = domain.HazardWildfire
			m.Event.Severity = domain.SeverityAwas
			m.Event.AreaName = "B"
			m.Event.Attributes = map[string]any{"detection_count": 2, "satellites": []string{"same"}, "max_confidence": "n", "total_frp_mw": 100, "detection_keys": []string{"a", "b"}}
		case 4:
			m.Source = domain.SourceSensorPantai
			m.Event.HazardType = domain.HazardCoastal
			m.Event.Severity = domain.SeveritySiaga
			m.Event.AreaName = "B"
		case 5:
			m.Event.OccurredAt = time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
		}
		m2Apply(t, s, b)
	}
	_, e := s.Pool.Exec(context.Background(), `UPDATE hazard_events SET ingested_at=$1`, start)
	require.NoError(t, e)
}
func analyticsQuery() analytics.Query {
	return analytics.Query{From: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), Interval: "hour"}
}
func TestM9AggregatesAndM8ImmutableCapture(t *testing.T) {
	for _, citus := range []bool{false, true} {
		t.Run(fmt.Sprint(citus), func(t *testing.T) {
			s, _ := m89Lab(t, citus)
			seedAnalytics(t, s)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stopSnapshots, e := s.ConfigureSnapshots(ctx)
			require.NoError(t, e)
			defer stopSnapshots()
			q := analyticsQuery()
			v, e := s.AnalyticsSummary(ctx, q)
			require.NoError(t, e)
			require.Equal(t, int64(5), v.Total)
			require.Equal(t, int64(1), v.Tsunami)
			require.Equal(t, int64(2), v.Types["SEISMIC"])
			require.Equal(t, int64(2), v.Severity["SIAGA"])
			require.Equal(t, []analytics.Area{{Name: "A", Count: 2}, {Name: "B", Count: 2}, {Name: "Å", Count: 1}}, v.Areas)
			require.True(t, v.DataComplete)
			require.Len(t, v.Series, 24)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			s.snapshots.fault = func(call context.Context, point string) error {
				if point == "views_pinned" {
					once.Do(func() { close(entered) })
					select {
					case <-release:
						return nil
					case <-call.Done():
						return call.Err()
					}
				}
				return nil
			}
			session, e := s.StartSnapshot(ctx, nil)
			require.NoError(t, e)
			require.Equal(t, "building", session.Status)
			<-entered
			correction := m2Batch("q1", 99, 1)
			correction.Mutations[0].Event.Severity = domain.SeverityAwas
			correction.Mutations[0].Event.OccurredAt = q.From
			retract := m2Batch("q2", 99, 1).Mutations[0]
			retract.Operation = "retract"
			retract.Event = nil
			correction.Mutations = append(correction.Mutations, retract)
			created := m2Batch("during-copy", 99, 0).Mutations[0]
			created.Event.OccurredAt = q.To.Add(time.Hour)
			correction.Mutations = append(correction.Mutations, created)
			started := time.Now()
			m2Apply(t, s, correction)
			require.Less(t, time.Since(started), canonical.Deadline)
			live, e := s.AnalyticsSummary(ctx, q)
			require.NoError(t, e)
			require.Equal(t, int64(4), live.Total)
			require.Zero(t, live.Tsunami)
			close(release)
			m2Eventually(t, 20*time.Second, func() bool { v, e := s.snapshotStatus(ctx, session.ID); return e == nil && v.Status == "ready" })
			q.Snapshot = session.ID
			frozen, e := s.AnalyticsSummary(ctx, q)
			require.NoError(t, e)
			require.Equal(t, int64(5), frozen.Total)
			require.Equal(t, int64(1), frozen.Tsunami)
			require.Equal(t, "snapshot", frozen.Mode)
			query := audit.Query{Snapshot: session.ID, Limit: 2, Scope: "audit"}
			ids := map[string]bool{}
			for {
				page, e := s.SnapshotEvents(ctx, query)
				require.NoError(t, e)
				require.False(t, page.Partial)
				for _, row := range page.Items {
					require.False(t, ids[row.HazardID])
					ids[row.HazardID] = true
				}
				if page.NextCursor == nil {
					break
				}
				query.After, e = s.Reads.Signer.DecodeSnapshot(*page.NextCursor, "audit/events", query.Scope, query.Filters(), &query.Snapshot)
				require.NoError(t, e)
				_, e = s.Reads.Signer.Decode(*page.NextCursor, "audit/events", query.Scope, query.Filters())
				require.Error(t, e)
			}
			require.Len(t, ids, 6)
			checksum, e := s.SnapshotChecksum(ctx, session.ID, "")
			require.NoError(t, e)
			require.Equal(t, int64(6), checksum.Count)
			require.Equal(t, fmt.Sprintf("%x", sha256.Sum256([]byte("coast\noutside\nq1\nq2\nvolcano\nwild"))), checksum.Checksum)
			// An authorized reader on another instance uses only durable frozen rows.
			second, e := New(ctx, s.Pool.Config().ConnString(), s.logger)
			require.NoError(t, e)
			defer second.Close()
			second.Canonical = s.Canonical
			second.Reads = s.Reads
			stop, e := second.ConfigureSnapshots(ctx)
			require.NoError(t, e)
			defer stop()
			sum, e := second.SnapshotChecksum(ctx, session.ID, "")
			require.NoError(t, e)
			require.Equal(t, checksum.Checksum, sum.Checksum)
			// A correction between aggregate-unit reads invalidates every panel,
			// rather than retaining an old unit contribution from the first attempt.
			var change sync.Once
			queries := 0
			s.analytics.testAfterShard = func(call context.Context) error {
				queries++
				var changed error
				change.Do(func() { _, changed = m2Retry(s, m2Batch("q2", 100, 2)) })
				return changed
			}
			q.Snapshot = ""
			consistent, e := s.AnalyticsSummary(ctx, q)
			require.NoError(t, e)
			require.Equal(t, int64(5), consistent.Total)
			require.Zero(t, consistent.Tsunami)
			require.GreaterOrEqual(t, queries, 2)
			s.analytics.testAfterShard = nil
			_, e = s.Pool.Exec(ctx, `UPDATE platform_audit_captures SET idle_expires_at=clock_timestamp()-interval '1 second' WHERE capture_id=$1`, session.ID)
			require.NoError(t, e)
			_, e = s.SnapshotEvents(ctx, audit.Query{Snapshot: session.ID, Scope: "audit", Limit: 100})
			m2Error(t, e, "cursor_expired")
			for i := 0; i < 3; i++ {
				s.cleanupSnapshots(ctx)
			}
			var rows int
			require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM audit_snapshot_records WHERE capture_id=$1`, session.ID).Scan(&rows))
			require.Zero(t, rows)
		})
	}
}
func TestM8LostViewAndStalePublicationAbort(t *testing.T) {
	s, _ := m89Lab(t, false)
	seedAnalytics(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop, e := s.ConfigureSnapshots(ctx)
	require.NoError(t, e)
	defer stop()
	entered, release := make(chan struct{}), make(chan struct{})
	s.snapshots.fault = func(call context.Context, point string) error {
		if point == "views_pinned" {
			close(entered)
			select {
			case <-release:
			case <-call.Done():
				return call.Err()
			}
		}
		return nil
	}
	v, e := s.StartSnapshot(ctx, nil)
	require.NoError(t, e)
	<-entered
	_, e = s.Pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND state='idle in transaction' AND backend_xmin IS NOT NULL AND pid<>pg_backend_pid()`)
	require.NoError(t, e)
	close(release)
	m2Eventually(t, 10*time.Second, func() bool { v, e := s.snapshotStatus(ctx, v.ID); return e == nil && v.Status == "aborted" })
	require.Error(t, s.PublishCapture(ctx, CaptureToken{ID: v.ID, Epoch: 1, Holder: "stale"}, func(context.Context, pgx.Tx) error { return nil }))
}
func TestM9M8MissingWorkerAndContinuation(t *testing.T) {
	if os.Getenv("M2_EVIDENCE_DIR") == "" {
		t.Skip("owned fault controller absent")
	}
	s, dsn := m89Lab(t, true)
	seedAnalytics(t, s)
	ctx := context.Background()
	stop, e := s.ConfigureSnapshots(ctx)
	require.NoError(t, e)
	defer stop()
	v, e := s.StartSnapshot(ctx, nil)
	require.NoError(t, e)
	m2Eventually(t, 20*time.Second, func() bool { v, e := s.snapshotStatus(ctx, v.ID); return e == nil && v.Status == "ready" })
	m2Fault(t, "stop", "worker2")
	defer m2Fault(t, "start", "worker2")
	call, cancel := context.WithTimeout(ctx, 2*time.Second)
	page, e := s.SnapshotEvents(call, audit.Query{Snapshot: v.ID, Scope: "audit", Limit: 100})
	cancel()
	require.NoError(t, e)
	require.True(t, page.Partial)
	require.Empty(t, page.Items)
	require.NotNil(t, page.NextCursor)
	require.Contains(t, page.Unavailable, "worker2")
	q := analyticsQuery()
	result, e := s.AnalyticsSummary(ctx, q)
	require.NoError(t, e)
	require.False(t, result.DataComplete)
	require.True(t, result.Partial)
	require.Less(t, result.Total, int64(5))
	sum, e := s.SnapshotChecksum(ctx, v.ID, "")
	require.NoError(t, e)
	require.True(t, sum.Partial)
	require.Less(t, sum.Count, int64(6))
	m2Fault(t, "start", "worker2")
	u, _ := url.Parse(os.Getenv("M2_WORKER2_URL"))
	db, _ := url.Parse(dsn)
	u.Path = db.Path
	config, e := pgxpool.ParseConfig(u.String())
	require.NoError(t, e)
	config.MaxConns = 2
	p, e := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, e)
	defer p.Close()
	s.snapshots.reads[2].Close()
	s.snapshots.reads[2] = p
	pos, e := s.Reads.Signer.DecodeSnapshot(*page.NextCursor, "audit/events", "audit", audit.Query{}.Filters(), &v.ID)
	require.NoError(t, e)
	page, e = s.SnapshotEvents(ctx, audit.Query{Snapshot: v.ID, Scope: "audit", Limit: 100, After: pos})
	attempts := 1
	started := time.Now()
	m2Eventually(t, 20*time.Second, func() bool {
		if e == nil && !page.Partial {
			return true
		}
		attempts++
		page, e = s.SnapshotEvents(ctx, audit.Query{Snapshot: v.ID, Scope: "audit", Limit: 100, After: pos})
		return e == nil && !page.Partial
	})
	require.NoError(t, e)
	require.Len(t, page.Items, 6)
	require.Nil(t, page.NextCursor)
	t.Logf("same-position continuation recovered in %s across %d bounded read attempts", time.Since(started), attempts)
}

func TestM8OwnerCancellationAndSourceScope(t *testing.T) {
	s, _ := m89Lab(t, false)
	seedAnalytics(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	stop, e := s.ConfigureSnapshots(ctx)
	require.NoError(t, e)
	entered := make(chan struct{})
	s.snapshots.fault = func(call context.Context, point string) error {
		if point == "views_pinned" {
			close(entered)
			<-call.Done()
			return call.Err()
		}
		return nil
	}
	source := domain.SourceBMKG
	v, e := s.StartSnapshot(context.Background(), &source)
	require.NoError(t, e)
	<-entered
	cancel()
	stop()
	raw, e := s.snapshotStatus(context.Background(), v.ID)
	require.NoError(t, e)
	require.Equal(t, "aborted", raw.Status)
	// A successor aborts the lost cut rather than rebuilding it with new views.
	stop, e = s.ConfigureSnapshots(context.Background())
	require.NoError(t, e)
	defer stop()
	v, e = s.StartSnapshot(context.Background(), &source)
	require.NoError(t, e)
	m2Eventually(t, 10*time.Second, func() bool {
		raw, e := s.snapshotStatus(context.Background(), v.ID)
		return e == nil && raw.Status == "ready"
	})
	var before, after time.Time
	require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT idle_expires_at FROM platform_audit_captures WHERE capture_id=$1`, v.ID).Scan(&before))
	_, e = s.SnapshotChecksum(context.Background(), v.ID, "PVMBG")
	m2Error(t, e, "bad_request")
	require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT idle_expires_at FROM platform_audit_captures WHERE capture_id=$1`, v.ID).Scan(&after))
	require.True(t, before.Equal(after))
	sum, e := s.SnapshotChecksum(context.Background(), v.ID, "")
	require.NoError(t, e)
	require.NotNil(t, sum.Source)
	require.Equal(t, "BMKG", *sum.Source)
	require.Equal(t, int64(3), sum.Count)
}
func TestM8PopulatedUpgradeAndRuntimeGrants(t *testing.T) {
	for _, citus := range []bool{false, true} {
		t.Run(fmt.Sprint(citus), func(t *testing.T) {
			s, _ := m89Lab(t, citus)
			seedAnalytics(t, s)
			ctx := context.Background()
			admin := m2Connection(t, s.Pool.Config().ConnString())
			var before, after string
			fingerprint := `SELECT md5(string_agg(row_to_json(h)::text,'' ORDER BY hazard_id)) FROM hazard_events h`
			require.NoError(t, admin.QueryRow(ctx, fingerprint).Scan(&before))
			_, e := admin.Exec(ctx, `DROP TABLE audit_snapshot_units,audit_snapshot_markers,audit_snapshot_records; DELETE FROM canonical_schema_migrations WHERE version='0010_audit_snapshots.sql'`)
			require.NoError(t, e)
			role := "snapshot_runtime_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			_, e = admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" NOLOGIN")
			require.NoError(t, e)
			target := "postgres"
			if citus {
				target = "citus"
			}
			require.NoError(t, migrations.Up(ctx, admin, filepath.Join("..", "..", "..", "store", "migrations"), target, role))
			require.NoError(t, admin.QueryRow(ctx, fingerprint).Scan(&after))
			require.Equal(t, before, after)
			var pending int
			require.NoError(t, admin.QueryRow(ctx, `SELECT count(*) FROM hazard_outbox`).Scan(&pending))
			require.Equal(t, 6, pending)
			conn := m2Connection(t, s.Pool.Config().ConnString())
			_, e = conn.Exec(ctx, "SET ROLE "+pgx.Identifier{role}.Sanitize())
			require.NoError(t, e)
			_, e = conn.Exec(ctx, `SELECT capture_id FROM audit_snapshot_records LIMIT 0`)
			require.NoError(t, e)
			_, e = conn.Exec(ctx, `CREATE TABLE forbidden_snapshot_ddl(id int)`)
			require.Error(t, e)
			_, e = conn.Exec(ctx, `SELECT * FROM canonical_schema_migrations`)
			require.Error(t, e)
		})
	}
}

func TestM8ByteBoundedSnapshotPages(t *testing.T) {
	s, _ := m89Lab(t, false)
	seedAnalytics(t, s)
	ctx := context.Background()
	_, e := s.Pool.Exec(ctx, `UPDATE hazard_events SET attributes=attributes||jsonb_build_object('synthetic_padding',repeat('x',200000))`)
	require.NoError(t, e)
	stop, e := s.ConfigureSnapshots(ctx)
	require.NoError(t, e)
	defer stop()
	v, e := s.StartSnapshot(ctx, nil)
	require.NoError(t, e)
	m2Eventually(t, 15*time.Second, func() bool { status, e := s.snapshotStatus(ctx, v.ID); return e == nil && status.Status == "ready" })
	sum, e := s.SnapshotChecksum(ctx, v.ID, "")
	require.NoError(t, e)
	require.Equal(t, int64(6), sum.Count)
	units, e := s.snapshotUnits(ctx, v.ID)
	require.NoError(t, e)
	require.Len(t, units, 1)
	require.True(t, units[0].Verified)
	require.Equal(t, int64(6), units[0].Count)
}
