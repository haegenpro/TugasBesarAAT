package wildfireprocessor

import (
	"aggregator/internal/canonical"
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"aggregator/internal/wildfire"
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func stepTo(t *testing.T, l lab, phase string) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		var actual string
		err := l.c.QueryRow(context.Background(), `SELECT phase FROM wildfire_jobs WHERE namespace=$1 ORDER BY position DESC LIMIT 1`, l.e.Config.Namespace).Scan(&actual)
		if err == nil && actual == phase {
			return
		}
		_, err = l.e.Step(context.Background(), l.p)
		require.NoError(t, err)
	}
	t.Fatal("phase not reached", phase)
}
func waitFor(t *testing.T, predicate func() bool) {
	t.Helper()
	until := time.Now().Add(1500 * time.Millisecond)
	for !predicate() {
		if time.Now().After(until) {
			t.Fatal("synchronized condition not observed")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestM6ReceiptContinuesDuringLateCorrection(t *testing.T) {
	l := databaseLab(t, true)
	m := oracle(t, l.e.Config)
	ops := fixtureOperations(t, "bridge")[:2]
	for _, op := range ops {
		applyBoth(t, l, m, op)
	}
	now := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	clock := wildfire.Operation{Version: 1, Namespace: l.e.Config.Namespace, Offset: 3, Kind: "heartbeat", VirtualNow: &now}
	applyBoth(t, l, m, clock)
	d := *ops[0].Detection
	d.FRPMW = 70
	old, _ := wildfire.ParseDetection(d, 1, nil)
	late := wildfire.Operation{Version: 1, Namespace: l.e.Config.Namespace, Offset: 4, Kind: "update", TargetKey: old.Key, Detection: &d, Payload: []byte("late")}
	l.append(t, late)
	stepTo(t, l, "activate")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	l.e.Fault = func(ctx context.Context, point string) error {
		if point == "before_activation" {
			once.Do(func() { close(entered) })
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { _, err := l.e.Step(context.Background(), l.p); done <- err }()
	<-entered
	next := wildfire.Operation{Version: 1, Namespace: l.e.Config.Namespace, Offset: 5, Kind: "heartbeat", Payload: []byte("live-receipt")}
	started := time.Now()
	l.append(t, next)
	elapsed := time.Since(started)
	require.Less(t, elapsed, canonical.Deadline)
	close(release)
	require.NoError(t, <-done)
	l.e.Fault = nil
	_, err := m.Apply(late)
	require.NoError(t, err)
	_, err = m.Apply(next)
	require.NoError(t, err)
	l.prepare(t, 5)
	blocked := errors.New("fixture canonical delivery paused")
	l.e.Fault = func(_ context.Context, point string) error {
		if point == "before_canonical_apply" {
			return blocked
		}
		return nil
	}
	for i := 0; i < 5; i++ {
		_, err = l.e.StepDelivery(context.Background(), l.p, l.d)
		if err != nil {
			break
		}
	}
	require.ErrorIs(t, err, blocked)
	l.e.Fault = nil
	l.e.metrics(context.Background())
	metrics := httptest.NewRecorder()
	platform.Metrics(metrics, httptest.NewRequest("GET", "/metrics", nil))
	require.Contains(t, metrics.Body.String(), "app_wildfire_correction_pending 1\n")
	require.Contains(t, metrics.Body.String(), "app_wildfire_blocked_output 1\n")
	l.deliver(t, 5)
	l.e.metrics(context.Background())
	metrics = httptest.NewRecorder()
	platform.Metrics(metrics, httptest.NewRequest("GET", "/metrics", nil))
	require.Contains(t, metrics.Body.String(), "app_wildfire_correction_pending 0\n")
	require.Contains(t, metrics.Body.String(), "app_wildfire_blocked_output 0\n")
	compareOracle(t, l, m)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	peak, err := peakResidentBytes()
	require.NoError(t, err)
	require.Less(t, peak, uint64(512<<20), "component test exceeds the Aggregator process memory class")
	evidence := map[string]any{"receipt_during_correction_ms": elapsed.Milliseconds(), "prepared_through": 5, "applied_through": 5, "heap_sample_bytes": mem.HeapAlloc, "runtime_sys_sample_bytes": mem.Sys, "peak_resident_bytes": peak, "scope": "single-copy fixture; process-lifetime peak RSS is not production resource fit or sustained-feed performance"}
	if dir := os.Getenv("M2_EVIDENCE_DIR"); dir != "" {
		b, _ := json.MarshalIndent(evidence, "", "  ")
		require.NoError(t, os.WriteFile(filepath.Join(dir, "wildfire-fairness.json"), b, 0600))
	}
	t.Log(evidence)
}

func TestM6TakeoverCannotPassProtectedCommit(t *testing.T) {
	l := databaseLab(t, false)
	l.append(t, fixtureOperations(t, "bridge")[0])
	ctx := context.Background()
	config, err := pgxpool.ParseConfig(l.c.Config().ConnString())
	require.NoError(t, err)
	config.ConnConfig.RuntimeParams["application_name"] = "wildfire-takeover"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer pool.Close()
	successor, err := ownership.NewProcessor(pool, 15*time.Second)
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	l.e.Fault = func(call context.Context, point string) error {
		if point == "before_preparation_commit" {
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
	committed := make(chan error, 1)
	go func() { _, err := l.e.Step(ctx, l.p); committed <- err }()
	<-entered
	taken := make(chan canonical.Lease, 1)
	takeError := make(chan error, 1)
	go func() {
		if err := successor.Release(ctx, l.p); err != nil {
			takeError <- err
			return
		}
		lease, err := successor.Acquire(ctx, canonical.AcquireOwnership{Scope: "processor:SATELIT", Holder: "protected-successor"})
		if err != nil {
			takeError <- err
			return
		}
		taken <- lease
	}()
	waitFor(t, func() bool {
		var n int
		return l.c.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE application_name='wildfire-takeover' AND wait_event_type='Lock'`).Scan(&n) == nil && n > 0
	})
	select {
	case <-taken:
		t.Fatal("takeover crossed the protected commit")
	default:
	}
	close(release)
	require.NoError(t, <-committed)
	l.e.Fault = nil
	var lease canonical.Lease
	select {
	case lease = <-taken:
	case err := <-takeError:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("takeover failed after releasing commit")
	}
	_, err = l.e.Step(ctx, l.p)
	require.Error(t, err)
	l.p = lease.Ownership
	l.prepare(t, 1)
	l.deliver(t, 1)
}

func TestM6PreparedReferenceIntegrityAndUnsupportedQuota(t *testing.T) {
	l := databaseLab(t, true)
	for _, op := range fixtureOperations(t, "bridge")[:2] {
		l.append(t, op)
		l.prepare(t, op.Offset)
		if op.Offset == 1 {
			l.deliver(t, 1)
		}
	}
	_, err := l.e.StepDelivery(context.Background(), l.p, l.d)
	require.NoError(t, err)
	var id, sum string
	var count int64
	require.NoError(t, l.c.QueryRow(context.Background(), `SELECT batch_id,output_sha256,output_count FROM wildfire_jobs WHERE position=2`).Scan(&id, &sum, &count))
	ref := canonical.PreparedReference{ID: id, Count: count, SHA256: sum}
	mutations, err := canonical.ResolvePrepared(context.Background(), l.e, l.e.Config.PreparedBytes, ref)
	require.NoError(t, err)
	require.Len(t, mutations, 1)
	wrong := ref
	wrong.ID = "../../arbitrary"
	_, err = canonical.ResolvePrepared(context.Background(), l.e, l.e.Config.PreparedBytes, wrong)
	require.Error(t, err)
	_, err = canonical.ResolvePrepared(context.Background(), l.e, 1, ref)
	require.Error(t, err)
	_, err = l.c.Exec(context.Background(), `UPDATE wildfire_output_chunks SET sha256='corrupt' WHERE position=2`)
	require.NoError(t, err)
	_, err = l.e.StepDelivery(context.Background(), l.p, l.d)
	require.Error(t, err)
	var applied int64
	require.NoError(t, l.c.QueryRow(context.Background(), `SELECT applied_through FROM wildfire_runs`).Scan(&applied))
	require.Equal(t, int64(1), applied)
	// Restore the exact immutable fixture chunk digest, not a different request.
	rows, err := l.c.Query(context.Background(), `SELECT route_key,ordinal,payload FROM wildfire_output_chunks WHERE position=2`)
	require.NoError(t, err)
	type chunk struct {
		route   string
		ordinal int
		b       []byte
	}
	chunks := []chunk{}
	for rows.Next() {
		var c chunk
		require.NoError(t, rows.Scan(&c.route, &c.ordinal, &c.b))
		chunks = append(chunks, c)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	for _, c := range chunks {
		_, err = l.c.Exec(context.Background(), `UPDATE wildfire_output_chunks SET sha256=$1 WHERE route_key=$2 AND namespace=$3 AND position=2 AND ordinal=$4`, rawinput.Hash(c.b), c.route, l.e.Config.Namespace, c.ordinal)
		require.NoError(t, err)
	}
	l.deliver(t, 2)
}

func TestM6InterruptedActivationDoesNotExposeStaging(t *testing.T) {
	l := databaseLab(t, true)
	l.append(t, fixtureOperations(t, "bridge")[0])
	stepTo(t, l, "activate")
	interrupted := errors.New("synchronized pre-activation failure")
	l.e.Fault = func(context.Context, string) error { return interrupted }
	_, err := l.e.Step(context.Background(), l.p)
	require.ErrorIs(t, err, interrupted)
	var prepared int64
	require.NoError(t, l.c.QueryRow(context.Background(), `SELECT prepared_through FROM wildfire_runs`).Scan(&prepared))
	require.Zero(t, prepared)
	l.e.Fault = nil
	l.prepare(t, 1)
	l.deliver(t, 1)
}

func TestM6PreparedCancellationRetainsProgress(t *testing.T) {
	l := databaseLab(t, true)
	l.append(t, fixtureOperations(t, "bridge")[0])
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	worked, err := l.e.Step(cancelled, l.p)
	require.Error(t, err)
	require.False(t, worked)
	var prepared, applied int64
	require.NoError(t, l.c.QueryRow(context.Background(), `SELECT prepared_through,applied_through FROM input_sessions`).Scan(&prepared, &applied))
	require.Zero(t, prepared)
	require.Zero(t, applied)
	l.prepare(t, 1)
	worked, err = l.e.StepDelivery(cancelled, l.p, l.d)
	require.Error(t, err)
	require.False(t, worked)
	require.NoError(t, l.c.QueryRow(context.Background(), `SELECT applied_through FROM input_sessions`).Scan(&applied))
	require.Zero(t, applied)
	l.deliver(t, 1)
}
