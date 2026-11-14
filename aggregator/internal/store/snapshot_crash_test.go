package store

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestM8CrashHelper(t *testing.T) {
	if os.Getenv("AUDIT_CAPTURE_CHILD") != "1" {
		t.Skip("explicitly launched subprocess only")
	}
	ctx := context.Background()
	s, e := New(ctx, os.Getenv("AUDIT_CAPTURE_DSN"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, e)
	defer s.Close()
	s.Canonical.Durability = snapshotLabDurability{s: s}
	m4ReadSetup(t, s, os.Getenv("AUDIT_CAPTURE_DSN"), false)
	stop, e := s.ConfigureSnapshots(ctx)
	require.NoError(t, e)
	defer stop()
	s.snapshots.fault = func(context.Context, string) error { select {} }
	v, e := s.StartSnapshot(ctx, nil)
	require.NoError(t, e)
	raw, _ := json.Marshal(v)
	require.NoError(t, os.WriteFile(os.Getenv("AUDIT_CAPTURE_SIGNAL"), raw, 0600))
	select {}
}
func TestM8KilledOwnerCleanupDoesNotBlockWrites(t *testing.T) {
	s, dsn := m89Lab(t, false)
	seedAnalytics(t, s)
	signal := filepath.Join(t.TempDir(), "capture.json")
	cmd := exec.Command(os.Args[0], "-test.run=^TestM8CrashHelper$", "-test.timeout=90s")
	cmd.Env = append(os.Environ(), "AUDIT_CAPTURE_CHILD=1", "AUDIT_CAPTURE_DSN="+dsn, "AUDIT_CAPTURE_SIGNAL="+signal)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	deadline := time.Now().Add(10 * time.Second)
	var id string
	for time.Now().Before(deadline) {
		if raw, e := os.ReadFile(signal); e == nil {
			var v struct {
				ID string `json:"snapshot_id"`
			}
			require.NoError(t, json.Unmarshal(raw, &v))
			id = v.ID
			break
		}
		select {
		case e := <-done:
			t.Fatalf("capture child exited before signal: %v", e)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.NotEmpty(t, id)
	require.NoError(t, cmd.Process.Kill())
	require.Error(t, <-done)
	start := time.Now()
	m2Apply(t, s, m2Batch("after-owner-kill", 100, 0))
	require.Less(t, time.Since(start), 2*time.Second)
	deadline = time.Now().Add(20 * time.Second)
	cleaned := false
	for time.Now().Before(deadline) {
		s.cleanupSnapshots(context.Background())
		require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT state='aborted' AND cleaned_at IS NOT NULL FROM platform_audit_captures WHERE capture_id=$1`, id).Scan(&cleaned))
		if cleaned {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	require.True(t, cleaned, "authoritative owner expiry must permit cleanup without resuming owner")
	t.Logf("killed capture owner; canonical write resumed before owner expiry; terminal cleanup=%s", time.Since(start))
}
