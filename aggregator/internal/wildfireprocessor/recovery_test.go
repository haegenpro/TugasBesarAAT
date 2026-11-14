package wildfireprocessor

import (
	"aggregator/internal/canonical"
	"aggregator/internal/ownership"
	"aggregator/internal/rawinput"
	"aggregator/internal/store"
	"aggregator/internal/wildfire"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func oracle(t *testing.T, c Config) *wildfire.Model {
	t.Helper()
	m, err := wildfire.New(wildfire.Config{Version: 1, AlgorithmVersion: 2, Namespace: c.Namespace, InitialOffset: c.InitialOffset, AllowedLatenessSeconds: c.AllowedLatenessSeconds, ProvinceJSON: c.ProvinceJSON, ProvinceSHA256: c.ProvinceSHA256})
	require.NoError(t, err)
	return m
}
func applyBoth(t *testing.T, l lab, m *wildfire.Model, op wildfire.Operation) {
	t.Helper()
	l.append(t, op)
	_, err := m.Apply(op)
	require.NoError(t, err)
	l.prepare(t, op.Offset)
	l.deliver(t, op.Offset)
	compareOracle(t, l, m)
}

func TestM6LateCorrectionsTombstonesAndDuplicates(t *testing.T) {
	l := databaseLab(t, true)
	m := oracle(t, l.e.Config)
	ops := fixtureOperations(t, "bridge")
	for _, op := range ops {
		applyBoth(t, l, m, op)
	}
	pos := int64(len(ops))
	clock := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	applyBoth(t, l, m, wildfire.Operation{Version: 1, Namespace: l.e.Config.Namespace, Offset: pos + 1, Kind: "heartbeat", VirtualNow: &clock, Payload: []byte("clock")})
	pos++
	original := *ops[0].Detection
	early := original
	early.Satellite = "N21"
	early.AcqDate = "2026-11-11"
	early.AcqTime = "2359"
	early.Longitude = "0.001"
	applyBoth(t, l, m, wildfire.Operation{Version: 1, Namespace: l.e.Config.Namespace, Offset: pos + 1, Kind: "detection", Detection: &early, Payload: []byte{0, 255}})
	pos++
	var idsBefore []string
	for _, f := range m.Snapshot().Fires {
		idsBefore = append(idsBefore, f.ID)
	}
	// Material and coordinate-changing updates retain original lexical identity
	// history and are classified against the persisted pre-operation watermark.
	replacement := original
	replacement.Longitude = "0.003"
	replacement.FRPMW = 99
	old, _ := wildfire.ParseDetection(original, 1, nil)
	applyBoth(t, l, m, wildfire.Operation{Version: 1, Namespace: l.e.Config.Namespace, Offset: pos + 1, Kind: "update", TargetKey: old.Key, Detection: &replacement, Payload: []byte("replacement")})
	pos++
	updated, _ := wildfire.ParseDetection(replacement, pos, nil)
	applyBoth(t, l, m, wildfire.Operation{Version: 1, Namespace: l.e.Config.Namespace, Offset: pos + 1, Kind: "retract", TargetKey: updated.Key, Payload: []byte("retract")})
	pos++
	applyBoth(t, l, m, wildfire.Operation{Version: 1, Namespace: l.e.Config.Namespace, Offset: pos + 1, Kind: "detection", Detection: &replacement, Payload: []byte("restore")})
	pos++
	duplicate := envelope(t, wildfire.Operation{Version: 1, Namespace: l.e.Config.Namespace, Offset: pos, Kind: "detection", Detection: &replacement, Payload: []byte("restore")})
	receipt, err := l.s.AppendHistory(context.Background(), rawinput.AppendRequest{Version: 1, Ownership: l.p, Messages: []rawinput.Envelope{duplicate}})
	require.NoError(t, err)
	require.Equal(t, "duplicate", receipt.Items[0].Outcome)
	wrong := duplicate
	wrong.Payload = append(append([]byte{}, wrong.Payload...), byte(' '))
	wrong.Size = int64(len(wrong.Payload))
	wrong.Digest = rawinput.Hash(wrong.Payload)
	_, err = l.s.AppendHistory(context.Background(), rawinput.AppendRequest{Version: 1, Ownership: l.p, Messages: []rawinput.Envelope{wrong}})
	require.Error(t, err)
	var late int64
	require.NoError(t, l.c.QueryRow(context.Background(), `SELECT late_count FROM wildfire_runs`).Scan(&late))
	require.GreaterOrEqual(t, late, int64(4))
	require.NotEmpty(t, idsBefore)
	// Backward clock samples do not regress the durable watermark.
	backward := clock.Add(-24 * time.Hour)
	applyBoth(t, l, m, wildfire.Operation{Version: 1, Namespace: l.e.Config.Namespace, Offset: pos + 1, Kind: "heartbeat", VirtualNow: &backward, Payload: nil})
	var durable time.Time
	require.NoError(t, l.c.QueryRow(context.Background(), `SELECT watermark FROM wildfire_runs`).Scan(&durable))
	require.True(t, durable.Equal(*m.Snapshot().Watermark))
}

func TestM6QuotaAndConfigurationFailureRetainObligations(t *testing.T) {
	l := databaseLab(t, false)
	ops := fixtureOperations(t, "bridge")[:2]
	l.append(t, ops[0])
	l.prepare(t, 1)
	l.deliver(t, 1)
	l.append(t, ops[1])
	small := l.e.Config
	small.StagingBytes = 4096
	var err error
	l.e, err = New(l.s, small)
	require.NoError(t, err)
	// Force the persisted staging budget near its ceiling after a real work step.
	_, err = l.e.Step(context.Background(), l.p)
	require.NoError(t, err)
	_, err = l.c.Exec(context.Background(), `UPDATE wildfire_jobs SET staged_bytes=4090 WHERE position=2`)
	require.NoError(t, err)
	_, err = l.e.Step(context.Background(), l.p)
	// The next checkpoint may advance an empty phase; keep stepping until it
	// needs durable queue/graph bytes, never treating a quota as a completed input.
	for i := 0; err == nil && i < 20; i++ {
		_, err = l.e.Step(context.Background(), l.p)
	}
	var pending *Pending
	require.ErrorAs(t, err, &pending)
	var received, prepared int64
	require.NoError(t, l.c.QueryRow(context.Background(), `SELECT received_through,prepared_through FROM input_sessions WHERE source='SATELIT'`).Scan(&received, &prepared))
	require.Equal(t, int64(2), received)
	require.Equal(t, int64(1), prepared)
	// Semantic changes are never accepted as a restart with different lateness.
	correct := fixtureConfig(t)
	l.e, err = New(l.s, correct)
	require.NoError(t, err)
	l.prepare(t, 2)
	l.deliver(t, 2)
	changed := correct
	late := int64(5)
	changed.AllowedLatenessSeconds = &late
	bad, err := New(l.s, changed)
	require.NoError(t, err)
	require.ErrorContains(t, bad.Check(context.Background()), "configuration mismatch")
	_, err = bad.Step(context.Background(), l.p)
	require.ErrorContains(t, err, "configuration mismatch")
}

func TestM6LostCommitReplyRetriesImmutableBatch(t *testing.T) {
	l := databaseLab(t, true)
	ops := fixtureOperations(t, "bridge")[:2]
	for _, op := range ops {
		l.append(t, op)
		l.prepare(t, op.Offset)
		if op.Offset == 1 {
			l.deliver(t, 1)
		}
	}
	_, err := l.e.StepDelivery(context.Background(), l.p, l.d)
	require.NoError(t, err) // seal
	var before string
	require.NoError(t, l.c.QueryRow(context.Background(), `SELECT output_sha256 FROM wildfire_jobs WHERE position=2`).Scan(&before))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proxy, err := newCommitProxy(ctx, l.c.Config().ConnString())
	require.NoError(t, err)
	defer proxy.Close()
	original := l.s.Pool
	proxied, err := pgxpool.New(ctx, proxy.DSN)
	require.NoError(t, err)
	defer proxied.Close()
	l.s.Pool = proxied
	_, err = l.e.StepDelivery(ctx, l.p, l.d)
	l.s.Pool = original
	require.Error(t, err)
	select {
	case <-proxy.Dropped:
	case <-time.After(3 * time.Second):
		t.Fatal("real COMMIT reply was not intercepted")
	}
	called := false
	_, err = l.s.DeliverNext(ctx, func(context.Context, store.OutboxEvent) error { called = true; return nil })
	require.Error(t, err)
	require.False(t, called)
	require.NoError(t, l.s.RecoverAttempts(ctx))
	l.deliver(t, 2)
	var operations, revision, intents int
	require.NoError(t, l.c.QueryRow(ctx, `SELECT (SELECT count(*) FROM canonical_operations),(SELECT revision FROM hazard_events),(SELECT count(*) FROM hazard_outbox)`).Scan(&operations, &revision, &intents))
	require.Equal(t, 1, operations)
	require.Equal(t, 1, revision)
	require.Equal(t, 1, intents)
	var after string
	require.NoError(t, l.c.QueryRow(ctx, `SELECT output_sha256 FROM wildfire_jobs WHERE position=2`).Scan(&after))
	require.Equal(t, before, after)
}

func TestM6CrashHelper(t *testing.T) {
	if os.Getenv("WF_CRASH_CHILD") != "1" {
		t.Skip("explicitly launched subprocess helper")
	}
	ctx := context.Background()
	s, err := store.New(ctx, os.Getenv("WF_CHILD_DSN"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	defer s.Close()
	s.Canonical = store.CanonicalOptions{Durability: localDurability{s}, MultiEntityValidated: true}
	proc, err := ownership.NewProcessor(s.Pool, 15*time.Second)
	require.NoError(t, err)
	s.Processor = proc
	delivery, err := ownership.New(s.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	s.Canonical.Fence = delivery
	e, err := New(s, fixtureConfig(t))
	require.NoError(t, err)
	var p, d canonical.Ownership
	require.NoError(t, json.Unmarshal([]byte(os.Getenv("WF_CHILD_PROCESSOR")), &p))
	require.NoError(t, json.Unmarshal([]byte(os.Getenv("WF_CHILD_DELIVERY")), &d))
	e.Fault = func(call context.Context, point string) error {
		if point != os.Getenv("WF_CHILD_POINT") {
			return nil
		}
		if want := os.Getenv("WF_CHILD_PHASE"); want != "" {
			var phase string
			if err := s.Pool.QueryRow(call, `SELECT phase FROM wildfire_jobs WHERE namespace=$1 ORDER BY position DESC LIMIT 1`, e.Config.Namespace).Scan(&phase); err != nil || phase != want {
				return nil
			}
		}
		require.NoError(t, os.WriteFile(os.Getenv("WF_CHILD_SIGNAL"), []byte(point), 0600))
		select {}
	}
	for {
		if os.Getenv("WF_CHILD_DELIVER") == "1" {
			_, err = e.StepDelivery(ctx, p, d)
		} else {
			_, err = e.Step(ctx, p)
		}
		require.NoError(t, err)
	}
}

func TestM6KilledWorkerResumesStagesAndAppliedOutcome(t *testing.T) {
	for _, point := range []string{"staged", "activated", "canonical"} {
		t.Run(point, func(t *testing.T) {
			l := databaseLab(t, true)
			ops := fixtureOperations(t, "bridge")[:2]
			l.append(t, ops[0])
			l.prepare(t, 1)
			l.deliver(t, 1)
			l.append(t, ops[1])
			phase, hook := "graph", "after_preparation_commit"
			deliver := false
			if point == "activated" {
				phase = "active"
			}
			if point == "canonical" {
				phase = ""
				hook = "after_canonical_apply"
				deliver = true
				l.prepare(t, 2)
				_, err := l.e.StepDelivery(context.Background(), l.p, l.d)
				require.NoError(t, err)
			}
			signal := filepath.Join(t.TempDir(), "fault-point")
			p, _ := json.Marshal(l.p)
			d, _ := json.Marshal(l.d)
			cmd := exec.Command(os.Args[0], "-test.run=^TestM6CrashHelper$", "-test.timeout=90s")
			cmd.Env = append(os.Environ(), "WF_CRASH_CHILD=1", "WF_CHILD_DSN="+l.c.Config().ConnString(), "WF_CHILD_PROCESSOR="+string(p), "WF_CHILD_DELIVERY="+string(d), "WF_CHILD_POINT="+hook, "WF_CHILD_PHASE="+phase, "WF_CHILD_SIGNAL="+signal, fmt.Sprintf("WF_CHILD_DELIVER=%d", boolInt(deliver)))
			output := &bytes.Buffer{}
			cmd.Stdout = output
			cmd.Stderr = output
			require.NoError(t, cmd.Start())
			defer func() { _ = cmd.Process.Kill() }()
			deadline := time.Now().Add(20 * time.Second)
			for {
				if _, err := os.Stat(signal); err == nil {
					break
				}
				if time.Now().After(deadline) {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
					t.Fatalf("child did not reach synchronized point: %s", output.String())
				}
				time.Sleep(10 * time.Millisecond)
			}
			require.NoError(t, cmd.Process.Kill())
			require.Error(t, cmd.Wait())
			require.NoError(t, l.processor.Release(context.Background(), l.p))
			lease, err := l.processor.Acquire(context.Background(), canonical.AcquireOwnership{Scope: "processor:SATELIT", Holder: "successor-" + point})
			require.NoError(t, err)
			old := l.p
			l.p = lease.Ownership
			l.e, err = New(l.s, fixtureConfig(t))
			require.NoError(t, err)
			_, err = l.e.Step(context.Background(), old)
			require.Error(t, err)
			l.prepare(t, 2)
			l.deliver(t, 2)
			m := oracle(t, l.e.Config)
			for _, op := range ops {
				_, err = m.Apply(op)
				require.NoError(t, err)
			}
			compareOracle(t, l, m)
			var n int
			require.NoError(t, l.c.QueryRow(context.Background(), `SELECT count(*) FROM canonical_operations`).Scan(&n))
			require.Equal(t, 1, n)
		})
	}
}
