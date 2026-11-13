package store

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/timeutil"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var faultSequence atomic.Int64

func m2Fault(t *testing.T, action, service string) {
	t.Helper()
	dir := os.Getenv("M2_EVIDENCE_DIR")
	require.NotEmpty(t, dir, "faults require the run-owned harness")
	n := faultSequence.Add(1)
	raw, _ := json.Marshal(map[string]any{"sequence": n, "action": action, "service": service})
	path := filepath.Join(dir, fmt.Sprintf("fault-request-%d.json", n))
	require.NoError(t, os.WriteFile(path+".tmp", raw, 0600))
	require.NoError(t, os.Rename(path+".tmp", path))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		raw, e := os.ReadFile(filepath.Join(dir, fmt.Sprintf("fault-response-%d.json", n)))
		if e == nil {
			var response struct {
				Error string `json:"error"`
				DSN   string `json:"dsn"`
			}
			require.NoError(t, json.Unmarshal(raw, &response))
			require.Empty(t, response.Error)
			// Random loopback bindings may change across Docker stop/start. Refresh
			// subsequent test connections from this run-owned controller response.
			if action == "start" && response.DSN != "" {
				key := map[string]string{"coord": "M2_DATABASE_URL", "worker1": "M2_WORKER1_URL", "worker2": "M2_WORKER2_URL"}[service]
				if key != "" {
					require.NoError(t, os.Setenv(key, response.DSN))
				}
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("fault handshake timed out", action, service)
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func m2Eventually(t *testing.T, limit time.Duration, fn func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	for !fn() {
		select {
		case <-ctx.Done():
			t.Fatal("observable recovery condition timed out")
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func TestM2LostCommitReply(t *testing.T) {
	dsn, c := m2Database(t, true)
	m2Schema(t, c, "citus", false)
	s := m2Store(t, dsn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proxy, e := newCommitProxy(ctx, dsn)
	require.NoError(t, e)
	defer proxy.Close()
	s.Pool.Close()
	s.Pool, e = pgxpool.New(ctx, proxy.DSN)
	require.NoError(t, e)
	b := m2Batch("lost-reply", 1, 0)
	_, e = s.ApplyBatch(ctx, b)
	require.Error(t, e)
	select {
	case <-proxy.Dropped:
	case <-time.After(3 * time.Second):
		t.Fatal("real COMMIT reply was not dropped")
	}
	s.Pool.Close()
	s.Pool, e = pgxpool.New(ctx, dsn)
	require.NoError(t, e)
	require.NoError(t, s.RecoverAttempts(ctx))
	result := m2Apply(t, s, b)
	require.Equal(t, "duplicate", result.Outcome)
	var operations, outbox int
	require.NoError(t, c.QueryRow(ctx, "SELECT (SELECT count(*) FROM canonical_operations),(SELECT count(*) FROM hazard_outbox)").Scan(&operations, &outbox))
	require.Equal(t, 1, operations)
	require.Equal(t, 1, outbox)
	t.Log("actual COMMIT reply dropped; fenced recovery and original identity retry retained one operation and one publication intent")
}
func TestM2InterruptedDistributedCommit(t *testing.T) {
	dsn, c := m2Database(t, true)
	m2Schema(t, c, "citus", false)
	s := m2Store(t, dsn)
	ctx := context.Background()
	b := m2Batch("interrupt-a", 1, 0)
	other := m2Batch("interrupt-b", 1, 0)
	owner := func(ref string) string {
		var n string
		require.NoError(t, c.QueryRow(ctx, "SELECT n.nodename FROM pg_dist_placement p JOIN pg_dist_node n USING(groupid) WHERE p.shardid=get_shard_id_for_distribution_column('hazard_events',canonical_route('BMKG',$1)) AND n.noderole='primary'", ref).Scan(&n))
		return n
	}
	firstOwner := owner(b.Mutations[0].SourceRefID)
	for i := 0; firstOwner == owner(other.Mutations[0].SourceRefID); i++ {
		require.Less(t, i, 100)
		other = m2Batch(fmt.Sprintf("interrupt-b-%d", i), 1, 0)
	}
	b.Mutations = append(b.Mutations, other.Mutations[0])
	tx, e := s.beginMutation(ctx)
	require.NoError(t, e)
	defer tx.Rollback(ctx)
	require.NoError(t, s.Canonical.Fence.Protect(ctx, tx, b.Ownership, b.Mutations))
	require.NoError(t, s.Canonical.Durability.Prepare(ctx, tx))
	result := canonical.Result{BatchID: b.BatchID, Outcome: "applied", Items: []canonical.Item{}}
	for _, m := range b.Mutations {
		item, e := s.applyMutation(ctx, tx, b.BatchID, m)
		require.NoError(t, e)
		result.Items = append(result.Items, item)
	}
	raw, _ := json.Marshal(result)
	fp, _ := canonical.Fingerprint(b.Mutations)
	_, e = tx.Exec(ctx, "INSERT INTO canonical_batches VALUES($1,$2,$3)", b.BatchID, fp, raw)
	require.NoError(t, e)
	paused := owner(other.Mutations[0].SourceRefID)
	m2Fault(t, "pause", paused)
	stillPaused := true
	defer func() {
		if stillPaused {
			m2Fault(t, "unpause", paused)
		}
	}()
	commitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	pid := int32(tx.Conn().PgConn().PID())
	go func() { done <- tx.Commit(commitCtx) }()
	workerKey := "M2_WORKER1_URL"
	if firstOwner == "worker2" {
		workerKey = "M2_WORKER2_URL"
	}
	worker := m2Connection(t, os.Getenv(workerKey))
	m2Eventually(t, 4*time.Second, func() bool {
		var n int
		return worker.QueryRow(ctx, "SELECT count(*) FROM pg_prepared_xacts").Scan(&n) == nil && n > 0
	})
	_, e = c.Exec(ctx, "SELECT pg_catalog.pg_terminate_backend($1::integer)", pid)
	require.NoError(t, e)
	m2Fault(t, "unpause", paused)
	stillPaused = false
	select {
	case e = <-done:
		require.Error(t, e)
	case <-commitCtx.Done():
		t.Fatal("interrupted commit did not return")
	}
	m2Eventually(t, 20*time.Second, func() bool { return s.RecoverAttempts(ctx) == nil })
	replay := m2Apply(t, s, b)
	require.Contains(t, []string{"applied", "duplicate"}, replay.Outcome)
	var entities, ops, intents int
	require.NoError(t, c.QueryRow(ctx, "SELECT (SELECT count(*) FROM canonical_entities),(SELECT count(*) FROM canonical_operations),(SELECT count(*) FROM hazard_outbox)").Scan(&entities, &ops, &intents))
	require.Equal(t, 2, entities)
	require.Equal(t, 2, ops)
	require.Equal(t, 2, intents)
	t.Log("observed participant PREPARE, terminated coordinator backend, recovered decisions, retried original batch without duplicate effects")
}
func TestM2RecoveryAndPublicationFence(t *testing.T) {
	dsn, c := m2Database(t, false)
	m2Schema(t, c, "postgres", false)
	s := m2Store(t, dsn)
	ctx := context.Background()
	tx, e := s.beginMutation(ctx)
	require.NoError(t, e)
	tracked := tx.(*mutationTx)
	id := tracked.id
	// Losing the admission session does not allow recovery to overtake the data
	// transaction's protected attempt row lock.
	_, e = c.Exec(ctx, "SELECT pg_terminate_backend($1)", int32(tracked.admission.Conn().PgConn().PID()))
	require.NoError(t, e)
	require.Error(t, s.RecoverAttempts(ctx))
	var state string
	require.NoError(t, c.QueryRow(ctx, "SELECT state FROM canonical_attempts WHERE attempt_id=$1", id).Scan(&state))
	require.Equal(t, "pending", state)
	// Explicitly abort data work; recovery then establishes an aborted outcome.
	require.NoError(t, tracked.Tx.Rollback(ctx))
	releaseAdmission(tracked.admission)
	tracked.closed = true
	require.NoError(t, s.RecoverAttempts(ctx))
	require.NoError(t, c.QueryRow(ctx, "SELECT state FROM canonical_attempts WHERE attempt_id=$1", id).Scan(&state))
	require.Equal(t, "aborted", state)
	var ignored string
	e = c.QueryRow(ctx, "SELECT state FROM canonical_attempts WHERE attempt_id=$1 AND state='pending'", id).Scan(&ignored)
	require.ErrorIs(t, e, pgx.ErrNoRows)
	// A committed intent remains unpublished if terminal attempt resolution is
	// unavailable. The data commit marker itself is not publication permission.
	tx, e = s.beginMutation(ctx)
	require.NoError(t, e)
	tracked = tx.(*mutationTx)
	batch := m2Batch("publish-pending", 1, 0)
	_, e = s.applyMutation(ctx, tx, batch.BatchID, batch.Mutations[0])
	require.NoError(t, e)
	_, e = tracked.Tx.Exec(ctx, "INSERT INTO canonical_attempt_commits VALUES($1)", tracked.id)
	require.NoError(t, e)
	require.NoError(t, tracked.Tx.Commit(ctx))
	releaseAdmission(tracked.admission)
	tracked.closed = true
	published := 0
	_, e = s.DeliverNext(ctx, func(context.Context, OutboxEvent) error { published++; return nil })
	require.Error(t, e)
	require.Zero(t, published)
	require.NoError(t, s.RecoverAttempts(ctx))
	found, e := s.DeliverNext(ctx, func(context.Context, OutboxEvent) error { published++; return nil })
	require.NoError(t, e)
	require.True(t, found)
	require.Equal(t, 1, published)
}

func TestM2OwnerAndAdmission(t *testing.T) {
	dsn, c := m2Database(t, false)
	m2Schema(t, c, "postgres", false)
	s := m2Store(t, dsn)
	ctx := context.Background()
	b := m2Batch("owner-commit", 1, 0)
	m2Apply(t, s, b)
	tx, e := s.beginMutation(ctx)
	require.NoError(t, e)
	require.NoError(t, labFence{}.Protect(ctx, tx, b.Ownership, b.Mutations))
	contender, e := c.Begin(ctx)
	require.NoError(t, e)
	_, e = contender.Exec(ctx, "SELECT epoch FROM m2_authority WHERE scope='fixture' FOR UPDATE NOWAIT")
	require.Error(t, e)
	require.NoError(t, contender.Rollback(ctx))
	require.NoError(t, tx.Commit(ctx))
	_, e = c.Exec(ctx, "UPDATE m2_authority SET epoch=2,holder='successor' WHERE scope='fixture'")
	require.NoError(t, e)
	_, e = s.ApplyBatch(ctx, m2Batch("old-owner", 1, 0))
	m2Error(t, e, "stale_owner")
	require.Equal(t, "duplicate", m2Apply(t, s, b).Outcome)
	holder := m2Connection(t, dsn)
	_, e = holder.Exec(ctx, "SELECT pg_advisory_lock($1)", AdmissionKey)
	require.NoError(t, e)
	done := make(chan error, 1)
	go func() {
		candidate := m2Batch("legacy-admission", 1, 0).Mutations[0]
		event := domain.HazardEvent{HazardID: uuid.NewString(), Source: candidate.Source, SourceRefID: candidate.SourceRefID, HazardType: candidate.Event.HazardType, Severity: candidate.Event.Severity, AreaName: candidate.Event.AreaName, OccurredAt: timeutil.New(candidate.Event.OccurredAt), IngestedAt: timeutil.New(time.Now().UTC()), Attributes: candidate.Event.Attributes}
		_, err := s.SaveDedup(ctx, event, "fixture")
		done <- err
	}()
	m2Eventually(t, time.Second, func() bool {
		var n int
		return c.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory'").Scan(&n) == nil && n > 0
	})
	_, e = c.Exec(ctx, "SELECT pg_terminate_backend($1)", int32(holder.PgConn().PID()))
	require.NoError(t, e)
	select {
	case e = <-done:
		require.NoError(t, e)
	case <-time.After(3 * time.Second):
		t.Fatal("legacy writer did not resume after holder termination")
	}
}

func TestM2LegacyRegression(t *testing.T) {
	dsn, c := m2Database(t, false)
	m2Schema(t, c, "postgres", false)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	list, e := exec.CommandContext(ctx, "go", "test", "-list", ".", ".").CombinedOutput()
	require.NoError(t, e, string(list))
	names := []string{}
	for _, line := range strings.Split(string(list), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Test") && !strings.HasPrefix(line, "TestM2") {
			names = append(names, regexp.QuoteMeta(line))
		}
	}
	require.NotEmpty(t, names)
	cmd := exec.CommandContext(ctx, "go", "test", "-mod=readonly", "-count=1", "-timeout=75s", "-v", ".", "-run", "^("+strings.Join(names, "|")+")$")
	cmd.Env = append(os.Environ(), "TEST_DATABASE_URL="+dsn)
	output, e := cmd.CombinedOutput()
	if dir := os.Getenv("M2_EVIDENCE_DIR"); dir != "" {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "legacy-store-tests.log"), output, 0600))
	}
	require.NoError(t, e, string(output))
}

func TestM2LegacyNamespaceHandoff(t *testing.T) {
	dsn, c := m2Database(t, false)
	m2Schema(t, c, "postgres", false)
	s := m2Store(t, dsn)
	ctx := context.Background()
	b := m2Batch("handoff", 1, 1)
	m := b.Mutations[0]
	e := domain.HazardEvent{HazardID: uuid.NewString(), Source: m.Source, SourceRefID: m.SourceRefID, HazardType: m.Event.HazardType, Severity: m.Event.Severity, AreaName: m.Event.AreaName, OccurredAt: timeutil.New(m.Event.OccurredAt), IngestedAt: timeutil.New(time.Now()), Attributes: m.Event.Attributes}
	_, err := s.SaveDedup(ctx, e, "fixture")
	require.NoError(t, err)
	_, err = s.ApplyBatch(ctx, b)
	m2Error(t, err, "stale_operation")
	require.NoError(t, s.TransitionNamespace(ctx, b.Ownership, m.Source, m.SourceRefID, "", "fixture-v1"))
	require.NoError(t, s.TransitionNamespace(ctx, b.Ownership, m.Source, m.SourceRefID, "", "fixture-v1"))
	r := m2Apply(t, s, b)
	require.Equal(t, "no_change", r.Outcome)
	require.Equal(t, e.HazardID, r.Items[0].HazardID)
	_, err = s.SaveDedup(ctx, e, "legacy-after-handoff")
	require.Error(t, err)
}
