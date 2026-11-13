package store

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/migrations"
	"aggregator/internal/ownership"
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func platformAuthority(t *testing.T, ttl, budget time.Duration) (*Store, *ownership.Authority, *pgx.Conn) {
	t.Helper()
	dsn, c := m2Database(t, false)
	m2Schema(t, c, "postgres", false)
	s := m2Store(t, dsn)
	a, err := ownership.New(s.Pool, ttl, budget)
	require.NoError(t, err)
	s.Canonical.Fence = a
	// m2Store installs labDurability, explicitly single-copy and test-only.
	// These tests exercise authority safety, not production acknowledgement.
	return s, a, c
}

func platformAcquire(t *testing.T, a *ownership.Authority, holder string) canonical.Lease {
	t.Helper()
	l, err := a.Acquire(context.Background(), canonical.AcquireOwnership{Scope: "canonical:BMKG", Holder: holder})
	require.NoError(t, err)
	return l
}

func TestPlatformOwnershipCanonicalTakeover(t *testing.T) {
	s, a, c := platformAuthority(t, 15*time.Second, time.Second)
	ctx := context.Background()
	first := platformAcquire(t, a, "process-a")
	repeat := platformAcquire(t, a, "process-a")
	require.Equal(t, first, repeat, "acquisition retry must not secretly extend the lease")
	_, err := a.Acquire(ctx, canonical.AcquireOwnership{Scope: first.Ownership.Scope, Holder: "process-b"})
	m2Error(t, err, "ownership_conflict")
	renewed, err := a.Renew(ctx, first.Ownership)
	require.NoError(t, err)
	require.Equal(t, first.Ownership, renewed.Ownership)
	require.True(t, renewed.ExpiresAt.After(first.ExpiresAt))
	b := m2Batch("platform-first", 1, 0)
	b.Ownership = first.Ownership
	_, err = s.ApplyBatch(ctx, b)
	require.NoError(t, err)
	require.NoError(t, a.Release(ctx, first.Ownership))
	require.NoError(t, a.Release(ctx, first.Ownership))
	second := platformAcquire(t, a, "process-b")
	require.Greater(t, second.Ownership.Epoch, first.Ownership.Epoch)
	_, err = a.Renew(ctx, first.Ownership)
	m2Error(t, err, "stale_owner")
	m2Error(t, a.Release(ctx, first.Ownership), "stale_owner")
	pending := m2Batch("platform-pending", 2, 0)
	pending.Ownership = first.Ownership
	_, err = s.ApplyBatch(ctx, pending)
	m2Error(t, err, "stale_owner")
	pending.Ownership = second.Ownership
	_, err = s.ApplyBatch(ctx, pending)
	require.NoError(t, err, "successor must be able to deliver the original pending batch")
	duplicate, err := s.ApplyBatch(ctx, b)
	require.NoError(t, err)
	require.Equal(t, "duplicate", duplicate.Outcome, "committed identity resolution precedes current owner checks")
	wrong := m2Batch("wrong-source", 3, 0)
	wrong.Ownership = second.Ownership
	wrong.Mutations[0].Source = domain.SourcePVMBG
	_, err = s.ApplyBatch(ctx, wrong)
	m2Error(t, err, "stale_owner")
	var entities, operations, intents, checks int
	require.NoError(t, c.QueryRow(ctx, "SELECT (SELECT count(*) FROM canonical_entities),(SELECT count(*) FROM canonical_operations),(SELECT count(*) FROM hazard_outbox),(SELECT count(*) FROM canonical_ownership_checks)").Scan(&entities, &operations, &intents, &checks))
	require.Equal(t, 2, entities)
	require.Equal(t, 2, operations)
	require.Equal(t, 2, intents)
	require.Zero(t, checks, "deferred checks must not accumulate after commit")
	t.Log("real authority + canonical writes: takeover rejects stale work, successor drains original intent, retries retain one effect")
}

func TestPlatformOwnershipConcurrentAcquisition(t *testing.T) {
	_, a, _ := platformAuthority(t, 15*time.Second, time.Second)
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, holder := range []string{"one", "two"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := a.Acquire(context.Background(), canonical.AcquireOwnership{Scope: "canonical:BMKG", Holder: holder})
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else {
			m2Error(t, err, "ownership_conflict")
		}
	}
	require.Equal(t, 1, success)
}

func TestPlatformOwnershipCommitExpiry(t *testing.T) {
	s, a, c := platformAuthority(t, 600*time.Millisecond, 150*time.Millisecond)
	ctx := context.Background()
	l := platformAcquire(t, a, "busy-old-owner")
	b := m2Batch("expired-in-transaction", 1, 0)
	b.Ownership = l.Ownership
	tx, err := s.beginMutation(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	require.NoError(t, a.Protect(ctx, tx, l.Ownership, b.Mutations))
	_, err = s.applyMutation(ctx, tx, b.BatchID, b.Mutations[0])
	require.NoError(t, err)
	// Keep the backend active beyond expiry so this specifically tests deferred
	// commit validation, independently of idle-session termination.
	for i := 0; i < 16; i++ {
		_, err = tx.Exec(ctx, "SELECT pg_sleep(0.05)")
		require.NoError(t, err)
	}
	err = tx.Commit(ctx)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, canonical.StaleOwnerSQLState, pgErr.Code)
	require.NoError(t, s.RecoverAttempts(ctx))
	var n int
	require.NoError(t, c.QueryRow(ctx, "SELECT count(*) FROM canonical_entities").Scan(&n))
	require.Zero(t, n, "expiry must roll back data and publication intent together")
	_, err = a.Renew(ctx, l.Ownership)
	m2Error(t, err, "stale_owner")
	next := platformAcquire(t, a, "successor")
	require.Greater(t, next.Ownership.Epoch, l.Ownership.Epoch)
}

func TestPlatformOwnershipFrozenTransaction(t *testing.T) {
	s, a, c := platformAuthority(t, 600*time.Millisecond, 150*time.Millisecond)
	ctx := context.Background()
	l := platformAcquire(t, a, "frozen-client")
	b := m2Batch("frozen-before-commit", 1, 0)
	b.Ownership = l.Ownership
	tx, err := s.beginMutation(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	require.NoError(t, a.Protect(ctx, tx, l.Ownership, b.Mutations))
	_, err = s.applyMutation(ctx, tx, b.BatchID, b.Mutations[0])
	require.NoError(t, err)
	pid := int32(tx.Conn().PgConn().PID())
	require.Eventually(t, func() bool {
		var n int
		return c.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE pid=$1", pid).Scan(&n) == nil && n == 0
	}, 2*time.Second, 10*time.Millisecond, "database must release a frozen client's transaction")
	_, err = c.Exec(ctx, "UPDATE canonical_ownership SET expires_at=clock_timestamp()-interval '1 second' WHERE scope=$1", l.Ownership.Scope)
	require.NoError(t, err)
	next := platformAcquire(t, a, "replacement")
	require.Greater(t, next.Ownership.Epoch, l.Ownership.Epoch)
	require.Error(t, tx.Commit(ctx), "resumed client must not commit its old transaction")
	require.NoError(t, s.RecoverAttempts(ctx))
	b.Ownership = l.Ownership
	_, err = s.ApplyBatch(ctx, b)
	m2Error(t, err, "stale_owner")
	b.Ownership = next.Ownership
	_, err = s.ApplyBatch(ctx, b)
	require.NoError(t, err)
	t.Log("stalled client backend terminated by database timeout; replacement owns a newer epoch; old transaction and token both rejected")
}

func TestPlatformOwnershipLockThroughCommit(t *testing.T) {
	s, a, c := platformAuthority(t, 15*time.Second, time.Second)
	ctx := context.Background()
	l := platformAcquire(t, a, "owner")
	b := m2Batch("protected", 1, 0)
	tx, err := s.beginMutation(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	require.NoError(t, a.Protect(ctx, tx, l.Ownership, b.Mutations))
	done := make(chan error, 1)
	go func() { done <- a.Release(ctx, l.Ownership) }()
	require.Eventually(t, func() bool {
		var n int
		return c.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%canonical_ownership%'").Scan(&n) == nil && n > 0
	}, 700*time.Millisecond, 10*time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("release overtook protected transaction: %v", err)
	default:
	}
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, <-done)
	next := platformAcquire(t, a, "new-owner")
	require.Greater(t, next.Ownership.Epoch, l.Ownership.Epoch)
}

func TestPlatformOwnershipHTTP(t *testing.T) {
	s, a, _ := platformAuthority(t, 15*time.Second, time.Second)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	canonical.HTTP{Repository: s, Ownership: a, Token: "integration-service"}.Register(r)
	call := func(path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer integration-service")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}
	w := call("/internal/ownership/acquire", `{"scope":"canonical:BMKG","holder":"http-process"}`)
	require.Equal(t, 200, w.Code, w.Body.String())
	var l canonical.Lease
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &l))
	b := m2Batch("http-owned", 1, 0)
	b.Ownership = l.Ownership
	raw, err := json.Marshal(b)
	require.NoError(t, err)
	w = call("/internal/canonical/batches", string(raw))
	require.Equal(t, 200, w.Code, w.Body.String())
	owner, err := json.Marshal(l.Ownership)
	require.NoError(t, err)
	w = call("/internal/ownership/release", string(owner))
	require.Equal(t, 200, w.Code, w.Body.String())
	b = m2Batch("http-old-owner", 2, 0)
	b.Ownership = l.Ownership
	raw, err = json.Marshal(b)
	require.NoError(t, err)
	w = call("/internal/canonical/batches", string(raw))
	require.Equal(t, 403, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "stale_owner")
}

func TestPlatformOwnershipRestrictedRuntime(t *testing.T) {
	_, _, c := platformAuthority(t, 15*time.Second, time.Second)
	ctx := context.Background()
	role := "runtime_" + c.Config().Database
	quoted := pgx.Identifier{role}.Sanitize()
	_, err := c.Exec(ctx, "CREATE ROLE "+quoted+" NOLOGIN")
	require.NoError(t, err)
	defer func() {
		_, err := c.Exec(ctx, "DROP OWNED BY "+quoted+"; DROP ROLE "+quoted)
		require.NoError(t, err)
	}()
	require.NoError(t, migrations.Up(ctx, c, filepath.Join("..", "..", "..", "store", "migrations"), "postgres", role))
	u, err := url.Parse(c.Config().ConnString())
	require.NoError(t, err)
	query := u.Query()
	query.Set("options", "-c role="+role)
	u.RawQuery = query.Encode()
	runtime, err := New(ctx, u.String(), nil)
	require.NoError(t, err)
	defer runtime.Close()
	var current string
	require.NoError(t, runtime.Pool.QueryRow(ctx, "SELECT current_user").Scan(&current))
	require.Equal(t, role, current)
	a, err := ownership.New(runtime.Pool, 15*time.Second, time.Second)
	require.NoError(t, err)
	runtime.Canonical = CanonicalOptions{Fence: a, Durability: labDurability{}}
	l := platformAcquire(t, a, "restricted-process")
	b := m2Batch("restricted-owner", 1, 0)
	b.Ownership = l.Ownership
	_, err = runtime.ApplyBatch(ctx, b)
	require.NoError(t, err, "ownership and its deferred trigger must work with restricted runtime grants")
	_, err = runtime.Pool.Exec(ctx, "ALTER TABLE canonical_ownership ADD forbidden int")
	require.Error(t, err, "runtime must not obtain administrator DDL permissions")
}

func TestPlatformOwnershipUnavailableAuthority(t *testing.T) {
	s, a, _ := platformAuthority(t, 15*time.Second, 200*time.Millisecond)
	_, err := a.Acquire(context.Background(), canonical.AcquireOwnership{Scope: "unknown", Holder: "worker"})
	m2Error(t, err, "invalid_scope")
	_, err = a.Acquire(context.Background(), canonical.AcquireOwnership{Scope: "canonical:BMKG", Holder: " "})
	m2Error(t, err, "invalid_request")
	s.Pool.Close()
	_, err = a.Acquire(context.Background(), canonical.AcquireOwnership{Scope: "canonical:BMKG", Holder: "worker"})
	m2Error(t, err, "temporarily_unavailable")
}
