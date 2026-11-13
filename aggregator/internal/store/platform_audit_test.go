package store

import (
	"context"
	"testing"
	"time"

	"aggregator/internal/canonical"
	"aggregator/internal/ownership"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestPlatformAuditGateExpiryAndStaleCleanup(t *testing.T) {
	dsn, admin := m2Database(t, false)
	m2Schema(t, admin, "postgres", false)
	s := m2Store(t, dsn)
	ctx := context.Background()
	current := uuid.NewString()
	_, err := admin.Exec(ctx, "UPDATE platform_audit_gate SET epoch=2,capture_id=$1,holder='current',expires_at=clock_timestamp()+interval '30 seconds'", current)
	require.NoError(t, err)
	err = s.CommitSeismic(ctx, nil, "blocked", 1)
	require.Error(t, err)
	err = s.AbortCapture(ctx, CaptureToken{ID: uuid.NewString(), Epoch: 1, Holder: "old"}, "old cleanup")
	require.NoError(t, err)
	var holder string
	require.NoError(t, admin.QueryRow(ctx, "SELECT holder FROM platform_audit_gate WHERE singleton").Scan(&holder))
	require.Equal(t, "current", holder)
	_, err = admin.Exec(ctx, "UPDATE platform_audit_gate SET expires_at=clock_timestamp()-interval '1 second'")
	require.NoError(t, err)
	require.NoError(t, s.CommitSeismic(ctx, nil, "after-expiry", 1))
}

func TestPlatformAuditLateCopyCommitRejected(t *testing.T) {
	dsn, admin := m2Database(t, false)
	m2Schema(t, admin, "postgres", false)
	s := m2Store(t, dsn)
	ctx := context.Background()
	token := CaptureToken{ID: uuid.NewString(), Epoch: 1, Holder: "builder"}
	_, err := admin.Exec(ctx, `INSERT INTO platform_audit_captures(capture_id,epoch,holder,state,views_pinned,topology,owner_expires_at,copy_deadline,absolute_expires_at)
 VALUES($1,1,'builder','building',true,'postgres',clock_timestamp()+interval '1 second',clock_timestamp()+interval '5 minutes',clock_timestamp()+interval '1 hour')`, token.ID)
	require.NoError(t, err)
	tx, err := s.Pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	require.NoError(t, s.ProtectCaptureCopy(ctx, tx, token))
	_, err = tx.Exec(ctx, "SELECT pg_sleep(1.1)")
	require.NoError(t, err)
	err = tx.Commit(ctx)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, canonical.StaleOwnerSQLState, pgErr.Code)
}

func TestPlatformAuditCleanupRetriesTerminalIDs(t *testing.T) {
	dsn, admin := m2Database(t, false)
	m2Schema(t, admin, "postgres", false)
	s := m2Store(t, dsn)
	ctx := context.Background()
	id := uuid.NewString()
	_, err := admin.Exec(ctx, `INSERT INTO platform_audit_captures(capture_id,epoch,holder,state,views_pinned,topology,owner_expires_at,copy_deadline,idle_expires_at,absolute_expires_at)
 VALUES($1,1,'builder','ready',true,'postgres',clock_timestamp(),clock_timestamp(),clock_timestamp()-interval '1 second',clock_timestamp()+interval '1 hour')`, id)
	require.NoError(t, err)
	require.Error(t, s.TouchSnapshot(ctx, id))
	for i := 0; i < 2; i++ {
		ids, err := s.ExpireCaptures(ctx)
		require.NoError(t, err)
		require.Contains(t, ids, id)
	}
	require.NoError(t, s.MarkCaptureCleaned(ctx, id))
	ids, err := s.ExpireCaptures(ctx)
	require.NoError(t, err)
	require.NotContains(t, ids, id)
}

func TestPlatformProcessorScopeIsolation(t *testing.T) {
	dsn, admin := m2Database(t, false)
	m2Schema(t, admin, "postgres", false)
	s := m2Store(t, dsn)
	a, err := ownership.NewProcessor(s.Pool, 15*time.Second)
	require.NoError(t, err)
	s.Processor = a
	ctx := context.Background()
	lease, err := a.Acquire(ctx, canonical.AcquireOwnership{Scope: "processor:SATELIT", Holder: "processor"})
	require.NoError(t, err)
	_, err = a.Acquire(ctx, canonical.AcquireOwnership{Scope: "canonical:SATELIT", Holder: "processor"})
	m2Error(t, err, "invalid_scope")
	tx, err := s.Pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	require.NoError(t, s.ProtectProcessorState(ctx, tx, lease.Ownership))
	require.NoError(t, tx.Commit(ctx))
}
