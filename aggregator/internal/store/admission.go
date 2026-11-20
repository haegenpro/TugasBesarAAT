package store

import (
	"aggregator/internal/canonical"
	"aggregator/internal/middleware"
	"aggregator/internal/platform"
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

// Experimental shared admission key, matching the M1 snapshot adapter. This is
// not the Role 2 production lease/expiry contract.
const AdmissionKey int64 = 4810301

// Live readers share this lock. Publishers hold it from registration through
// the durable decision so worker commits cannot fracture a read.
const visibilityKey int64 = 4810302
const attemptLockSalt int64 = 726103

type mutationTx struct {
	pgx.Tx
	s          *Store
	admission  *pgxpool.Conn
	id         string
	topology   string
	registered bool
	closed     bool
}

func attemptID(tx pgx.Tx) any {
	if t, ok := tx.(*mutationTx); ok {
		return t.id
	}
	return nil
}
func (s *Store) beginMutation(ctx context.Context) (pgx.Tx, error) {
	admission, err := s.control.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	id := uuid.NewString()
	release := func() { releaseAdmission(admission) }
	if _, err = admission.Exec(ctx, "SELECT pg_advisory_lock_shared($1),pg_advisory_lock(hashtextextended($2,$3))", AdmissionKey, id, attemptLockSalt); err != nil {
		release()
		return nil, err
	}
	var auditClosed bool
	if err = admission.QueryRow(ctx, "SELECT coalesce(expires_at>clock_timestamp(),false) FROM platform_audit_gate WHERE singleton").Scan(&auditClosed); err != nil || auditClosed {
		release()
		return nil, canonical.Unavailable()
	}
	topology, _, err := readPrimaryTopology(ctx, admission)
	if err != nil {
		release()
		return nil, err
	}
	// Registration commits on the control connection after this transaction
	// stages its work. The later row fence must see that committed registration.
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		release()
		return nil, err
	}
	t := &mutationTx{Tx: tx, s: s, admission: admission, id: id, topology: topology}
	if s.Canonical.Durability != nil {
		if err = s.Canonical.Durability.Prepare(ctx, tx); err != nil {
			_ = t.Rollback(context.Background())
			return nil, err
		}
	}
	idleTimeout := s.idleTransactionTimeout
	if idleTimeout == 0 {
		idleTimeout = defaultIdleTransactionTimeout
	}
	// Reapply locally for admitted mutations, including custom pool adapters.
	// Store.New also installs the connection-startup default to cover BEGIN's
	// gap before this statement. A later ownership fence may shorten the budget.
	if _, err = tx.Exec(ctx, "SELECT set_config('idle_in_transaction_session_timeout',$1,true)", timeoutMilliseconds(idleTimeout)); err != nil {
		_ = t.Rollback(context.Background())
		return nil, err
	}
	return t, nil
}
func releaseAdmission(c *pgxpool.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.Exec(ctx, "SELECT pg_advisory_unlock_all()"); err != nil {
		_ = c.Hijack().Close(ctx)
		return
	}
	c.Release()
}

// Stage changes in the data transaction before publishing the attempt. Every
// path to a data COMMIT must durably register first, while the admission session
// excludes live reads. Mixed-version writers remain covered by the persisted
// pending state and generation checks on both sides of a read.
func (t *mutationTx) prepareCommit(ctx context.Context) error {
	if t.registered {
		return nil
	}
	if _, err := t.admission.Exec(ctx, "SELECT pg_advisory_lock($1)", visibilityKey); err != nil {
		return canonical.Unavailable()
	}
	reg, err := t.admission.Begin(ctx)
	if err != nil {
		return err
	}
	defer reg.Rollback(ctx)
	if err = checkAuditAdmission(ctx, reg); err != nil {
		return err
	}
	if t.s.Canonical.Durability != nil {
		if err = t.s.Canonical.Durability.Prepare(ctx, reg); err != nil {
			return err
		}
	}
	if _, err = reg.Exec(ctx, "INSERT INTO canonical_attempts(attempt_id,state,topology) VALUES($1,'pending',$2)", t.id, t.topology); err != nil {
		return err
	}
	if _, err = reg.Exec(ctx, "UPDATE canonical_visibility SET generation=generation+1 WHERE singleton"); err != nil {
		return err
	}
	if err = reg.Commit(ctx); err != nil {
		return err
	}
	// Recovery takes FOR UPDATE. It cannot finalize this attempt while the
	// transaction can still commit, even if its admission connection drops.
	var state string
	if err = t.Tx.QueryRow(ctx, "SELECT state FROM canonical_attempts WHERE attempt_id=$1 FOR SHARE", t.id).Scan(&state); err != nil {
		return err
	}
	if state != "pending" {
		return canonical.Unavailable()
	}
	t.registered = true
	return nil
}

func (t *mutationTx) Commit(ctx context.Context) error {
	if t.closed {
		return pgx.ErrTxClosed
	}
	t.closed = true
	defer releaseAdmission(t.admission)
	defer t.Tx.Rollback(ctx)
	if err := t.prepareCommit(ctx); err != nil {
		return err
	}
	if _, err := t.Tx.Exec(ctx, "INSERT INTO canonical_attempt_commits(attempt_id) VALUES($1)", t.id); err != nil {
		return err
	}
	// Recheck the admission session after staging and registration. A frozen
	// caller cannot commit after losing that session. Recovery decides any
	// pending outcome using the row fence and distributed decision evidence.
	if _, err := t.admission.Exec(ctx, "SELECT 1"); err != nil {
		return canonical.Unavailable()
	}
	err := t.Tx.Commit(ctx)
	if err != nil {
		return err
	} // Unknown outcomes remain pending for fenced recovery.
	if err = t.s.resolveAttempt(ctx, t.id); err != nil {
		return canonical.Unavailable()
	}
	return nil
}
func (t *mutationTx) Rollback(ctx context.Context) error {
	if t.closed {
		return pgx.ErrTxClosed
	}
	t.closed = true
	defer releaseAdmission(t.admission)
	cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := t.Tx.Rollback(cleanup)
	if err != nil {
		return err
	}
	if t.registered {
		return t.s.resolveAttempt(cleanup, t.id)
	}
	return nil
}

// distributedSettled is intentionally conservative: any prepared decision on
// any participating primary blocks finalization, and unavailable workers block.
func (s *Store) primaryTopology(ctx context.Context) (string, int, error) {
	return readPrimaryTopology(ctx, s.Pool)
}

type readQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func readPrimaryTopology(ctx context.Context, q readQuerier) (string, int, error) {
	var citus bool
	if err := q.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='citus')").Scan(&citus); err != nil {
		return "", 0, err
	}
	if !citus {
		return "postgres", 0, nil
	}
	var topology string
	var count, inactive int
	err := q.QueryRow(ctx, "SELECT coalesce(jsonb_agg(jsonb_build_array(nodeid,nodename,nodeport,groupid) ORDER BY nodeid),'[]'::jsonb)::text,count(*),count(*) FILTER(WHERE NOT isactive) FROM pg_dist_node WHERE groupid>0 AND noderole='primary'").Scan(&topology, &count, &inactive)
	if err != nil {
		return "", 0, err
	}
	if count == 0 || inactive != 0 {
		return "", 0, fmt.Errorf("required primary topology unavailable")
	}
	return topology, count, nil
}
func (s *Store) distributedSettled(ctx context.Context, expected string) error {
	topology, workers, err := s.primaryTopology(ctx)
	if err != nil {
		return err
	}
	if topology != expected {
		adapter, ok := s.Canonical.Durability.(interface {
			PermitTopologyChange(context.Context, string, string) error
		})
		if !ok || adapter.PermitTopologyChange(ctx, expected, topology) != nil {
			return fmt.Errorf("attempt topology changed; explicit integration recovery required")
		}
	}

	var n int
	if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM pg_prepared_xacts").Scan(&n); err != nil {
		return err
	}
	if n != 0 {
		return fmt.Errorf("unresolved prepared transactions")
	}
	if workers == 0 {
		return nil
	}
	rows, err := s.Pool.Query(ctx, "SELECT success,result FROM run_command_on_workers('SELECT count(*) FROM pg_prepared_xacts')")
	if err != nil {
		return err
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		seen++
		var success bool
		var result string
		if err = rows.Scan(&success, &result); err != nil {
			return err
		}
		if !success || result != "0" {
			return fmt.Errorf("worker decision unresolved or unavailable")
		}
	}
	if seen != workers {
		return fmt.Errorf("missing primary decision evidence")
	}
	return rows.Err()
}
func (s *Store) resolveAttempt(ctx context.Context, id string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if s.Canonical.Durability != nil {
		if err = s.Canonical.Durability.Prepare(ctx, tx); err != nil {
			return err
		}
	}
	var state, topology string
	if err = tx.QueryRow(ctx, "SELECT state,topology FROM canonical_attempts WHERE attempt_id=$1 FOR UPDATE NOWAIT", id).Scan(&state, &topology); err != nil {
		return err
	}
	if state != "pending" {
		return tx.Commit(ctx)
	}
	if err = s.distributedSettled(ctx, topology); err != nil {
		return err
	}
	var committed bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM canonical_attempt_commits WHERE attempt_id=$1)", id).Scan(&committed); err != nil {
		return err
	}
	if adapter, ok := s.Canonical.Durability.(interface {
		ConfirmTransaction(context.Context, pgx.Tx) error
	}); ok {
		if err = adapter.ConfirmTransaction(ctx, tx); err != nil {
			return err
		}
	}
	state = "aborted"
	if committed {
		state = "committed"
	}
	if _, err = tx.Exec(ctx, "UPDATE canonical_attempts SET state=$2,resolved_at=now() WHERE attempt_id=$1", id, state); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "UPDATE canonical_visibility SET generation=generation+1 WHERE singleton"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// RecoverAttempts never infers outcomes from age. PostgreSQL timeouts release
// abandoned data/control sessions. Recovery must still acquire the attempt's
// session lock and row lock and inspect the atomic commit marker. The global
// prepared-decision probe must succeed before a terminal outcome is published.
func (s *Store) RecoverAttempts(ctx context.Context) error {
	rows, err := s.Pool.Query(ctx, "SELECT attempt_id::text FROM canonical_attempts WHERE state='pending' ORDER BY created_at LIMIT 32")
	if err != nil {
		return err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		c, e := s.control.Acquire(ctx)
		if e != nil {
			return e
		}
		var acquired bool
		e = c.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtextextended($1,$2))", id, attemptLockSalt).Scan(&acquired)
		if e == nil && acquired {
			e = s.resolveAttempt(ctx, id)
		}
		releaseAdmission(c)
		if e != nil {
			return e
		}
	}
	return nil
}
func (s *Store) visibility(ctx context.Context) (int64, bool, error) {
	if s.Reads.Authority != nil {
		if _, err := s.Reads.Authority(ctx, s.Pool); err != nil {
			return 0, false, err
		}
	}
	return readVisibility(ctx, s.Pool)
}

func readVisibility(ctx context.Context, q readQuerier) (int64, bool, error) {
	var g int64
	var pending bool
	var primary bool
	err := q.QueryRow(ctx, "SELECT generation,EXISTS(SELECT 1 FROM canonical_attempts WHERE state='pending'),NOT pg_is_in_recovery() FROM canonical_visibility WHERE singleton").Scan(&g, &pending, &primary)
	if err == nil && !primary {
		err = canonical.Unavailable()
	}
	return g, pending, err
}

// Use separate READ COMMITTED statements on the same coordinator session.
// The shared lock prevents new commits during the read. The durable checks also
// cover uncertain outcomes and writers from before the lock was introduced.
func (s *Store) readAtStableCut(ctx context.Context, read func(context.Context) error) (bool, error) {
	pool := s.control
	if s.readGuard != nil {
		pool = s.readGuard
	}
	guard, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return false, canonical.Unavailable()
	}
	defer guard.Rollback(ctx)
	if _, err = guard.Exec(ctx, "SELECT pg_advisory_xact_lock_shared($1)", visibilityKey); err != nil {
		return false, canonical.Unavailable()
	}
	before, pending, err := readVisibility(ctx, guard)
	if err != nil || pending {
		return pending && err == nil, canonical.Unavailable()
	}
	var anchor replicationStatus
	if s.Reads.Authority != nil {
		anchor, err = s.Reads.Authority(ctx, guard)
		if err != nil {
			return false, canonical.Unavailable()
		}
	}
	resultErr := read(ctx)
	after, pending, err := readVisibility(ctx, guard)
	if err != nil {
		return false, canonical.Unavailable()
	}
	if pending || before != after {
		return true, canonical.Unavailable()
	}
	if s.Reads.Authority != nil {
		current, err := s.Reads.Authority(ctx, guard)
		if err != nil || !samePrimary(anchor, current) {
			return false, canonical.Unavailable()
		}
	}
	if err = guard.Rollback(ctx); err != nil || ctx.Err() != nil {
		return false, canonical.Unavailable()
	}
	return false, resultErr
}

func (s *Store) validatedRead(ctx context.Context, read func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(middleware.EnsureCorrelationID(ctx), canonical.Deadline)
	defer cancel()
	ctx, span := platform.Start(ctx, "canonical live read")
	defer span.End()
	for {
		retry, err := s.readAtStableCut(ctx, read)
		if !retry {
			return err
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return canonical.Unavailable()
		case <-timer.C:
		}
	}
}
func (s *Store) publicationReady(ctx context.Context, tx pgx.Tx, id *string) error {
	if id != nil {
		var state string
		if err := tx.QueryRow(ctx, "SELECT state FROM canonical_attempts WHERE attempt_id=$1", *id).Scan(&state); err != nil {
			return err
		}
		if state != "committed" {
			return canonical.Unavailable()
		}
	} // Migrated M1 intents have no attempt ID, but still need WAL evidence.
	if adapter, ok := s.Canonical.Durability.(interface {
		ConfirmTransaction(context.Context, pgx.Tx) error
	}); ok {
		return adapter.ConfirmTransaction(ctx, tx)
	}
	return nil
}

var _ pgx.Tx = (*mutationTx)(nil)
