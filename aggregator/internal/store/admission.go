package store

import (
	"aggregator/internal/canonical"
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
const attemptLockSalt int64 = 726103

type mutationTx struct {
	pgx.Tx
	s         *Store
	admission *pgxpool.Conn
	id        string
	closed    bool
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
	topology, _, err := s.primaryTopology(ctx)
	if err != nil {
		release()
		return nil, err
	}
	reg, err := admission.Begin(ctx)
	if err != nil {
		release()
		return nil, err
	}
	if err = checkAuditAdmission(ctx, reg); err != nil {
		_ = reg.Rollback(context.Background())
		release()
		return nil, err
	}
	if s.Canonical.Durability != nil {
		if err = s.Canonical.Durability.Prepare(ctx, reg); err != nil {
			_ = reg.Rollback(context.Background())
			release()
			return nil, err
		}
	}
	if _, err = reg.Exec(ctx, "INSERT INTO canonical_attempts(attempt_id,state,topology) VALUES($1,'pending',$2)", id, topology); err == nil {
		_, err = reg.Exec(ctx, "UPDATE canonical_visibility SET generation=generation+1 WHERE singleton")
	}
	if err != nil {
		reg.Rollback(context.Background())
		release()
		return nil, err
	}
	if err = reg.Commit(ctx); err != nil {
		release()
		return nil, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		release()
		return nil, err
	}
	t := &mutationTx{Tx: tx, s: s, admission: admission, id: id}
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
	var state string
	// Recovery takes FOR UPDATE; it cannot finalize an attempt while a protected
	// transaction holds this lock, nor can a finalized attempt start new work.
	err = tx.QueryRow(ctx, "SELECT state FROM canonical_attempts WHERE attempt_id=$1 FOR SHARE", id).Scan(&state)
	if err != nil || state != "pending" {
		t.Rollback(context.Background())
		if err != nil {
			return nil, err
		}
		return nil, canonical.Unavailable()
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
func (t *mutationTx) Commit(ctx context.Context) error {
	if t.closed {
		return pgx.ErrTxClosed
	}
	t.closed = true
	defer releaseAdmission(t.admission)
	// A long-running or resumed mutation must not commit after its separate
	// admission session has expired. This refreshes only this live caller's
	// session; it cannot keep a frozen process alive through another worker.
	if _, err := t.admission.Exec(ctx, "SELECT 1"); err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = t.Tx.Rollback(cleanup)
		// Leave the pending attempt for normal fenced decision recovery. A lost
		// connection or timeout must not be turned into an assumed abort/success.
		return canonical.Unavailable()
	}
	if _, err := t.Tx.Exec(ctx, "INSERT INTO canonical_attempt_commits(attempt_id) VALUES($1)", t.id); err != nil {
		t.Tx.Rollback(context.Background())
		return err
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
	return t.s.resolveAttempt(cleanup, t.id)
}

// distributedSettled is intentionally conservative: any prepared decision on
// any participating primary blocks finalization, and unavailable workers block.
func (s *Store) primaryTopology(ctx context.Context) (string, int, error) {
	return readPrimaryTopology(ctx, s.Pool)
}

func readPrimaryTopology(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (string, int, error) {
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
	var g int64
	var pending bool
	err := s.Pool.QueryRow(ctx, "SELECT generation,EXISTS(SELECT 1 FROM canonical_attempts WHERE state='pending') FROM canonical_visibility WHERE singleton").Scan(&g, &pending)
	return g, pending, err
}
func (s *Store) validatedRead(ctx context.Context, read func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, canonical.Deadline)
	defer cancel()
	for {
		before, pending, err := s.visibility(ctx)
		if err != nil {
			return canonical.Unavailable()
		}
		if !pending {
			resultErr := read(ctx)
			after, pending, e := s.visibility(ctx)
			if e != nil {
				return canonical.Unavailable()
			}
			if !pending && before == after {
				return resultErr
			}
		}
		timer := time.NewTimer(5 * time.Millisecond)
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
