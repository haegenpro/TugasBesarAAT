package store

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"time"
)

// A small publication ledger closes the coordinator-crash gap between Citus'
// durable commit decision and completion on every worker. Normal reads use one
// local indexed check, rather than probing all PostgreSQL replicas per request.
type visibilityTx struct {
	pgx.Tx
	store *Store
	id    string
}

func (s *Store) beginCanonicalTx(ctx context.Context) (pgx.Tx, error) {
	tx, e := s.Pool.Begin(ctx)
	if e != nil || !s.Distributed {
		return tx, e
	}
	return &visibilityTx{Tx: tx, store: s}, nil
}
func markVisibility(ctx context.Context, tx pgx.Tx) error {
	tracked, ok := tx.(*visibilityTx)
	if !ok || tracked.id != "" {
		return nil
	}
	if e := tx.QueryRow(ctx, `SELECT pg_current_xact_id()::text`).Scan(&tracked.id); e != nil {
		return e
	}
	_, e := tx.Exec(ctx, `INSERT INTO canonical_visibility(txid) VALUES($1) ON CONFLICT DO NOTHING`, tracked.id)
	return e
}
func (tx *visibilityTx) Commit(ctx context.Context) error {
	if e := tx.Tx.Commit(ctx); e != nil {
		return e
	}
	if tx.id == "" {
		return nil
	}
	// A lost acknowledgement leaves the ledger pending. Recovery verifies the
	// worker commit state before publishing it; a retry never invents completion.
	mark, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	_, e := tx.store.Pool.Exec(mark, `UPDATE canonical_visibility SET published=true,published_at=clock_timestamp() WHERE txid=$1`, tx.id)
	return e
}
func (s *Store) RunVisibilityRecovery(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.recoverVisibility(ctx)
		}
	}
}
func (s *Store) recoverVisibility(ctx context.Context) error {
	call, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var pending bool
	if e := s.Pool.QueryRow(call, `SELECT EXISTS(SELECT 1 FROM canonical_visibility WHERE NOT published)`).Scan(&pending); e != nil || !pending {
		return e
	}
	tx, e := s.Pool.Begin(call)
	if e != nil {
		return e
	}
	defer tx.Rollback(call)
	// Queue the writer lock so a continuous stream of shared reads cannot
	// starve recovery. Each foreground wait is bounded to 200 milliseconds.
	if _, e = tx.Exec(call, `SELECT pg_advisory_xact_lock($1)`, visibilityLock); e != nil {
		return e
	}
	members, e := s.membersFrom(call, tx)
	if e != nil {
		return e
	}
	decisions, e := tx.Query(call, `SELECT gid FROM pg_dist_transaction WHERE outer_xid::text IN(SELECT txid FROM canonical_visibility WHERE NOT published)`)
	if e != nil {
		return e
	}
	gids := []string{}
	for decisions.Next() {
		var gid string
		if e = decisions.Scan(&gid); e != nil {
			decisions.Close()
			return e
		}
		gids = append(gids, gid)
	}
	e = decisions.Err()
	decisions.Close()
	if e != nil {
		return e
	}
	checked := 0
	for _, member := range members {
		if member.Role != "primary" {
			continue
		}
		connection, e := s.memberConnection(call, member)
		if e != nil {
			return e
		}
		var prepared int
		e = connection.QueryRow(call, `SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database() AND owner=current_user AND gid=ANY($1)`, gids).Scan(&prepared)
		connection.Close(call)
		if e != nil {
			return e
		}
		if prepared != 0 {
			return errors.New("Citus commit recovery has not finished")
		}
		checked++
	}
	if checked != 2 {
		return errors.New("two current Citus worker primaries required before publication")
	}
	if _, e = tx.Exec(call, `UPDATE canonical_visibility SET published=true,published_at=clock_timestamp() WHERE NOT published`); e != nil {
		return e
	}
	if _, e = tx.Exec(call, `DELETE FROM canonical_visibility WHERE published AND published_at<clock_timestamp()-interval '5 minutes'`); e != nil {
		return e
	}
	return tx.Commit(call)
}
