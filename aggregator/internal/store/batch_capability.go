package store

import (
	"aggregator/internal/canonical"
	"context"
	"fmt"
	"regexp"
	"sync"

	"github.com/jackc/pgx/v5"
)

// A process flag cannot establish a distributed transaction's safety. Each new
// multi-entity batch verifies the pinned commit/recovery configuration on its
// actual coordinator transaction and every configured primary. beginMutation
// already verifies original physical identities, topology and synchronous WAL
// quorums; Commit resolves distributed decisions, and Confirm covers the final
// batch/decision WAL on all groups before success is returned.
func (r *replicatedDurability) PrepareBatch(ctx context.Context, tx pgx.Tx) error {
	if r.cfg.Mode == "postgres" {
		// All entities share this one PostgreSQL transaction; its synchronous
		// replication preparation/confirmation is still mandatory.
		return r.Prepare(ctx, tx)
	}
	if r.cfg.Mode != "citus" || len(r.cfg.Groups) != 5 {
		return canonical.Unavailable()
	}
	if err := checkBatchBackend(ctx, tx); err != nil {
		return canonical.Unavailable()
	}
	errors := make([]error, len(r.pools))
	var wg sync.WaitGroup
	for index, pool := range r.pools {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			errors[index] = checkBatchBackend(ctx, pool)
		}(index)
	}
	wg.Wait()
	for _, err := range errors {
		if err != nil {
			return canonical.Unavailable()
		}
	}
	return nil
}

type batchBackend interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func checkBatchBackend(ctx context.Context, q batchBackend) error {
	var version string
	var prepared, recovery int
	err := q.QueryRow(ctx, `SELECT
		coalesce((SELECT extversion FROM pg_extension WHERE extname='citus'),''),
		current_setting('max_prepared_transactions')::integer,
		coalesce((SELECT setting::integer FROM pg_settings WHERE name='citus.recover_2pc_interval'),0)`).Scan(&version, &prepared, &recovery)
	if err != nil {
		return err
	}
	return validateBatchBackend(version, prepared, recovery)
}

var batchCitusVersion = regexp.MustCompile(`^14\.2(?:\.[0-9]+)?(?:-[0-9]+)?$`)

func validateBatchBackend(version string, prepared, recovery int) error {
	// This implementation is reviewed against the formation's pinned 14.2
	// built-in 2PC path. Unknown versions require review rather than a guessed GUC.
	if !batchCitusVersion.MatchString(version) || prepared < 1 || recovery < 1 {
		return fmt.Errorf("multi-entity batch requires Citus 14.2, prepared transactions and active distributed recovery")
	}
	return nil
}
