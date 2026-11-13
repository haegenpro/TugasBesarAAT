package store

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"context"
	"github.com/jackc/pgx/v5"
)

// ProtectProcessorState must be called before any prepared-state, lineage,
// progress or output-intent write in the caller's transaction. The storage owner
// supplies those records, idempotent recovery and post-commit confirmation.
// Preparation deliberately does not close canonical mutation admission.
func (s *Store) ProtectProcessorState(ctx context.Context, tx pgx.Tx, owner canonical.Ownership) error {
	if s.Processor == nil || s.Canonical.Durability == nil {
		return canonical.Unavailable()
	}
	if err := s.Canonical.Durability.Prepare(ctx, tx); err != nil {
		return err
	}
	return s.Processor.Protect(ctx, tx, owner, []canonical.Mutation{{Source: domain.SourceSatelit}})
}
