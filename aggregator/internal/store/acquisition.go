package store

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"context"
	"github.com/jackc/pgx/v5"
)

func (s *Store) AcquisitionRequired() bool { return s.Acquisition != nil }

func (s *Store) protectAcquisition(ctx context.Context, tx pgx.Tx, endpoint string) error {
	if s.Acquisition == nil {
		if acquisition.HasGuard(ctx) {
			return canonical.Unavailable() // Guarded work must never silently become unfenced.
		}
		return nil // Existing single-instance development mode.
	}
	source, ok := acquisition.Source(endpoint)
	if !ok {
		return acquisition.ErrNotOwner
	}
	owner, err := acquisition.Token(ctx, source)
	if err != nil {
		return err
	}
	return s.Acquisition.Protect(ctx, tx, owner, []canonical.Mutation{{Source: domain.Source(source)}})
}

func (s *Store) CheckAcquisition(ctx context.Context) error {
	if s.Acquisition == nil {
		return nil
	}
	var count int
	err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM canonical_ownership
 WHERE (scope='acquisition:BMKG' AND source='BMKG') OR
       (scope='acquisition:PVMBG' AND source='PVMBG')`).Scan(&count)
	if err != nil || count != 2 {
		return canonical.Unavailable()
	}
	return nil
}
