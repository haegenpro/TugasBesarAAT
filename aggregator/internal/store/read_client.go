package store

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/pagecursor"
	"context"
)

func (s *Store) CheckReadControl(ctx context.Context) error {
	if s.Reads.Authority != nil {
		if err := s.Reads.Authority(ctx); err != nil {
			return err
		}
	}
	var valid bool
	err := s.Pool.QueryRow(ctx, `SELECT NOT pg_is_in_recovery() AND to_regclass('canonical_visibility') IS NOT NULL AND to_regclass('canonical_attempts') IS NOT NULL AND to_regclass('hazard_events') IS NOT NULL`).Scan(&valid)
	if err != nil || !valid {
		return canonical.Unavailable()
	}
	return nil
}
func (s *Store) distributedPage(ctx context.Context, f Filters, p PageRequest) (Page, error) {
	out := Page{Events: []domain.HazardEvent{}, Metadata: map[string]domain.RecordMetadata{}, UnavailableUnits: []string{}}
	if p.Limit < 1 || p.Limit > 1000 {
		return out, ErrCursor
	}
	scope := filterScope(f)
	pos := pagecursor.Position{Endpoint: "client/hazards", Scope: "client", Filters: scope}
	if p.Cursor != "" {
		var err error
		pos, err = s.Reads.Signer.Decode(p.Cursor, "client/hazards", "client", scope)
		if err != nil {
			return out, ErrCursor
		}
		if pos.ID != "" {
			f.BeforeTime = &pos.Time
			f.BeforeID = pos.ID
		}
	}
	var result readResult
	err := s.validatedRead(ctx, func(ctx context.Context) error {
		var err error
		result, err = s.readDistributed(ctx, readSpec{Since: f.Since, AfterTime: f.BeforeTime, AfterID: f.BeforeID, Type: f.HazardType, Severity: f.Severity, Area: f.AreaName, Limit: p.Limit + 1})
		return err
	})
	if err != nil {
		return out, canonical.Unavailable()
	}
	out.Events = result.Events
	out.Metadata = result.Metadata
	out.UnavailableUnits = result.Unavailable
	out.Partial = result.Missing || len(result.Unavailable) > 0
	more := len(out.Events) > p.Limit
	if more {
		out.Events = out.Events[:p.Limit]
	}
	if more || result.Missing {
		if len(out.Events) > 0 {
			last := out.Events[len(out.Events)-1]
			pos.Time = last.OccurredAt.Time
			pos.ID = last.HazardID
		}
		pos.Units = requiredUnits(result.Inventory)
		next, err := s.Reads.Signer.Encode(pos)
		if err != nil {
			return out, err
		}
		out.NextCursor = &next
	}
	return out, nil
}
func (s *Store) GetByIDAvailability(ctx context.Context, id string) (domain.HazardEvent, domain.RecordMetadata, []string, error) {
	if s.Reads.Inventory == nil {
		e, m, err := s.GetByIDWithMetadata(ctx, id)
		return e, m, []string{}, err
	}
	var result readResult
	err := s.validatedRead(ctx, func(ctx context.Context) error {
		var err error
		result, err = s.readDistributed(ctx, readSpec{ID: id, Limit: 2})
		if err != nil {
			return err
		}
		if len(result.Events) == 0 {
			if result.Missing {
				return canonical.Unavailable()
			}
			return ErrNotFound
		}
		if len(result.Events) != 1 {
			return canonical.Unavailable()
		}
		return nil
	})
	if err != nil {
		return domain.HazardEvent{}, domain.RecordMetadata{}, nil, err
	}
	e := result.Events[0]
	return e, result.Metadata[e.HazardID], result.Unavailable, nil
}
