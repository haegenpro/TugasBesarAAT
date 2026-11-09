package store

import (
	"aggregator/internal/volcano"
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
)

func (s *Store) Get(ctx context.Context, id string) (volcano.Reference, bool, error) {
	var r volcano.Reference
	err := s.Pool.QueryRow(ctx, `SELECT area_name,latitude,longitude FROM volcano_reference WHERE volcano_id=$1`, id).Scan(&r.AreaName, &r.Latitude, &r.Longitude)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, false, nil
	}
	return r, err == nil, err
}
