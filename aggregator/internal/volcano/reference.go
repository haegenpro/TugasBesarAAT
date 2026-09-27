// Package volcano provides the static volcano_id -> area_name/lat/lon
// lookup, backed by the Canonical Store's volcano_reference table. Coords
// are never hardcoded in mapping code.
package volcano

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Reference struct {
	AreaName  string
	Latitude  float64
	Longitude float64
}

// Lookuper is the interface mapping/poller code depends on, so volcanic
// mapping can be tested against a fake without a real Postgres.
type Lookuper interface {
	Get(ctx context.Context, volcanoID string) (Reference, bool, error)
}

type Lookup struct {
	pool *pgxpool.Pool
}

func NewLookup(pool *pgxpool.Pool) *Lookup {
	return &Lookup{pool: pool}
}

func (l *Lookup) Get(ctx context.Context, volcanoID string) (Reference, bool, error) {
	var ref Reference
	err := l.pool.QueryRow(ctx,
		`SELECT area_name, latitude, longitude FROM volcano_reference WHERE volcano_id = $1`,
		volcanoID,
	).Scan(&ref.AreaName, &ref.Latitude, &ref.Longitude)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Reference{}, false, nil
		}
		return Reference{}, false, err
	}
	return ref, true, nil
}
