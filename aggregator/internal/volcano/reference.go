// Package volcano provides the static volcano_id -> area_name/lat/lon
// lookup, backed by the Canonical Store's volcano_reference table. Coords
// are never hardcoded in mapping code.
package volcano

import "context"

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
