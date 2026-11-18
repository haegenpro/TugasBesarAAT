package coastal

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/ingestion"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// SharedStations makes validators read the owner's persisted snapshot. They
// never need an acquisition token or open their own connection to the sensor.
type SharedStations struct{ Source *ingestion.SourceStore }

func (s SharedStations) FetchStations(ctx context.Context) ([]Station, error) {
	var body []byte
	if err := s.Source.DB.Pool.QueryRow(ctx, `SELECT payload FROM coastal_station_cache WHERE namespace=$1`, s.Source.Namespace).Scan(&body); err != nil {
		return nil, fmt.Errorf("shared station snapshot unavailable: %w", err)
	}
	var stations []Station
	if err := json.Unmarshal(body, &stations); err != nil {
		return nil, err
	}
	return stations, nil
}
func (s SharedStations) Refresh(ctx context.Context, client *Client, ttl time.Duration) error {
	if ttl < time.Second {
		return fmt.Errorf("station TTL must be at least one second")
	}
	owner, err := acquisition.Token(ctx, Source)
	if err != nil {
		return err
	}
	var due bool
	var window string
	if err = s.Source.DB.Pool.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM coastal_station_cache WHERE namespace=$1 AND fetched_at>clock_timestamp()-$2::double precision*interval '1 second'),floor(extract(epoch FROM clock_timestamp())/$2)::bigint::text`, s.Source.Namespace, ttl.Seconds()).Scan(&due, &window); err != nil {
		return err
	}
	if !due {
		return nil
	}
	items, err := client.FetchStations(acquisition.WithRequestKey(ctx, "stations:"+s.Source.Namespace+":"+window))
	if err != nil {
		return err
	}
	body, err := json.Marshal(items)
	if err != nil {
		return err
	}
	tx, err := s.Source.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = s.Source.Durability.Prepare(ctx, tx); err != nil {
		return err
	}
	if err = s.Source.Fence.Protect(ctx, tx, owner, []canonical.Mutation{{Source: domain.Source(Source)}}); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO coastal_station_cache(namespace,payload) VALUES($1,$2) ON CONFLICT(namespace) DO UPDATE SET payload=excluded.payload,fetched_at=clock_timestamp()`, s.Source.Namespace, body); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return s.Source.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM coastal_station_cache WHERE namespace=$1 AND payload=$2::jsonb)`, s.Source.Namespace, body)
}
