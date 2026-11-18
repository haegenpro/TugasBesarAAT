package coastal

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/canonical"
	"aggregator/internal/ingestion"
	"aggregator/internal/ownership"
	"context"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestSharedStationsOnlySourceOwnerFetches(t *testing.T) {
	db := database(t)
	authority, err := ownership.NewAcquisition(db.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	progress := &ingestion.SourceStore{DB: db, Durability: labDurability{pool: db.Pool}, Fence: authority, Source: Source, Namespace: "stations-fixture"}
	shared := SharedStations{Source: progress}
	var requests atomic.Int64
	sensor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{"items":[{"station_id":"one","name":"Station One","province":"11","latitude":1,"longitude":2}]}`))
	}))
	defer sensor.Close()
	sourceHTTP, err := acquisition.Client(authority, Source, sensor.URL, 0, 4096)
	require.NoError(t, err)
	client := &Client{BaseURL: sensor.URL, HTTP: sourceHTTP, Timeout: time.Second, Limiter: &Limiter{}, Stations: &Breaker{Name: "stations"}}
	ctx := context.Background()
	require.ErrorIs(t, shared.Refresh(ctx, client, time.Minute), acquisition.ErrNotOwner)
	for range 2 {
		coordinator, err := acquisition.New(authority, slog.New(slog.NewTextHandler(io.Discard, nil)))
		require.NoError(t, err)
		coordinator.Run(ctx, Source, func(owned context.Context) { require.NoError(t, shared.Refresh(owned, client, time.Minute)) })
		// Standby validators have no ownership, yet all read the same snapshot.
		cache := &StationCache{Client: shared, TTL: time.Minute}
		station, err := cache.Lookup(ctx, "one")
		require.NoError(t, err)
		require.Equal(t, "Station One", station.Name)
	}
	require.EqualValues(t, 1, requests.Load(), "replica count and ownership turnover must not multiply station fetches")
}
func TestCoastalEmptyPageAdvancesDispatchGeneration(t *testing.T) {
	db := database(t)
	authority, err := ownership.NewAcquisition(db.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	progress := &ingestion.SourceStore{DB: db, Durability: labDurability{pool: db.Pool}, Fence: authority, Source: Source, Namespace: "empty-fixture"}
	var requests atomic.Int64
	sensor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{"generated_at":"2026-11-18T00:00:00Z","items":[],"next_cursor":null}`))
	}))
	defer sensor.Close()
	sourceHTTP, err := acquisition.Client(authority, Source, sensor.URL, 0, 4096)
	require.NoError(t, err)
	client := &Client{BaseURL: sensor.URL, HTTP: sourceHTTP, Timeout: time.Second, Limiter: &Limiter{}, Observations: &Breaker{Name: "observations"}, Stations: &Breaker{Name: "stations"}}
	acquirer := &Acquirer{Client: client, Progress: progress, PageSize: 100, Status: &Status{StaleAfter: time.Minute, Observations: client.Observations, Stations: client.Stations}}
	for range 3 {
		coordinator, err := acquisition.New(authority, slog.New(slog.NewTextHandler(io.Discard, nil)))
		require.NoError(t, err)
		coordinator.Run(context.Background(), Source, func(owned context.Context) { _, err := acquirer.Step(owned); require.NoError(t, err) })
	}
	p, err := progress.Progress(context.Background(), 0)
	require.NoError(t, err)
	require.EqualValues(t, 0, p.Position)
	require.EqualValues(t, 3, p.Generation)
	require.EqualValues(t, 3, requests.Load())
}
