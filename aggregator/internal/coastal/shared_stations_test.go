package coastal

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/canonical"
	"aggregator/internal/ingestion"
	"aggregator/internal/ownership"
	"aggregator/internal/rawinput"
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

func TestSharedStationsSlowOrFailedRefreshDoesNotBlockObservations(t *testing.T) {
	db := database(t)
	authority, err := ownership.NewAcquisition(db.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	progress := &ingestion.SourceStore{DB: db, Durability: labDurability{pool: db.Pool}, Fence: authority, Source: Source, Namespace: "station-isolation"}
	var seed struct {
		Items []Station `json:"items"`
	}
	require.NoError(t, json.Unmarshal([]byte(stationsBody), &seed))
	payload, err := json.Marshal(seed.Items)
	require.NoError(t, err)
	_, err = db.Pool.Exec(context.Background(), `INSERT INTO coastal_station_cache(namespace,payload,fetched_at)VALUES($1,$2,clock_timestamp()-interval '10 minutes')`, progress.Namespace, payload)
	require.NoError(t, err)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var metadataCalls, observationCalls atomic.Int64
	sensor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/stations" {
			if metadataCalls.Add(1) == 1 {
				close(started)
			}
			select {
			case <-release:
				w.WriteHeader(503)
				_, _ = w.Write([]byte("metadata unavailable"))
			case <-r.Context().Done():
			}
			return
		}
		observationCalls.Add(1)
		_, _ = w.Write([]byte(`{"items":[],"next_cursor":null}`))
	}))
	defer sensor.Close()
	sourceHTTP, err := acquisition.Client(authority, Source, sensor.URL, 0, 4096)
	require.NoError(t, err)
	client := &Client{BaseURL: sensor.URL, HTTP: sourceHTTP, Timeout: 5 * time.Second, Limiter: &Limiter{}, Observations: &Breaker{Name: "observations", Threshold: 3}, Stations: &Breaker{Name: "stations", Threshold: 1, Cooldown: time.Minute, MaxCooldown: time.Minute}}
	shared := SharedStations{Source: progress}
	acquirer := &Acquirer{Client: client, Progress: progress, PageSize: 100, Interval: 20 * time.Millisecond, Status: &Status{StaleAfter: time.Minute, Observations: client.Observations, Stations: client.Stations}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	acquirer.RefreshStations = func(ctx context.Context) error { return shared.Refresh(ctx, client, time.Minute) }
	coordinator, err := acquisition.New(authority, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); coordinator.Run(ctx, Source, acquirer.Run) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("station refresh did not drain with its source generation")
		}
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("refresh did not start")
	}
	require.Eventually(t, func() bool {
		p, err := progress.Progress(context.Background(), 0)
		return err == nil && p.Generation > 0
	}, time.Second, 20*time.Millisecond, "observation progress must not wait for a five-second metadata timeout")
	validator := &Validator{Stations: &StationCache{Client: shared, TTL: time.Minute}}
	observed, station, outcome, err := validator.Check(context.Background(), rawinput.Envelope{Payload: []byte(observation)})
	require.NoError(t, err)
	require.Equal(t, accepted, outcome)
	require.True(t, observed.Critical)
	require.Equal(t, "Padang", station.Name)
	once.Do(func() { close(release) })
	require.Eventually(t, func() bool { return client.Stations.State() == Open }, time.Second, 20*time.Millisecond)
	before, err := progress.Progress(context.Background(), 0)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		p, err := progress.Progress(context.Background(), 0)
		return err == nil && p.Generation > before.Generation
	}, time.Second, 20*time.Millisecond, "observations must also continue after metadata fails")
	require.EqualValues(t, 1, metadataCalls.Load(), "station breaker must prevent a metadata request surge")
	require.Positive(t, observationCalls.Load())
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("source did not drain")
	}
}

func TestSharedStationsMalformedRefreshRetainsPreviousSnapshotAndRecovers(t *testing.T) {
	db := database(t)
	authority, err := ownership.NewAcquisition(db.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	progress := &ingestion.SourceStore{DB: db, Durability: labDurability{pool: db.Pool}, Fence: authority, Source: Source, Namespace: "stations-invalid"}
	var initial struct {
		Items []Station `json:"items"`
	}
	require.NoError(t, json.Unmarshal([]byte(stationsBody), &initial))
	bytes, err := json.Marshal(initial.Items)
	require.NoError(t, err)
	_, err = db.Pool.Exec(context.Background(), `INSERT INTO coastal_station_cache(namespace,payload,fetched_at)VALUES($1,$2,clock_timestamp()-interval '10 minutes')`, progress.Namespace, bytes)
	require.NoError(t, err)
	var calls atomic.Int64
	sensor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			_, _ = w.Write([]byte(`{"items":`))
			return
		}
		_, _ = w.Write([]byte(strings.Replace(stationsBody, "Padang", "Updated Station", 1)))
	}))
	defer sensor.Close()
	sourceHTTP, err := acquisition.Client(authority, Source, sensor.URL, 0, 4096)
	require.NoError(t, err)
	client := &Client{BaseURL: sensor.URL, HTTP: sourceHTTP, Timeout: time.Second, Limiter: &Limiter{}, Stations: &Breaker{Name: "stations", Threshold: 100}}
	shared := SharedStations{Source: progress}
	coordinator, err := acquisition.New(authority, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	coordinator.Run(context.Background(), Source, func(owned context.Context) {
		require.Error(t, shared.Refresh(owned, client, time.Minute))
		old, err := shared.FetchStations(owned)
		require.NoError(t, err)
		require.Equal(t, "Padang", old[0].Name)
		require.NoError(t, shared.Refresh(owned, client, time.Minute))
		updated, err := shared.FetchStations(owned)
		require.NoError(t, err)
		require.Equal(t, "Updated Station", updated[0].Name)
	})
	require.EqualValues(t, 2, calls.Load())
	var kept []byte
	require.NoError(t, db.Pool.QueryRow(context.Background(), `SELECT payload FROM ingestion_failure_payloads`).Scan(&kept))
	require.Equal(t, []byte(`{"items":`), kept)
	p, err := progress.Progress(context.Background(), 0)
	require.NoError(t, err)
	require.Zero(t, p.Generation)
	require.Zero(t, p.Position)
}
