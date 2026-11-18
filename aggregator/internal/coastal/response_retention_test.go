package coastal

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/canonical"
	"aggregator/internal/ingestion"
	"aggregator/internal/ownership"
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestReassessmentMalformed200MustAllowFreshObservationRead(t *testing.T) {
	db := database(t)
	authority, err := ownership.NewAcquisition(db.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	progress := &ingestion.SourceStore{DB: db, Durability: labDurability{pool: db.Pool}, Fence: authority, Source: Source, Namespace: "review-malformed"}
	var calls atomic.Int64
	sensor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			_, _ = w.Write([]byte(`{"items":`))
			return
		}
		_, _ = w.Write([]byte(`{"generated_at":"2026-11-18T00:00:00Z","items":[],"next_cursor":null}`))
	}))
	defer sensor.Close()
	sourceHTTP, err := acquisition.Client(authority, Source, sensor.URL, 0, 4096)
	require.NoError(t, err)
	client := &Client{BaseURL: sensor.URL, HTTP: sourceHTTP, Timeout: time.Second, Limiter: &Limiter{}, Observations: &Breaker{Name: "observations", Threshold: 100}, Stations: &Breaker{Name: "stations"}}
	acquirer := &Acquirer{Client: client, Progress: progress, PageSize: 100, Status: &Status{StaleAfter: time.Minute, Observations: client.Observations, Stations: client.Stations}}
	coordinator, err := acquisition.New(authority, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	coordinator.Run(context.Background(), Source, func(owned context.Context) {
		_, err := acquirer.Step(owned)
		require.Error(t, err)
		before, e := progress.Progress(owned, 0)
		require.NoError(t, e)
		require.EqualValues(t, 0, before.Generation)
		require.EqualValues(t, 0, before.Position)
		require.Nil(t, before.Cursor)
		var kept []byte
		var disposition string
		require.NoError(t, db.Pool.QueryRow(owned, `SELECT payload FROM ingestion_failure_payloads WHERE namespace='review-malformed'`).Scan(&kept))
		require.Equal(t, []byte(`{"items":`), kept)
		require.NoError(t, db.Pool.QueryRow(owned, `SELECT state FROM acquisition_dispatch WHERE scope='acquisition:SENSOR_PANTAI'`).Scan(&disposition))
		require.Equal(t, "quarantined", disposition)
		for range 3 {
			_, err = acquirer.Step(owned)
		}
		p, progressErr := progress.Progress(owned, 0)
		require.NoError(t, progressErr)
		t.Logf("REVIEW_OBSERVATION: upstream_calls=%d generation=%d last_error=%v", calls.Load(), p.Generation, err)
		require.NoError(t, err, "sensor recovers after its first bad response; subsequent acquisition must reach it")
		require.GreaterOrEqual(t, calls.Load(), int64(2))
	})
}

type failingQuarantine struct {
	ingestion.Durability
	fail bool
}

func (f *failingQuarantine) Prepare(ctx context.Context, tx pgx.Tx) error {
	if f.fail {
		return errors.New("retention unavailable")
	}
	return f.Durability.Prepare(ctx, tx)
}
func TestResponseQuarantineFailureDoesNotEvictCachedInput(t *testing.T) {
	db := database(t)
	authority, err := ownership.NewAcquisition(db.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	durability := &failingQuarantine{Durability: labDurability{pool: db.Pool}, fail: true}
	progress := &ingestion.SourceStore{DB: db, Durability: durability, Fence: authority, Source: Source, Namespace: "retain-before-retire"}
	var calls atomic.Int64
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			_, _ = w.Write([]byte(`{"items":`))
			return
		}
		_, _ = w.Write([]byte(`{"items":[],"next_cursor":null}`))
	}))
	defer source.Close()
	sourceHTTP, err := acquisition.Client(authority, Source, source.URL, 0, 4096)
	require.NoError(t, err)
	client := &Client{BaseURL: source.URL, HTTP: sourceHTTP, Timeout: time.Second, Limiter: &Limiter{}, Observations: &Breaker{Name: "observations", Threshold: 100}, Stations: &Breaker{Name: "stations"}}
	acquirer := &Acquirer{Client: client, Progress: progress, PageSize: 100, Status: &Status{StaleAfter: time.Minute, Observations: client.Observations, Stations: client.Stations}}
	coordinator, err := acquisition.New(authority, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	coordinator.Run(context.Background(), Source, func(owned context.Context) {
		for range 2 {
			_, err = acquirer.Step(owned)
			require.Error(t, err)
		}
		require.EqualValues(t, 1, calls.Load())
		var state string
		var retained int
		require.NoError(t, db.Pool.QueryRow(owned, `SELECT state FROM acquisition_dispatch`).Scan(&state))
		require.Equal(t, "complete", state)
		require.NoError(t, db.Pool.QueryRow(owned, `SELECT count(*) FROM ingestion_failure_payloads`).Scan(&retained))
		require.Zero(t, retained)
		before, err := progress.Progress(owned, 0)
		require.NoError(t, err)
		require.Zero(t, before.Generation)
		durability.fail = false
		_, err = acquirer.Step(owned)
		require.Error(t, err)
		require.EqualValues(t, 1, calls.Load(), "preserve the same cached body before another read")
		_, err = acquirer.Step(owned)
		require.NoError(t, err)
		require.EqualValues(t, 2, calls.Load())
		after, err := progress.Progress(owned, 0)
		require.NoError(t, err)
		require.EqualValues(t, 1, after.Generation)
		require.Zero(t, after.Position)
		require.Nil(t, after.Cursor)
	})
}

type failDisposition struct {
	*ownership.Authority
	fail bool
}

func (f *failDisposition) FinishResponse(ctx context.Context, d acquisition.Dispatch, state string) error {
	if f.fail {
		return canonical.Unavailable()
	}
	return f.Authority.FinishResponse(ctx, d, state)
}
func TestResponseQuarantineSurvivesTakeoverBeforeReceiptRetirement(t *testing.T) {
	db := database(t)
	authority, err := ownership.NewAcquisition(db.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	repo := &failDisposition{Authority: authority, fail: true}
	progress := &ingestion.SourceStore{DB: db, Durability: labDurability{pool: db.Pool}, Fence: authority, Source: Source, Namespace: "retain-takeover"}
	var calls atomic.Int64
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			_, _ = w.Write([]byte(`{"items":`))
			return
		}
		_, _ = w.Write([]byte(`{"items":[],"next_cursor":null}`))
	}))
	defer source.Close()
	sourceHTTP, err := acquisition.Client(repo, Source, source.URL, 0, 4096)
	require.NoError(t, err)
	client := &Client{BaseURL: source.URL, HTTP: sourceHTTP, Timeout: time.Second, Limiter: &Limiter{}, Observations: &Breaker{Name: "observations", Threshold: 100}, Stations: &Breaker{Name: "stations"}}
	acquirer := &Acquirer{Client: client, Progress: progress, PageSize: 100, Status: &Status{StaleAfter: time.Minute, Observations: client.Observations, Stations: client.Stations}}
	for turn := 0; turn < 2; turn++ {
		coordinator, err := acquisition.New(authority, slog.New(slog.NewTextHandler(io.Discard, nil)))
		require.NoError(t, err)
		coordinator.Run(context.Background(), Source, func(owned context.Context) {
			_, err := acquirer.Step(owned)
			require.Error(t, err)
			if turn == 1 {
				_, err = acquirer.Step(owned)
				require.NoError(t, err)
			}
		})
		repo.fail = false
	}
	var count int
	require.NoError(t, db.Pool.QueryRow(context.Background(), `SELECT count(*) FROM ingestion_failure_payloads`).Scan(&count))
	require.Equal(t, 1, count)
	require.EqualValues(t, 2, calls.Load())
	p, err := progress.Progress(context.Background(), 0)
	require.NoError(t, err)
	require.EqualValues(t, 1, p.Generation)
}
func TestResponseQuarantineRejectsInvalidEnvelopeShapes(t *testing.T) {
	for _, bad := range []string{`{"generated_at":"2026-11-18T00:00:00Z"}`, `{"items":{}}`, `{"items":[],"next_cursor":3}`, "{\"items\":[],\"unknown\":\"\xff\"}"} {
		t.Run(fmt.Sprintf("body-%x", []byte(bad)), func(t *testing.T) {
			db := database(t)
			authority, err := ownership.NewAcquisition(db.Pool, 15*time.Second, canonical.Deadline)
			require.NoError(t, err)
			progress := &ingestion.SourceStore{DB: db, Durability: labDurability{pool: db.Pool}, Fence: authority, Source: Source, Namespace: "shape-fixture"}
			var calls atomic.Int64
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					_, _ = w.Write([]byte(bad))
					return
				}
				_, _ = w.Write([]byte(`{"items":[]}`))
			}))
			defer source.Close()
			sourceHTTP, err := acquisition.Client(authority, Source, source.URL, 0, 4096)
			require.NoError(t, err)
			client := &Client{BaseURL: source.URL, HTTP: sourceHTTP, Timeout: time.Second, Limiter: &Limiter{}, Observations: &Breaker{Name: "observations", Threshold: 100}, Stations: &Breaker{Name: "stations"}}
			acquirer := &Acquirer{Client: client, Progress: progress, PageSize: 100, Status: &Status{StaleAfter: time.Minute, Observations: client.Observations, Stations: client.Stations}}
			coordinator, err := acquisition.New(authority, slog.New(slog.NewTextHandler(io.Discard, nil)))
			require.NoError(t, err)
			coordinator.Run(context.Background(), Source, func(owned context.Context) {
				_, err := acquirer.Step(owned)
				require.Error(t, err)
				_, err = acquirer.Step(owned)
				require.NoError(t, err)
			})
			var kept []byte
			require.NoError(t, db.Pool.QueryRow(context.Background(), `SELECT payload FROM ingestion_failure_payloads`).Scan(&kept))
			require.Equal(t, []byte(bad), kept)
			require.EqualValues(t, 2, calls.Load())
		})
	}
}
