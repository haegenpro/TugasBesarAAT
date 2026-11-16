package coastal

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/ingestion"
	"aggregator/internal/ownership"
	"aggregator/internal/rawinput"
	"aggregator/internal/store"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

type labDurability struct{ pool *pgxpool.Pool }

func (labDurability) Prepare(context.Context, pgx.Tx) error { return nil }
func (d labDurability) ConfirmRecord(ctx context.Context, q string, args ...any) error {
	var ok bool
	if err := d.pool.QueryRow(ctx, q, args...).Scan(&ok); err != nil {
		return err
	}
	if !ok {
		return canonical.Unavailable()
	}
	return nil
}

type recorder struct {
	mu   sync.Mutex
	msgs []*nats.Msg
	fail bool
}

func (r *recorder) PublishMsg(_ context.Context, m *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return nil, errors.New("broker unavailable")
	}
	r.msgs = append(r.msgs, m)
	return &jetstream.PubAck{}, nil
}

func (r *recorder) ids() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []string{}
	for _, m := range r.msgs {
		out = append(out, m.Header.Get("Nats-Msg-Id"))
	}
	return out
}

func database(t *testing.T) *store.Store {
	t.Helper()
	raw := os.Getenv("M2_DATABASE_URL")
	if raw == "" {
		t.Skip("isolated database unavailable")
	}
	u, err := url.Parse(raw)
	require.NoError(t, err)
	require.Contains(t, []string{"127.0.0.1", "localhost"}, u.Hostname())
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, raw)
	require.NoError(t, err)
	name := "coastal_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	require.NoError(t, err)
	u.Path = "/" + name
	c, err := pgx.Connect(ctx, u.String())
	require.NoError(t, err)
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	require.NoError(t, err)
	require.NoError(t, ingestion.Migrate(ctx, c, root, ""))
	c.Close(ctx)
	db, err := store.New(ctx, u.String(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
		_, _ = admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
		admin.Close(ctx)
	})
	return db
}

func TestAcquisitionPublishesPagesAndResumesFromDurableCursor(t *testing.T) {
	db := database(t)
	pages := map[string]string{
		"":   `{"generated_at":"2026-11-12T00:31:00Z","items":[` + observation + `,{"critical":false,"observation_id":"obs-000121","observed_at":"2026-11-12T00:30:15Z","quality":"suspect","sea_level_anomaly_m":0.1,"station_id":"stn-padang-01","wave_height_m":0.9}],"next_cursor":"c2"}`,
		"c2": `{"generated_at":"2026-11-12T00:31:15Z","items":[],"next_cursor":null}`,
	}
	var mu sync.Mutex
	requested := []string{}
	sensorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requested = append(requested, r.URL.Query().Get("cursor"))
		mu.Unlock()
		_, _ = w.Write([]byte(pages[r.URL.Query().Get("cursor")]))
	}))
	defer sensorServer.Close()
	schema, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "raw-input-v1.avsc"))
	require.NoError(t, err)
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/versions") {
			_, _ = w.Write([]byte(`{"id":1}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": string(schema)})
	}))
	defer registry.Close()
	authority, err := ownership.NewAcquisition(db.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	b := &Breaker{Name: "observations", Threshold: 3, Cooldown: time.Second}
	client := &Client{BaseURL: sensorServer.URL, Token: "t", HTTP: http.DefaultClient, Timeout: time.Second, Limiter: &Limiter{}, Stations: &Breaker{Name: "stations"}, Observations: b}
	progress := &ingestion.SourceStore{DB: db, Durability: labDurability{pool: db.Pool}, Fence: authority, Source: Source, Namespace: "coastal-fixture"}
	repository := &Repository{Source: progress, StaleAfter: time.Minute}
	run := func(steps int, fail bool) *recorder {
		broker := &recorder{fail: fail}
		a := &Acquirer{Client: client, Progress: progress, Sender: rawinput.Sender{JS: broker, Registry: avrowire.NewClient(registry.URL), SchemaText: string(schema)}, PageSize: 100, Interval: time.Millisecond, Status: &Status{StaleAfter: time.Minute, Observations: b, Stations: client.Stations}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		a.Repository = repository
		coordinator, err := acquisition.New(authority, slog.New(slog.NewTextHandler(io.Discard, nil)))
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		coordinator.Run(ctx, Source, func(owned context.Context) {
			owner, err := acquisition.Token(owned, Source)
			require.NoError(t, err)
			require.NoError(t, repository.StartCoverage(owned, owner))
			for range steps {
				_, err := a.Step(owned)
				if fail {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
		})
		return broker
	}
	failed := run(1, true)
	require.Empty(t, failed.msgs)
	expected, err := repository.Expectations(context.Background(), time.Now().Add(-time.Hour), 0, 0, time.Time{})
	require.NoError(t, err)
	require.Len(t, expected.Items, 1, "a critical publish failure must still be counted as expected")
	first := run(1, false)
	require.Len(t, first.msgs, 2)
	reader, err := avrowire.NewReader(string(schema))
	require.NoError(t, err)
	e, err := rawinput.Decode(context.Background(), avrowire.NewClient(registry.URL), reader, first.msgs[0].Data)
	require.NoError(t, err)
	require.Equal(t, "obs-000120", *e.Ref)
	require.Equal(t, int64(0), *e.Position)
	require.Equal(t, "2026-11-12T00:30:00Z", *e.EventTime)
	require.Equal(t, "2026-11-12T00:31:00Z", *e.VirtualNow)
	received, err := time.Parse(time.RFC3339Nano, e.ReceivedAt)
	require.NoError(t, err)
	require.WithinDuration(t, expected.Items[0].FirstReceivedAt, received, time.Microsecond)
	_, err = db.Pool.Exec(context.Background(), `UPDATE source_progress SET position=0,cursor=NULL`)
	require.NoError(t, err)
	replayed := run(1, false)
	require.Equal(t, first.ids(), replayed.ids())
	later := run(2, false)
	require.Empty(t, later.msgs)
	p, err := progress.Progress(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), p.Position)
	require.Equal(t, "c2", *p.Cursor)
	require.Equal(t, []string{"", "", "", "c2", "c2"}, requested)
}
