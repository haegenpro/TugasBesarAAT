package satellite

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
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

type broker struct {
	mu        sync.Mutex
	positions []int64
	schema    string
	registry  *avrowire.Client
}

func (b *broker) PublishMsg(ctx context.Context, m *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	reader, err := avrowire.NewReader(b.schema)
	if err != nil {
		return nil, err
	}
	e, err := rawinput.Decode(ctx, b.registry, reader, m.Data)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.positions = append(b.positions, *e.Position)
	return &jetstream.PubAck{}, nil
}

func detectionEvent(offset int64) string {
	return fmt.Sprintf("id: %d\nevent: detection\ndata: {\"satellite\":\"N20\",\"acq_date\":\"2026-11-12\",\"acq_time\":\"%04d\",\"latitude\":-2.5,\"longitude\":113.8,\"confidence\":\"h\",\"frp\":1}\n\n", offset, offset%2400)
}

func labStore(t *testing.T) *store.Store {
	t.Helper()
	raw := os.Getenv("M2_DATABASE_URL")
	if raw == "" {
		t.Skip("isolated database unavailable")
	}
	u, err := url.Parse(raw)
	require.NoError(t, err)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, raw)
	require.NoError(t, err)
	name := "satellite_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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

func TestConnectorResumesWithoutGapsOrDuplicates(t *testing.T) {
	db := labStore(t)
	var connections atomic.Int64
	requested := make(chan int64, 16)
	feed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/session" {
			_, _ = w.Write([]byte(`{"latest_offset":12}`))
			return
		}
		from, _ := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)
		n := connections.Add(1)
		requested <- from
		if n == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		write := func(s string) { _, _ = io.WriteString(w, s); flusher.Flush() }
		switch n {
		case 2:
			for o := from; o < from+4; o++ {
				write(detectionEvent(o))
			}
		case 3:
			write(detectionEvent(from - 1))
			write(detectionEvent(from))
			write("id: " + strconv.FormatInt(from+1, 10) + "\nevent: detection\ndata: {\"broken\":true}\n\n")
			write(detectionEvent(from + 3))
		default:
			write("event: heartbeat\ndata: {\"virtual_now\":\"2026-11-12T02:52:00Z\"}\n\n")
			var burst strings.Builder
			for o := from; o <= 12; o++ {
				burst.WriteString(detectionEvent(o))
				if o == from+1 {
					burst.WriteString("event: heartbeat\ndata:\n\n")
				}
			}
			write(burst.String())
			write(": idle\n\n")
			<-r.Context().Done()
		}
	}))
	defer feed.Close()
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
	client := avrowire.NewClient(registry.URL)
	b := &broker{schema: string(schema), registry: client}
	progress := &ingestion.SourceStore{DB: db, Durability: labDurability{pool: db.Pool}, Fence: authority, Source: Source, Namespace: "session-a"}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sourceHTTP, err := acquisition.Client(authority, Source, feed.URL, 0, 64<<10)
	require.NoError(t, err)
	c := &Connector{MetadataKey: func(ctx context.Context) (string, error) { return authority.DispatchWindow(ctx, 5*time.Second) }, BaseURL: feed.URL, Token: "t", HTTP: sourceHTTP, Progress: progress, Sender: rawinput.Sender{JS: b, Registry: client, SchemaText: string(schema)}, Start: 1, IdleTimeout: 500 * time.Millisecond, FlushEvery: 2, FlushAfter: time.Hour, MaxEventSize: 1 << 20, Logger: logger}
	coordinator, err := acquisition.New(authority, logger)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); coordinator.Run(ctx, Source, c.Run) }()
	require.Eventually(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.positions) == 12
	}, 10*time.Second, 20*time.Millisecond)
	cancel()
	<-done
	want := []int64{}
	for o := int64(1); o <= 12; o++ {
		want = append(want, o)
	}
	require.Equal(t, want, b.positions)
	p, err := progress.Progress(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, int64(13), p.Position)
	var quarantined int
	require.NoError(t, db.Pool.QueryRow(context.Background(), `SELECT count(*) FROM ingestion_failures WHERE source='SATELIT' AND status='parked'`).Scan(&quarantined))
	require.Equal(t, 1, quarantined)
	close(requested)
	froms := []int64{}
	for f := range requested {
		froms = append(froms, f)
	}
	require.Equal(t, []int64{1, 1, 5, 7}, froms[:4])
}
