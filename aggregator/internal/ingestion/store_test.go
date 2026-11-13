package ingestion

import (
	"aggregator/internal/canonical"
	"aggregator/internal/ownership"
	"aggregator/internal/rawinput"
	"aggregator/internal/store"
	"bytes"
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type testDurability struct{ pool *pgxpool.Pool }

func (d testDurability) Prepare(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "SET LOCAL synchronous_commit=on")
	return err
}
func (d testDurability) ConfirmRecord(ctx context.Context, q string, args ...any) error {
	var exists bool
	err := d.pool.QueryRow(ctx, q, args...).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		return canonical.Unavailable()
	}
	return nil
}
func ingestionLab(t *testing.T) (*Store, canonical.Ownership) {
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
	t.Cleanup(func() { admin.Close(context.Background()) })
	name := "ingestion_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	require.NoError(t, err)
	u.Path = "/" + name
	c, err := pgx.Connect(ctx, u.String())
	require.NoError(t, err)
	t.Cleanup(func() { c.Close(context.Background()) })
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	require.NoError(t, err)
	require.NoError(t, Migrate(ctx, c, root, ""))
	require.NoError(t, Migrate(ctx, c, root, ""))
	db, err := store.New(ctx, u.String(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	t.Cleanup(db.Close)
	if os.Getenv("M2_EVIDENCE_DIR") == "" {
		t.Cleanup(func() {
			db.Close()
			c.Close(ctx)
			_, _ = admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
		})
	}
	a, err := ownership.NewAcquisition(db.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	lease, err := a.Acquire(ctx, canonical.AcquireOwnership{Scope: "acquisition:BMKG", Holder: "ingestion-fixture"})
	require.NoError(t, err)
	return &Store{DB: db, Durability: testDurability{pool: db.Pool}, Fence: a, Namespace: "ingestion-fixture", Start: 7, Quota: 4 << 20}, lease.Ownership
}
func TestM3IngestionBytesQuotaAndPublishBoundary(t *testing.T) {
	s, owner := ingestionLab(t)
	ctx := context.Background()
	payload := bytes.Repeat([]byte{0xff, 0xfe, 0}, 50000)
	meta := CaptureContext{Cursor: rawinput.CaptureCursor{Version: 1, Entity: "seismic-events", History: true, RequestStarted: time.Now().UTC()}, ReceivedAt: "2026-11-13T00:00:00Z", CorrelationID: "fixture", Owner: owner, HTTPStatus: 200}
	capture, err := s.Capture(ctx, owner, meta, bytes.NewReader(payload))
	require.NoError(t, err)
	require.Equal(t, int64(7), capture.Position)
	stored, err := s.Payload(ctx, *capture)
	require.NoError(t, err)
	require.Equal(t, payload, stored)
	var chunks int
	require.NoError(t, s.DB.Pool.QueryRow(ctx, `SELECT count(*) FROM ingestion_payload_chunks`).Scan(&chunks))
	require.Equal(t, 3, chunks)
	cursor := meta.Cursor.Encode()
	e := rawinput.Envelope{Version: 1, Source: "BMKG", Namespace: s.Namespace, Position: &capture.Position, Operation: "update", Cursor: &cursor, ReceivedAt: meta.ReceivedAt, Payload: payload, Size: int64(len(payload)), Digest: rawinput.Hash(payload), ContentType: "application/avro", CorrelationID: "fixture", OwnershipScope: owner.Scope, OwnershipEpoch: owner.Epoch, OwnershipHolder: owner.Holder}
	e.MessageID = rawinput.ID(e)
	require.NoError(t, s.Finalize(ctx, owner, *capture, e))
	next, err := s.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, e.MessageID, next.Envelope.MessageID)
	var through int64
	require.NoError(t, s.DB.Pool.QueryRow(ctx, `SELECT published_through FROM ingestion_state`).Scan(&through))
	require.Equal(t, int64(6), through)
	// A publish reply may be lost. Until confirmed, no source cursor advances.
	require.NoError(t, s.Published(ctx, owner, *capture, e))
	require.NoError(t, s.Published(ctx, owner, *capture, e))
	next, err = s.Next(ctx)
	require.NoError(t, err)
	require.Nil(t, next)
	_, err = s.Capture(ctx, owner, meta, io.LimitReader(zeroReader{}, s.Quota))
	require.Error(t, err)
	require.NoError(t, s.DB.Pool.QueryRow(ctx, `SELECT next_position FROM ingestion_state`).Scan(&through))
	require.Equal(t, int64(8), through)
}

type zeroReader struct{}

func (zeroReader) Read(b []byte) (int, error) { clear(b); return len(b), nil }
