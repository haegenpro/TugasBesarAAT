package store

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/ownership"
	"aggregator/internal/publisher"
	"aggregator/internal/rawinput"
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/linkedin/goavro/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func m3Path(t *testing.T, broker, loseReply bool) {
	t.Helper()
	dsn, c := m2Database(t, false)
	m2Schema(t, c, "postgres", false)
	s := m2Store(t, dsn)
	s.Canonical.Durability = m3LabDurability{pool: s.Pool}
	ctx := context.Background()
	p, err := ownership.NewProcessor(s.Pool, 15*time.Second)
	require.NoError(t, err)
	s.Processor = p
	pl, err := p.Acquire(ctx, canonical.AcquireOwnership{Scope: "processor:BMKG", Holder: "processor"})
	require.NoError(t, err)
	authority, err := ownership.New(s.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	s.Canonical.Fence = authority
	delivery, err := authority.Acquire(ctx, canonical.AcquireOwnership{Scope: "canonical:BMKG", Holder: "delivery"})
	require.NoError(t, err)
	rawSchema, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "raw-input-v1.avsc"))
	require.NoError(t, err)
	seismic, err := os.ReadFile(filepath.Join("..", "..", "schemas", "seismic.avsc"))
	require.NoError(t, err)
	reader, err := avrowire.NewSourceReader(string(seismic), "SeismicHistoryPage")
	require.NoError(t, err)
	// Real HTTP registry protocol + registered codecs. This local test registry
	// is intentionally not evidence of deployed Apicurio durability/availability.
	texts := map[int]string{1: string(rawSchema), 2: string(seismic)}
	next := 3
	registryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/schemas/ids/") {
			id, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/schemas/ids/"))
			_ = json.NewEncoder(w).Encode(map[string]string{"schema": texts[id]})
			return
		}
		if r.Method == "PUT" {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		var in struct {
			Schema string `json:"schema"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&in))
		id := next
		next++
		texts[id] = in.Schema
		_ = json.NewEncoder(w).Encode(map[string]int{"id": id})
	}))
	defer registryServer.Close()
	registry := avrowire.NewClient(registryServer.URL)
	registry.SetCache(SchemaCache{Pool: s.Pool})
	codec, err := goavro.NewCodec(string(seismic))
	require.NoError(t, err)
	upstream, err := avrowire.Encode(&avrowire.Schema{ID: 2, Text: string(seismic), Codec: codec}, []any{map[string]any{"event_id": "m3-quake", "magnitude": 6.1, "depth_km": 10.0, "epicenter_lat": -6.0, "epicenter_lon": 107.0, "region_name": "Fixture", "occurred_at": "2026-11-13T00:00:00Z", "potential_tsunami": false}})
	require.NoError(t, err)
	position := int64(1)
	cursor := rawinput.CaptureCursor{Version: 1, Entity: "seismic-events", Full: true, SourceSequence: 5, RequestStarted: time.Date(2026, 11, 13, 0, 1, 0, 0, time.UTC)}.Encode()
	tp := "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	e := rawinput.Envelope{Version: 1, Source: "BMKG", Namespace: "m3-path", Operation: "update", Position: &position, Cursor: &cursor, ReceivedAt: "2026-11-13T00:01:01Z", Payload: upstream, Size: int64(len(upstream)), Digest: rawinput.Hash(upstream), ContentType: avrowire.MediaType, CorrelationID: "m3-path-trace", Traceparent: &tp, OwnershipScope: "acquisition:BMKG", OwnershipEpoch: 1, OwnershipHolder: "producer"}
	e.MessageID = rawinput.ID(e)
	s.Inputs.Starts = map[string]int64{InputSession("BMKG", e.Namespace): 1}
	var js jetstream.JetStream
	var message jetstream.Msg
	var nc *nats.Conn
	if broker {
		nc, err = nats.Connect(os.Getenv("M3_NATS_URL"))
		require.NoError(t, err)
		defer nc.Close()
		js, err = jetstream.New(nc)
		require.NoError(t, err)
		consumer, err := rawinput.Ensure(ctx, js, rawinput.BMKG, 3)
		require.NoError(t, err)
		schema, err := registry.Lookup(ctx, 1)
		require.NoError(t, err)
		frame, err := rawinput.Encode(schema, e)
		require.NoError(t, err)
		_, err = js.Publish(ctx, rawinput.BMKG.Subject, frame, jetstream.WithMsgID(e.MessageID))
		require.NoError(t, err)
		fetch, err := consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
		require.NoError(t, err)
		for m := range fetch.Messages() {
			message = m
		}
		require.NotNil(t, message)
		rawReader, err := avrowire.NewReader(string(rawSchema))
		require.NoError(t, err)
		e, err = rawinput.Decode(ctx, registry, rawReader, message.Data())
		require.NoError(t, err)
	}
	request := rawinput.AppendRequest{Version: 1, Ownership: pl.Ownership, Messages: []rawinput.Envelope{e}}
	receipt, err := s.AppendHistory(ctx, request)
	require.NoError(t, err)
	require.Equal(t, int64(1), receipt.Through)
	// Synchronized crash window: the durable receipt exists, but transport ACK
	// has not occurred. Explicit Nak forces real redelivery without timing sleeps.
	if broker {
		require.NoError(t, message.Nak())
		consumer, err := js.Consumer(ctx, rawinput.BMKG.Name, rawinput.BMKG.Consumer)
		require.NoError(t, err)
		fetch, err := consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
		require.NoError(t, err)
		message = nil
		for m := range fetch.Messages() {
			message = m
		}
		require.NotNil(t, message)
	}
	receipt, err = s.AppendHistory(ctx, request)
	require.NoError(t, err)
	require.Equal(t, "duplicate", receipt.Items[0].Outcome)
	if broker {
		require.NoError(t, message.DoubleAck(ctx))
	}
	prepared, err := s.PrepareBMKG(ctx, pl.Ownership, e.Namespace, registry, map[string]*avrowire.SourceReader{"seismic-events": reader})
	require.NoError(t, err)
	require.True(t, prepared)
	var received, pf, applied int64
	require.NoError(t, c.QueryRow(ctx, `SELECT received_through,prepared_through,applied_through FROM input_sessions`).Scan(&received, &pf, &applied))
	require.Equal(t, int64(1), pf)
	require.Zero(t, applied)
	// Canonical is intentionally paused after preparation; the durable input and
	// immutable operation identities remain available independently of its ACK.
	var count int
	require.NoError(t, c.QueryRow(ctx, `SELECT count(*) FROM hazard_events`).Scan(&count))
	require.Zero(t, count)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if loseReply {
		var preparedBody []byte
		require.NoError(t, c.QueryRow(ctx, `SELECT batch FROM input_outputs`).Scan(&preparedBody))
		proxy, err := newCommitProxy(ctx, dsn)
		require.NoError(t, err)
		defer proxy.Close()
		s.Pool.Close()
		s.Pool, err = pgxpool.New(ctx, proxy.DSN)
		require.NoError(t, err)
		attempt, stop := context.WithTimeout(ctx, canonical.Deadline)
		_, err = s.ApplyInputOutput(attempt, pl.Ownership, delivery.Ownership, e.Namespace)
		stop()
		require.Error(t, err)
		select {
		case <-proxy.Dropped:
		case <-time.After(3 * time.Second):
			t.Fatal("actual COMMIT reply was not dropped")
		}
		s.Pool.Close()
		s.Pool, err = pgxpool.New(ctx, dsn)
		require.NoError(t, err)
		s.Canonical.Durability = m3LabDurability{pool: s.Pool}
		called := false
		_, err = s.DeliverNext(ctx, func(context.Context, OutboxEvent) error { called = true; return nil })
		require.Error(t, err)
		require.False(t, called, "publication must wait for canonical decision resolution")
		require.NoError(t, s.RecoverAttempts(ctx))
		// Recovery uses new process-incarnation authority, never the token of
		// an unknown/expired sender. Immutable prepared identities stay intact.
		processor, err := ownership.NewProcessor(s.Pool, 15*time.Second)
		require.NoError(t, err)
		currentDelivery, err := ownership.New(s.Pool, 15*time.Second, canonical.Deadline)
		require.NoError(t, err)
		require.NoError(t, processor.Release(ctx, pl.Ownership))
		require.NoError(t, currentDelivery.Release(ctx, delivery.Ownership))
		pl, err = processor.Acquire(ctx, canonical.AcquireOwnership{Scope: "processor:BMKG", Holder: "processor-successor"})
		require.NoError(t, err)
		delivery, err = currentDelivery.Acquire(ctx, canonical.AcquireOwnership{Scope: "canonical:BMKG", Holder: "delivery-successor"})
		require.NoError(t, err)
		s.Processor = processor
		s.Canonical.Fence = currentDelivery
		var persisted []byte
		require.NoError(t, c.QueryRow(ctx, `SELECT batch FROM input_outputs`).Scan(&persisted))
		require.JSONEq(t, string(preparedBody), string(persisted))
	}
	ok, err := s.ApplyInputOutput(ctx, pl.Ownership, delivery.Ownership, e.Namespace)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = s.ApplyInputOutput(ctx, pl.Ownership, delivery.Ownership, e.Namespace)
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, c.QueryRow(ctx, `SELECT applied_through FROM input_sessions`).Scan(&applied))
	require.Equal(t, int64(1), applied)
	var id string
	require.NoError(t, c.QueryRow(ctx, `SELECT hazard_id::text FROM hazard_events`).Scan(&id))
	event, err := s.GetByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "m3-quake", event.SourceRefID)
	if loseReply {
		var operations, revision, outboxes int
		require.NoError(t, c.QueryRow(ctx, `SELECT (SELECT count(*) FROM canonical_operations),(SELECT revision FROM hazard_events),(SELECT count(*) FROM hazard_outbox)`).Scan(&operations, &revision, &outboxes))
		require.Equal(t, 1, operations)
		require.Equal(t, 1, revision)
		require.Equal(t, 1, outboxes)
	}
	if broker {
		stream := "M3_CANONICAL"
		subject := "m3.canonical"
		_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: stream, Subjects: []string{subject}, Replicas: 3, Storage: jetstream.FileStorage})
		require.NoError(t, err)
		pub, err := publisher.New(ctx, os.Getenv("M3_NATS_URL"), stream, subject, registry, slog.New(slog.NewTextHandler(io.Discard, nil)))
		require.NoError(t, err)
		defer pub.Close()
		ok, err = s.DeliverNext(ctx, func(ctx context.Context, e OutboxEvent) error { return pub.Publish(ctx, e.Hazard, e.CorrelationID) })
		require.NoError(t, err)
		require.True(t, ok)
		st, err := js.Stream(ctx, stream)
		require.NoError(t, err)
		m, err := st.GetMsg(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, "m3-path-trace", m.Header.Get("X-Correlation-ID"))
		require.Contains(t, m.Header.Get("Traceparent"), "0123456789abcdef0123456789abcdef")
	}
}
func TestM3PreparedAndAppliedAreDistinct(t *testing.T)          { m3Path(t, false, false) }
func TestM3PersistedOutputSurvivesLostCommitReply(t *testing.T) { m3Path(t, false, true) }
func TestM3RealBrokerReceiptRedeliveryAndOutbox(t *testing.T) {
	if os.Getenv("M3_NATS_URL") == "" {
		t.Skip("run-owned three-replica broker unavailable")
	}
	m3Path(t, true, false)
}
