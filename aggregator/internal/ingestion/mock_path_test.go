package ingestion_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"aggregator/internal/acquisition"
	"aggregator/internal/api"
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/ingestion"
	"aggregator/internal/migrations"
	"aggregator/internal/ownership"
	"aggregator/internal/pipeline"
	"aggregator/internal/platform"
	"aggregator/internal/poller"
	"aggregator/internal/publisher"
	"aggregator/internal/rawinput"
	"aggregator/internal/server"
	"aggregator/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// These adapters are confined to this test binary. No production command can
// select a single-copy acknowledgement provider.
type pathDurability struct{ pool *pgxpool.Pool }

func (d pathDurability) Prepare(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "SET LOCAL synchronous_commit=on")
	return err
}
func (d pathDurability) Confirm(context.Context, string) error { return nil }
func (d pathDurability) ConfirmRecord(ctx context.Context, q string, args ...any) error {
	var yes bool
	err := d.pool.QueryRow(ctx, q, args...).Scan(&yes)
	if err != nil {
		return err
	}
	if !yes {
		return canonical.Unavailable()
	}
	return nil
}

type observedSpan struct{ Name, TraceID, SpanID, ParentID, Service string }
type spanSink struct {
	mu     sync.Mutex
	spans  []observedSpan
	source chan struct{}
}

func (s *spanSink) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, span := range spans {
		s.spans = append(s.spans, observedSpan{Name: span.Name(), TraceID: span.SpanContext().TraceID().String(), SpanID: span.SpanContext().SpanID().String(), ParentID: span.Parent().SpanID().String(), Service: "role1-component"})
	}
	return nil
}
func (s *spanSink) Shutdown(context.Context) error { return nil }
func (s *spanSink) collect(w http.ResponseWriter, r *http.Request) {
	b, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		w.WriteHeader(400)
		return
	}
	var batch collectortrace.ExportTraceServiceRequest
	if proto.Unmarshal(b, &batch) != nil {
		w.WriteHeader(400)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, group := range batch.ResourceSpans {
		service := ""
		for _, a := range group.Resource.Attributes {
			if a.Key == "service.name" {
				service = a.Value.GetStringValue()
			}
		}
		for _, scope := range group.ScopeSpans {
			for _, span := range scope.Spans {
				s.spans = append(s.spans, observedSpan{Name: span.Name, TraceID: hex.EncodeToString(span.TraceId), SpanID: hex.EncodeToString(span.SpanId), ParentID: hex.EncodeToString(span.ParentSpanId), Service: service})
				if service == "bmkg-mock" {
					select {
					case s.source <- struct{}{}:
					default:
					}
				}
			}
		}
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write([]byte{})
}
func newPathDatabase(t *testing.T, root, kind string) *store.Store {
	t.Helper()
	ctx := context.Background()
	raw := os.Getenv("M2_DATABASE_URL")
	u, err := url.Parse(raw)
	require.NoError(t, err)
	require.Contains(t, []string{"127.0.0.1", "localhost"}, u.Hostname())
	admin, err := pgx.Connect(ctx, raw)
	require.NoError(t, err)
	t.Cleanup(func() { admin.Close(ctx) })
	name := "m3_" + kind + "_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	require.NoError(t, err)
	u.Path = "/" + name
	conn, err := pgx.Connect(ctx, u.String())
	require.NoError(t, err)
	defer conn.Close(ctx)
	if kind == "input" {
		require.NoError(t, ingestion.Migrate(ctx, conn, root, ""))
	} else {
		require.NoError(t, migrations.Up(ctx, conn, filepath.Join(root, "store", "migrations"), "postgres", ""))
	}
	db, err := store.New(ctx, u.String(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	t.Cleanup(db.Close)
	if dir := os.Getenv("M2_EVIDENCE_DIR"); dir != "" {
		file, err := os.OpenFile(filepath.Join(dir, "databases.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		require.NoError(t, err)
		b, _ := json.Marshal(map[string]string{"test": t.Name(), "database": name, "scope": "run-owned single-copy component"})
		_, err = file.Write(append(b, '\n'))
		require.NoError(t, err)
		require.NoError(t, file.Close())
	} else {
		t.Cleanup(func() {
			db.Close()
			_, _ = admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
		})
	}
	return db
}

func TestM3RealBMKGConnectorToReadAndPublication(t *testing.T) {
	if os.Getenv("M2_DATABASE_URL") == "" || os.Getenv("M3_NATS_URL") == "" {
		t.Skip("run-owned database/three-replica broker required")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	sink := &spanSink{source: make(chan struct{}, 32)}
	collector := httptest.NewServer(http.HandlerFunc(sink.collect))
	defer collector.Close()
	oldProvider, oldPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(sink), sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", "role1-component"))))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(oldProvider)
		otel.SetTextMapPropagator(oldPropagator)
	}()
	var registryMu sync.Mutex
	texts := map[int]string{}
	ids := map[string]int{}
	registryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		registryMu.Lock()
		defer registryMu.Unlock()
		if r.Method == "PUT" {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		if strings.HasPrefix(r.URL.Path, "/schemas/ids/") {
			id, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/schemas/ids/"))
			text, ok := texts[id]
			if !ok {
				w.WriteHeader(404)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"schema": text})
			return
		}
		var in struct {
			Schema string `json:"schema"`
		}
		if json.NewDecoder(r.Body).Decode(&in) != nil {
			w.WriteHeader(400)
			return
		}
		id, ok := ids[in.Schema]
		if !ok {
			id = len(ids) + 1
			ids[in.Schema] = id
			texts[id] = in.Schema
		}
		_ = json.NewEncoder(w).Encode(map[string]int{"id": id})
	}))
	defer registryServer.Close()

	// Run the repository's actual BMKG mock in a child process, not a replacement
	// source handler. It has fresh credentials and a private retained archive.
	executable := filepath.Join(t.TempDir(), "bmkg-mock.exe")
	build := exec.CommandContext(ctx, "go", "-C", filepath.Join(root, "bmkg-mock"), "build", "-o", executable, "./cmd/server")
	buildOutput, err := build.CombinedOutput()
	require.NoError(t, err, string(buildOutput))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	sourceURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	key := uuid.NewString()
	childCtx, stopChild := context.WithCancel(ctx)
	defer stopChild()
	child := exec.CommandContext(childCtx, executable)
	child.Env = append(os.Environ(), "BMKG_PORT="+strconv.Itoa(port), "BMKG_API_KEY="+key, "BMKG_DELAY_MS=0", "BMKG_EVENT_INTERVAL_MS=10000", "SCHEMA_REGISTRY_URL="+registryServer.URL, "SOURCE_ARCHIVE_PATH="+filepath.Join(t.TempDir(), "archive.sqlite"), "OTEL_EXPORTER_OTLP_ENDPOINT="+collector.URL, "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="+collector.URL+"/v1/traces")
	var output bytes.Buffer
	child.Stdout = &output
	child.Stderr = &output
	require.NoError(t, child.Start())
	childDone := make(chan error, 1)
	go func() { childDone <- child.Wait() }()
	defer func() {
		stopChild()
		<-childDone
		if dir := os.Getenv("M2_EVIDENCE_DIR"); dir != "" {
			_ = os.WriteFile(filepath.Join(dir, "bmkg-native.log"), output.Bytes(), 0600)
		}
	}()
	httpClient := platform.Client(2 * time.Second)
	deadline := time.Now().Add(15 * time.Second)
	for {
		req, _ := http.NewRequestWithContext(ctx, "GET", sourceURL+"/health", nil)
		response, e := httpClient.Do(req)
		if e == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				break
			}
		}
		require.True(t, time.Now().Before(deadline), "real source did not become healthy")
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}

	inputDB := newPathDatabase(t, root, "input")
	canonicalDB := newPathDatabase(t, root, "canonical")
	inputAuthority, err := ownership.NewAcquisition(inputDB.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	owned := &ingestion.Store{DB: inputDB, Durability: pathDurability{pool: inputDB.Pool}, Fence: inputAuthority, Namespace: "real-mock-v1", Start: 1, Quota: 16 << 20}
	registry := avrowire.NewClient(registryServer.URL)
	registry.SetCache(store.SchemaCache{Pool: inputDB.Pool})
	seismic, err := avrowire.LoadSourceReader(filepath.Join(root, "aggregator/schemas/seismic.avsc"), "SeismicHistoryPage")
	require.NoError(t, err)
	warning, err := avrowire.LoadSourceReader(filepath.Join(root, "aggregator/schemas/tsunami.avsc"), "WarningHistoryPage")
	require.NoError(t, err)
	readers := map[string]*avrowire.SourceReader{"seismic-events": seismic, "tsunami-warnings": warning}
	rawText, err := os.ReadFile(filepath.Join(root, "docs/fixtures/raw-input-v1.avsc"))
	require.NoError(t, err)
	rawReader, err := avrowire.NewReader(string(rawText))
	require.NoError(t, err)
	nc, err := nats.Connect(os.Getenv("M3_NATS_URL"))
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	consumer, err := rawinput.Ensure(ctx, js, rawinput.BMKG, 3)
	require.NoError(t, err)
	rootCtx, rootSpan := platform.Start(ctx, "M3 real source to read and publication")
	defer rootSpan.End()
	worker := &ingestion.Worker{Store: owned, Registry: registry, SchemaText: string(rawText), Readers: readers, JS: js, HTTP: httpClient, BaseURL: sourceURL, APIKey: key}
	coordinator, err := acquisition.New(inputAuthority, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	step := make(chan error, 1)
	go coordinator.Run(rootCtx, "BMKG", func(ownedCtx context.Context) {
		call, stop := context.WithTimeout(ownedCtx, 2*time.Second)
		defer stop()
		step <- worker.Step(call)
	})
	select {
	case err = <-step:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	processor, err := ownership.NewProcessor(canonicalDB.Pool, 30*time.Second)
	require.NoError(t, err)
	delivery, err := ownership.New(canonicalDB.Pool, 30*time.Second, canonical.Deadline)
	require.NoError(t, err)
	canonicalDB.Processor = processor
	canonicalDB.Canonical.Fence = delivery
	canonicalDB.Canonical.Durability = pathDurability{pool: canonicalDB.Pool}
	canonicalDB.Inputs.Starts = map[string]int64{store.InputSession("BMKG", owned.Namespace): 1}
	p, err := processor.Acquire(rootCtx, canonical.AcquireOwnership{Scope: "processor:BMKG", Holder: "component-processor"})
	require.NoError(t, err)
	d, err := delivery.Acquire(rootCtx, canonical.AcquireOwnership{Scope: "canonical:BMKG", Holder: "component-delivery"})
	require.NoError(t, err)
	messages, err := consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	require.NoError(t, err)
	var message jetstream.Msg
	for m := range messages.Messages() {
		message = m
	}
	require.NotNil(t, message)
	receiver := pipeline.Worker{Store: canonicalDB, Registry: registry, Reader: rawReader}
	require.NoError(t, receiver.Handoff(rootCtx, message, "BMKG", owned.Namespace, p.Ownership))
	var count int
	require.NoError(t, canonicalDB.Pool.QueryRow(rootCtx, `SELECT count(*) FROM hazard_events`).Scan(&count))
	require.Zero(t, count, "durable handoff must not imply canonical application")
	_, err = canonicalDB.PrepareBMKG(rootCtx, p.Ownership, owned.Namespace, registry, readers)
	require.NoError(t, err)
	for {
		applied, err := canonicalDB.ApplyInputOutput(rootCtx, p.Ownership, d.Ownership, owned.Namespace)
		require.NoError(t, err)
		if !applied {
			break
		}
	}
	var id string
	require.NoError(t, canonicalDB.Pool.QueryRow(rootCtx, `SELECT hazard_id::text FROM hazard_events WHERE source='BMKG' AND source_ref_id='evt-00001'`).Scan(&id))
	deps := &api.Dependencies{}
	deps.SetStoreReady(true)
	router := server.New(slog.New(slog.NewTextHandler(io.Discard, nil)), api.New(canonicalDB, poller.NewPVMBGStatus(), deps, time.Minute, time.Minute))
	readServer := httptest.NewServer(platform.Handler("aggregator", router))
	defer readServer.Close()
	request, err := http.NewRequestWithContext(rootCtx, "GET", readServer.URL+"/internal/hazards/"+id, nil)
	require.NoError(t, err)
	platform.Inject(rootCtx, request.Header)
	response, err := httpClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, 200, response.StatusCode)
	var result struct {
		Data struct {
			ID string `json:"hazard_id"`
		} `json:"data"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
	response.Body.Close()
	require.Equal(t, id, result.Data.ID)
	subject, stream := "m3.real.canonical", "M3_REAL_CANONICAL"
	_, err = js.CreateStream(rootCtx, jetstream.StreamConfig{Name: stream, Subjects: []string{subject}, Replicas: 3, Storage: jetstream.FileStorage})
	require.NoError(t, err)
	pub, err := publisher.New(rootCtx, os.Getenv("M3_NATS_URL"), stream, subject, registry, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	defer pub.Close()
	published, err := canonicalDB.DeliverNext(rootCtx, func(ctx context.Context, e store.OutboxEvent) error {
		return pub.Publish(ctx, e.Hazard, e.CorrelationID)
	})
	require.NoError(t, err)
	require.True(t, published)
	rootSpan.End()
	traceID := rootSpan.SpanContext().TraceID().String()
	// Wait for the actual child process's OTLP export, not a guessed delay.
	found := false
	wait := time.NewTimer(12 * time.Second)
	defer wait.Stop()
	for !found {
		sink.mu.Lock()
		for _, span := range sink.spans {
			if span.Service == "bmkg-mock" && span.TraceID == traceID {
				found = true
				break
			}
		}
		sink.mu.Unlock()
		if found {
			break
		}
		select {
		case <-sink.source:
		case <-wait.C:
			t.Fatal("real source trace export not observed")
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	sink.mu.Lock()
	spans := []observedSpan{}
	names := map[string]bool{}
	for _, span := range sink.spans {
		if span.TraceID == traceID {
			spans = append(spans, span)
			names[span.Name] = true
		}
	}
	sink.mu.Unlock()
	require.True(t, names["durable raw handoff"])
	require.True(t, names["prepare BMKG input"])
	require.True(t, names["canonical output delivery"])
	require.True(t, names["publish "+subject])
	if dir := os.Getenv("M2_EVIDENCE_DIR"); dir != "" {
		b, err := json.MarshalIndent(map[string]any{"scope": "actual BMKG process/HTTP + real PostgreSQL/JetStream + test registry and single-copy durability adapter", "trace_id": traceID, "spans": spans}, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "real-source-trace.json"), b, 0600))
	}
}
