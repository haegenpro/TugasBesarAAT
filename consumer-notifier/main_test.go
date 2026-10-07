package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type testMsg struct {
	meta     *jetstream.MsgMetadata
	raw      []byte
	header   nats.Header
	ackErr   error
	nakErr   error
	ackCalls int
	nakCalls int
}

func (m *testMsg) Metadata() (*jetstream.MsgMetadata, error) { return m.meta, nil }
func (m *testMsg) Data() []byte                              { return m.raw }
func (m *testMsg) Headers() nats.Header                      { return m.header }
func (m *testMsg) Subject() string                           { return "hazard.events" }
func (m *testMsg) Reply() string                             { return "" }
func (m *testMsg) Ack() error                                { return nil }
func (m *testMsg) DoubleAck(context.Context) error           { m.ackCalls++; return m.ackErr }
func (m *testMsg) Nak() error                                { return nil }
func (m *testMsg) NakWithDelay(time.Duration) error          { m.nakCalls++; return m.nakErr }
func (m *testMsg) InProgress() error                         { return nil }
func (m *testMsg) Term() error                               { return nil }
func (m *testMsg) TermWithReason(string) error               { return nil }

const testSchema = `{"type":"record","name":"HazardEnvelope","fields":[{"name":"envelope_version","type":"int"},{"name":"published_at","type":"string"},{"name":"correlation_id","type":"string"},{"name":"payload","type":{"type":"record","name":"HazardEvent","fields":[{"name":"hazard_id","type":"string"},{"name":"source","type":"string"},{"name":"source_ref_id","type":"string"},{"name":"hazard_type","type":"string"},{"name":"severity","type":"string"},{"name":"area_name","type":"string"},{"name":"latitude","type":"double"},{"name":"longitude","type":"double"},{"name":"occurred_at","type":"string"},{"name":"ingested_at","type":"string"},{"name":"attributes","type":{"type":"map","values":"bytes"}}]}}]}`

func testFrame(t *testing.T) []byte {
	t.Helper()
	codec, err := goavro.NewCodec(testSchema)
	if err != nil {
		t.Fatal(err)
	}
	data, err := codec.BinaryFromNative(nil, map[string]any{
		"envelope_version": int32(1), "published_at": "2026-10-02T00:00:00Z", "correlation_id": "cid-1",
		"payload": map[string]any{"hazard_id": "hazard-1", "source": "PVMBG", "source_ref_id": "report-1", "hazard_type": "VOLCANIC", "severity": "SIAGA", "area_name": "Merapi", "latitude": -7.0, "longitude": 110.0, "occurred_at": "2026-10-02T00:00:00Z", "ingested_at": "2026-10-02T00:00:01Z", "attributes": map[string]any{"confidence_level": []byte(`0.88`)}}})
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, 5)
	binary.BigEndian.PutUint32(frame[1:], 17)
	return append(frame, data...)
}

func TestDecodeCachesSchemaAndPreservesAttributeType(t *testing.T) {
	path := filepath.Join(t.TempDir(), "consumer.db")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	lookups := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lookups++
		if r.URL.Path != "/schemas/ids/17" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": testSchema})
	}))
	defer server.Close()
	client := newSchemaClient(server.URL, db)
	frame := testFrame(t)
	for i := 0; i < 2; i++ {
		e, err := client.decode(context.Background(), frame)
		if err != nil {
			t.Fatal(err)
		}
		if e.HazardID != "hazard-1" || e.CorrelationID != "cid-1" || e.Payload.Attributes["confidence_level"] != float64(0.88) {
			t.Fatalf("unexpected envelope: %+v", e)
		}
	}
	if lookups != 1 {
		t.Fatalf("registry lookups = %d, want 1", lookups)
	}
	if _, err := client.decode(context.Background(), frame[:6]); !errors.Is(err, errCorruptPayload) {
		t.Fatalf("truncated Avro should be corrupt: %v", err)
	}
	// The validated writer schema must still work after a process restart and registry outage.
	server.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	offline := newSchemaClient(server.URL, reopened)
	if _, err := offline.decode(context.Background(), frame); err != nil {
		t.Fatalf("persistent schema cache did not survive restart: %v", err)
	}
}

func TestLookupClassifiesRegistryFailures(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/17") {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := newSchemaClient(server.URL, db)
	if _, err := client.codec(context.Background(), 17); !errors.Is(err, errUnknownSchema) {
		t.Fatalf("404 = %v", err)
	}
	if _, err := client.codec(context.Background(), 18); !errors.Is(err, errRegistryUnavailable) {
		t.Fatalf("503 = %v", err)
	}
}

func TestStoreDeduplicatesAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "consumer.db")
	e := envelope{HazardID: "hazard-1", CorrelationID: "cid-1", Payload: hazardEvent{HazardID: "hazard-1", Severity: "SIAGA", Source: "PVMBG", AreaName: "Merapi", OccurredAt: "now"}}
	meta := &jetstream.MsgMetadata{Stream: "HAZARDS", Sequence: jetstream.SequencePair{Stream: 41}}
	raw := []byte("same published message")
	for i := 0; i < 2; i++ {
		db, err := openDB(path)
		if err != nil {
			t.Fatal(err)
		}
		a := &app{haltController: &haltController{}, cfg: config{Role: "notifier"}, db: db}
		if err := a.save(context.Background(), meta, raw, e); err != nil {
			t.Fatal(err)
		}
		processed, err := a.alreadyProcessed(context.Background(), meta, raw)
		if err != nil || !processed {
			t.Fatalf("processed=%v error=%v", processed, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM records`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("records = %d, want 1", count)
	}
	if err := db.QueryRow(`SELECT count(*) FROM processed_messages`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("processed messages = %d, want 1", count)
	}
	if _, err := (&app{haltController: &haltController{}, db: db}).alreadyProcessed(context.Background(), meta, []byte("different bytes")); !errors.Is(err, errConsumerIncompatible) {
		t.Fatalf("reused stream sequence should halt: %v", err)
	}
}

func TestUnknownSchemaCounterSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "consumer.db")
	meta := &jetstream.MsgMetadata{Stream: "HAZARDS", Sequence: jetstream.SequencePair{Stream: 41}}
	start := time.Now().Add(-2 * time.Minute).Truncate(time.Millisecond)
	for want := 1; want <= 5; want++ {
		db, err := openDB(path)
		if err != nil {
			t.Fatal(err)
		}
		a := &app{haltController: &haltController{}, cfg: config{Role: "dashboard"}, db: db}
		got, err := a.recordUnknown(context.Background(), meta, 17, start.Add(time.Duration(want)*time.Second))
		if err != nil || got.Attempts != want || !got.FirstSeen.Equal(start.Add(time.Second)) {
			t.Fatalf("retry state = %+v, error = %v; want attempts %d and first seen %s", got, err, want, start.Add(time.Second))
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSaveRollsBackMessageIdentityOnConflictingHazard(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{haltController: &haltController{}, cfg: config{Role: "dashboard"}, db: db}
	e := envelope{HazardID: "h1", Payload: hazardEvent{HazardID: "h1", Severity: "SIAGA"}}
	first := &jetstream.MsgMetadata{Stream: "HAZARDS", Sequence: jetstream.SequencePair{Stream: 1}}
	if err := a.save(context.Background(), first, []byte("first"), e); err != nil {
		t.Fatal(err)
	}
	e.Payload.Severity = "AWAS"
	second := &jetstream.MsgMetadata{Stream: "HAZARDS", Sequence: jetstream.SequencePair{Stream: 2}}
	if err := a.save(context.Background(), second, []byte("second"), e); !errors.Is(err, errConsumerIncompatible) {
		t.Fatalf("conflicting immutable hazard should halt: %v", err)
	}
	processed, err := a.alreadyProcessed(context.Background(), second, []byte("second"))
	if err != nil || processed {
		t.Fatalf("failed message committed identity: processed=%v error=%v", processed, err)
	}
}

func TestUnknownSchemaStormCountsDistinctMessages(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{haltController: &haltController{}, db: db}
	now := time.Now()
	for i := 1; i <= unknownStormLimit; i++ {
		meta := &jetstream.MsgMetadata{Stream: "HAZARDS", Sequence: jetstream.SequencePair{Stream: uint64(i)}}
		state, err := a.recordUnknown(context.Background(), meta, 17, now)
		if err != nil || state.Recent != i {
			t.Fatalf("message %d: state=%+v err=%v", i, state, err)
		}
	}
}

func TestCorruptPayloadStormCountsDistinctMessages(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{haltController: &haltController{}, db: db}
	now := time.Now()
	for i := 1; i <= corruptStormLimit; i++ {
		meta := &jetstream.MsgMetadata{Stream: "HAZARDS", Sequence: jetstream.SequencePair{Stream: uint64(i)}}
		recent, err := a.recordCorrupt(context.Background(), meta, now)
		if err != nil || recent != i {
			t.Fatalf("message %d: recent=%d err=%v", i, recent, err)
		}
	}
}

func TestDecodeSeparatesCorruptBytesFromUnsupportedEnvelope(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": testSchema})
	}))
	defer server.Close()
	client := newSchemaClient(server.URL, db)
	frame := testFrame(t)
	if _, err := client.decode(context.Background(), frame[:4]); !errors.Is(err, errCorruptPayload) {
		t.Fatalf("invalid frame: %v", err)
	}
	frame[5] = 4 // Avro zig-zag encoding for envelope version 2.
	if _, err := client.decode(context.Background(), frame); !errors.Is(err, errConsumerIncompatible) {
		t.Fatalf("valid but unsupported envelope: %v", err)
	}
}

func TestSQLiteConstraintIsDistinctFromStoreOutage(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`INSERT INTO records(hazard_id,payload,correlation_id,processed_at) VALUES ('h','{}','c','now')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO records(hazard_id,payload,correlation_id,processed_at) VALUES ('h','{}','c','now')`)
	if classifyStoreFailure(err) != storeConstraint {
		t.Fatalf("duplicate key classification: %v", err)
	}
	if classifyStoreFailure(context.DeadlineExceeded) != storeTransient {
		t.Fatal("deadline should retry")
	}
	if _, err := db.Exec(`PRAGMA query_only=ON`); err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO records(hazard_id,payload,correlation_id,processed_at) VALUES ('h2','{}','c','now')`)
	if classifyStoreFailure(err) != storeEnvironmental {
		t.Fatalf("read-only database should halt: %v", err)
	}
}

func TestSQLiteBusyRetries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "consumer.db")
	first, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	second.SetMaxOpenConns(1)
	if _, err := second.Exec(`PRAGMA busy_timeout=0`); err != nil {
		t.Fatal(err)
	}
	tx, err := first.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO records(hazard_id,payload,correlation_id,processed_at) VALUES ('held','{}','c','now')`); err != nil {
		t.Fatal(err)
	}
	_, err = second.Exec(`INSERT INTO records(hazard_id,payload,correlation_id,processed_at) VALUES ('other','{}','c','now')`)
	if err == nil || classifyStoreFailure(err) != storeTransient {
		t.Fatalf("busy database should retry: %v", err)
	}
}

func TestAckFailureAfterCommitOnlyRetriesAckOnRedelivery(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{haltController: &haltController{}, cfg: config{Role: "dashboard"}, db: db}
	meta := &jetstream.MsgMetadata{Stream: "HAZARDS", Sequence: jetstream.SequencePair{Stream: 41}}
	raw := []byte("original frame")
	e := envelope{HazardID: "h1", Payload: hazardEvent{HazardID: "h1", Severity: "SIAGA"}}
	if err := a.save(context.Background(), meta, raw, e); err != nil {
		t.Fatal(err)
	}
	failed := &testMsg{meta: meta, raw: raw, ackErr: errors.New("ack confirmation lost")}
	if a.process(context.Background(), nil, failed) || failed.ackCalls != 1 {
		t.Fatalf("failed ack should reconnect: calls=%d", failed.ackCalls)
	}
	redelivered := &testMsg{meta: meta, raw: raw}
	if !a.process(context.Background(), nil, redelivered) || redelivered.ackCalls != 1 {
		t.Fatalf("redelivery should confirm ack without decoding: calls=%d", redelivered.ackCalls)
	}
	var records int
	if err := db.QueryRow(`SELECT count(*) FROM records`).Scan(&records); err != nil || records != 1 {
		t.Fatalf("records=%d error=%v", records, err)
	}
}

func TestRegistryNakFailureRequestsReconnect(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	a := &app{haltController: &haltController{}, cfg: config{Role: "dashboard"}, db: db, schemas: newSchemaClient(server.URL, db)}
	msg := &testMsg{meta: &jetstream.MsgMetadata{Stream: "HAZARDS", Sequence: jetstream.SequencePair{Stream: 41}}, raw: testFrame(t), nakErr: errors.New("connection lost")}
	if a.process(context.Background(), nil, msg) || msg.nakCalls != 1 || a.pauseUntil.Load() <= time.Now().UnixNano() {
		t.Fatalf("NAK failure should reconnect and pause: nak_calls=%d", msg.nakCalls)
	}
}

func TestHaltPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "consumer.db")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	a := &app{haltController: &haltController{}, db: db}
	a.halt("consumer_incompatible: unsupported envelope")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := &app{haltController: &haltController{}, db: reopened}
	if err := restarted.refreshHalt(context.Background()); err != nil || !restarted.halted.Load() {
		t.Fatalf("halt lost after restart: halted=%v error=%v", restarted.halted.Load(), err)
	}
}

func TestUnknownSchemaCarriesCorrelationIntoRegistryAndLogs(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	seenHeader := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenHeader = r.Header.Get(correlationHeader)
		http.NotFound(w, r)
	}))
	defer server.Close()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(previous)
	header := nats.Header{}
	header.Set(correlationHeader, "cid-unknown")
	msg := &testMsg{meta: &jetstream.MsgMetadata{Stream: "HAZARDS", Sequence: jetstream.SequencePair{Stream: 41}}, raw: testFrame(t), header: header}
	a := &app{haltController: &haltController{}, cfg: config{Role: "dashboard"}, db: db, schemas: newSchemaClient(server.URL, db)}
	if !a.process(context.Background(), nil, msg) || msg.nakCalls != 1 {
		t.Fatal("unknown schema should be delayed")
	}
	if seenHeader != "cid-unknown" {
		t.Fatalf("registry correlation header = %q", seenHeader)
	}
	for _, want := range []string{`"correlation_id":"cid-unknown"`, `"operation":"registry_lookup"`, `"status_code":404`, `"operation":"delayed_nak"`, `"latency_ms":`} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("logs missing %s: %s", want, logs.String())
		}
	}
}

func TestCorrelationHeaderMustMatchDecodedEnvelope(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": testSchema})
	}))
	defer server.Close()
	header := nats.Header{}
	header.Set(correlationHeader, "different-id")
	msg := &testMsg{meta: &jetstream.MsgMetadata{Stream: "HAZARDS", Sequence: jetstream.SequencePair{Stream: 41}}, header: header}
	ctx := withMessageTrace(context.Background(), "dashboard", msg, msg.meta)
	_, err = newSchemaClient(server.URL, db).decode(ctx, testFrame(t))
	if !errors.Is(err, errCorruptPayload) {
		t.Fatalf("header/envelope mismatch = %v", err)
	}
}

func TestSuccessfulMessageKeepsCorrelationThroughLookupAndAck(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	seenHeader := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenHeader = r.Header.Get(correlationHeader)
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": testSchema})
	}))
	defer server.Close()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(previous)
	header := nats.Header{}
	header.Set(correlationHeader, "cid-1")
	msg := &testMsg{meta: &jetstream.MsgMetadata{Stream: "HAZARDS", Sequence: jetstream.SequencePair{Stream: 41}}, raw: testFrame(t), header: header}
	a := &app{haltController: &haltController{}, cfg: config{Role: "dashboard"}, db: db, schemas: newSchemaClient(server.URL, db)}
	if !a.process(context.Background(), nil, msg) || msg.ackCalls != 1 {
		t.Fatalf("valid message must be stored and acked: ack_calls=%d", msg.ackCalls)
	}
	if seenHeader != "cid-1" {
		t.Fatalf("registry correlation header = %q", seenHeader)
	}
	for _, want := range []string{`"operation":"registry_lookup"`, `"operation":"original_double_ack"`, `"msg":"event processed"`, `"correlation_id":"cid-1"`} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("logs missing %s: %s", want, logs.String())
		}
	}
}

type ambiguousPublishJS struct {
	jetstream.JetStream
	failLookup bool
}

func (j ambiguousPublishJS) Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := j.JetStream.Publish(ctx, subject, data, opts...)
	if err != nil {
		return ack, err
	}
	return nil, errors.New("publish confirmation lost")
}

func (j ambiguousPublishJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if j.failLookup {
		return nil, errors.New("DLQ lookup unavailable")
	}
	return j.JetStream.Stream(ctx, name)
}

func TestDLQRetainsOneEntryAfterLostAckAndAmbiguousPublish(t *testing.T) {
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("set TEST_NATS_URL for JetStream integration test")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	streamName := "DLQ_TEST_" + suffix
	prefix := "dlqtest." + suffix
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: streamName, Subjects: []string{prefix + ".>"}, Storage: jetstream.FileStorage,
		Discard: jetstream.DiscardNew, DiscardNewPerSubject: true, MaxMsgsPerSubject: 1,
		MaxAge: time.Hour, Duplicates: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), streamName) })
	db, err := openDB(filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{haltController: &haltController{}, cfg: config{Role: "dashboard", Durable: "dashboard-v1", DLQStream: streamName, DLQSubject: prefix + ".dashboard"}, db: db}
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(previous)
	meta := &jetstream.MsgMetadata{Stream: "HAZARDS", Sequence: jetstream.SequencePair{Stream: 41}}
	header := nats.Header{}
	header.Set(correlationHeader, "cid-41")
	first := &testMsg{meta: meta, raw: []byte("failed bytes"), header: header, ackErr: errors.New("ack lost")}
	firstCtx := withMessageTrace(ctx, "dashboard", first, meta)
	if err := a.quarantine(firstCtx, js, first, meta, "corrupt_payload", errCorruptPayload); err == nil || first.ackCalls != 1 {
		t.Fatalf("first publish must succeed before lost ack: ack_calls=%d err=%v", first.ackCalls, err)
	}
	// Let the ordinary message-ID deduplication window expire. The unique
	// subject must still reject the second publication after redelivery.
	time.Sleep(1200 * time.Millisecond)
	redelivered := &testMsg{meta: meta, raw: first.raw, header: header}
	if err := a.quarantine(firstCtx, js, redelivered, meta, "corrupt_payload", errCorruptPayload); err != nil || redelivered.ackCalls != 1 {
		t.Fatalf("verified duplicate must ack original: ack_calls=%d err=%v", redelivered.ackCalls, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("retained DLQ messages=%d, want 1", info.State.Msgs)
	}
	secondMeta := &jetstream.MsgMetadata{Stream: "HAZARDS", Sequence: jetstream.SequencePair{Stream: 42}}
	ambiguous := &testMsg{meta: secondMeta, raw: []byte("other bytes"), header: header}
	ambiguousCtx := withMessageTrace(ctx, "dashboard", ambiguous, secondMeta)
	if err := a.quarantine(ambiguousCtx, ambiguousPublishJS{JetStream: js}, ambiguous, secondMeta, "corrupt_payload", errCorruptPayload); err != nil || ambiguous.ackCalls != 1 {
		t.Fatalf("verified ambiguous publish must ack: ack_calls=%d err=%v", ambiguous.ackCalls, err)
	}
	thirdMeta := &jetstream.MsgMetadata{Stream: "HAZARDS", Sequence: jetstream.SequencePair{Stream: 43}}
	unverified := &testMsg{meta: thirdMeta, raw: []byte("third bytes"), header: header}
	unverifiedCtx := withMessageTrace(ctx, "dashboard", unverified, thirdMeta)
	if err := a.quarantine(unverifiedCtx, ambiguousPublishJS{JetStream: js, failLookup: true}, unverified, thirdMeta, "corrupt_payload", errCorruptPayload); err == nil || unverified.ackCalls != 0 {
		t.Fatalf("unverified publish must remain unacked: ack_calls=%d err=%v", unverified.ackCalls, err)
	}
	fourthMeta := &jetstream.MsgMetadata{Stream: "HAZARDS", Sequence: jetstream.SequencePair{Stream: 44}}
	wrongSubject := a.dlqSubject(fourthMeta)
	wrongEntry, err := json.Marshal(deadLetter{Consumer: a.cfg.Durable, OriginalStream: "HAZARDS", OriginalSubject: "hazard.events", StreamSequence: 44, Raw: []byte("different bytes")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, wrongSubject, wrongEntry); err != nil {
		t.Fatal(err)
	}
	conflicting := &testMsg{meta: fourthMeta, raw: []byte("actual bytes"), header: header}
	conflictCtx := withMessageTrace(ctx, "dashboard", conflicting, fourthMeta)
	if a.quarantineOrRetry(conflictCtx, js, conflicting, fourthMeta, "corrupt_payload", errCorruptPayload) || conflicting.ackCalls != 0 || !a.halted.Load() {
		t.Fatalf("conflicting DLQ entry must halt without ack: ack_calls=%d halted=%v", conflicting.ackCalls, a.halted.Load())
	}
	for _, want := range []string{`"operation":"dlq_publish"`, `"operation":"original_double_ack"`, `"operation":"dlq_subject_lookup"`, `"latency_ms":`, `"correlation_id":"cid-41"`} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("logs missing %s: %s", want, logs.String())
		}
	}
}
