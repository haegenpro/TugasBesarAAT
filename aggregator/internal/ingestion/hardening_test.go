package ingestion_test

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/audit"
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/ingestion"
	"aggregator/internal/ownership"
	"aggregator/internal/pagecursor"
	"aggregator/internal/pipeline"
	"aggregator/internal/rawinput"
	"aggregator/internal/store"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/linkedin/goavro/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestM3HardeningReferencesFailuresAndAuthorizedReplay(t *testing.T) {
	if os.Getenv("M2_DATABASE_URL") == "" {
		t.Skip("isolated database absent")
	}
	ctx := context.Background()
	root, e := filepath.Abs(filepath.Join("..", "..", ".."))
	require.NoError(t, e)
	inputDB := newPathDatabase(t, root, "input")
	canonicalDB := newPathDatabase(t, root, "canonical")
	a, e := ownership.NewAcquisition(inputDB.Pool, 30*time.Second, canonical.Deadline)
	require.NoError(t, e)
	owner, e := a.Acquire(ctx, canonical.AcquireOwnership{Scope: "acquisition:BMKG", Holder: "reference-owner"})
	require.NoError(t, e)
	owned := &ingestion.Store{DB: inputDB, Durability: pathDurability{pool: inputDB.Pool}, Fence: a, Namespace: "reference-v1", Start: 1, Quota: 4 << 20}
	p, e := ownership.NewProcessor(canonicalDB.Pool, 30*time.Second)
	require.NoError(t, e)
	processor, e := p.Acquire(ctx, canonical.AcquireOwnership{Scope: "processor:BMKG", Holder: "receiver"})
	require.NoError(t, e)
	canonicalDB.Processor = p
	canonicalDB.Canonical.Durability = pathDurability{pool: canonicalDB.Pool}
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/internal/ingestion/legacy-failures" {
			if r.Header.Get("Authorization") != "Bearer service-only" {
				w.WriteHeader(401)
				return
			}
			io.WriteString(w, `{"items":[]}`)
			return
		}
		var v map[string]string
		_ = json.NewDecoder(r.Body).Decode(&v)
		if v["access_token"] != "ops-token" {
			w.WriteHeader(403)
			return
		}
		io.WriteString(w, `{"client_id":"ops","role":"internal","scope":"ops"}`)
	}))
	defer auth.Close()
	router := gin.New()
	ingestion.HTTP{Store: owned, ServiceToken: "service-only", Signer: pagecursor.Signer{Key: []byte(strings.Repeat("k", 32))}, AuthURL: auth.URL, AuthKey: "introspection", LegacyURL: auth.URL, Client: http.DefaultClient}.Register(router)
	server := httptest.NewServer(router)
	defer server.Close()
	request := func(method, path, token string, body []byte) *http.Response {
		req, e := http.NewRequest(method, server.URL+path, bytes.NewReader(body))
		require.NoError(t, e)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		req.Header.Set("Content-Type", "application/json")
		r, e := http.DefaultClient.Do(req)
		require.NoError(t, e)
		return r
	}
	r := request("GET", "/internal/ingestion/failures", "service-only", nil)
	raw, e := io.ReadAll(r.Body)
	r.Body.Close()
	require.NoError(t, e)
	require.Equal(t, 503, r.StatusCode, "an uninitialized ledger cannot establish complete absence")
	payload := bytes.Repeat([]byte{0, 0xff, 0xfe}, 400000)
	meta := ingestion.CaptureContext{Cursor: rawinput.CaptureCursor{Version: 1, Entity: "seismic-events", Full: true, SourceSequence: 1, RequestStarted: time.Now().UTC()}, ReceivedAt: "2026-01-01T00:00:00Z", Owner: owner.Ownership, CorrelationID: "unknown-schema"}
	capture, e := owned.Capture(ctx, owner.Ownership, meta, bytes.NewReader(payload))
	require.NoError(t, e)
	// Healthy captured backlog is not a failed record.
	r = request("GET", "/internal/ingestion/failures", "service-only", nil)
	raw, _ = io.ReadAll(r.Body)
	r.Body.Close()
	require.Contains(t, string(raw), `"items":[]`)
	env := rawinput.Envelope{Version: 1, Source: "BMKG", Namespace: owned.Namespace, Position: &capture.Position, Operation: "update", ReceivedAt: meta.ReceivedAt, PayloadRef: &capture.PayloadID, Size: capture.Size, Digest: capture.Digest, ContentType: "application/avro", CorrelationID: "reference", OwnershipScope: owner.Ownership.Scope, OwnershipEpoch: owner.Ownership.Epoch, OwnershipHolder: owner.Ownership.Holder}
	env.MessageID = rawinput.ID(env)
	canonicalDB.Inputs = store.InputOptions{Starts: map[string]int64{store.InputSession("BMKG", owned.Namespace): 1}, PayloadQuota: 4 << 20, Resolver: ingestion.HTTPResolver{Client: http.DefaultClient, BaseURL: server.URL, Token: "wrong"}}
	_, e = canonicalDB.AppendHistory(ctx, rawinput.AppendRequest{Version: 1, Ownership: processor.Ownership, Messages: []rawinput.Envelope{env}})
	require.Error(t, e)
	canonicalDB.Inputs.Resolver = ingestion.HTTPResolver{Client: http.DefaultClient, BaseURL: server.URL, Token: "service-only"}
	corrupt := env
	corrupt.Digest = strings.Repeat("0", 64)
	_, e = canonicalDB.AppendHistory(ctx, rawinput.AppendRequest{Version: 1, Ownership: processor.Ownership, Messages: []rawinput.Envelope{corrupt}})
	require.Error(t, e)
	canonicalDB.Inputs.PayloadQuota = 1 << 20
	_, e = canonicalDB.AppendHistory(ctx, rawinput.AppendRequest{Version: 1, Ownership: processor.Ownership, Messages: []rawinput.Envelope{env}})
	require.Error(t, e)
	canonicalDB.Inputs.PayloadQuota = 4 << 20
	_, e = canonicalDB.AppendHistory(ctx, rawinput.AppendRequest{Version: 1, Ownership: processor.Ownership, Messages: []rawinput.Envelope{env}})
	require.NoError(t, e)
	// A replacement Store has only durable SQL state and configured owner HTTP.
	restarted, e := store.New(ctx, canonicalDB.Pool.Config().ConnString(), slog.Default())
	require.NoError(t, e)
	defer restarted.Close()
	restarted.Processor = p
	restarted.Canonical.Durability = pathDurability{pool: restarted.Pool}
	restarted.Inputs = canonicalDB.Inputs
	receipt, e := restarted.AppendHistory(ctx, rawinput.AppendRequest{Version: 1, Ownership: processor.Ownership, Messages: []rawinput.Envelope{env}})
	require.NoError(t, e)
	require.Equal(t, "duplicate", receipt.Items[0].Outcome)
	var stored []byte
	require.NoError(t, canonicalDB.Pool.QueryRow(ctx, "SELECT original_payload FROM input_history").Scan(&stored))
	require.Equal(t, payload, stored)
	require.NoError(t, owned.Failure(ctx, owner.Ownership, *capture, "unknown schema; original bytes retained", false))
	message := rawinput.Identity("raw-v1", "BMKG", owned.Namespace, capture.Position, nil, "update")
	for _, token := range []string{"", "wrong", "ops-token"} {
		r = request("GET", "/internal/ingestion/failures/"+message+"/payload?namespace="+owned.Namespace, token, nil)
		if token == "ops-token" {
			raw, e = io.ReadAll(r.Body)
			require.NoError(t, e)
			require.Equal(t, 200, r.StatusCode)
			require.Equal(t, payload, raw)
		} else {
			require.Contains(t, []int{401, 403}, r.StatusCode)
		}
		r.Body.Close()
	}
	body, _ := json.Marshal(map[string]any{"request_id": "replay-fixed", "source": "BMKG", "namespace": owned.Namespace, "message_ids": []string{message}, "adapter_version": "bmkg-capture-v1"})
	r = request("POST", "/internal/ingestion/replays", "service-only", body)
	require.Equal(t, 403, r.StatusCode)
	r.Body.Close()
	r = request("POST", "/internal/ingestion/replays", "ops-token", body)
	raw, _ = io.ReadAll(r.Body)
	r.Body.Close()
	require.Equal(t, 202, r.StatusCode, string(raw))
	require.NoError(t, owned.Runnable(ctx, *capture))
	_, e = ingestion.HTTPResolver{Client: http.DefaultClient, BaseURL: "http://127.0.0.1:1", Token: "service-only"}.Open(ctx, capture.PayloadID)
	require.Error(t, e)
	proxyRouter := gin.New()
	audit.HTTP{Repository: canonicalDB, Token: "audit-only", Signer: pagecursor.Signer{Key: []byte(strings.Repeat("k", 32))}, IngestionURL: server.URL, IngestionToken: "service-only", Client: http.DefaultClient}.Register(proxyRouter)
	proxyServer := httptest.NewServer(proxyRouter)
	defer proxyServer.Close()
	for _, token := range []string{"wrong", "audit-only"} {
		req, _ := http.NewRequest("GET", proxyServer.URL+"/internal/audit/unprocessed?source=BMKG", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		reply, e := http.DefaultClient.Do(req)
		require.NoError(t, e)
		body, _ := io.ReadAll(reply.Body)
		reply.Body.Close()
		if token == "wrong" {
			require.Equal(t, 401, reply.StatusCode)
		} else {
			require.Equal(t, 200, reply.StatusCode, string(body))
			require.Contains(t, string(body), message)
		}
	}
	server.Close()
	req, _ := http.NewRequest("GET", proxyServer.URL+"/internal/audit/unprocessed", nil)
	req.Header.Set("Authorization", "Bearer audit-only")
	reply, e := http.DefaultClient.Do(req)
	require.NoError(t, e)
	reply.Body.Close()
	require.Equal(t, 503, reply.StatusCode, "unreachable failure authority cannot become an empty success")
}

func TestM3HardeningUnknownSchemaBrokerAndACK(t *testing.T) {
	if os.Getenv("M2_DATABASE_URL") == "" || os.Getenv("M3_NATS_URL") == "" {
		t.Skip("owned storage and broker required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	root, e := filepath.Abs(filepath.Join("..", "..", ".."))
	require.NoError(t, e)
	inputDB := newPathDatabase(t, root, "input")
	canonicalDB := newPathDatabase(t, root, "canonical")
	text, e := os.ReadFile(filepath.Join(root, "aggregator/schemas/seismic.avsc"))
	require.NoError(t, e)
	history, e := avrowire.HistorySchema(string(text), "SeismicHistoryPage")
	require.NoError(t, e)
	codec, e := goavro.NewCodec(history)
	require.NoError(t, e)
	body, e := avrowire.Encode(&avrowire.Schema{ID: 99, Text: history, Codec: codec}, map[string]any{"data": []any{map[string]any{"cursor": int64(1), "payload": map[string]any{"event_id": "large", "magnitude": 5.0, "depth_km": 10.0, "epicenter_lat": 0.0, "epicenter_lon": 0.0, "region_name": strings.Repeat("x", 800<<10), "occurred_at": "2026-01-01T00:00:00Z", "potential_tsunami": false}}}, "next_cursor": int64(1), "has_more": false})
	require.NoError(t, e)
	rawText, e := os.ReadFile(filepath.Join(root, "docs/fixtures/raw-input-v1.avsc"))
	require.NoError(t, e)
	var known atomic.Bool
	registryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/schemas/ids/") {
			if strings.HasSuffix(r.URL.Path, "/99") && known.Load() {
				_ = json.NewEncoder(w).Encode(map[string]string{"schema": history})
				return
			}
			if strings.HasSuffix(r.URL.Path, "/1") {
				_ = json.NewEncoder(w).Encode(map[string]string{"schema": string(rawText)})
				return
			}
			w.WriteHeader(404)
			return
		}
		if r.Method == "PUT" {
			io.WriteString(w, `{}`)
			return
		}
		io.WriteString(w, `{"id":1}`)
	}))
	defer registryServer.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", avrowire.MediaType)
		_, _ = w.Write(body)
	}))
	defer source.Close()
	registry := avrowire.NewClient(registryServer.URL)
	registry.SetCache(store.SchemaCache{Pool: inputDB.Pool})
	reader, e := avrowire.NewSourceReader(string(text), "SeismicHistoryPage")
	require.NoError(t, e)
	nc, e := nats.Connect(os.Getenv("M3_NATS_URL"))
	require.NoError(t, e)
	defer nc.Close()
	js, e := jetstream.New(nc)
	require.NoError(t, e)
	consumer, e := rawinput.Ensure(ctx, js, rawinput.BMKG, 3)
	require.NoError(t, e)
	a, e := ownership.NewAcquisition(inputDB.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, e)
	owned := &ingestion.Store{DB: inputDB, Durability: pathDurability{pool: inputDB.Pool}, Fence: a, Namespace: "broker-hardening", Start: 1, Quota: 4 << 20}
	worker := &ingestion.Worker{Store: owned, Registry: registry, SchemaText: string(rawText), Readers: map[string]*avrowire.SourceReader{"seismic-events": reader}, JS: js, HTTP: http.DefaultClient, BaseURL: source.URL, APIKey: "fixture"}
	coordinator, e := acquisition.New(a, slog.Default())
	require.NoError(t, e)
	actions := make(chan func(context.Context) error)
	results := make(chan error, 1)
	go coordinator.Run(ctx, "BMKG", func(ownedCtx context.Context) {
		for {
			select {
			case <-ownedCtx.Done():
				return
			case action := <-actions:
				results <- action(ownedCtx)
			}
		}
	})
	step := func(action func(context.Context) error) error {
		select {
		case actions <- action:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		select {
		case err := <-results:
			return err
		case <-ctx.Done():
			t.Fatal(ctx.Err())
			return ctx.Err()
		}
	}
	require.Error(t, step(worker.Step))
	var payload []byte
	var failureID string
	require.NoError(t, inputDB.Pool.QueryRow(ctx, "SELECT message_id FROM ingestion_failures").Scan(&failureID))
	require.NoError(t, inputDB.Pool.QueryRow(ctx, "SELECT data FROM ingestion_payload_chunks ORDER BY ordinal LIMIT 1").Scan(&payload))
	require.NotEmpty(t, payload)
	known.Store(true)
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"client_id":"ops","role":"internal"}`)
	}))
	defer auth.Close()
	router := gin.New()
	ingestion.HTTP{Store: owned, ServiceToken: "service", Signer: pagecursor.Signer{Key: []byte(strings.Repeat("s", 32))}, AuthURL: auth.URL, AuthKey: "ops", Client: http.DefaultClient}.Register(router)
	ownerServer := httptest.NewServer(router)
	defer ownerServer.Close()
	replay, _ := json.Marshal(map[string]any{"request_id": "schema-now-known", "source": "BMKG", "namespace": owned.Namespace, "message_ids": []string{failureID}, "adapter_version": "bmkg-capture-v1"})
	req, _ := http.NewRequest("POST", ownerServer.URL+"/internal/ingestion/replays", bytes.NewReader(replay))
	req.Header.Set("Authorization", "Bearer ops")
	resp, e := http.DefaultClient.Do(req)
	require.NoError(t, e)
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, 202, resp.StatusCode, string(b))
	worker.Registry = avrowire.NewClient(registryServer.URL)
	worker.Registry.SetCache(store.SchemaCache{Pool: inputDB.Pool})
	worker.AfterPublish = func(context.Context) error {
		return errors.New("crash after actual broker publication acknowledgement")
	}
	require.Error(t, step(worker.Step))
	var through int64
	require.NoError(t, inputDB.Pool.QueryRow(ctx, "SELECT published_through FROM ingestion_state").Scan(&through))
	require.Zero(t, through)
	worker.AfterPublish = nil
	require.NoError(t, step(worker.Step))
	stream, e := js.Stream(ctx, rawinput.BMKG.Name)
	require.NoError(t, e)
	info, e := stream.Info(ctx)
	require.NoError(t, e)
	require.Equal(t, uint64(1), info.State.Msgs, "retry must use the persisted transport identity")
	stored, e := stream.GetMsg(ctx, 1)
	require.NoError(t, e)
	p, e := ownership.NewProcessor(canonicalDB.Pool, 30*time.Second)
	require.NoError(t, e)
	pl, e := p.Acquire(ctx, canonical.AcquireOwnership{Scope: "processor:BMKG", Holder: "receiver"})
	require.NoError(t, e)
	canonicalDB.Processor = p
	canonicalDB.Canonical.Durability = pathDurability{pool: canonicalDB.Pool}
	canonicalDB.Inputs = store.InputOptions{Starts: map[string]int64{store.InputSession("BMKG", owned.Namespace): 1}, Resolver: ingestion.HTTPResolver{Client: http.DefaultClient, BaseURL: ownerServer.URL, Token: "service"}, PayloadQuota: 4 << 20}
	rawReader, e := avrowire.NewReader(string(rawText))
	require.NoError(t, e)
	receiver := pipeline.Worker{Store: canonicalDB, Registry: registry, Reader: rawReader, BeforeACK: func(context.Context) error { return errors.New("after durable receipt, before ACK") }}
	fetch, e := consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	require.NoError(t, e)
	var message jetstream.Msg
	for m := range fetch.Messages() {
		message = m
	}
	require.NotNil(t, message)
	require.Error(t, receiver.Handoff(ctx, message, "BMKG", owned.Namespace, pl.Ownership))
	require.NoError(t, message.Nak())
	receiver.BeforeACK = nil
	fetch, e = consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	require.NoError(t, e)
	for m := range fetch.Messages() {
		require.NoError(t, receiver.Handoff(ctx, m, "BMKG", owned.Namespace, pl.Ownership))
	}
	var original []byte
	require.NoError(t, canonicalDB.Pool.QueryRow(ctx, "SELECT original_payload FROM input_history").Scan(&original))
	require.Equal(t, body, original)
	unknown := append([]byte(nil), stored.Data...)
	binary.BigEndian.PutUint32(unknown[1:5], 999)
	_, e = js.Publish(ctx, rawinput.BMKG.Subject, unknown)
	require.NoError(t, e)
	fetch, e = consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	require.NoError(t, e)
	for m := range fetch.Messages() {
		require.Error(t, receiver.Handoff(ctx, m, "BMKG", owned.Namespace, pl.Ownership))
	}
	state, e := consumer.Info(ctx)
	require.NoError(t, e)
	require.Equal(t, 1, state.NumAckPending)
	var count int
	require.NoError(t, canonicalDB.Pool.QueryRow(ctx, "SELECT count(*) FROM input_history").Scan(&count))
	require.Equal(t, 1, count)
}
