package ingestion

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/ownership"
	"aggregator/internal/pagecursor"
	"aggregator/internal/rawinput"
	"bytes"
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestResponseQuarantineStreamsExactBytesThroughOpsOnlyInspection(t *testing.T) {
	store, _ := ingestionLab(t)
	authority := store.Fence.(*ownership.Authority)
	source := &SourceStore{DB: store.DB, Durability: store.Durability, Fence: authority, Source: "SENSOR_PANTAI", Namespace: "http-response-fixture"}
	original := append(bytes.Repeat([]byte{0, 0xff, 0xfe}, 50000), []byte("END")...)
	message := rawinput.Identity("http-response", source.Source, source.Namespace, "receipt-one")
	coordinator, err := acquisition.New(authority, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	coordinator.Run(context.Background(), source.Source, func(owned context.Context) {
		for range 2 {
			require.NoError(t, source.QuarantineResponse(owned, "receipt-one", "application/json", "invalid outer response", bytes.NewReader(original)))
		}
	})
	var count, largest int
	var size int64
	var chunked bool
	require.NoError(t, store.DB.Pool.QueryRow(context.Background(), `SELECT count(*) FROM ingestion_failure_payloads`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, store.DB.Pool.QueryRow(context.Background(), `SELECT size,chunked FROM ingestion_failure_payloads`).Scan(&size, &chunked))
	require.True(t, chunked)
	require.EqualValues(t, len(original), size)
	require.NoError(t, store.DB.Pool.QueryRow(context.Background(), `SELECT max(octet_length(data)) FROM ingestion_failure_payload_chunks`).Scan(&largest))
	require.LessOrEqual(t, largest, 65536)
	var copied bytes.Buffer
	written, err := store.WriteQuarantinedPayload(context.Background(), source.Namespace, message, &copied)
	require.NoError(t, err)
	require.EqualValues(t, len(original), written)
	require.Equal(t, original, copied.Bytes())
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if r.Header.Get("X-Internal-Key") != "auth-key" || body["access_token"] != "ops-token" {
			w.WriteHeader(403)
			return
		}
		_, _ = w.Write([]byte(`{"client_id":"ops","role":"internal","scope":"ops"}`))
	}))
	defer auth.Close()
	router := gin.New()
	HTTP{Store: store, ServiceToken: "service-only", Signer: pagecursor.Signer{Key: []byte(strings.Repeat("k", 32))}, AuthURL: auth.URL, AuthKey: "auth-key", Client: http.DefaultClient}.Register(router)
	for _, token := range []string{"service-only", "ops-token"} {
		request := httptest.NewRequest("GET", "/internal/ingestion/failures/"+message+"/payload?namespace="+source.Namespace, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if token != "ops-token" {
			require.NotEqual(t, 200, response.Code)
			continue
		}
		require.Equal(t, 200, response.Code)
		require.Equal(t, original, response.Body.Bytes())
		require.Equal(t, rawinput.Hash(original), response.Header().Get("X-Payload-SHA256"))
	}
	progress, err := source.Progress(context.Background(), 0)
	require.NoError(t, err)
	require.Zero(t, progress.Generation)
	require.Zero(t, progress.Position)
	require.Nil(t, progress.Cursor)
}

type incompleteResponse struct{ bytes.Reader }

func (r *incompleteResponse) Read(p []byte) (int, error) {
	n, _ := r.Reader.Read(p)
	return n, io.ErrUnexpectedEOF
}
func TestResponseQuarantineIncompleteOrOverQuotaRollsBack(t *testing.T) {
	for _, test := range []string{"incomplete", "quota"} {
		t.Run(test, func(t *testing.T) {
			store, _ := ingestionLab(t)
			authority := store.Fence.(*ownership.Authority)
			source := &SourceStore{DB: store.DB, Durability: store.Durability, Fence: authority, Source: "SENSOR_PANTAI", Namespace: "response-rollback"}
			var reader io.Reader = &incompleteResponse{Reader: *bytes.NewReader([]byte("partial"))}
			if test == "quota" {
				t.Setenv("FAILED_INPUT_QUOTA_BYTES", "32")
				reader = bytes.NewReader(bytes.Repeat([]byte("x"), 100000))
			}
			coordinator, err := acquisition.New(authority, slog.New(slog.NewTextHandler(io.Discard, nil)))
			require.NoError(t, err)
			coordinator.Run(context.Background(), source.Source, func(owned context.Context) {
				require.Error(t, source.QuarantineResponse(owned, "receipt-failure", "application/json", "invalid response", reader))
			})
			var count int
			require.NoError(t, store.DB.Pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM ingestion_failures)+(SELECT count(*) FROM ingestion_failure_payloads)+(SELECT count(*) FROM ingestion_failure_payload_chunks)`).Scan(&count))
			require.Zero(t, count)
		})
	}
}
func TestResponseCaptureOverflowKeepsAllBytes(t *testing.T) {
	store, initialOwner := ingestionLab(t)
	authority := store.Fence.(*ownership.Authority)
	require.NoError(t, authority.Release(context.Background(), initialOwner))
	original := append(bytes.Repeat([]byte("x"), 5000), []byte("END")...)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(original) }))
	defer upstream.Close()
	client, err := acquisition.Client(authority, "BMKG", upstream.URL, 0, 4096)
	require.NoError(t, err)
	coordinator, err := acquisition.New(authority, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	coordinator.Run(context.Background(), "BMKG", func(owned context.Context) {
		owner, err := acquisition.Token(owned, "BMKG")
		require.NoError(t, err)
		request, err := http.NewRequestWithContext(acquisition.WithRequestKey(owned, "raw:7"), "GET", upstream.URL+"/seismic-events", nil)
		require.NoError(t, err)
		response, err := client.Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		require.True(t, acquisition.ResponseOverflow(response))
		capture, err := store.Capture(owned, owner, CaptureContext{Owner: owner, HTTPStatus: 200, ReceivedAt: ReceivedNow(), Cursor: rawinput.CaptureCursor{Version: 1, Entity: "seismic-events", Full: true, RequestStarted: time.Now().UTC()}}, response.Body)
		require.NoError(t, err)
		require.NoError(t, acquisition.ConsumeResponse(owned, response))
		require.NoError(t, response.Body.Close())
		kept, err := store.Payload(owned, *capture)
		require.NoError(t, err)
		require.Equal(t, original, kept)
		var state string
		require.NoError(t, store.DB.Pool.QueryRow(owned, `SELECT state FROM acquisition_dispatch WHERE scope='acquisition:BMKG'`).Scan(&state))
		require.Equal(t, "consumed", state)
	})
}
