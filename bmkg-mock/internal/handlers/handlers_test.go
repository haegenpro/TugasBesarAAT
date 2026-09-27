package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bmkg-mock/internal/handlers"
	"bmkg-mock/internal/middleware"
	"bmkg-mock/internal/seed"
	"bmkg-mock/internal/server"
)

const testKey = "test-bmkg-key"

func newTestServer() http.Handler {
	logger := middleware.NewLogger()
	events, warnings := seed.Build()
	h := handlers.New(events, warnings, 0)
	return server.New(logger, testKey, h)
}

func TestHealthNoAuth(t *testing.T) {
	r := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestSeismicEvents_MissingAuth(t *testing.T) {
	r := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestSeismicEvents_WrongKey(t *testing.T) {
	r := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	req.Header.Set("X-BMKG-Key", "wrong-key")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestSeismicEvents_ValidAuth_SeededRecords(t *testing.T) {
	r := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	req.Header.Set("X-BMKG-Key", testKey)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var events []map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &events))
	assert.Len(t, events, 20)

	// timestamp serialization: ISO8601 UTC millisecond with Z suffix
	occurredAt, ok := events[0]["occurred_at"].(string)
	require.True(t, ok)
	_, err := time.Parse("2006-01-02T15:04:05.000Z07:00", occurredAt)
	assert.NoError(t, err)
	assert.Regexp(t, `Z$`, occurredAt)
}

func TestSeismicEvents_SinceFilter(t *testing.T) {
	r := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/seismic-events?since=2026-09-01T00:00:00.000Z", nil)
	req.Header.Set("X-BMKG-Key", testKey)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var all []map[string]any
	json.Unmarshal(w.Body.Bytes(), &all)

	req2 := httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	req2.Header.Set("X-BMKG-Key", testKey)
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)
	var unfiltered []map[string]any
	json.Unmarshal(w2.Body.Bytes(), &unfiltered)

	assert.Less(t, len(all), len(unfiltered))
}

func TestTsunamiWarnings_ValidAuth(t *testing.T) {
	r := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/tsunami-warnings", nil)
	req.Header.Set("X-BMKG-Key", testKey)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var warnings []map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &warnings))
	assert.NotEmpty(t, warnings)

	levels := map[string]bool{}
	for _, w := range warnings {
		levels[w["threat_level"].(string)] = true
	}
	assert.True(t, levels["Waspada"] || levels["Siaga"] || levels["Awas"])
}

func TestDelayBehavior(t *testing.T) {
	logger := middleware.NewLogger()
	events, warnings := seed.Build()
	h := handlers.New(events, warnings, 60)
	r := server.New(logger, testKey, h)

	req := httptest.NewRequest(http.MethodGet, "/seismic-events", nil)
	req.Header.Set("X-BMKG-Key", testKey)
	w := httptest.NewRecorder()

	start := time.Now()
	r.ServeHTTP(w, req)
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(60))
}
