package handlers_test

import (
	"encoding/json"
	"math/rand"
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
	req := httptest.NewRequest(http.MethodGet, "/seismic-events?since="+time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), nil)
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

func getSeismic(t *testing.T, r http.Handler, query string) []map[string]any {
	req := httptest.NewRequest(http.MethodGet, "/seismic-events"+query, nil)
	req.Header.Set("X-BMKG-Key", testKey)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var events []map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &events))
	return events
}

func TestGeneratedEvents_VisibleAndBounded(t *testing.T) {
	events, warnings := seed.Build()
	h := handlers.New(events, warnings, 0)
	r := server.New(middleware.NewLogger(), testKey, h)
	rng := rand.New(rand.NewSource(1))
	now := time.Now().UTC()

	cursor := getSeismic(t, r, "")[0]["updated_at"].(string)
	first, warning := seed.NextEvent(1700000000, 1, now, rng)
	h.AddEvent(first, warning)
	assert.Len(t, getSeismic(t, r, ""), 21)

	recent := getSeismic(t, r, "?since="+cursor)
	require.Len(t, recent, 1)
	assert.Equal(t, first.EventID, recent[0]["event_id"])

	// Past the cap the oldest generated event is evicted; seed events stay.
	for n := 2; n <= 105; n++ {
		ev, w := seed.NextEvent(1700000000, n, now, rng)
		h.AddEvent(ev, w)
	}
	all := getSeismic(t, r, "")
	assert.Len(t, all, 120)
	ids := map[string]bool{}
	for _, e := range all {
		ids[e["event_id"].(string)] = true
	}
	assert.False(t, ids[first.EventID])
	assert.True(t, ids["evt-00001"])
}
