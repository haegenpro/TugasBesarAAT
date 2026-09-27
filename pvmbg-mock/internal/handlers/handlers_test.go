package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"pvmbg-mock/internal/handlers"
	"pvmbg-mock/internal/middleware"
	"pvmbg-mock/internal/seed"
	"pvmbg-mock/internal/server"
	"pvmbg-mock/internal/state"
)

const testToken = "test-pvmbg-token"
const testAdminToken = "test-pvmbg-admin-token"

func newTestServer(minDelay, maxDelay int) (http.Handler, *state.Toggles) {
	logger := middleware.NewLogger()
	reports := seed.Build()
	toggles := state.New()
	h := handlers.New(reports, toggles, minDelay, maxDelay)
	return server.New(logger, testToken, testAdminToken, h, toggles), toggles
}

func TestHealth(t *testing.T) {
	r, _ := newTestServer(0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestVolcanicReports_MissingAuth(t *testing.T) {
	r, _ := newTestServer(0, 0)
	req := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestVolcanicReports_InvalidAuth(t *testing.T) {
	r, _ := newTestServer(0, 0)
	req := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestVolcanicReports_ValidAuth_ConfidenceHiddenByDefault(t *testing.T) {
	r, _ := newTestServer(0, 0)
	req := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var reports []map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &reports))
	assert.Len(t, reports, 20)
	for _, rep := range reports {
		_, present := rep["confidence_level"]
		assert.False(t, present, "confidence_level must be hidden before schema toggle")
	}
}

func TestSchemaToggle_NoRestartNeeded(t *testing.T) {
	r, _ := newTestServer(0, 0)

	toggle := httptest.NewRequest(http.MethodPost, "/admin/schema-version", nil)
	toggle.Header.Set("Authorization", "Bearer "+testAdminToken)
	wt := httptest.NewRecorder()
	r.ServeHTTP(wt, toggle)
	require.Equal(t, http.StatusOK, wt.Code)

	req := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var reports []map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &reports))
	found := false
	for _, rep := range reports {
		if _, present := rep["confidence_level"]; present {
			found = true
		}
	}
	assert.True(t, found, "confidence_level should appear after toggle")
}

func TestSchemaToggle_RequiresAdminAuth(t *testing.T) {
	r, _ := newTestServer(0, 0)
	req := httptest.NewRequest(http.MethodPost, "/admin/schema-version", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestSinceFilter(t *testing.T) {
	r, _ := newTestServer(0, 0)
	req := httptest.NewRequest(http.MethodGet, "/volcanic-reports?since=2026-09-01T00:00:00.000Z", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var filtered []map[string]any
	json.Unmarshal(w.Body.Bytes(), &filtered)

	req2 := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	req2.Header.Set("Authorization", "Bearer "+testToken)
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)
	var all []map[string]any
	json.Unmarshal(w2.Body.Bytes(), &all)

	assert.Less(t, len(filtered), len(all))
}

func TestOutage_DataEndpointFails_HealthStaysHealthy(t *testing.T) {
	r, toggles := newTestServer(0, 0)
	toggles.SetOutage(true)

	dataReq := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	dataReq.Header.Set("Authorization", "Bearer "+testToken)
	dataW := httptest.NewRecorder()
	start := time.Now()
	r.ServeHTTP(dataW, dataReq)
	elapsed := time.Since(start)

	assert.Equal(t, http.StatusServiceUnavailable, dataW.Code)
	assert.Less(t, elapsed, 200*time.Millisecond, "outage must fail fast, not hang")

	healthReq := httptest.NewRequest(http.MethodGet, "/health", nil)
	healthW := httptest.NewRecorder()
	r.ServeHTTP(healthW, healthReq)
	assert.Equal(t, http.StatusOK, healthW.Code)
}

func TestOutage_AdminEndpointStaysUsable(t *testing.T) {
	r, toggles := newTestServer(0, 0)
	toggles.SetOutage(true)

	req := httptest.NewRequest(http.MethodPost, "/admin/outage", nil)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	assert.False(t, toggles.Outage())
}

func TestRandomDelayWithinConfiguredRange(t *testing.T) {
	r, _ := newTestServer(50, 80)
	req := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	start := time.Now()
	r.ServeHTTP(w, req)
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(50))
	assert.LessOrEqual(t, elapsed.Milliseconds(), int64(200))
}
