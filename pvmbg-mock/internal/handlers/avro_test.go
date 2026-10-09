package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"pvmbg-mock/internal/avrowire"
	"pvmbg-mock/internal/history"
	"pvmbg-mock/internal/seed"
	"pvmbg-mock/internal/state"
)

// Run with TEST_SCHEMA_REGISTRY_URL to exercise the real registry's
// compatibility check and the live v1 -> v2 toggle.
func TestAvroSchemaEvolutionWithRegistry(t *testing.T) {
	registryURL := os.Getenv("TEST_SCHEMA_REGISTRY_URL")
	if registryURL == "" {
		t.Skip("TEST_SCHEMA_REGISTRY_URL is not set")
	}
	registry := avrowire.NewClient(registryURL)
	for _, schema := range []string{volcanicV1, volcanicV2()} {
		legacy := strings.Replace(schema, `,{"name":"updated_at","type":["null","string"],"default":null}`, "", 1)
		_, err := registry.Register(context.Background(), volcanicSubject, legacy)
		require.NoError(t, err)
	}
	toggles := state.New()
	h := New(seed.Build(), toggles, 0, 0)
	h.Registry = registry
	archive, err := history.Open(":memory:")
	require.NoError(t, err)
	defer archive.DB.Close()
	require.NoError(t, h.AttachArchive(archive))
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.GET("/history", h.History)
	router.GET("/volcanic-reports", h.VolcanicReports)
	router.POST("/admin/schema-version", h.AdminSchemaVersion)
	read := func() []map[string]any {
		req := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
		req.Header.Set("Authorization", "Bearer data-token")
		req.Header.Set("Accept", avrowire.MediaType)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		native, err := registry.Decode(context.Background(), w.Body.Bytes())
		require.NoError(t, err)
		records, err := avrowire.Records(native)
		require.NoError(t, err)
		return records
	}
	readHistory := func(confidence bool) {
		req := httptest.NewRequest(http.MethodGet, "/history?entity=volcanic-reports", nil)
		req.Header.Set("Accept", avrowire.MediaType)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		require.Equal(t, 200, w.Code, w.Body.String())
		native, e := registry.Decode(context.Background(), w.Body.Bytes())
		require.NoError(t, e)
		page := native.(map[string]any)
		items := page["data"].([]any)
		require.NotEmpty(t, items)
		payload := items[0].(map[string]any)["payload"].(map[string]any)
		require.Equal(t, confidence, payload["confidence_level"] != nil)
	}
	readHistory(false)
	before := read()
	require.NotEmpty(t, before[0]["updated_at"])
	require.NotContains(t, before[0], "confidence_level")

	// A cold registry outage must reject an uncached schema without changing
	// the source's JSON contract or flipping the admin toggle.
	h.Registry = avrowire.NewClient("http://127.0.0.1:1")
	cold := httptest.NewRecorder()
	coldRequest := httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	coldRequest.Header.Set("Accept", avrowire.MediaType)
	router.ServeHTTP(cold, coldRequest)
	require.Equal(t, http.StatusServiceUnavailable, cold.Code)
	failedToggle := httptest.NewRecorder()
	router.ServeHTTP(failedToggle, httptest.NewRequest(http.MethodPost, "/admin/schema-version", nil))
	require.Equal(t, http.StatusServiceUnavailable, failedToggle.Code)
	require.False(t, toggles.ShowConfidence())
	h.Registry = registry

	toggle := httptest.NewRequest(http.MethodPost, "/admin/schema-version", nil)
	toggle.Header.Set("Authorization", "Bearer admin-token")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, toggle)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	after := read()
	readHistory(true)
	require.Equal(t, 0.75, after[0]["confidence_level"])
	require.NotContains(t, after[1], "confidence_level")
	require.Len(t, after, len(before)+1)
	newReportID := after[len(after)-1]["report_id"]
	require.Equal(t, h.Reports[len(h.Reports)-1].ReportID, newReportID)
	for _, report := range before {
		require.NotEqual(t, report["report_id"], newReportID)
	}
	require.Equal(t, 0.88, after[len(after)-1]["confidence_level"])
}

func TestHistoryRegistrationFailureDoesNotFlipToggle(t *testing.T) {
	registryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "history-value") {
			http.Error(w, "unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":1}`))
	}))
	defer registryServer.Close()
	h := New(seed.Build(), state.New(), 0, 0)
	h.Registry = avrowire.NewClient(registryServer.URL)
	router := gin.New()
	router.POST("/admin/schema-version", h.AdminSchemaVersion)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest("POST", "/admin/schema-version", nil))
	require.Equal(t, 503, response.Code)
	require.False(t, h.Toggles.ShowConfidence())
}
