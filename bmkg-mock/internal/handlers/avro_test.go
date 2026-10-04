package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"bmkg-mock/internal/avrowire"
	"bmkg-mock/internal/seed"
)

func TestAvroSourcesWithRegistry(t *testing.T) {
	registryURL := os.Getenv("TEST_SCHEMA_REGISTRY_URL")
	if registryURL == "" {
		t.Skip("TEST_SCHEMA_REGISTRY_URL is not set")
	}
	registry := avrowire.NewClient(registryURL)
	events, warnings := seed.Build()
	h := New(events, warnings, 0)
	h.Registry = registry
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.GET("/seismic-events", h.SeismicEvents)
	router.GET("/tsunami-warnings", h.TsunamiWarnings)
	for _, path := range []string{"/seismic-events", "/tsunami-warnings"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Accept", avrowire.MediaType)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		native, err := registry.Decode(context.Background(), response.Body.Bytes())
		require.NoError(t, err)
		records, err := avrowire.Records(native)
		require.NoError(t, err)
		require.NotEmpty(t, records)
	}
}
