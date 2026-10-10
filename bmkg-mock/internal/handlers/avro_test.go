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

	"bmkg-mock/internal/avrowire"
	"bmkg-mock/internal/history"
	"bmkg-mock/internal/seed"
)

func TestAvroSourcesWithRegistry(t *testing.T) {
	registryURL := os.Getenv("TEST_SCHEMA_REGISTRY_URL")
	if registryURL == "" {
		t.Skip("TEST_SCHEMA_REGISTRY_URL is not set")
	}
	registry := avrowire.NewClient(registryURL)
	// Seed schemas from previous builds to check that removing extra metadata
	// remains FULL_TRANSITIVE compatible with an existing registry.
	for subject, schema := range map[string]string{"bmkg.seismic-events-value": seismicSchema, "bmkg.tsunami-warnings-value": warningSchema} {
		legacy := strings.Replace(schema, `]}}`, `,{"name":"updated_at","type":["null","string"],"default":null}]}}`, 1)
		_, err := registry.Register(context.Background(), subject, legacy)
		require.NoError(t, err)
	}
	events, warnings := seed.Build()
	h := New(events, warnings, 0)
	h.Registry = registry
	archive, err := history.Open(":memory:")
	require.NoError(t, err)
	defer archive.DB.Close()
	require.NoError(t, h.AttachArchive(archive))
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.GET("/history", h.History)
	router.GET("/seismic-events", h.SeismicEvents)
	router.GET("/tsunami-warnings", h.TsunamiWarnings)
	for _, path := range []string{"/seismic-events", "/tsunami-warnings", "/history?entity=seismic-events", "/history?entity=tsunami-warnings"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Accept", avrowire.MediaType)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		native, err := registry.Decode(context.Background(), response.Body.Bytes())
		require.NoError(t, err)
		if strings.HasPrefix(path, "/history") {
			page := native.(map[string]any)
			require.NotEmpty(t, page["data"])
			require.Positive(t, page["next_cursor"])
			continue
		}
		records, err := avrowire.Records(native)
		require.NoError(t, err)
		require.NotEmpty(t, records)
		require.NotContains(t, records[0], "updated_at")
	}
}
