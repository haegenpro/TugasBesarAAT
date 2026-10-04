package avrowire

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCachedSchemaSurvivesRegistryOutage(t *testing.T) {
	schemaText := `{"type":"record","name":"Example","fields":[{"name":"value","type":"string"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			w.WriteHeader(http.StatusOK)
		case http.MethodPost:
			_, _ = w.Write([]byte(`{"id":17}`))
		default:
			http.NotFound(w, r)
		}
	}))
	client := NewClient(server.URL)
	schema, err := client.Register(context.Background(), "example-value", schemaText)
	require.NoError(t, err)
	server.Close()

	cached, err := client.Register(context.Background(), "example-value", schemaText)
	require.NoError(t, err)
	require.Same(t, schema, cached)
	framed, err := Encode(cached, map[string]any{"value": "known"})
	require.NoError(t, err)
	decoded, err := client.Decode(context.Background(), framed)
	require.NoError(t, err)
	require.Equal(t, "known", decoded.(map[string]any)["value"])

	cold := NewClient(server.URL)
	_, err = cold.Decode(context.Background(), framed)
	require.Error(t, err)
}
