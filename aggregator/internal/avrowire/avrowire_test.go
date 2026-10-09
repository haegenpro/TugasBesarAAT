package avrowire

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

// This checks actual registry persistence, including metadata that standard
// Avro compatibility intentionally does not interpret.
func TestRegistryRetainsValidationMetadata(t *testing.T) {
	base := os.Getenv("TEST_SCHEMA_REGISTRY_URL")
	if base == "" {
		t.Skip("TEST_SCHEMA_REGISTRY_URL not set")
	}
	text := `{"type":"record","name":"MetadataProbe","fields":[{"name":"value","type":"double","x-validation":{"minimum":0,"maximum":1,"allowedValues":[0,0.5,1]}}]}`
	writer := NewClient(base)
	_, e := writer.Register(context.Background(), "validation-metadata-probe", strings.Replace(text, `,"x-validation":{"minimum":0,"maximum":1,"allowedValues":[0,0.5,1]}`, "", 1))
	require.NoError(t, e)
	schema, e := writer.Register(context.Background(), "validation-metadata-probe", text)
	require.NoError(t, e)
	reader := NewClient(base)
	loaded, e := reader.Lookup(context.Background(), schema.ID)
	require.NoError(t, e)
	require.Contains(t, loaded.Text, "x-validation")
	for _, tc := range []struct {
		v     float64
		valid bool
	}{{0, true}, {.5, true}, {1, true}, {.6, false}, {1.5, false}} {
		b, e := Encode(schema, map[string]any{"value": tc.v})
		require.NoError(t, e)
		_, e = reader.Decode(context.Background(), b)
		if tc.valid {
			require.NoError(t, e)
		} else {
			require.Error(t, e)
		}
	}
}
