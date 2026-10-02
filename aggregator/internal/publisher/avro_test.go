package publisher

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"aggregator/internal/avrowire"
)

func TestBrokerEnvelopeWithRegistry(t *testing.T) {
	registryURL := os.Getenv("TEST_SCHEMA_REGISTRY_URL")
	if registryURL == "" {
		t.Skip("TEST_SCHEMA_REGISTRY_URL is not set")
	}
	registry := avrowire.NewClient(registryURL)
	schema, err := registry.Register(context.Background(), "hazard.events-value", envelopeSchema)
	require.NoError(t, err)
	attribute, err := json.Marshal(1200)
	require.NoError(t, err)
	framed, err := avrowire.Encode(schema, map[string]any{
		"envelope_version": int32(1),
		"published_at":     "2026-09-24T12:34:56.123Z",
		"correlation_id":   "cid",
		"payload": map[string]any{
			"hazard_id": "id", "source": "PVMBG", "source_ref_id": "ref",
			"hazard_type": "VOLCANIC", "severity": "SIAGA", "area_name": "area",
			"latitude": float64(-6), "longitude": float64(106),
			"occurred_at": "2026-09-24T12:34:56.123Z", "ingested_at": "2026-09-24T12:34:56.123Z",
			"attributes": map[string]any{"ash_column_height_m": attribute},
		},
	})
	require.NoError(t, err)
	native, err := registry.Decode(context.Background(), framed)
	require.NoError(t, err)
	envelope := native.(map[string]any)
	attrs := envelope["payload"].(map[string]any)["attributes"].(map[string]any)
	require.JSONEq(t, "1200", string(attrs["ash_column_height_m"].([]byte)))
}
