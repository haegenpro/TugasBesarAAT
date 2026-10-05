package publisher

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"aggregator/internal/avrowire"
	"aggregator/internal/domain"
	"aggregator/internal/timeutil"
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

type captureJetStream struct {
	jetstream.JetStream
	message *nats.Msg
}

func (j *captureJetStream) PublishMsg(_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	j.message = msg
	return &jetstream.PubAck{}, nil
}

func TestPublishCarriesCorrelationHeaderAndEnvelope(t *testing.T) {
	codec, err := goavro.NewCodec(envelopeSchema)
	require.NoError(t, err)
	js := &captureJetStream{}
	p := &Publisher{js: js, subject: "hazard.events", logger: slog.New(slog.NewJSONHandler(io.Discard, nil)), schema: &avrowire.Schema{ID: 17, Codec: codec}}
	now := time.Now().UTC()
	event := domain.HazardEvent{
		HazardID: "h1", Source: domain.SourcePVMBG, SourceRefID: "r1", HazardType: domain.HazardVolcanic,
		Severity: domain.SeveritySiaga, AreaName: "Merapi", Latitude: -7.0, Longitude: 110.0,
		OccurredAt: timeutil.New(now), IngestedAt: timeutil.New(now), Attributes: map[string]any{"confidence_level": 0.88},
	}
	require.NoError(t, p.Publish(context.Background(), event, "cid-17"))
	require.Equal(t, "cid-17", js.message.Header.Get("X-Correlation-ID"))
	native, rest, err := codec.NativeFromBinary(js.message.Data[5:])
	require.NoError(t, err)
	require.Empty(t, rest)
	require.Equal(t, "cid-17", native.(map[string]any)["correlation_id"])
}
