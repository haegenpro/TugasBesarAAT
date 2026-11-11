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

func TestM2CanonicalAndRetractionAvro(t *testing.T) {
	codec, err := goavro.NewCodec(envelopeSchema)
	require.NoError(t, err)
	js := &captureJetStream{}
	p := &Publisher{js: js, subject: "hazard.events", logger: slog.New(slog.NewJSONHandler(io.Discard, nil)), schema: &avrowire.Schema{ID: 17, Codec: codec}}
	for _, source := range []domain.Source{domain.SourceSatelit, domain.SourceSensorPantai} {
		kind := domain.HazardWildfire
		if source == domain.SourceSensorPantai {
			kind = domain.HazardCoastal
		}
		event := domain.HazardEvent{HazardID: "fixture", Source: source, SourceRefID: "fixture", HazardType: kind, Severity: domain.SeverityWaspada, AreaName: "Fixture", OccurredAt: timeutil.New(time.Now()), IngestedAt: timeutil.New(time.Now()), Attributes: map[string]any{"satellites": []string{"N20", "N21"}, "detection_keys": []string{"a", "b"}, "exact": json.Number("9007199254740993")}}
		require.NoError(t, p.Publish(context.Background(), event, "fixture"))
		require.Equal(t, byte(0), js.message.Data[0])
		native, rest, err := codec.NativeFromBinary(js.message.Data[5:])
		require.NoError(t, err)
		require.Empty(t, rest)
		payload := native.(map[string]any)["payload"].(map[string]any)
		require.Equal(t, string(source), payload["source"])
		require.Equal(t, string(kind), payload["hazard_type"])
		attrs := payload["attributes"].(map[string]any)
		require.JSONEq(t, `["N20","N21"]`, string(attrs["satellites"].([]byte)))
		require.Equal(t, "9007199254740993", string(attrs["exact"].([]byte)))
	}
	codec, err = goavro.NewCodec(lifecycleSchema)
	require.NoError(t, err)
	p.schema = &avrowire.Schema{ID: 18, Codec: codec}
	require.NoError(t, p.PublishLifecycle(context.Background(), domain.LifecycleEvent{EventID: "retract", EventType: "hazard_retracted", RecordID: "fixture", Revision: 3, HazardRevision: 3, PublishedAt: time.Now().UTC(), CorrelationID: "fixture"}))
	native, rest, err := codec.NativeFromBinary(js.message.Data[5:])
	require.NoError(t, err)
	require.Empty(t, rest)
	envelope := native.(map[string]any)
	require.Equal(t, "hazard_retracted", envelope["event_type"])
	var snapshot domain.LifecycleEvent
	require.NoError(t, json.Unmarshal(envelope["snapshot"].([]byte), &snapshot))
	require.Nil(t, snapshot.Hazard)
	require.Equal(t, int64(3), snapshot.HazardRevision)
}
