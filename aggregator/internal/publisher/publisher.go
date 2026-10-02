// Package publisher publishes newly mapped HazardEvents to NATS JetStream,
// waiting for a durable publish acknowledgement.
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"aggregator/internal/avrowire"
	"aggregator/internal/domain"
	"aggregator/internal/middleware"
	"aggregator/internal/timeutil"
)

type Envelope struct {
	EnvelopeVersion int                `json:"envelope_version"`
	PublishedAt     string             `json:"published_at"`
	CorrelationID   string             `json:"correlation_id"`
	Payload         domain.HazardEvent `json:"payload"`
}

type Publisher struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	subject  string
	logger   *slog.Logger
	registry *avrowire.Client
	schema   *avrowire.Schema
}

const envelopeSchema = `{"type":"record","name":"HazardEnvelope","namespace":"id.go.bnpb","fields":[{"name":"envelope_version","type":"int"},{"name":"published_at","type":"string"},{"name":"correlation_id","type":"string"},{"name":"payload","type":{"type":"record","name":"HazardEvent","fields":[{"name":"hazard_id","type":"string"},{"name":"source","type":"string"},{"name":"source_ref_id","type":"string"},{"name":"hazard_type","type":"string"},{"name":"severity","type":"string"},{"name":"area_name","type":"string"},{"name":"latitude","type":"double"},{"name":"longitude","type":"double"},{"name":"occurred_at","type":"string"},{"name":"ingested_at","type":"string"},{"name":"attributes","type":{"type":"map","values":"bytes"}}]}}]}`

// New connects to NATS and requires the broker initializer's stream to exist.
func New(ctx context.Context, url, stream, subject string, registry *avrowire.Client, logger *slog.Logger) (*Publisher, error) {
	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}

	st, err := js.Stream(ctx, stream)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("JetStream stream %q is unavailable: %w", stream, err)
	}
	info, err := st.Info(ctx)
	if err != nil {
		nc.Close()
		return nil, err
	}
	found := false
	for _, configured := range info.Config.Subjects {
		if configured == subject {
			found = true
			break
		}
	}
	if !found {
		nc.Close()
		return nil, fmt.Errorf("JetStream stream %q does not contain subject %q", stream, subject)
	}

	return &Publisher{nc: nc, js: js, subject: subject, logger: logger, registry: registry}, nil
}

func (p *Publisher) Close() {
	p.nc.Close()
}

func (p *Publisher) Connected() bool {
	return p.nc.IsConnected()
}

// Publish sends the envelope and blocks until JetStream acknowledges the
// durable write. The outbox retries when acknowledgement is missing.
func (p *Publisher) Publish(ctx context.Context, event domain.HazardEvent, correlationID string) error {
	if p.schema == nil {
		schema, err := p.registry.Register(avrowire.WithCorrelationID(ctx, correlationID), p.subject+"-value", envelopeSchema)
		if err != nil {
			middleware.LogOutbound(p.logger, "aggregator", correlationID, "schema-registry", p.subject+"-value", 0, false, 0, err.Error())
			return fmt.Errorf("register broker schema: %w", err)
		}
		p.schema = schema
	}
	attributes := make(map[string]any, len(event.Attributes))
	for key, value := range event.Attributes {
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		attributes[key] = encoded
	}
	data, err := avrowire.Encode(p.schema, map[string]any{
		"envelope_version": int32(1),
		"published_at":     timeutil.FormatISOMilli(domain.Now()),
		"correlation_id":   correlationID,
		"payload": map[string]any{
			"hazard_id": event.HazardID, "source": string(event.Source), "source_ref_id": event.SourceRefID,
			"hazard_type": string(event.HazardType), "severity": string(event.Severity), "area_name": event.AreaName,
			"latitude": event.Latitude, "longitude": event.Longitude,
			"occurred_at": timeutil.FormatISOMilli(event.OccurredAt.Time), "ingested_at": timeutil.FormatISOMilli(event.IngestedAt.Time),
			"attributes": attributes,
		},
	})
	if err != nil {
		return err
	}

	start := time.Now()
	_, err = p.js.Publish(ctx, p.subject, data, jetstream.WithMsgID(event.HazardID))
	latency := time.Since(start).Milliseconds()

	success := err == nil
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	middleware.LogOutbound(p.logger, "aggregator", correlationID, "nats", p.subject, 0, success, latency, errMsg)
	return err
}
