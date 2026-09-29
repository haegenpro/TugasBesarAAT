// Package publisher publishes newly mapped HazardEvents to NATS JetStream on
// the hazard.events subject, waiting for a durable publish acknowledgement.
package publisher

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

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
	nc      *nats.Conn
	js      jetstream.JetStream
	subject string
	logger  *slog.Logger
}

// New connects to NATS, enables JetStream, and ensures a durable file-backed
// stream exists for the given subject.
func New(ctx context.Context, url, stream, subject string, logger *slog.Logger) (*Publisher, error) {
	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      stream,
		Subjects:  []string{subject},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		nc.Close()
		return nil, err
	}

	return &Publisher{nc: nc, js: js, subject: subject, logger: logger}, nil
}

func (p *Publisher) Close() {
	p.nc.Close()
}

// Publish sends the envelope and blocks until JetStream acknowledges the
// durable write, satisfying the at-least-once delivery requirement.
func (p *Publisher) Publish(ctx context.Context, event domain.HazardEvent, correlationID string) error {
	env := Envelope{
		EnvelopeVersion: 1,
		PublishedAt:     timeutil.FormatISOMilli(domain.Now()),
		CorrelationID:   correlationID,
		Payload:         event,
	}
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}

	start := time.Now()
	_, err = p.js.Publish(ctx, p.subject, data)
	latency := time.Since(start).Milliseconds()

	success := err == nil
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	middleware.LogOutbound(p.logger, "aggregator", correlationID, "nats", p.subject, 0, success, latency, errMsg)
	return err
}
