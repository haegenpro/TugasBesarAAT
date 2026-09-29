// Package publisher publishes newly mapped HazardEvents to NATS JetStream on
// the hazard.events subject, waiting for a durable publish acknowledgement.
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
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

// New connects to NATS and requires the broker initializer's stream to exist.
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

	return &Publisher{nc: nc, js: js, subject: subject, logger: logger}, nil
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
