package rawinput

import (
	"aggregator/internal/avrowire"
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Broker interface {
	PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

type Sender struct {
	JS         Broker
	Registry   *avrowire.Client
	SchemaText string
}

func (s Sender) Message(ctx context.Context, subject string, e Envelope) (*nats.Msg, error) {
	schema, err := s.Registry.Register(ctx, Subject, s.SchemaText)
	if err != nil {
		return nil, err
	}
	b, err := Encode(schema, e)
	if err != nil {
		return nil, err
	}
	if len(b) > MaxMessage {
		return nil, fmt.Errorf("raw envelope exceeds broker bound")
	}
	msg := &nats.Msg{Subject: subject, Data: b, Header: nats.Header{}}
	msg.Header.Set("Nats-Msg-Id", e.MessageID)
	msg.Header.Set("X-Correlation-ID", e.CorrelationID)
	if e.Traceparent != nil {
		msg.Header.Set("Traceparent", *e.Traceparent)
	}
	return msg, nil
}

func (s Sender) Send(ctx context.Context, subject string, e Envelope) error {
	msg, err := s.Message(ctx, subject, e)
	if err != nil {
		return err
	}
	_, err = s.JS.PublishMsg(ctx, msg)
	return err
}

func Backoff(failures int) time.Duration {
	limit := 250 * time.Millisecond
	for i := 1; i < failures && limit < 30*time.Second; i++ {
		limit *= 2
	}
	limit = min(limit, 30*time.Second)
	return time.Duration(rand.Int64N(int64(limit) + 1))
}

func Sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
