package coastal

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Quarantine interface {
	Quarantine(context.Context, rawinput.Envelope, []byte, string) error
}

type Validator struct {
	Stations   *StationCache
	Quarantine Quarantine
	Warnings   Warnings
	Registry   *avrowire.Client
	Reader     *avrowire.Reader
	Skew       time.Duration
	Logger     *slog.Logger
	Repository *Repository
}

type outcome int

const (
	accepted outcome = iota
	rejected
	retry
)

func (v *Validator) Check(ctx context.Context, e rawinput.Envelope) (Observation, Station, outcome, error) {
	var clock *time.Time
	if e.VirtualNow != nil {
		if t, err := time.Parse(time.RFC3339Nano, *e.VirtualNow); err == nil {
			clock = &t
		}
	}
	o, err := Parse(e.Payload, clock, v.Skew)
	if err != nil {
		return o, Station{}, rejected, err
	}
	s, err := v.Stations.Lookup(ctx, o.StationID)
	if errors.Is(err, ErrUnknownStation) {
		return o, s, rejected, violation("station_id %q is not a known station", o.StationID)
	}
	if err != nil {
		return o, s, retry, err
	}
	return o, s, accepted, nil
}

func (v *Validator) Run(ctx context.Context, consumer jetstream.Consumer) {
	consume(ctx, consumer, v.Logger, "coastal validation", v.handle)
}

func (v *Validator) handle(ctx context.Context, msg jetstream.Msg) error {
	e, err := rawinput.Decode(ctx, v.Registry, v.Reader, msg.Data())
	if err != nil {
		return terminal(msg, v.Logger, err)
	}
	o, s, result, err := v.Check(ctx, e)
	switch result {
	case rejected:
		if err := v.Quarantine.Quarantine(ctx, e, e.Payload, err.Error()); err != nil {
			return err
		}
		if v.Repository != nil {
			if rejectErr := v.Repository.Reject(ctx, e, err.Error()); rejectErr != nil {
				return rejectErr
			}
		}
		platform.Add("coastal_observations_quarantined_total", 1)
		return msg.DoubleAck(ctx)
	case retry:
		return err
	}
	if o.Critical {
		warning, err := v.Warnings.Build(ctx, e, o, s)
		if err != nil {
			return err
		}
		if err = v.Warnings.Publish(ctx, e, warning); err != nil {
			return err
		}
		platform.Add("coastal_warnings_published_total", 1)
		if received, err := time.Parse(time.RFC3339Nano, e.ReceivedAt); err == nil {
			platform.Observe("coastal_warning_publish_seconds", time.Since(received).Seconds())
		}
	}
	platform.Add("coastal_observations_validated_total", 1)
	return msg.DoubleAck(ctx)
}

func terminal(msg jetstream.Msg, logger *slog.Logger, err error) error {
	var problem *canonical.Error
	if errors.As(err, &problem) && problem.Status < 500 {
		logger.Error("unreadable coastal envelope terminated", "error", err)
		return msg.Term()
	}
	return err
}

func consume(ctx context.Context, consumer jetstream.Consumer, logger *slog.Logger, name string, handle func(context.Context, jetstream.Msg) error) {
	failures := 0
	for ctx.Err() == nil {
		batch, err := consumer.Fetch(32, jetstream.FetchMaxWait(time.Second))
		if err == nil {
			for msg := range batch.Messages() {
				call, cancel := context.WithTimeout(ctx, 5*time.Second)
				err = handle(call, msg)
				cancel()
				if err != nil {
					_ = msg.NakWithDelay(time.Second)
					break
				}
			}
			if err == nil {
				err = batch.Error()
			}
		}
		if err == nil || errors.Is(err, jetstream.ErrNoMessages) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
			failures = 0
			continue
		}
		if ctx.Err() != nil {
			return
		}
		failures++
		logger.Warn(name+" pending", "error", err, "failures", failures)
		if !rawinput.Sleep(ctx, rawinput.Backoff(failures)) {
			return
		}
	}
}
