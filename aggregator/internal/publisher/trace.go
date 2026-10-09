package publisher

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"aggregator/internal/middleware"
	"github.com/nats-io/nats.go"
)

func brokerCall[T any](ctx context.Context, logger *slog.Logger, operation string, call func() (T, error)) (T, error) {
	started := time.Now()
	result, err := call()
	reason := ""
	if err != nil {
		// Connection errors may embed a URL with credentials. Log a safe category.
		reason = "broker_error"
		switch {
		case errors.Is(err, context.Canceled):
			reason = "canceled"
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout):
			reason = "timeout"
		case errors.Is(err, nats.ErrNoServers):
			reason = "no_servers"
		}
	}
	middleware.LogOutbound(logger, "aggregator", middleware.CorrelationFromContext(ctx), "nats", operation, 0, err == nil, time.Since(started).Milliseconds(), reason)
	return result, err
}
