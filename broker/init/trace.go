package main

import (
	"context"
	"crypto/rand"
	"log/slog"
	"time"
)

type brokerTraceKey struct{}

func withBrokerTrace(ctx context.Context) context.Context {
	if cid, _ := ctx.Value(brokerTraceKey{}).(string); cid != "" {
		return ctx
	}
	return context.WithValue(ctx, brokerTraceKey{}, rand.Text())
}
func brokerLog(ctx context.Context, level slog.Level, message string, fields ...any) {
	cid, _ := ctx.Value(brokerTraceKey{}).(string)
	slog.Log(ctx, level, message, append([]any{"service", "broker", "component", "broker-init", "correlation_id", cid}, fields...)...)
}
func brokerCall[T any](ctx context.Context, operation string, call func() (T, error)) (result T, err error) {
	ctx = withBrokerTrace(ctx)
	started := time.Now()
	defer func() {
		fields := []any{"operation", operation, "target_service", "nats", "latency_ms", time.Since(started).Milliseconds(), "success", err == nil}
		level := slog.LevelInfo
		if err != nil {
			level = slog.LevelWarn
			fields = append(fields, "error", err.Error())
		}
		brokerLog(ctx, level, "outbound call", fields...)
	}()
	return call()
}
