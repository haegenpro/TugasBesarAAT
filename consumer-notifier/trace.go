package main

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const correlationHeader = "X-Correlation-ID"

type traceKey struct{}

type messageTrace struct {
	Service       string
	CorrelationID string
	Stream        string
	Sequence      uint64
}

func withServiceTrace(ctx context.Context, role string) context.Context {
	return context.WithValue(ctx, traceKey{}, messageTrace{Service: "consumer-" + role})
}

func withMessageTrace(ctx context.Context, role string, msg jetstream.Msg, meta *jetstream.MsgMetadata) context.Context {
	return context.WithValue(ctx, traceKey{}, messageTrace{
		Service: "consumer-" + role, CorrelationID: strings.TrimSpace(msg.Headers().Get(correlationHeader)),
		Stream: meta.Stream, Sequence: meta.Sequence.Stream,
	})
}

func withDecodedCorrelation(ctx context.Context, correlationID string) context.Context {
	trace, _ := ctx.Value(traceKey{}).(messageTrace)
	trace.CorrelationID = correlationID
	return context.WithValue(ctx, traceKey{}, trace)
}

func correlationFromContext(ctx context.Context) string {
	trace, _ := ctx.Value(traceKey{}).(messageTrace)
	return trace.CorrelationID
}

func traceFields(ctx context.Context) []any {
	trace, _ := ctx.Value(traceKey{}).(messageTrace)
	fields := []any{}
	if trace.Service != "" {
		fields = append(fields, "service", trace.Service)
	}
	if trace.CorrelationID != "" {
		fields = append(fields, "correlation_id", trace.CorrelationID)
	}
	if trace.Stream != "" {
		fields = append(fields, "original_stream", trace.Stream, "stream_sequence", trace.Sequence)
	}
	return fields
}

func logEvent(ctx context.Context, level slog.Level, message string, fields ...any) {
	slog.Log(ctx, level, message, append(traceFields(ctx), fields...)...)
}

func logOutbound(ctx context.Context, operation, target string, status int, started time.Time, err error) {
	fields := append(traceFields(ctx), "operation", operation, "target_service", target,
		"latency_ms", time.Since(started).Milliseconds(), "success", err == nil)
	if status != 0 {
		fields = append(fields, "status_code", status)
	}
	level := slog.LevelInfo
	if err != nil {
		level = slog.LevelWarn
		fields = append(fields, "error", err.Error())
	}
	slog.Log(ctx, level, "outbound call", fields...)
}
