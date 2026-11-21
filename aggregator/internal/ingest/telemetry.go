package ingest

import (
	"aggregator/internal/platform"
	"context"
	"log/slog"
	"net/http"

	"go.opentelemetry.io/otel/trace"
)

func traceMetadata(ctx context.Context, metadata map[string]any) map[string]any {
	out := make(map[string]any, len(metadata)+1)
	for key, value := range metadata {
		out[key] = value
	}
	header := http.Header{}
	platform.Inject(ctx, header)
	if parent := header.Get("Traceparent"); parent != "" {
		out["_traceparent"] = parent
	}
	return out
}
func inputSpan(ctx context.Context, i Input, name string) (context.Context, trace.Span) {
	header := http.Header{}
	if parent, ok := i.Metadata["_traceparent"].(string); ok {
		header.Set("Traceparent", parent)
	}
	return platform.Start(platform.Extract(ctx, header), name)
}
func inputLog(ctx context.Context, i Input, event string) {
	// Match trace sampling to keep the collector bounded during catch-up. Every
	// failure is logged independently; original inputs remain durably retained.
	if trace.SpanContextFromContext(ctx).IsSampled() {
		slog.Info(event, "source", i.Source, "message_id", i.ID,
			"correlation_id", i.CorrelationID, "trace_id", platform.TraceID(ctx))
	}
}
