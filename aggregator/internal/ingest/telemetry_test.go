package ingest

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestDurableTraceDoesNotMutateSourceMetadata(t *testing.T) {
	prior := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(prior)
	tid, err := trace.TraceIDFromHex("12345678901234567890123456789012")
	require.NoError(t, err)
	sid, err := trace.SpanIDFromHex("1234567890123456")
	require.NoError(t, err)
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}))
	original := map[string]any{"event_id": "42", "critical": true}
	stored := traceMetadata(ctx, original)
	require.NotContains(t, original, "_traceparent")
	require.Equal(t, "00-12345678901234567890123456789012-1234567890123456-01", stored["_traceparent"])
	restored, span := inputSpan(context.Background(), Input{Metadata: stored}, "retry after restart")
	defer span.End()
	require.Equal(t, tid, trace.SpanContextFromContext(restored).TraceID())
}

func TestMalformedFailureCursorsAreRejectedBeforeQuery(t *testing.T) {
	s := &Store{}
	for _, raw := range []string{`{}`, `{"Source":"SATELIT","At":"2026-11-21T00:00:00Z","ID":"bad"}`} {
		_, e := s.Failures(context.Background(), "SATELIT", 10, base64.RawURLEncoding.EncodeToString([]byte(raw)))
		require.ErrorIs(t, e, ErrConflict)
	}
}
