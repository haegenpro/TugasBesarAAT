package main

import (
	"clientapi/internal/platform"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestAuthRejectionPreservesRetryAfterAndReusesConnection(t *testing.T) {
	var connections atomic.Int32
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(429)
		io.WriteString(w, `{"error":{"code":"busy","message":"retry later"}}`)
	}))
	upstream.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	upstream.Start()
	defer upstream.Close()
	app := &api{cfg: config{AuthURL: upstream.URL}, client: upstream.Client(), log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	defer app.client.CloseIdleConnections()
	for range 3 {
		writer := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(writer)
		ctx.Request = httptest.NewRequest("POST", "/auth/login", nil)
		app.proxyAuth(ctx, "/internal/login", loginRequest{ClientID: "test", ClientSecret: "test-only"})
		if writer.Code != 429 || writer.Header().Get("Retry-After") != "1" {
			t.Fatalf("uncontrolled rejection: %d %v", writer.Code, writer.Header())
		}
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("rejections opened %d TCP connections instead of reusing one", got)
	}
}

func TestFieldTransportPreservesTraceAcrossUpstreamCalls(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer otel.SetTracerProvider(previous)
	defer provider.Shutdown(context.Background())
	otel.SetTextMapPropagator(propagation.TraceContext{})
	var parent string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parent = r.Header.Get("Traceparent")
		io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	t.Setenv("FIELD_HTTP_MAX_CONNS_PER_HOST", "12")
	app := &api{cfg: config{PriorityEnabled: true, AuthMaxInFlight: 4, AuthFieldReserved: 1, AuthPriorityKey: "test-only-key", MaxInFlight: 4, MaxInFlightPerClient: 4, FieldReserved: 1}, log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	app.router()
	defer app.fieldClient.CloseIdleConnections()
	ctx, span := platform.Start(context.Background(), "field-request")
	defer span.End()
	writer := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(writer)
	c.Request = httptest.NewRequest("GET", "/hazards", nil).WithContext(ctx)
	c.Set("field_auth_lane", true)
	var output map[string]any
	status, err := app.call(c, "aggregator", "GET", upstream.URL, nil, false, &output)
	if err != nil || status != 200 {
		t.Fatalf("upstream failed: %d %v", status, err)
	}
	if parent == "" {
		t.Fatal("Field upstream request lost trace propagation")
	}
	if len(exporter.GetSpans()) < 1 {
		t.Fatal("Field upstream call was not traced")
	}
}
