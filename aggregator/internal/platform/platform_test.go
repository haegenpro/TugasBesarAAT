package platform

import (
	"bytes"
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPTraceContinuesAcrossServiceCall(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTracerProvider(old)
	defer tp.Shutdown(context.Background())
	downstreamIDs := make(chan string, 1)
	downstream := httptest.NewServer(Handler("downstream", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downstreamIDs <- TraceID(r.Context())
		w.WriteHeader(200)
	})))
	defer downstream.Close()
	upstream := Handler("upstream", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, _ := http.NewRequestWithContext(r.Context(), "GET", downstream.URL, nil)
		resp, err := Client(0).Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		resp.Body.Close()
		w.WriteHeader(200)
	}))
	req := httptest.NewRequest("GET", "/hazards", nil)
	req.Header.Set("traceparent", "00-01234567890123456789012345678901-0123456789012345-01")
	w := httptest.NewRecorder()
	upstream.ServeHTTP(w, req)
	downstreamID := <-downstreamIDs
	if downstreamID != "01234567890123456789012345678901" || w.Header().Get("X-Trace-ID") != downstreamID {
		t.Fatalf("trace ancestry lost: %q", downstreamID)
	}
	if len(exp.GetSpans()) < 3 {
		t.Fatal("missing server/client spans")
	}
}
func TestRejectMalformedAndOversizedPayloadWithoutLosingCapturedBytes(t *testing.T) {
	t.Setenv("MAX_PAYLOAD_BYTES", "8")
	b, truncated, err := ReadPayload(strings.NewReader("123456789"))
	if err == nil || !truncated || string(b) != "12345678" {
		t.Fatal("overflow not detected or prefix changed")
	}
	for _, b := range [][]byte{[]byte(`{"truncated":`), {0xff}, bytes.Repeat([]byte("["), 65)} {
		if ValidateJSON(b) == nil {
			t.Fatalf("accepted malformed payload %q", b)
		}
	}
	if ValidateJSON([]byte(`{"ok":[1]}`)) != nil {
		t.Fatal("valid payload rejected")
	}
}

func TestHTTPRejectsOversizedOrDeepJSONBeforeHandler(t *testing.T) {
	t.Setenv("MAX_PAYLOAD_BYTES", "32")
	t.Setenv("MAX_JSON_DEPTH", "2")
	for _, tc := range []struct {
		body   string
		status int
	}{{`{"a":{"b":{}}}`, 400}, {`{"a":"` + strings.Repeat("x", 40) + `"}`, 413}} {
		called := false
		handler := Handler("test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
		req := httptest.NewRequest("POST", "/auth/login", strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if called || w.Code != tc.status {
			t.Fatalf("called=%v status=%d expected=%d", called, w.Code, tc.status)
		}
	}
}
