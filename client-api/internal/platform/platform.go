// Package platform contains service-local operational plumbing, never shared business logic.
package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func Int(key string, fallback int) int {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		panic(fmt.Sprintf("%s must be a nonnegative integer", key))
	}
	return n
}
func Bool(key string, fallback bool) bool {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	v, e := strconv.ParseBool(raw)
	if e != nil {
		panic(key + " must be boolean")
	}
	return v
}
func Duration(key string, fallback time.Duration) time.Duration {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	d, e := time.ParseDuration(raw)
	if e != nil || d <= 0 {
		panic(key + " must be a positive duration")
	}
	return d
}
func Init(service string) func(context.Context) error {
	for _, key := range []string{"DB_MAX_OPEN_CONNS", "HTTP_MAX_IDLE_CONNS", "HTTP_MAX_IDLE_CONNS_PER_HOST", "HTTP_MAX_CONNS_PER_HOST", "MAX_PAYLOAD_BYTES", "MAX_JSON_DEPTH", "CONSUMER_WORKERS", "NATS_MAX_ACK_PENDING"} {
		if _, ok := os.LookupEnv(key); ok && Int(key, 1) < 1 {
			panic(key + " must be positive")
		}
	}
	_ = Int("DB_MAX_IDLE_CONNS", 8)
	for key, fallback := range map[string]time.Duration{"DB_STATEMENT_TIMEOUT": 5 * time.Second, "DB_LOCK_TIMEOUT": 2 * time.Second, "SHUTDOWN_TIMEOUT": 30 * time.Second, "NATS_ACK_WAIT": 60 * time.Second} {
		_ = Duration(key, fallback)
	}
	host, _ := os.Hostname()
	if v := os.Getenv("INSTANCE_ID"); v != "" {
		host = v
	}
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service), attribute.String("service.instance.id", host))), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample()))}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" {
		exp, err := otlptracehttp.New(context.Background(), otlptracehttp.WithTimeout(2*time.Second))
		if err != nil {
			panic("invalid trace exporter configuration")
		}
		opts = append(opts, sdktrace.WithBatcher(exp, sdktrace.WithMaxQueueSize(1024), sdktrace.WithMaxExportBatchSize(256), sdktrace.WithExportTimeout(2*time.Second)))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp.Shutdown
}
func Start(ctx context.Context, name string) (context.Context, trace.Span) {
	return otel.Tracer("application").Start(ctx, name)
}
func Inject(ctx context.Context, h http.Header) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(h))
}
func Extract(ctx context.Context, h http.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(h))
}
func TraceID(ctx context.Context) string { return trace.SpanContextFromContext(ctx).TraceID().String() }
func ProblemStatus(status int, message string) map[string]string {
	code := "bad_request"
	switch status {
	case 401:
		code = "unauthorized"
	case 403:
		code = "forbidden"
	case 404:
		code = "not_found"
	case 405:
		code = "method_not_allowed"
	case 413:
		code = "payload_too_large"
	case 429:
		code = "too_many_requests"
	case 503:
		code = "unavailable"
	case 500:
		code = "internal_error"
	}
	return map[string]string{"code": code, "message": message}
}
func WriteError(w http.ResponseWriter, status int, message string) {
	p := ProblemStatus(status, message)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": p})
}

// ReadPayload rejects overflow rather than accepting a truncated document. The caller may
// persist the captured prefix with truncated=true when the response exceeds the bound.
func ReadPayload(r io.Reader) ([]byte, bool, error) {
	max := Int("MAX_PAYLOAD_BYTES", 4<<20)
	b, e := io.ReadAll(io.LimitReader(r, int64(max)+1))
	if e != nil {
		return b, false, e
	}
	if len(b) > max {
		return b[:max], true, errors.New("payload_too_large")
	}
	return b, false, nil
}
func ValidateJSON(b []byte) error {
	if !utf8.Valid(b) {
		return errors.New("invalid_utf8")
	}
	depth, max := 0, Int("MAX_JSON_DEPTH", 64)
	quoted, escaped := false, false
	for _, c := range b {
		if quoted {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		if c == '"' {
			quoted = true
			continue
		}
		switch c {
		case '{', '[':
			depth++
			if depth > max {
				return errors.New("json_too_deep")
			}
		case '}', ']':
			depth--
		}
	}
	if !json.Valid(b) {
		return errors.New("invalid_json")
	}
	return nil
}

type Transport struct{ Base http.RoundTripper }

func (t Transport) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx, span := otel.Tracer("application").Start(r.Context(), r.Method+" "+r.URL.Host, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	r = r.Clone(ctx)
	r.Header = r.Header.Clone()
	Inject(ctx, r.Header)
	resp, err := t.Base.RoundTrip(r)
	if err != nil {
		span.SetStatus(codes.Error, "transport error")
	} else {
		span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	}
	return resp, err
}
func Client(timeout time.Duration) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = Int("HTTP_MAX_IDLE_CONNS", 128)
	t.MaxIdleConnsPerHost = Int("HTTP_MAX_IDLE_CONNS_PER_HOST", 32)
	t.MaxConnsPerHost = Int("HTTP_MAX_CONNS_PER_HOST", 64)
	return &http.Client{Timeout: timeout, Transport: Transport{t}}
}

type sample struct {
	Count, Errors int64
	Sum           float64
	Buckets       [9]int64
}

var mu sync.Mutex
var requests = map[string]*sample{}
var gauges = map[string]float64{}
var bounds = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 5}

func Gauge(name string, value float64) { mu.Lock(); defer mu.Unlock(); gauges[name] = value }
func Add(name string, value float64)   { mu.Lock(); defer mu.Unlock(); gauges[name] += value }
func Metrics(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for labels, s := range requests {
		fmt.Fprintf(w, "app_http_requests_total{%s} %d\napp_http_errors_total{%s} %d\napp_http_request_duration_seconds_sum{%s} %g\napp_http_request_duration_seconds_count{%s} %d\n", labels, s.Count, labels, s.Errors, labels, s.Sum, labels, s.Count)
		for i, b := range bounds {
			fmt.Fprintf(w, "app_http_request_duration_seconds_bucket{%s,le=%q} %d\n", labels, strconv.FormatFloat(b, 'g', -1, 64), s.Buckets[i])
		}
		fmt.Fprintf(w, "app_http_request_duration_seconds_bucket{%s,le=\"+Inf\"} %d\n", labels, s.Count)
	}
	for n, v := range gauges {
		fmt.Fprintf(w, "app_%s %g\n", n, v)
	}
}

type response struct {
	http.ResponseWriter
	status int
}

func (w *response) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
		w.ResponseWriter.WriteHeader(code)
	}
}
func (w *response) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func (w *response) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func route(path string) string {
	switch path {
	case "/health", "/ready", "/metrics", "/hazards", "/notices", "/internal/hazards", "/internal/notices", "/internal/login", "/internal/logout", "/internal/refresh", "/internal/introspect", "/auth/login", "/auth/logout", "/auth/refresh", "/records", "/history", "/seismic-events", "/tsunami-warnings", "/volcanic-reports", "/admin/outage", "/admin/schema-version":
		return path
	}
	for _, prefix := range []string{"/hazards/", "/notices/", "/internal/hazards/", "/internal/notices/"} {
		if strings.HasPrefix(path, prefix) {
			return prefix + ":id"
		}
	}
	return "unmatched"
}
func Handler(service string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			Metrics(w, r)
			return
		}
		started := time.Now()
		path := route(r.URL.Path)
		ctx, span := otel.Tracer("application").Start(Extract(r.Context(), r.Header), r.Method+" "+path, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		w.Header().Set("X-Trace-ID", TraceID(ctx))
		r = r.WithContext(ctx)
		r.Body = http.MaxBytesReader(w, r.Body, int64(Int("MAX_PAYLOAD_BYTES", 4<<20)))
		rw := &response{ResponseWriter: w}
		invalid := false
		if r.Body != nil && r.ContentLength != 0 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			b, truncated, err := ReadPayload(r.Body)
			_ = r.Body.Close()
			var overflow *http.MaxBytesError
			if truncated || errors.As(err, &overflow) {
				WriteError(rw, 413, "JSON payload too large")
				invalid = true
			} else if err != nil || ValidateJSON(b) != nil {
				WriteError(rw, 400, "invalid JSON payload")
				invalid = true
			} else {
				r.Body = io.NopCloser(bytes.NewReader(b))
			}
		}
		if !invalid {
			next.ServeHTTP(rw, r)
		}
		status := rw.status
		if status == 0 {
			status = 200
		}
		method := r.Method
		switch method {
		case "GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD":
		default:
			method = "OTHER"
		}
		elapsed := time.Since(started).Seconds()
		labels := fmt.Sprintf("service=%q,method=%q,route=%q,status=%q", service, method, path, strconv.Itoa(status))
		mu.Lock()
		s := requests[labels]
		if s == nil {
			s = &sample{}
			requests[labels] = s
		}
		s.Count++
		s.Sum += elapsed
		if status >= 500 {
			s.Errors++
		}
		for i, b := range bounds {
			if elapsed <= b {
				s.Buckets[i]++
			}
		}
		mu.Unlock()
		span.SetAttributes(attribute.Int("http.response.status_code", status), attribute.String("http.route", path))
		if status >= 500 {
			span.SetStatus(codes.Error, "request failed")
		}
		slog.Debug("request trace", "service", service, "trace_id", TraceID(ctx), "correlation_id", r.Header.Get("X-Correlation-ID"), "route", path, "latency_ms", time.Since(started).Milliseconds())
	})
}

// SQLTracer emits query durations without recording SQL text, arguments, or credentials.
type SQLTracer struct{}

func (SQLTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	ctx, _ = Start(ctx, "postgresql query")
	return ctx
}
func (SQLTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryEndData) {
	s := trace.SpanFromContext(ctx)
	if d.Err != nil {
		s.SetStatus(codes.Error, "query failed")
	}
	s.End()
}
func Serve(ctx context.Context, srv *http.Server) error {
	if srv.ReadHeaderTimeout == 0 {
		srv.ReadHeaderTimeout = 5 * time.Second
	}
	if srv.ReadTimeout == 0 {
		srv.ReadTimeout = 10 * time.Second
	}
	if srv.WriteTimeout == 0 {
		srv.WriteTimeout = 15 * time.Second
	}
	if srv.IdleTimeout == 0 {
		srv.IdleTimeout = time.Minute
	}
	done := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		select {
		case <-ctx.Done():
			c, cancel := context.WithTimeout(context.Background(), Duration("SHUTDOWN_TIMEOUT", 30*time.Second))
			defer cancel()
			if err := srv.Shutdown(c); err != nil {
				_ = srv.Close()
			}
		case <-done:
		}
	}()
	err := srv.ListenAndServe()
	close(done)
	<-drained
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
