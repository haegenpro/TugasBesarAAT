package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type correlationKey struct{}

func correlation(ctx context.Context) string {
	if value, ok := ctx.Value(correlationKey{}).(string); ok && value != "" {
		return value
	}
	var id [16]byte
	_, _ = rand.Read(id[:])
	return hex.EncodeToString(id[:])
}

func setupTelemetry() (func(context.Context) error, error) {
	ratio := 0.05
	if raw := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); raw != "" {
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil || value != value || value < 0 || value > 1 {
			return nil, fmt.Errorf("invalid trace sampling ratio")
		}
		ratio = value
	}
	queue, err := envInt("OTEL_BSP_MAX_QUEUE_SIZE", 128, 1, 2147483647)
	if err != nil {
		return nil, err
	}
	batch, err := envInt("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", 32, 1, queue)
	if err != nil {
		return nil, err
	}
	timeoutMS, err := envInt("OTEL_BSP_EXPORT_TIMEOUT", 1000, 1, 2147483647)
	if err != nil {
		return nil, err
	}
	exportTimeout := time.Duration(timeoutMS) * time.Millisecond
	options := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", "situation-summarizer"))),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		exporter, err := otlptracehttp.New(context.Background(), otlptracehttp.WithTimeout(exportTimeout))
		if err != nil {
			return nil, err
		}
		options = append(options, sdktrace.WithBatcher(exporter, sdktrace.WithMaxQueueSize(queue),
			sdktrace.WithMaxExportBatchSize(batch), sdktrace.WithExportTimeout(exportTimeout)))
	}
	provider := sdktrace.NewTracerProvider(options...)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return provider.Shutdown, nil
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
		w.ResponseWriter.WriteHeader(code)
	}
}
func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func observeHTTP(next http.Handler, log *slog.Logger, metrics *Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		started := time.Now()
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		id := r.Header.Get("X-Correlation-ID")
		if len(id) == 0 || len(id) > 128 {
			id = correlation(ctx)
		}
		ctx = context.WithValue(ctx, correlationKey{}, id)
		route := "other"
		switch r.URL.Path {
		case "/summary", "/chat", "/health", "/ready":
			route = r.URL.Path[1:]
		}
		ctx, span := otel.Tracer("situation-summarizer").Start(ctx, r.Method+" /"+route, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		w.Header().Set("X-Correlation-ID", id)
		out := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(out, r.WithContext(ctx))
		if out.status == 0 {
			out.status = http.StatusOK
		}
		seconds := time.Since(started).Seconds()
		metrics.Add(fmt.Sprintf("summarizer_http_requests_total{route=%q,status=%q}", route, strconv.Itoa(out.status)), 1)
		metrics.Add("summarizer_http_request_seconds_sum", seconds)
		metrics.Add("summarizer_http_request_seconds_count", 1)
		for _, bound := range []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60} {
			if seconds <= bound {
				metrics.Add(fmt.Sprintf("summarizer_http_request_seconds_bucket{le=%q}", strconv.FormatFloat(bound, 'g', -1, 64)), 1)
			}
		}
		metrics.Add("summarizer_http_request_seconds_bucket{le=\"+Inf\"}", 1)
		span.SetAttributes(attribute.Int("http.response.status_code", out.status))
		if out.status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(out.status))
		}
		log.Info("http request", "correlation_id", id, "trace_id", span.SpanContext().TraceID().String(),
			"method", r.Method, "path", r.URL.Path, "status_code", out.status, "latency_ms", seconds*1000)
	})
}

type tracedTransport struct {
	base   http.RoundTripper
	target string
	log    *slog.Logger
}
type observedBody struct {
	io.ReadCloser
	done func()
	once sync.Once
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if e != nil {
		b.once.Do(b.done)
	}
	return n, e
}
func (b *observedBody) Close() error { err := b.ReadCloser.Close(); b.once.Do(b.done); return err }

func (t tracedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	started := time.Now()
	id := correlation(req.Context())
	ctx, span := otel.Tracer("situation-summarizer").Start(req.Context(), req.Method+" "+t.target, trace.WithSpanKind(trace.SpanKindClient))
	request := req.Clone(ctx)
	request.Header.Set("X-Correlation-ID", id)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(request.Header))
	response, err := t.base.RoundTrip(request)
	finish := func() {
		status := 0
		if response != nil {
			status = response.StatusCode
			span.SetAttributes(attribute.Int("http.response.status_code", status))
		}
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "outbound request failed")
		}
		span.End()
		t.log.Info("outbound request", "correlation_id", id, "target_service", t.target, "status_code", status,
			"latency_ms", time.Since(started).Seconds()*1000, "success", err == nil && status < 400)
	}
	if err != nil {
		finish()
		return response, err
	}
	response.Body = &observedBody{ReadCloser: response.Body, done: finish}
	return response, nil
}

func httpClient(target string, timeout time.Duration, log *slog.Logger) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxConnsPerHost = 4
	transport.MaxIdleConnsPerHost = 4
	transport.MaxIdleConns = 8
	return &http.Client{Timeout: timeout, Transport: tracedTransport{base: transport, target: target, log: log},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
