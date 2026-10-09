package main

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
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

// Fetch(1) returns before the broker response arrives. Drain its one-message
// batch and read the final error before logging, then let the caller process
// the message so local persistence and ACK time do not inflate fetch latency.
func receiveFetch(ctx context.Context, batch jetstream.MessageBatch, started time.Time) (jetstream.Msg, error) {
	var message jetstream.Msg
	for msg := range batch.Messages() {
		message = msg
	}
	err := batch.Error()
	logOutbound(ctx, "consumer_fetch", "nats", 0, started, err)
	return message, err
}

// Track the status actually written by handlers, including http.Error responses.
type loggedResponse struct {
	http.ResponseWriter
	status int
}

func (w *loggedResponse) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *loggedResponse) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}
func (w *loggedResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (a *app) httpRequestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cid := strings.TrimSpace(r.Header.Get(correlationHeader))
		if cid == "" {
			cid = uuid.NewString()
		}
		w.Header().Set(correlationHeader, cid)
		ctx := context.WithValue(r.Context(), traceKey{}, messageTrace{Service: "consumer-" + a.cfg.Role, CorrelationID: cid})
		response := &loggedResponse{ResponseWriter: w}
		started := time.Now()
		completed := false
		defer func() {
			status := response.status
			if status == 0 {
				status = http.StatusOK
			}
			if !completed {
				status = http.StatusInternalServerError
			}
			level := slog.LevelInfo
			if status >= 500 {
				level = slog.LevelWarn
			}
			logEvent(ctx, level, "http request", "method", r.Method, "path", r.URL.Path, "status_code", status, "latency_ms", time.Since(started).Milliseconds())
		}()
		next.ServeHTTP(response, r.WithContext(ctx))
		completed = true
	})
}
