// Package middleware provides the correlation-ID propagation and the
// structured slog JSON request logger used in place of Gin's default
// plain-text logger.
package middleware

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const CorrelationIDHeader = "X-Correlation-ID"
const correlationIDKey = "correlation_id"

func CorrelationID() gin.HandlerFunc {
	return func(c *gin.Context) {
		cid := c.GetHeader(CorrelationIDHeader)
		if cid == "" {
			cid = uuid.NewString()
		}
		c.Set(correlationIDKey, cid)
		c.Writer.Header().Set(CorrelationIDHeader, cid)
		c.Next()
	}
}

func GetCorrelationID(c *gin.Context) string {
	if v, ok := c.Get(correlationIDKey); ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// JSONLogger replaces Gin's default access logger with one structured JSON
// line per request via slog, carrying the correlation ID and latency.
func JSONLogger(logger *slog.Logger, service string) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		latency := time.Since(start).Milliseconds()
		level := slog.LevelInfo
		if c.Writer.Status() >= 500 {
			level = slog.LevelError
		} else if c.Writer.Status() >= 400 {
			level = slog.LevelWarn
		}
		logger.LogAttrs(c.Request.Context(), level, "http request",
			slog.String("service", service),
			slog.String("correlation_id", GetCorrelationID(c)),
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.Int("status_code", c.Writer.Status()),
			slog.Int64("latency_ms", latency),
		)
	}
}

const isoMilli = "2006-01-02T15:04:05.000Z07:00"

// NewLogger returns a slog.Logger writing single-line JSON to stdout with
// the field set required by the deployment conventions doc: timestamp,
// service, correlation_id, level, message, latency_ms.
func NewLogger() *slog.Logger {
	opts := &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.TimeKey:
				a.Key = "timestamp"
				a.Value = slog.StringValue(a.Value.Time().UTC().Format(isoMilli))
			case slog.MessageKey:
				a.Key = "message"
			case slog.LevelKey:
				lvl := a.Value.Any().(slog.Level)
				a.Value = slog.StringValue(strings.ToLower(lvl.String()))
			}
			return a
		},
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

// LogOutbound logs one structured line per outbound call the Aggregator
// makes (to BMKG, PVMBG, or NATS), success or failure, per the deployment
// conventions doc. Pass statusCode = 0 when not applicable (e.g. NATS).
func LogOutbound(logger *slog.Logger, service, correlationID, targetService, targetEndpoint string, statusCode int, success bool, latencyMs int64, errMsg string) {
	level := slog.LevelInfo
	if !success {
		level = slog.LevelWarn
	}
	attrs := []slog.Attr{
		slog.String("service", service),
		slog.String("correlation_id", correlationID),
		slog.String("target_service", targetService),
		slog.String("target_endpoint", targetEndpoint),
		slog.Bool("success", success),
		slog.Int64("latency_ms", latencyMs),
	}
	if statusCode > 0 {
		attrs = append(attrs, slog.Int("status_code", statusCode))
	}
	msg := "outbound call"
	if errMsg != "" {
		attrs = append(attrs, slog.String("error", errMsg))
		msg = "outbound call failed"
	}
	logger.LogAttrs(context.Background(), level, msg, attrs...)
}
