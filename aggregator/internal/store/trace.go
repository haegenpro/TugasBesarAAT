package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"time"

	"aggregator/internal/middleware"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Trace at the driver boundary so transactions and volcano lookups are covered
// too. SQL text, bind values, DSNs, and server error details are never logged.
type sqlTracer struct{ logger *slog.Logger }
type sqlTraceKey struct{}
type sqlTrace struct {
	started     time.Time
	operation   string
	fingerprint string
}

var (
	_ pgx.QueryTracer       = (*sqlTracer)(nil)
	_ pgx.ConnectTracer     = (*sqlTracer)(nil)
	_ pgx.PrepareTracer     = (*sqlTracer)(nil)
	_ pgxpool.AcquireTracer = (*sqlTracer)(nil)
)

func startSQL(ctx context.Context, operation, query string) context.Context {
	fingerprint := ""
	if query != "" {
		digest := sha256.Sum256([]byte(query))
		fingerprint = hex.EncodeToString(digest[:])
	}
	ctx = middleware.EnsureCorrelationID(ctx)
	return context.WithValue(ctx, sqlTraceKey{}, sqlTrace{time.Now(), operation, fingerprint})
}

func (t *sqlTracer) end(ctx context.Context, err error) {
	trace, ok := ctx.Value(sqlTraceKey{}).(sqlTrace)
	if !ok {
		return
	}
	logSQL(t.logger, ctx, trace.operation, trace.fingerprint, trace.started, err)
}

func logSQL(logger *slog.Logger, ctx context.Context, operation, fingerprint string, started time.Time, err error) {
	level := slog.LevelInfo
	fields := []any{"service", "aggregator", "correlation_id", middleware.CorrelationFromContext(ctx),
		"target_service", "postgres", "target_endpoint", operation, "success", err == nil,
		"latency_ms", time.Since(started).Milliseconds()}
	if fingerprint != "" {
		fields = append(fields, "query_fingerprint", fingerprint)
	}
	if err != nil {
		level = slog.LevelWarn
		category := "database_error"
		var serverError *pgconn.PgError
		switch {
		case errors.Is(err, context.Canceled):
			category = "canceled"
		case errors.Is(err, context.DeadlineExceeded):
			category = "timeout"
		case errors.As(err, &serverError):
			fields = append(fields, "sqlstate", serverError.Code)
		}
		fields = append(fields, "error", category)
	}
	logger.Log(ctx, level, "outbound call", fields...)
}

func (t *sqlTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	operation := "query"
	if words := strings.Fields(data.SQL); len(words) > 0 {
		switch verb := strings.ToUpper(words[0]); verb {
		case "SELECT", "INSERT", "UPDATE", "DELETE", "BEGIN", "COMMIT", "ROLLBACK", "CREATE", "ALTER", "DROP", "WITH":
			operation = "query:" + strings.ToLower(verb)
		}
	}
	return startSQL(ctx, operation, data.SQL)
}

func (t *sqlTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	t.end(ctx, data.Err)
}

func (t *sqlTracer) TraceConnectStart(ctx context.Context, _ pgx.TraceConnectStartData) context.Context {
	return startSQL(ctx, "connect", "")
}

func (t *sqlTracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	t.end(ctx, data.Err)
}

func (t *sqlTracer) TracePrepareStart(ctx context.Context, _ *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	return startSQL(ctx, "prepare", data.SQL)
}

func (t *sqlTracer) TracePrepareEnd(ctx context.Context, _ *pgx.Conn, data pgx.TracePrepareEndData) {
	t.end(ctx, data.Err)
}

func (t *sqlTracer) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	return startSQL(ctx, "pool_acquire", "")
}

func (t *sqlTracer) TraceAcquireEnd(ctx context.Context, _ *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	t.end(ctx, data.Err)
}
