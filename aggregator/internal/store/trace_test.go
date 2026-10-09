package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"aggregator/internal/middleware"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func decodeSQLLogs(t *testing.T, output string) []map[string]any {
	t.Helper()
	var result []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		var item map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &item))
		require.Equal(t, "postgres", item["target_service"])
		require.NotEmpty(t, item["correlation_id"])
		require.GreaterOrEqual(t, item["latency_ms"].(float64), float64(0))
		result = append(result, item)
	}
	return result
}

func TestSQLTraceKeepsCorrelationAndRedactsSensitiveData(t *testing.T) {
	var output bytes.Buffer
	tracer := &sqlTracer{logger: slog.New(slog.NewJSONHandler(&output, nil))}
	ctx := middleware.WithCorrelationID(context.Background(), "request-cid")
	query := "SELECT 'private-literal', $1"
	start := tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: query, Args: []any{"private-argument"}})
	tracer.TraceQueryEnd(start, nil, pgx.TraceQueryEndData{})
	start = tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: query})
	tracer.TraceQueryEnd(start, nil, pgx.TraceQueryEndData{Err: &pgconn.PgError{Code: "22P02", Message: "private-server-message", Detail: "private-detail"}})
	start = tracer.TracePrepareStart(ctx, nil, pgx.TracePrepareStartData{SQL: query})
	tracer.TracePrepareEnd(start, nil, pgx.TracePrepareEndData{})
	start = tracer.TraceConnectStart(ctx, pgx.TraceConnectStartData{})
	tracer.TraceConnectEnd(start, pgx.TraceConnectEndData{Err: errors.New("postgres://private-password@host")})
	start = tracer.TraceAcquireStart(ctx, nil, pgxpool.TraceAcquireStartData{})
	tracer.TraceAcquireEnd(start, nil, pgxpool.TraceAcquireEndData{Err: context.DeadlineExceeded})
	logs := decodeSQLLogs(t, output.String())
	require.Len(t, logs, 5)
	for _, entry := range logs {
		require.Equal(t, "request-cid", entry["correlation_id"])
	}
	require.Equal(t, true, logs[0]["success"])
	require.Equal(t, false, logs[1]["success"])
	require.Equal(t, "22P02", logs[1]["sqlstate"])
	require.Equal(t, logs[0]["query_fingerprint"], logs[2]["query_fingerprint"])
	require.Equal(t, "timeout", logs[4]["error"])
	require.NotContains(t, output.String(), "private-")
}

func TestDriverTraceCoversHTTPQueriesTransactionsAndFailures(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL for driver trace integration")
	}
	var output bytes.Buffer
	st, err := New(context.Background(), dsn, slog.New(slog.NewJSONHandler(&output, nil)))
	require.NoError(t, err)
	t.Cleanup(st.Close)
	ctx, cancel := context.WithTimeout(middleware.WithCorrelationID(context.Background(), "maintenance-cid"), 5*time.Second)
	defer cancel()
	require.NoError(t, st.Ping(ctx))
	tx, err := st.Pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "SELECT 1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	_, err = st.Pool.Exec(ctx, "SELECT $1::int", "private-invalid-number")
	require.Error(t, err)
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(middleware.CorrelationID())
	router.GET("/query", func(c *gin.Context) {
		var value int
		queryErr := st.Pool.QueryRow(c.Request.Context(), "SELECT 2").Scan(&value)
		if queryErr != nil {
			c.Status(http.StatusServiceUnavailable)
			return
		}
		c.JSON(http.StatusOK, value)
	})
	request := httptest.NewRequest(http.MethodGet, "/query", nil)
	request.Header.Set(middleware.CorrelationIDHeader, "http-cid")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "http-cid", response.Header().Get(middleware.CorrelationIDHeader))
	st.Close() // Stop background connections before inspecting the shared writer.
	logs := decodeSQLLogs(t, output.String())
	operations := map[string]bool{}
	httpQuery := false
	failedQuery := false
	for _, entry := range logs {
		operations[entry["target_endpoint"].(string)] = true
		httpQuery = httpQuery || (entry["target_endpoint"] == "query:select" && entry["correlation_id"] == "http-cid")
		failedQuery = failedQuery || (entry["target_endpoint"] == "query:select" && entry["success"] == false && entry["sqlstate"] == "22P02")
	}
	for _, operation := range []string{"connect", "pool_acquire", "ping", "query:begin", "query:commit", "query:select", "prepare"} {
		require.True(t, operations[operation], "missing driver operation %s", operation)
	}
	require.True(t, httpQuery, "HTTP correlation must reach the driver")
	require.True(t, failedQuery, "failed queries must be traced")
	require.NotContains(t, output.String(), "private-invalid-number")
}
