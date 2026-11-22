package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"clientapi/internal/platform"

	"github.com/gin-gonic/gin"
)

func TestFailureClassNamesTheCause(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	reset := &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
	for want, err := range map[string]error{
		"refused":  fmt.Errorf("Post: %w", refused),
		"reset":    fmt.Errorf("Post: %w", reset),
		"timeout":  fmt.Errorf("Post: %w", context.DeadlineExceeded),
		"canceled": fmt.Errorf("Post: %w", context.Canceled),
		"eof":      fmt.Errorf("Post: %w", io.ErrUnexpectedEOF),
		"other":    errors.New("tls: bad certificate"),
	} {
		if got := failureClass(err); got != want {
			t.Errorf("failureClass(%v) = %s, want %s", err, got, want)
		}
	}
}

// metric reads one counter from the Client API's metrics endpoint.
func metric(t *testing.T, name string) float64 {
	t.Helper()
	w := httptest.NewRecorder()
	platform.Metrics(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(w.Body.String(), "\n") {
		if strings.HasPrefix(line, name+" ") {
			v, _ := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, name)), 64)
			return v
		}
	}
	return 0
}

// A failed or 5xx call to ingestion is counted by cause, so the cause of Field report 503s is
// visible in metrics without the rotated logs.
func TestIngestionFailuresAreCountedByCause(t *testing.T) {
	gone := httptest.NewServer(http.NotFoundHandler())
	refusedURL := gone.URL
	gone.Close() // nothing listens there any more: connection refused
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, `{"error":"busy"}`)
	}))
	t.Cleanup(failing.Close)
	for _, test := range []struct {
		url, metric string
	}{
		{refusedURL, "app_upstream_failures_total_ingestion_refused"},
		{failing.URL, "app_upstream_failures_total_ingestion_status_5xx"},
	} {
		before := metric(t, test.metric)
		a := &api{cfg: config{IngestionURL: test.url, FieldReportToken: "t"}, client: http.DefaultClient, log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/field-reports", strings.NewReader(`{}`))
		a.forwardIngestion(c, http.MethodPost, "/internal/field-reports", http.Header{}, []byte(`{}`))
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s: status %d, want 503", test.url, w.Code)
		}
		if got := metric(t, test.metric); got != before+1 {
			t.Fatalf("%s = %v after one failure, want %v", test.metric, got, before+1)
		}
	}
}
