package clients

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestInvalidOriginalBytesAreCapturedWithoutJSONNormalization(t *testing.T) {
	for _, tc := range []struct {
		name      string
		raw       []byte
		limit     string
		truncated bool
	}{{"invalid UTF-8", []byte{0xff}, "1024", false}, {"truncated JSON", []byte(`[{"broken":`), "1024", false}, {"oversized", bytes.Repeat([]byte("x"), 64), "8", true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("MAX_PAYLOAD_BYTES", tc.limit)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(tc.raw) }))
			defer server.Close()
			c := NewPVMBGClient(server.URL, "key", slog.New(slog.NewTextHandler(io.Discard, nil)))
			var captured Failure
			c.CaptureFailure = func(ctx context.Context, f Failure) error { captured = f; return nil }
			_, err := c.FetchVolcanicReports(context.Background(), "failure-test", time.Time{})
			if !errors.Is(err, ErrContractViolation) {
				t.Fatalf("invalid data accepted: %v", err)
			}
			expected := tc.raw
			if tc.truncated {
				expected = expected[:8]
			}
			if !bytes.Equal(captured.Payload, expected) || captured.Truncated != tc.truncated || captured.Retryable {
				t.Fatalf("failure capture changed bytes or classification: %+v", captured)
			}
		})
	}
}
func TestSourceUnavailabilityIsClassifiedRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		w.Write([]byte("temporarily unavailable"))
	}))
	defer server.Close()
	c := NewPVMBGClient(server.URL, "key", slog.New(slog.NewTextHandler(io.Discard, nil)))
	var captured Failure
	c.CaptureFailure = func(ctx context.Context, f Failure) error { captured = f; return nil }
	_, err := c.FetchVolcanicReports(context.Background(), "retry-test", time.Time{})
	if err == nil || !captured.Retryable || string(captured.Payload) != "temporarily unavailable" {
		t.Fatalf("wrong transient failure classification: %+v %v", captured, err)
	}
}
