package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/gin-gonic/gin"
)

type loggedCall struct {
	message string
	fields  map[string]any
}

type captureLogger struct{ calls []loggedCall }

func (l *captureLogger) Info(message string, args ...any)  { l.record(message, args...) }
func (l *captureLogger) Warn(message string, args ...any)  { l.record(message, args...) }
func (l *captureLogger) Error(message string, args ...any) { l.record(message, args...) }
func (l *captureLogger) record(message string, args ...any) {
	fields := map[string]any{}
	for i := 0; i < len(args); i += 2 {
		fields[args[i].(string)] = args[i+1]
	}
	l.calls = append(l.calls, loggedCall{message, fields})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type observedBody struct {
	io.Reader
	beforeRead func()
}

func (b observedBody) Read(p []byte) (int, error) { b.beforeRead(); return b.Reader.Read(p) }

func TestUpstreamTimingsIncludeDecodeAndContractFailures(t *testing.T) {
	for _, valid := range []bool{true, false} {
		t.Run(map[bool]string{true: "valid", false: "invalid"}[valid], func(t *testing.T) {
			log := &captureLogger{}
			payload := `{"data":[],"count":0,"as_of":"now","source_status":{}}`
			if !valid {
				payload = `{"records":[],"count":0,"as_of":"now","source_status":{}}`
			}
			transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("X-Correlation-ID") != "decode-timing" {
					t.Fatal("correlation ID not forwarded")
				}
				body := observedBody{Reader: strings.NewReader(payload), beforeRead: func() {
					for _, call := range log.calls {
						if call.message == "outbound request" {
							t.Fatal("completed outbound timing was logged before the body was decoded")
						}
					}
				}}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(body), Header: make(http.Header)}, nil
			})
			a := &api{client: &http.Client{Transport: transport}, log: log}
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request = httptest.NewRequest(http.MethodGet, "/hazards", nil)
			ctx.Set("correlation_id", "decode-timing")
			var list aggregatorList
			status, err := a.call(ctx, "aggregator", http.MethodGet, "http://upstream.invalid/internal/hazards", nil, false, &list)
			if status != 200 || (err == nil) != valid {
				t.Fatalf("unexpected result: status=%d err=%v", status, err)
			}
			last := log.calls[len(log.calls)-1]
			if last.message != "outbound request" || last.fields["success"] != valid {
				t.Fatalf("incorrect completed call log: %#v", last)
			}
			for _, field := range []string{"latency_ms", "headers_latency_ms", "decode_latency_ms"} {
				if value, ok := last.fields[field].(int64); !ok || value < 0 {
					t.Fatalf("missing/invalid timing %s: %#v", field, last.fields[field])
				}
			}
		})
	}
}

func TestIdempotentCallsRetryOnceWhenThePodRefusesTheConnection(t *testing.T) {
	refused := &url.Error{Op: "Get", URL: "http://upstream.invalid", Err: &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}}
	reset := os.NewSyscallError("read", syscall.ECONNRESET)
	for _, tc := range []struct {
		name   string
		method string
		errs   []error // returned by successive attempts before the first success
		calls  int
		served bool
	}{
		{"a refused connection is retried", http.MethodGet, []error{refused}, 2, true},
		{"a reset connection is retried", http.MethodGet, []error{reset}, 2, true},
		{"only one retry is made", http.MethodGet, []error{refused, refused}, 2, false},
		{"POST is never retried", http.MethodPost, []error{refused}, 1, false},
		{"timeouts are not retried", http.MethodGet, []error{context.DeadlineExceeded}, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := &captureLogger{}
			calls := 0
			transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if calls <= len(tc.errs) {
					return nil, tc.errs[calls-1]
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
			})
			a := &api{client: &http.Client{Transport: transport}, log: log}
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request = httptest.NewRequest(tc.method, "/hazards", nil)
			status, err := a.call(ctx, "aggregator", tc.method, "http://upstream.invalid/internal/hazards", nil, false, nil)
			if calls != tc.calls || (err == nil) != tc.served || (tc.served && status != 200) {
				t.Fatalf("attempts=%d (want %d) status=%d err=%v (served want %v)", calls, tc.calls, status, err, tc.served)
			}
		})
	}
}
