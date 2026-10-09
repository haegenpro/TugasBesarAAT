package main

import (
	"bytes"
	"encoding/json"
	"github.com/google/uuid"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestHTTPRequestLogsCorrelationStatusAndLatency(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	a := &app{haltController: &haltController{}, cfg: config{Role: "notifier"}, db: db}
	for _, test := range []struct {
		path, cid string
		status    int
	}{
		{"/health", "provided-http-trace", 200}, {"/ready", "", 503}, {"/records?token=do-not-log", "storage-trace", 503}, {"/missing", "missing-trace", 404},
	} {
		t.Run(test.path, func(t *testing.T) {
			var logs bytes.Buffer
			old := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			defer slog.SetDefault(old)
			req := httptest.NewRequest("GET", test.path, nil)
			if test.cid != "" {
				req.Header.Set(correlationHeader, test.cid)
			}
			w := httptest.NewRecorder()
			a.router().ServeHTTP(w, req)
			if w.Code != test.status {
				t.Fatalf("status=%d want=%d", w.Code, test.status)
			}
			cid := w.Header().Get(correlationHeader)
			if test.cid != "" && cid != test.cid {
				t.Fatal("incoming correlation ID was lost")
			}
			if test.cid == "" {
				if _, err := uuid.Parse(cid); err != nil {
					t.Fatal("missing generated correlation ID")
				}
			}
			var entry map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &entry); err != nil {
				t.Fatalf("expected one JSON request log: %v", err)
			}
			if entry["service"] != "consumer-notifier" || entry["correlation_id"] != cid || entry["status_code"] != float64(test.status) || entry["method"] != "GET" || entry["msg"] != "http request" {
				t.Fatalf("incomplete request log: %v", entry)
			}
			if latency, ok := entry["latency_ms"].(float64); !ok || latency < 0 {
				t.Fatal("missing outbound/request duration")
			}
			if strings.Contains(logs.String(), "do-not-log") {
				t.Fatal("query value leaked into log")
			}
		})
	}
}
func TestHTTPTraceAvailableToHandler(t *testing.T) {
	a := &app{cfg: config{Role: "notifier"}}
	handler := a.httpRequestLog(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if correlationFromContext(r.Context()) != "handler-trace" {
			t.Fatal("handler cannot propagate correlation ID")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set(correlationHeader, "handler-trace")
	handler.ServeHTTP(httptest.NewRecorder(), req)
}
