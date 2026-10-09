package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProtectedRequestRefreshesAndRetries(t *testing.T) {
	refreshes := 0
	var correlations []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		correlations = append(correlations, r.Header.Get("X-Correlation-ID"))
		switch r.URL.Path {
		case "/auth/refresh":
			refreshes++
			json.NewEncoder(w).Encode(session{AccessToken: "new-access", RefreshToken: "new-refresh", Role: "field", ClientID: "field-id"})
		case "/hazards":
			if r.Header.Get("Authorization") != "Bearer new-access" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"data": []any{}, "count": 0})
		}
	}))
	defer server.Close()
	var logs bytes.Buffer
	a := &app{log: slog.New(slog.NewJSONHandler(&logs, nil)), baseURL: server.URL, client: server.Client(), session: session{AccessToken: "old-access", RefreshToken: "old-refresh"}}
	status, _, err := a.protected(http.MethodGet, "/hazards")
	if err != nil || status != http.StatusOK || refreshes != 1 || a.previousAccess != "old-access" || a.session.RefreshToken != "new-refresh" {
		t.Fatalf("status=%d err=%v refreshes=%d session=%+v", status, err, refreshes, a.session)
	}
	if len(correlations) != 3 || correlations[0] == "" || correlations[0] != correlations[1] || correlations[0] != correlations[2] {
		t.Fatalf("refresh/retry split the request trace: %v", correlations)
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("missing per-call latency logs: %d", len(lines))
	}
	for i, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatal(err)
		}
		if entry["correlation_id"] != correlations[0] || entry["service"] != "client-tui" || entry["target_service"] != "client-api" {
			t.Fatal("incomplete client trace", entry)
		}
		expected := float64(200)
		if i == 0 {
			expected = 401
		}
		if entry["status_code"] != expected {
			t.Fatal("wrong logged status", entry)
		}
		if _, ok := entry["latency_ms"].(float64); !ok {
			t.Fatal("missing call latency")
		}
	}
	for _, secret := range []string{"old-access", "new-access", "old-refresh", "new-refresh"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatal("token leaked into client log")
		}
	}
	oldStatus, _, err := a.request(http.MethodGet, "/hazards", nil, a.previousAccess)
	if err != nil || oldStatus != http.StatusUnauthorized {
		t.Fatalf("old token status=%d err=%v", oldStatus, err)
	}
}

func TestRefreshOutageKeepsSessionForRetry(t *testing.T) {
	refreshes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/refresh" {
			refreshes++
			if refreshes == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			json.NewEncoder(w).Encode(session{AccessToken: "new-access", RefreshToken: "new-refresh", Role: "field"})
			return
		}
		if r.Header.Get("Authorization") != "Bearer new-access" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	a := &app{baseURL: server.URL, client: server.Client(), session: session{AccessToken: "old-access", RefreshToken: "old-refresh"}}
	if _, _, err := a.protected(http.MethodGet, "/hazards"); err == nil || a.session.RefreshToken != "old-refresh" {
		t.Fatal("temporary auth outage discarded the session")
	}
	status, _, err := a.protected(http.MethodGet, "/hazards")
	if err != nil || status != http.StatusOK || a.session.RefreshToken != "new-refresh" {
		t.Fatalf("retry status=%d err=%v", status, err)
	}
}

type failingTransport struct{}

func (failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("connection failed")
}
func TestFailedCallStillLogsWithoutCredentials(t *testing.T) {
	var logs bytes.Buffer
	a := &app{client: &http.Client{Transport: failingTransport{}}, baseURL: "http://unreachable", log: slog.New(slog.NewJSONHandler(&logs, nil))}
	_, _, err := a.request("POST", "/auth/login?secret=hidden-query", map[string]string{"client_secret": "hidden-body"}, "hidden-token")
	if err == nil {
		t.Fatal("expected transport failure")
	}
	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["success"] != false || entry["status_code"] != float64(0) || entry["correlation_id"] == "" || entry["target_endpoint"] != "/auth/login" {
		t.Fatal("missing failed-call trace", entry)
	}
	if _, ok := entry["latency_ms"].(float64); !ok {
		t.Fatal("missing failed-call latency")
	}
	if strings.Contains(logs.String(), "hidden-") {
		t.Fatal("credentials leaked into log")
	}
}
