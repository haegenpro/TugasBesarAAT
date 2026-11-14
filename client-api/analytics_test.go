package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func previewAnalytics(partial bool) map[string]any {
	counts := map[string]int64{"SEISMIC": 2, "VOLCANIC": 1, "WILDFIRE": 1, "COASTAL": 1}
	n := 5
	severity := map[string]int64{"NORMAL": 1, "WASPADA": 1, "SIAGA": 2, "AWAS": 1}
	areas := []analyticsArea{{Name: "A", Count: 2}, {Name: "B", Count: 2}, {Name: "Å", Count: 1}}
	if partial {
		n = 3
		counts = map[string]int64{"SEISMIC": 2, "VOLCANIC": 1, "WILDFIRE": 0, "COASTAL": 0}
		severity = map[string]int64{"NORMAL": 1, "WASPADA": 1, "SIAGA": 1, "AWAS": 0}
		areas = []analyticsArea{{Name: "A", Count: 2}, {Name: "Å", Count: 1}}
	}
	return map[string]any{"filters": map[string]string{"from": "2026-01-01T00:00:00Z", "to": "2026-01-02T00:00:00Z", "interval": "day"}, "total": n, "by_type": counts, "by_severity": severity, "by_area": areas, "series": []analyticsBucket{{At: "2026-01-01T00:00:00Z", Counts: counts, Tsunami: 1}}, "tsunami_warning_overlay": 1, "as_of": "2026-01-02T00:00:00Z", "mode": "live", "generation": 12, "data_complete": !partial, "partial": partial, "unavailable_units": func() []string {
		if partial {
			return []string{"worker2"}
		}
		return []string{}
	}(), "source_freshness": map[string]string{"BMKG": "fresh", "SATELIT": "policy_unavailable"}}
}
func analyticsTestServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	mode := &atomic.Int32{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer server-only-analytics" {
			http.Error(w, "forbidden", 403)
			return
		}
		if mode.Load() == 2 {
			w.WriteHeader(503)
			io.WriteString(w, `{"error":{"code":"temporarily_unavailable","message":"fixture storage paused"}}`)
			return
		}
		result := previewAnalytics(mode.Load() == 1)
		if r.URL.Query().Get("interval") == "hour" {
			result["filters"].(map[string]string)["interval"] = "hour"
			series := []analyticsBucket{}
			for hour := 0; hour < 24; hour++ {
				b := analyticsBucket{At: time.Date(2026, 1, 1, hour, 0, 0, 0, time.UTC).Format(time.RFC3339), Counts: map[string]int64{"SEISMIC": 0, "VOLCANIC": 0, "WILDFIRE": 0, "COASTAL": 0}}
				switch hour {
				case 0:
					b.Counts["SEISMIC"] = 1
				case 1:
					b.Counts["SEISMIC"] = 1
					b.Tsunami = 1
				case 2:
					b.Counts["VOLCANIC"] = 1
				case 3:
					if mode.Load() != 1 {
						b.Counts["WILDFIRE"] = 1
					}
				case 4:
					if mode.Load() != 1 {
						b.Counts["COASTAL"] = 1
					}
				}
				series = append(series, b)
			}
			result["series"] = series
		}
		_ = json.NewEncoder(w).Encode(result)
	}))
	t.Cleanup(upstream.Close)
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Internal-Key") != "auth-only" {
			w.WriteHeader(401)
			return
		}
		if r.URL.Path == "/internal/introspect" {
			var v map[string]string
			_ = json.NewDecoder(r.Body).Decode(&v)
			if v["access_token"] != "fixture-access" {
				w.WriteHeader(401)
				return
			}
			io.WriteString(w, `{"client_id":"fixture-client","role":"media","scope":"hazards:summary"}`)
			return
		}
		if r.URL.Path == "/internal/logout" {
			w.WriteHeader(204)
			return
		}
		var credentials map[string]string
		_ = json.NewDecoder(r.Body).Decode(&credentials)
		if r.URL.Path == "/internal/login" && (credentials["client_id"] != "fixture-client" || credentials["client_secret"] != "fixture-secret") {
			w.WriteHeader(401)
			return
		}
		io.WriteString(w, `{"access_token":"fixture-access","refresh_token":"fixture-refresh","token_type":"Bearer","expires_in":900}`)
	}))
	t.Cleanup(auth.Close)
	app := &api{cfg: config{AnalyticsEnabled: true, AnalyticsToken: "server-only-analytics", AuthInternalKey: "auth-only", AuthURL: auth.URL, AggregatorURL: upstream.URL}, log: slog.New(slog.NewTextHandler(io.Discard, nil)), client: http.DefaultClient}
	server := httptest.NewServer(app.router())
	t.Cleanup(server.Close)
	return server, mode
}
func TestAnalyticsAuthenticatedAggregateBoundary(t *testing.T) {
	server, mode := analyticsTestServer(t)
	for _, token := range []string{"", "bad", "fixture-access"} {
		req, _ := http.NewRequest("GET", server.URL+"/analytics/summary", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		response, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		want := 401
		if token == "fixture-access" {
			want = 200
		}
		if response.StatusCode != want {
			t.Fatalf("status %d: %s", response.StatusCode, body)
		}
		if strings.Contains(string(body), "server-only-analytics") || strings.Contains(string(body), "attributes") || strings.Contains(string(body), "hazard_id") {
			t.Fatal("aggregate-only boundary leaked data")
		}
	}
	mode.Store(2)
	req, _ := http.NewRequest("GET", server.URL+"/analytics/summary", nil)
	req.Header.Set("Authorization", "Bearer fixture-access")
	response, e := http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	response.Body.Close()
	if response.StatusCode != 503 {
		t.Fatal("upstream unavailable not preserved")
	}
	response, e = http.Get(server.URL + "/analytics")
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || !strings.Contains(response.Header.Get("Content-Security-Policy"), "script-src 'self'") {
		t.Fatal("embedded page/CSP missing")
	}
}
func TestAnalyticsBrowserHarness(t *testing.T) {
	dir := os.Getenv("ANALYTICS_BROWSER_EVIDENCE")
	if dir == "" {
		t.Skip("explicit browser harness only")
	}
	server, mode := analyticsTestServer(t)
	if e := os.WriteFile(filepath.Join(dir, "browser-url.txt"), []byte(server.URL+"/analytics"), 0600); e != nil {
		t.Fatal(e)
	}
	deadline := time.NewTimer(5 * time.Minute)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatal("browser validation timed out")
		case <-ticker.C:
			if b, e := os.ReadFile(filepath.Join(dir, "mode.txt")); e == nil {
				switch strings.TrimSpace(string(b)) {
				case "partial":
					mode.Store(1)
				case "unavailable":
					mode.Store(2)
				case "complete":
					mode.Store(0)
				case "done":
					return
				}
				_ = os.WriteFile(filepath.Join(dir, "mode-ack.txt"), b, 0600)
			}
		}
	}
}
