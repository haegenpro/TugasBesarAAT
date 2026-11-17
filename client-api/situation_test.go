package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func situationServer(t *testing.T, summarizerURL string) *httptest.Server {
	t.Helper()
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(principal{ClientID: "media-demo", Role: "media", Scope: "hazards:summary"})
	}))
	t.Cleanup(auth.Close)
	a := &api{cfg: config{AuthURL: auth.URL, AggregatorURL: auth.URL, AuthInternalKey: "internal-secret", SituationURL: summarizerURL}, log: slog.New(slog.NewJSONHandler(io.Discard, nil)), client: http.DefaultClient}
	server := httptest.NewServer(a.router())
	t.Cleanup(server.Close)
	return server
}

func readSituation(t *testing.T, server *httptest.Server) (map[string]any, time.Duration) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/situation", nil)
	req.Header.Set("Authorization", "Bearer media-token")
	started := time.Now()
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("situation returned %d", resp.StatusCode)
	}
	var body map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body, time.Since(started)
}

func TestSituationPassesSummaryThrough(t *testing.T) {
	summarizer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"status":"fresh","ai_summary":{"headline":"Gempa AWAS di Aceh"},"facts":{"counts":{"total":2}}}`)
	}))
	defer summarizer.Close()
	body, _ := readSituation(t, situationServer(t, summarizer.URL))
	if body["status"] != "fresh" || body["ai_summary"].(map[string]any)["headline"] != "Gempa AWAS di Aceh" {
		t.Fatalf("summary changed in transit: %v", body)
	}
}

func TestSituationAnswersQuicklyWhenSummarizerIsSlowOrDown(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer slow.Close()
	body, elapsed := readSituation(t, situationServer(t, slow.URL))
	if body["status"] != "unavailable" || elapsed > 2*time.Second {
		t.Fatalf("slow summarizer: status=%v elapsed=%s", body["status"], elapsed)
	}
	down := httptest.NewServer(http.NotFoundHandler())
	url := down.URL
	down.Close()
	body, elapsed = readSituation(t, situationServer(t, url))
	if body["status"] != "unavailable" || elapsed > 2*time.Second {
		t.Fatalf("stopped summarizer: status=%v elapsed=%s", body["status"], elapsed)
	}
}
