package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func situationServer(t *testing.T, summarizerURL string) *httptest.Server {
	t.Helper()
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(principal{ClientID: "media-demo", Role: "media", Scope: "hazards:summary"})
	}))
	t.Cleanup(auth.Close)
	a := &api{cfg: config{AuthURL: auth.URL, AggregatorURL: auth.URL, AuthInternalKey: "internal-secret", SituationURL: summarizerURL}, log: slog.New(slog.NewJSONHandler(io.Discard, nil)), client: http.DefaultClient, chatClient: http.DefaultClient}
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

func postChat(t *testing.T, server *httptest.Server, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/situation/chat", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer media-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}

func TestSituationChatOutlivesTheServerWriteTimeout(t *testing.T) {
	var question map[string]any
	summarizer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/chat" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		json.NewDecoder(r.Body).Decode(&question)
		time.Sleep(400 * time.Millisecond)
		io.WriteString(w, `{"answer":"Ada satu gempa AWAS di Aceh.","references":[{"hazard_id":"hz-1"}]}`)
	}))
	defer summarizer.Close()
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(principal{ClientID: "media-demo", Role: "media", Scope: "hazards:summary"})
	}))
	defer auth.Close()
	a := &api{cfg: config{AuthURL: auth.URL, AggregatorURL: auth.URL, AuthInternalKey: "internal-secret", SituationURL: summarizer.URL}, log: slog.New(slog.NewJSONHandler(io.Discard, nil)), client: http.DefaultClient, chatClient: http.DefaultClient}
	server := httptest.NewUnstartedServer(a.router())
	server.Config.WriteTimeout = 100 * time.Millisecond
	server.Start()
	defer server.Close()
	code, out := postChat(t, server, `{"question":"Ada gempa besar?","history":[]}`)
	if code != http.StatusOK || out["answer"] != "Ada satu gempa AWAS di Aceh." || question["question"] != "Ada gempa besar?" {
		t.Fatalf("chat not relayed: %d %v (summarizer saw %v)", code, out, question)
	}
}

func TestSituationChatRelaysRefusalsAndSurvivesOutages(t *testing.T) {
	summarizer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"status":"unavailable","reason":"kuota chat AI hari ini sudah habis"}`)
	}))
	defer summarizer.Close()
	if code, out := postChat(t, situationServer(t, summarizer.URL), `{"question":"halo"}`); code != http.StatusTooManyRequests || out["reason"] != "kuota chat AI hari ini sudah habis" {
		t.Fatalf("refusal not relayed: %d %v", code, out)
	}
	down := httptest.NewServer(http.NotFoundHandler())
	url := down.URL
	down.Close()
	if code, out := postChat(t, situationServer(t, url), `{"question":"halo"}`); code != http.StatusServiceUnavailable || out["status"] != "unavailable" {
		t.Fatalf("stopped summarizer: %d %v", code, out)
	}
}
