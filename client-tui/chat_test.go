package main

import (
	"bufio"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestChatReport(t *testing.T) {
	report, answer := chatReport(http.StatusOK, []byte(`{"answer":"Ada satu gempa AWAS di Aceh.","references":[{"hazard_id":"hz-1","hazard_type":"SEISMIC","severity":"AWAS","area_name":"Aceh","occurred_at":"2026-11-17T07:59:07Z"}],"model":"nemotron","data_as_of":"2026-11-17T08:00:00Z","disclaimer":"Bukan peringatan resmi."}`))
	for _, want := range []string{"AI: Ada satu gempa AWAS di Aceh.", "Rujukan:", "AWAS", "Aceh | 2026-11-17T07:59:07Z | hz-1", "Model : nemotron", "Data  : 17 Nov 2026 08:00:00 UTC (15:00:00 WIB)", "Bukan peringatan resmi."} {
		if !strings.Contains(report, want) {
			t.Fatalf("report is missing %q:\n%s", want, report)
		}
	}
	if answer != "Ada satu gempa AWAS di Aceh." {
		t.Fatalf("answer not kept for history: %q", answer)
	}
	for status, body := range map[int]string{
		http.StatusTooManyRequests: `{"status":"unavailable","reason":"kuota chat AI hari ini sudah habis"}`,
		http.StatusNotFound:        `{"error":"not found"}`,
		http.StatusBadGateway:      `<html>bad gateway</html>`,
		http.StatusOK:              `{"answer":""}`,
	} {
		if report, answer := chatReport(status, []byte(body)); answer != "" || report == "" {
			t.Errorf("HTTP %d should not produce an answer: %q", status, report)
		}
	}
}

func TestReadableTime(t *testing.T) {
	cases := map[string]string{
		"2026-11-17T13:22:10.51347962Z": "17 Nov 2026 13:22:10 UTC (20:22:10 WIB)",
		"2026-11-17T18:30:00Z":          "17 Nov 2026 18:30:00 UTC (18 Nov 01:30:00 WIB)",
		"bukan waktu":                   "bukan waktu",
	}
	for raw, want := range cases {
		if got := readableTime(raw); got != want {
			t.Errorf("readableTime(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestChatSessionSendsBoundedHistory(t *testing.T) {
	var seen []chatRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req chatRequest
		json.NewDecoder(r.Body).Decode(&req)
		seen = append(seen, req)
		if r.Header.Get("Authorization") != "Bearer access" || r.URL.Path != "/situation/chat" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"answer": "jawaban " + req.Question, "references": []any{}})
	}))
	defer server.Close()
	questions := []string{"satu", "", "dua", "tiga", "empat", "lima", "enam", "keluar"}
	a := &app{input: bufio.NewReader(strings.NewReader(strings.Join(questions, "\n") + "\n")), client: server.Client(), chatClient: server.Client(), baseURL: server.URL, log: slog.New(slog.NewJSONHandler(io.Discard, nil)), session: session{AccessToken: "access"}}
	stdout := os.Stdout
	os.Stdout, _ = os.Open(os.DevNull)
	a.chat()
	os.Stdout = stdout
	if len(seen) != 6 {
		t.Fatalf("expected 6 questions, got %d", len(seen))
	}
	last := seen[5]
	if last.Question != "enam" || len(last.History) != chatMemory || last.History[0].Content != "dua" || last.History[chatMemory-1].Content != "jawaban lima" {
		t.Fatalf("history not bounded to the latest turns: %+v", last.History)
	}
}
