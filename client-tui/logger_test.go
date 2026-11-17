package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestInteractiveCallLineKeepsTraceFieldsReadable(t *testing.T) {
	var out bytes.Buffer
	log := slog.New(&callLine{out: &out})
	log.Info("outbound call", "service", "client-tui", "correlation_id", "NW66PI4U", "target_service", "client-api", "target_endpoint", "/situation/chat", "method", "POST", "status_code", 200, "latency_ms", int64(8121), "success", true)
	log.Info("outbound call", "correlation_id", "XK2", "target_endpoint", "/auth/login", "method", "POST", "status_code", 0, "latency_ms", int64(3), "success", false, "error", "request failed")
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 || strings.Contains(out.String(), "{") {
		t.Fatalf("expected two compact lines, got:\n%s", out.String())
	}
	for _, want := range []string{"POST /situation/chat", "200", "8121 ms", "NW66PI4U"} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("line is missing %q: %s", want, lines[0])
		}
	}
	if !strings.Contains(lines[1], "gagal terhubung") {
		t.Fatalf("failed call not explained: %s", lines[1])
	}
}

func TestNonInteractiveOutputStaysJSON(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "log")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	newLogger(file).Info("outbound call", "correlation_id", "abc", "latency_ms", 5)
	data, _ := os.ReadFile(file.Name())
	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(data), &entry); err != nil || entry["correlation_id"] != "abc" {
		t.Fatalf("redirected logs must stay structured JSON: %s", data)
	}
}
