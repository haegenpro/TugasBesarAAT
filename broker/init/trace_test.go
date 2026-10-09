package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestBrokerCallsLogSuccessAndFailureWithSharedTrace(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(old)
	ctx := withBrokerTrace(context.Background())
	if value, err := brokerCall(ctx, "lookup", func() (int, error) { return 7, nil }); err != nil || value != 7 {
		t.Fatal(value, err)
	}
	failure := errors.New("broker unavailable")
	if _, err := brokerCall(ctx, "publish", func() (int, error) { return 0, failure }); !errors.Is(err, failure) {
		t.Fatal("error was lost")
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 2 {
		t.Fatal("missing outgoing logs")
	}
	var cid string
	for i, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatal(err)
		}
		if entry["service"] != "broker" || entry["component"] != "broker-init" || entry["target_service"] != "nats" || entry["success"] != (i == 0) {
			t.Fatal("incomplete broker trace", entry)
		}
		current, ok := entry["correlation_id"].(string)
		if !ok || current == "" {
			t.Fatal("missing trace ID")
		}
		if i == 0 {
			cid = current
		} else if cid != current {
			t.Fatal("trace split across outgoing calls")
		}
		if _, ok := entry["latency_ms"].(float64); !ok {
			t.Fatal("missing call latency")
		}
	}
}
