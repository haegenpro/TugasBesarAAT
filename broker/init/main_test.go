package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestLoadConfigRejectsShortDLQRetention(t *testing.T) {
	t.Setenv("NATS_PORT", "4222")
	t.Setenv("NATS_MONITOR_PORT", "8222")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("NATS_STREAM", "HAZARDS")
	t.Setenv("NATS_SUBJECT", "hazard.events")
	t.Setenv("NATS_STREAM_MAX_AGE", "24h")
	t.Setenv("NATS_DLQ_STREAM", "HAZARDS_DLQ")
	t.Setenv("NATS_DLQ_SUBJECT_PREFIX", "hazard.dlq")
	t.Setenv("NATS_DLQ_MAX_AGE", "12h")
	t.Setenv("NATS_LIFECYCLE_STREAM", "REPORT_LIFECYCLE")
	t.Setenv("NATS_LIFECYCLE_SUBJECT", "report.lifecycle")
	t.Setenv("NATS_LIFECYCLE_MAX_AGE", "24h")
	if _, err := loadConfig(); err == nil {
		t.Fatal("DLQ retention shorter than source retention should be rejected")
	}
}

func TestEnsureStreamRejectsSecondDLQEntryOnSameSubject(t *testing.T) {
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("set TEST_NATS_URL for JetStream integration test")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	cfg := Config{NATSURL: url, StreamName: "MAIN_TEST_" + suffix, StreamSubject: "main." + suffix,
		StreamMaxAge: time.Hour, DLQStream: "DLQ_TEST_" + suffix, DLQPrefix: "dlqtest." + suffix, DLQMaxAge: 2 * time.Hour}
	ctx := context.Background()
	if err := ensureStream(ctx, js, cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = js.DeleteStream(context.Background(), cfg.StreamName)
		_ = js.DeleteStream(context.Background(), cfg.DLQStream)
	})
	if err := checkStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	subject := cfg.DLQPrefix + ".dashboard.HAZARDS.41"
	if _, err := js.Publish(ctx, subject, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, subject, []byte("second")); err == nil {
		t.Fatal("second DLQ publication on the same subject should be rejected")
	}
	stream, err := js.Stream(ctx, cfg.DLQStream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("DLQ retained %d messages, want 1", info.State.Msgs)
	}
}
