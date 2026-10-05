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

func TestLatestForConsumerPrefix(t *testing.T) {
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
	streamName := "INSPECT_TEST_" + suffix
	prefix := "dlqinspect." + suffix
	ctx := context.Background()
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: streamName, Subjects: []string{prefix + ".>"}, Storage: jetstream.FileStorage,
		Discard: jetstream.DiscardNew, DiscardNewPerSubject: true, MaxMsgsPerSubject: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), streamName) })
	consumerPrefix := prefix + ".dashboard"
	for _, item := range []struct{ subject, data string }{
		{consumerPrefix + ".dashboard-v1.HAZARDS.41", "first"},
		{consumerPrefix + ".dashboard-v1.HAZARDS.42", "second"},
		{prefix + ".notifier.notifier-v1.HAZARDS.43", "other consumer"},
	} {
		if _, err := js.Publish(ctx, item.subject, []byte(item.data)); err != nil {
			t.Fatal(err)
		}
	}
	latest, err := latestForPrefix(ctx, stream, consumerPrefix)
	if err != nil {
		t.Fatal(err)
	}
	if string(latest.Data) != "second" {
		t.Fatalf("latest dashboard record = %q", latest.Data)
	}
	if _, err := latestForPrefix(ctx, stream, prefix+".missing"); err == nil {
		t.Fatal("missing prefix should report no records")
	}
}
