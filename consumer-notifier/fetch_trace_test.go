package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type fetchLogWriter struct{ lines chan []byte }

func (w fetchLogWriter) Write(p []byte) (int, error) {
	w.lines <- append([]byte(nil), p...)
	return len(p), nil
}

type delayedFetchBatch struct {
	messages chan jetstream.Msg
	entered  chan struct{}
	errRead  chan struct{}
	err      error
}

func (b *delayedFetchBatch) Messages() <-chan jetstream.Msg {
	close(b.entered)
	return b.messages
}

func (b *delayedFetchBatch) Error() error {
	close(b.errRead)
	return b.err
}

func TestFetchTraceWaitsForBatchCompletionAndFinalError(t *testing.T) {
	for _, tc := range []struct {
		name    string
		message jetstream.Msg
		err     error
	}{
		{"message", &testMsg{}, nil},
		{"empty", nil, nil},
		{"failure", nil, errors.New("batch interrupted")},
		{"message then failure", &testMsg{}, errors.New("batch interrupted")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lines := make(chan []byte, 2)
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(fetchLogWriter{lines}, nil)))
			defer slog.SetDefault(previous)
			batch := &delayedFetchBatch{messages: make(chan jetstream.Msg), entered: make(chan struct{}), errRead: make(chan struct{})}
			closed := false
			defer func() {
				if !closed {
					close(batch.messages)
				}
			}()
			ctx := withDecodedCorrelation(withServiceTrace(context.Background(), "notifier"), "fetch-cid")
			type result struct {
				msg jetstream.Msg
				err error
			}
			completed := make(chan result, 1)
			go func() {
				msg, err := receiveFetch(ctx, batch, time.Now().Add(-100*time.Millisecond))
				completed <- result{msg, err}
			}()
			<-batch.entered
			if tc.message != nil {
				batch.messages <- tc.message
			}
			select {
			case <-lines:
				t.Fatal("fetch was logged before the asynchronous batch completed")
			case <-batch.errRead:
				t.Fatal("final batch error was read before completion")
			default:
			}
			// The broker's final error becomes available only when delivery ends.
			batch.err = tc.err
			close(batch.messages)
			closed = true
			var got result
			select {
			case got = <-completed:
			case <-time.After(time.Second):
				t.Fatal("completed batch did not return")
			}
			if got.msg != tc.message || !errors.Is(got.err, tc.err) {
				t.Fatalf("message or final error lost: %+v", got)
			}
			var entry map[string]any
			if err := json.Unmarshal(<-lines, &entry); err != nil {
				t.Fatal(err)
			}
			if entry["service"] != "consumer-notifier" || entry["correlation_id"] != "fetch-cid" || entry["operation"] != "consumer_fetch" || entry["success"] != (tc.err == nil) || entry["latency_ms"].(float64) < 100 {
				t.Fatalf("incorrect fetch trace: %v", entry)
			}
			if tc.err != nil && entry["error"] != tc.err.Error() {
				t.Fatal("final batch error missing from trace")
			}
			if len(lines) != 0 {
				t.Fatal("fetch was logged more than once")
			}
		})
	}
}

func TestFetchTraceMeasuresRealBrokerWait(t *testing.T) {
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("set TEST_NATS_URL for real fetch timing integration")
	}
	nc, err := nats.Connect(url, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	name := fmt.Sprintf("TRACE_FETCH_%d", time.Now().UnixNano())
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{name}})
	if err != nil {
		t.Fatal(err)
	}
	defer js.DeleteStream(ctx, name)
	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		t.Fatal(err)
	}
	for _, publish := range []bool{false, true} {
		t.Run(fmt.Sprintf("delayed_message=%t", publish), func(t *testing.T) {
			var output bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
			defer slog.SetDefault(previous)
			published := make(chan error, 1)
			if publish {
				go func() {
					time.Sleep(150 * time.Millisecond)
					_, err := js.Publish(ctx, name, []byte("delayed fixture"))
					published <- err
				}()
			}
			started := time.Now()
			wait := 150 * time.Millisecond
			if publish {
				wait = 2 * time.Second
			}
			batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(wait))
			if err != nil {
				t.Fatal(err)
			}
			msg, err := receiveFetch(withServiceTrace(ctx, "notifier"), batch, started)
			if err != nil {
				t.Fatal(err)
			}
			if publish {
				if err := <-published; err != nil {
					t.Fatal(err)
				}
				if msg == nil || string(msg.Data()) != "delayed fixture" {
					t.Fatal("delayed message not received")
				}
				if err := msg.DoubleAck(ctx); err != nil {
					t.Fatal(err)
				}
			} else if msg != nil {
				t.Fatal("empty stream returned a message")
			}
			var entry map[string]any
			if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
				t.Fatal(err)
			}
			if entry["success"] != true || entry["latency_ms"].(float64) < 100 {
				t.Fatalf("trace omitted the broker wait: %v", entry)
			}
			t.Logf("measured fetch latency: %v ms", entry["latency_ms"])
		})
	}
}
