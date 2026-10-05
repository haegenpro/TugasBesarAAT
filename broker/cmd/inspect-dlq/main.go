// Command inspect-dlq prints the latest dead-letter record under one consumer prefix.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	url, stream := os.Getenv("NATS_URL"), os.Getenv("NATS_DLQ_STREAM")
	if url == "" || stream == "" || len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: NATS_URL=... NATS_DLQ_STREAM=... go run ./cmd/inspect-dlq <consumer DLQ prefix>")
		os.Exit(2)
	}
	nc, err := nats.Connect(url, nats.Timeout(3*time.Second))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	msg, err := latestForPrefix(ctx, s, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var record any
	if err := json.Unmarshal(msg.Data, &record); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	output := map[string]any{"subject": msg.Subject, "dlq_sequence": msg.Sequence, "record": record}
	encoded, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(encoded))
}

func latestForPrefix(ctx context.Context, s jetstream.Stream, prefix string) (*jetstream.RawStreamMsg, error) {
	info, err := s.Info(ctx, jetstream.WithSubjectFilter(prefix+".>"))
	if err != nil {
		return nil, err
	}
	var msg *jetstream.RawStreamMsg
	for subject := range info.State.Subjects {
		candidate, lookupErr := s.GetLastMsgForSubject(ctx, subject)
		if lookupErr != nil {
			return nil, lookupErr
		}
		if msg == nil || candidate.Sequence > msg.Sequence {
			msg = candidate
		}
	}
	if msg == nil {
		return nil, fmt.Errorf("no DLQ entries under prefix %q", prefix)
	}
	return msg, nil
}
