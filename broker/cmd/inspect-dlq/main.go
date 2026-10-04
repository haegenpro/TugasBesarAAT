// Command inspect-dlq prints the latest dead-letter record for one consumer.
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
		fmt.Fprintln(os.Stderr, "usage: NATS_URL=... NATS_DLQ_STREAM=... go run ./cmd/inspect-dlq <DLQ subject>")
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
	msg, err := s.GetLastMsgForSubject(ctx, os.Args[1])
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
