// Command publish-invalid publishes a deliberately unknown schema ID to the
// hazard stream. It is a demo fixture for consumer retry and DLQ behavior.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	url := os.Getenv("NATS_URL")
	subject := os.Getenv("NATS_SUBJECT")
	if url == "" || subject == "" {
		fmt.Fprintln(os.Stderr, "NATS_URL and NATS_SUBJECT are required")
		os.Exit(2)
	}
	id := uint64(999999999)
	if len(os.Args) > 1 {
		parsed, err := strconv.ParseUint(os.Args[1], 10, 32)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		id = parsed
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
	frame := make([]byte, 6)
	binary.BigEndian.PutUint32(frame[1:5], uint32(id))
	frame[5] = 0
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	correlationID := fmt.Sprintf("demo-invalid-%d", time.Now().UnixNano())
	msg := nats.NewMsg(subject)
	msg.Header.Set("X-Correlation-ID", correlationID)
	msg.Data = frame
	ack, err := js.PublishMsg(ctx, msg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("published unknown schema ID %d at stream sequence %d, correlation ID %s\n", id, ack.Sequence, correlationID)
}
