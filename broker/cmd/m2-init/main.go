// m2-init provisions bounded replicated streams without expiring pending data.
package main

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"os"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "M2 stream initialization failed:", e)
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nc, e := nats.Connect(os.Getenv("NATS_URL"), nats.Timeout(3*time.Second))
	if e != nil {
		return e
	}
	defer nc.Close()
	js, e := jetstream.New(nc)
	if e != nil {
		return e
	}
	streams := []struct {
		name, subject string
		mib           int64
	}{{"HAZARDS", "hazard.events", 128}, {"LIFECYCLE", "hazard.lifecycle", 128}, {"HAZARDS_DLQ", "hazard.dlq.>", 64}, {"RAW_SATELLITE", "raw.satellite", 192}, {"RAW_COASTAL", "raw.coastal.normal", 96}, {"RAW_CRITICAL", "raw.coastal.critical", 64}, {"RAW_FIELD", "raw.field", 64}, {"COASTAL_WARNINGS", "coastal.warning", 64}}
	for _, st := range streams {
		config := jetstream.StreamConfig{Name: st.name, Subjects: []string{st.subject}, Replicas: 3, Storage: jetstream.FileStorage, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardNew, MaxBytes: st.mib << 20, MaxAge: 0, Duplicates: 2 * time.Minute}
		if st.name == "HAZARDS_DLQ" {
			config.DiscardNewPerSubject = true
			config.MaxMsgsPerSubject = 1
		}
		if _, e = js.CreateOrUpdateStream(ctx, config); e != nil {
			return e
		}
		fmt.Println(st.name, "ready: R3, no expiry, bounded, reject new on capacity")
	}
	return nil
}
