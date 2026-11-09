package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"sync"
	"testing"
)

func TestConcurrentWorkersDeduplicateAndKeepHighestRevision(t *testing.T) {
	dsn := testDSN(t)
	db1, err := openDB(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	db2, err := openDB(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	workers := []*app{{db: db1, cfg: config{Role: "notifier"}}, {db: db2, cfg: config{Role: "notifier"}}}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for n := range 32 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			rev := int64(n%8 + 2)
			h := hazardEvent{HazardID: "concurrent-hazard", Severity: "SIAGA"}
			if rev == 9 {
				h.Severity = "AWAS"
			}
			e := lifecycleEvent{EventID: fmt.Sprint(rev), EventType: "hazard_corrected", RecordID: h.HazardID, Revision: rev, HazardRevision: rev, Hazard: &h}
			meta := &jetstream.MsgMetadata{Stream: "REPORT_LIFECYCLE", Sequence: jetstream.SequencePair{Stream: uint64(rev)}}
			errs <- workers[n%2].saveLifecycle(context.Background(), meta, []byte(fmt.Sprint(rev)), e)
		}(n)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var revision, count int64
	var payload string
	if err = db1.QueryRow(`SELECT revision FROM record_versions WHERE record_id='hazard:concurrent-hazard'`).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	if revision != 9 {
		t.Fatalf("revision regressed to %d", revision)
	}
	if err = db1.QueryRow(`SELECT payload FROM records WHERE hazard_id='concurrent-hazard'`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var h map[string]any
	json.Unmarshal([]byte(payload), &h)
	if h["severity"] != "AWAS" {
		t.Fatalf("projection regressed: %s", payload)
	}
	if err = db1.QueryRow(`SELECT count(*) FROM processed_messages`).Scan(&count); err != nil || count != 8 {
		t.Fatalf("inbox not idempotent: %d %v", count, err)
	}
	if err = db1.QueryRow(`SELECT count(*) FROM lifecycle_events`).Scan(&count); err != nil || count != 8 {
		t.Fatalf("events not idempotent: %d %v", count, err)
	}
}
