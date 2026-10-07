package main

import (
	"context"
	"encoding/json"
	"github.com/nats-io/nats.go/jetstream"
	"path/filepath"
	"testing"
)

func TestCorrectionBeforeCanonicalCannotRegressProjection(t *testing.T) {
	db, err := openDB(filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{db: db, cfg: config{Role: "notifier"}}
	ctx := context.Background()
	h := hazardEvent{HazardID: "stable-hazard", Source: "BMKG", Severity: "AWAS", AreaName: "Coast", OccurredAt: "2026-10-07T00:00:00Z"}
	e := lifecycleEvent{EventID: "correction", EventType: "hazard_corrected", RecordID: h.HazardID, Revision: 2, HazardRevision: 2, Hazard: &h, CorrelationID: "test"}
	meta := &jetstream.MsgMetadata{Stream: "REPORT_LIFECYCLE"}
	meta.Sequence.Stream = 1
	if err = a.saveLifecycle(ctx, meta, []byte("correction"), e); err != nil {
		t.Fatal(err)
	}
	initial := h
	initial.Severity = "SIAGA"
	meta = &jetstream.MsgMetadata{Stream: "HAZARDS"}
	meta.Sequence.Stream = 1
	if err = a.save(ctx, meta, []byte("initial"), envelope{HazardID: h.HazardID, Payload: initial, CorrelationID: "test"}); err != nil {
		t.Fatal(err)
	}
	var b string
	if err = db.QueryRow(`SELECT payload FROM records WHERE hazard_id=?`, h.HazardID).Scan(&b); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err = json.Unmarshal([]byte(b), &got); err != nil {
		t.Fatal(err)
	}
	if got["severity"] != "AWAS" {
		t.Fatalf("regressed projection: %s", b)
	}
	// A retry after the broker duplicate window still records the correction only once.
	meta = &jetstream.MsgMetadata{Stream: "REPORT_LIFECYCLE"}
	meta.Sequence.Stream = 2
	if err = a.saveLifecycle(ctx, meta, []byte("retry"), e); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = db.QueryRow(`SELECT count(*) FROM notifications`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("duplicate notification count: %d", count)
	}
}
