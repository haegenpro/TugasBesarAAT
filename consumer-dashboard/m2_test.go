package main

import (
	"context"
	"encoding/json"
	"github.com/nats-io/nats.go/jetstream"
	"testing"
)

func TestM2EnumsAndRetractionDecode(t *testing.T) {
	for _, source := range []string{"SATELIT", "SENSOR_PANTAI"} {
		if !validSource(source) {
			t.Fatal(source)
		}
	}
	for _, kind := range []string{"WILDFIRE", "COASTAL"} {
		if !validHazardType(kind) {
			t.Fatal(kind)
		}
	}
	e := lifecycleEvent{EventID: "retract", EventType: "hazard_retracted", RecordID: "stable", Revision: 3, HazardRevision: 3, PublishedAt: "2026-11-11T00:00:00Z", CorrelationID: "test"}
	raw, _ := json.Marshal(e)
	outer := map[string]any{"event_id": e.EventID, "record_id": e.RecordID, "event_type": e.EventType, "revision": e.Revision, "snapshot": raw}
	if _, err := decodeLifecycle(outer, "test"); err != nil {
		t.Fatal(err)
	}
	e.HazardRevision = 2
	raw, _ = json.Marshal(e)
	outer["snapshot"] = raw
	if _, err := decodeLifecycle(outer, "test"); err == nil {
		t.Fatal("accepted mismatched retraction revision")
	}
}
func TestM2RetractionCannotResurrect(t *testing.T) {
	db, err := openDB(testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{haltController: &haltController{}, db: db, cfg: config{Role: "notifier"}}
	ctx := context.Background()
	h := hazardEvent{HazardID: "stable", Source: "SATELIT", HazardType: "WILDFIRE", Severity: "WASPADA", AreaName: "Fixture", OccurredAt: "2026-11-11T00:00:00Z", IngestedAt: "2026-11-11T00:00:01Z", Attributes: map[string]any{"satellites": []any{"N20", "N21"}}}
	save := func(e lifecycleEvent, seq uint64) {
		meta := &jetstream.MsgMetadata{Stream: "LIFECYCLE"}
		meta.Sequence.Stream = seq
		if err = a.saveLifecycle(ctx, meta, []byte(e.EventID), e); err != nil {
			t.Fatal(err)
		}
	}
	correction := lifecycleEvent{EventID: "correct", EventType: "hazard_corrected", RecordID: h.HazardID, Revision: 2, HazardRevision: 2, Hazard: &h, CorrelationID: "test"}
	save(correction, 1)
	retraction := lifecycleEvent{EventID: "retract", EventType: "hazard_retracted", RecordID: h.HazardID, Revision: 3, HazardRevision: 3, CorrelationID: "test"}
	save(retraction, 2)
	initial := &jetstream.MsgMetadata{Stream: "HAZARDS"}
	initial.Sequence.Stream = 1
	if err = a.save(ctx, initial, []byte("delayed"), envelope{HazardID: h.HazardID, Payload: h, CorrelationID: "test"}); err != nil {
		t.Fatal(err)
	}
	save(correction, 3)
	save(retraction, 4)
	var count, version int
	if err = db.QueryRow("SELECT count(*) FROM records").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("tombstone resurrected")
	}
	if err = db.QueryRow("SELECT revision FROM record_versions WHERE record_id='hazard:stable'").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 3 {
		t.Fatal(version)
	}
	restored := correction
	restored.EventID = "restored"
	restored.Revision = 4
	restored.HazardRevision = 4
	save(restored, 5)
	if err = db.QueryRow("SELECT count(*) FROM records").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("restoration missing")
	}
	if err = db.QueryRow("SELECT count(*) FROM notifications").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatal("duplicate notification", count)
	}
}
