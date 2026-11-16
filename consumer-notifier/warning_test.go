package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
)

func scheduled(items ...delivery) []string {
	out := []string{}
	for _, d := range schedule(items, time.Unix(1000, 0), time.Minute, 3) {
		out = append(out, d.warning.NotificationID)
	}
	return out
}

func pending(id, observation string, age time.Duration) delivery {
	return delivery{warning: warning{NotificationID: id, ObservationID: observation, received: time.Unix(1000, 0).Add(-age)}}
}

func TestScheduleInterleavesBacklogWithoutReorderingAnObservation(t *testing.T) {
	got := scheduled(
		pending("old-1", "o1", time.Hour), pending("f1", "a", 0), pending("old-2", "o2", time.Hour),
		pending("f2", "b", 0), pending("f3", "c", 0), pending("f4", "d", 0), pending("f5", "e", 0),
		pending("old-1-correction", "o1", 0),
	)
	want := []string{"f1", "f2", "f3", "old-1", "old-1-correction", "f4", "f5", "old-2"}
	if len(got) != len(want) {
		t.Fatalf("scheduled %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("scheduled %v, want %v", got, want)
		}
	}
	if got := scheduled(pending("x", "x", time.Hour), pending("y", "y", time.Hour)); got[0] != "x" || got[1] != "y" {
		t.Fatalf("backlog alone keeps arrival order: %v", got)
	}
}

func warningSchema(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "docs", "fixtures", "coastal-warning-v1.avsc"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func warningFrame(t *testing.T, schema, received string, wave float64) []byte {
	t.Helper()
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	data, err := codec.BinaryFromNative(nil, map[string]any{
		"warning_version": int32(1), "notification_id": "warning-v1:abc", "recipient": "TIM_LAPANGAN", "source": "SENSOR_PANTAI",
		"source_namespace": "coastal", "observation_id": "obs-000120", "operation_id": "coastal-op-v1:x", "operation_position": int64(120), "operation": "create",
		"observed_at": "2026-11-12T00:30:00Z", "first_received_at": received, "critical": true,
		"station_id": "stn-padang-01", "province": "Sumatera Barat", "latitude": -0.9492, "longitude": 100.3543, "sea_level_anomaly_m": 1.46, "wave_height_m": wave,
		"quality": "ok", "payload_sha256": "00", "correlation_id": "cid", "traceparent": goavro.Union("null", nil), "hazard_id": goavro.Union("null", nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, 5)
	binary.BigEndian.PutUint32(frame[1:], 41)
	return append(frame, data...)
}

func TestWarningReceiptIsTransactionalIdempotentAndAudited(t *testing.T) {
	db, err := openDB(testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := warningSchema(t)
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": schema})
	}))
	defer registry.Close()
	r := &warningRecipient{cfg: warningConfig{BacklogAfter: time.Minute, FreshQuota: 3}, db: db, schemas: newSchemaClient(registry.URL, db)}
	ctx := context.Background()
	deliverFrame := func(frame []byte) *testMsg {
		msg := &testMsg{raw: frame}
		d := delivery{msg: msg, digest: digestOf(frame)}
		d.warning, d.err = r.schemas.decodeWarning(ctx, frame)
		if err := r.deliver(ctx, d); err != nil {
			t.Fatal(err)
		}
		return msg
	}
	received := time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)
	first := deliverFrame(warningFrame(t, schema, received, 1.11))
	repeat := deliverFrame(warningFrame(t, schema, received, 1.11))
	changed := deliverFrame(warningFrame(t, schema, received, 9.99))
	if first.ackCalls != 1 || repeat.ackCalls != 1 || changed.ackCalls != 1 {
		t.Fatalf("every delivery must be acknowledged after commit: %d %d %d", first.ackCalls, repeat.ackCalls, changed.ackCalls)
	}
	var receipts, inbox, conflicts int
	db.QueryRow(`SELECT count(*) FROM coastal_warning_receipts`).Scan(&receipts)
	db.QueryRow(`SELECT count(*) FROM notifications WHERE kind='coastal_warning'`).Scan(&inbox)
	db.QueryRow(`SELECT count(*) FROM coastal_warning_conflicts`).Scan(&conflicts)
	if receipts != 1 || inbox != 1 || conflicts != 1 {
		t.Fatalf("receipts=%d inbox=%d conflicts=%d", receipts, inbox, conflicts)
	}
	report, err := r.slo(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if report.Delivered != 1 || report.OnTime != 1 || report.Conflicts != 1 || *report.OnTimeRatio != 1 || *report.BudgetUsed != 0 {
		t.Fatalf("unexpected SLO report %+v", report)
	}
	items, err := (&projectionStore{db: db}).readRecords(ctx, "warnings")
	if err != nil || len(items) != 1 {
		t.Fatalf("warnings view: %v %v", items, err)
	}
	unreadable := &testMsg{raw: []byte{0, 0, 0, 0, 41, 1}}
	d := delivery{msg: unreadable, digest: digestOf(unreadable.raw)}
	d.warning, d.err = r.schemas.decodeWarning(ctx, unreadable.raw)
	if d.err == nil || !errors.Is(d.err, errCorruptPayload) {
		t.Fatalf("corrupt warning decoded: %v", d.err)
	}
	if err := r.deliver(ctx, d); err != nil {
		t.Fatal(err)
	}
	db.QueryRow(`SELECT count(*) FROM coastal_warning_conflicts`).Scan(&conflicts)
	if conflicts != 2 {
		t.Fatalf("unreadable warning was not retained: %d", conflicts)
	}
}

func TestWarningConfigIsNotifierOnly(t *testing.T) {
	t.Setenv("WARNING_DURABLE", "tim-lapangan-v1")
	t.Setenv("WARNING_STREAM", "BNPB_CRITICAL")
	t.Setenv("WARNING_SUBJECT", "bnpb.warning.coastal")
	if _, enabled, err := loadWarningConfig(config{Role: "notifier", NATSURL: "nats://x"}); err != nil || !enabled {
		t.Fatalf("notifier warning config rejected: %v", err)
	}
	if _, _, err := loadWarningConfig(config{Role: "dashboard"}); err == nil {
		t.Fatal("dashboard accepted coastal warning delivery")
	}
}
