package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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
	b, err := os.ReadFile(filepath.Join("..", "docs", "m2", "fixtures", "coastal-warning-v1.avsc"))
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
	// Reacquiring the same observation on a later sensor page changes transport
	// provenance, but must not turn a logical duplicate into a payload conflict.
	reacquired := deliverFrame(warningFrame(t, schema, time.Now().UTC().Format(time.RFC3339Nano), 1.11))
	changed := deliverFrame(warningFrame(t, schema, received, 9.99))
	if first.ackCalls != 1 || repeat.ackCalls != 1 || reacquired.ackCalls != 1 || changed.ackCalls != 1 {
		t.Fatalf("every delivery must be acknowledged after commit: %d %d %d", first.ackCalls, repeat.ackCalls, changed.ackCalls)
	}
	var receipts, inbox, conflicts int
	db.QueryRow(`SELECT count(*) FROM coastal_warning_receipts`).Scan(&receipts)
	db.QueryRow(`SELECT count(*) FROM notifications WHERE kind='coastal_warning'`).Scan(&inbox)
	db.QueryRow(`SELECT count(*) FROM coastal_warning_conflicts`).Scan(&conflicts)
	if receipts != 1 || inbox != 1 || conflicts != 1 {
		t.Fatalf("receipts=%d inbox=%d conflicts=%d", receipts, inbox, conflicts)
	}
	firstReceived, _ := time.Parse(time.RFC3339Nano, received)
	authority := expectedServer(t, []expectedWarning{{Sequence: 1, NotificationID: "warning-v1:abc", FirstReceivedAt: firstReceived}}, time.Now().UTC())
	r.cfg.ExpectationsURL, r.cfg.ExpectationsToken = authority.URL, "slo-test-token"
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
	t.Setenv("WARNING_EXPECTATIONS_URL", "http://coastal-processor:8080")
	t.Setenv("COASTAL_SLO_TOKEN", "dedicated-slo")
	if _, enabled, err := loadWarningConfig(config{Role: "notifier", NATSURL: "nats://x"}); err != nil || !enabled {
		t.Fatalf("notifier warning config rejected: %v", err)
	}
	if _, _, err := loadWarningConfig(config{Role: "dashboard"}); err == nil {
		t.Fatal("dashboard accepted coastal warning delivery")
	}
}

func TestWarningDeliveryThroughRealBroker(t *testing.T) {
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("TEST_NATS_URL required for real warning transport")
	}
	db, err := openDB(testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	nc, err := nats.Connect(url)
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
	suffix := fmt.Sprint(time.Now().UnixNano())
	cfg := warningConfig{Stream: "WARNING_TEST_" + suffix, Subject: "warnings.test." + suffix, Durable: "recipient", BacklogAfter: time.Minute, FreshQuota: 3}
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: cfg.Stream, Subjects: []string{cfg.Subject}, Storage: jetstream.FileStorage, Retention: jetstream.InterestPolicy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), cfg.Stream) })
	schema := warningSchema(t)
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": schema})
	}))
	defer registry.Close()
	recipient := &warningRecipient{cfg: cfg, db: db, schemas: newSchemaClient(registry.URL, db)}
	consumer, err := recipient.consumer(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	received := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err = js.Publish(ctx, cfg.Subject, warningFrame(t, schema, received, 1.11)); err != nil {
		t.Fatal(err)
	}
	if err = recipient.cycle(ctx, consumer); err != nil {
		t.Fatal(err)
	}
	// Recreate the recipient and its durable subscription, then deliver the
	// same logical warning with a later acquisition timestamp.
	recipient = &warningRecipient{cfg: cfg, db: db, schemas: newSchemaClient(registry.URL, db)}
	consumer, err = recipient.consumer(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.Publish(ctx, cfg.Subject, warningFrame(t, schema, time.Now().UTC().Format(time.RFC3339Nano), 1.11)); err != nil {
		t.Fatal(err)
	}
	if err = recipient.cycle(ctx, consumer); err != nil {
		t.Fatal(err)
	}
	var receipts, inbox, conflicts int
	if err = db.QueryRow(`SELECT (SELECT count(*) FROM coastal_warning_receipts),(SELECT count(*) FROM notifications WHERE kind='coastal_warning'),(SELECT count(*) FROM coastal_warning_conflicts)`).Scan(&receipts, &inbox, &conflicts); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 || inbox != 1 || conflicts != 0 {
		t.Fatalf("receipts=%d inbox=%d conflicts=%d", receipts, inbox, conflicts)
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.NumAckPending != 0 {
		t.Fatalf("unacknowledged warnings: %d", info.NumAckPending)
	}
}
