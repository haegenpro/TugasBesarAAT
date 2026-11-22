package ingest

import (
	"aggregator/internal/satellite"
	"aggregator/internal/wildfire"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"aggregator/internal/platform"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// baselineEvents is a fixed, seeded satellite stream with known expected results:
// detections, partial updates, retractions of earlier detections, and malformed
// records that must be parked without stopping the stream. The same n always
// yields the same bytes, so two builds are measured against the same input.
func baselineEvents(n int) (events []satellite.Event, malformed int) {
	type target struct {
		key       string
		retracted bool
	}
	var live []target
	for pos := 0; pos < n; pos++ {
		var data string
		kind := "detection"
		switch {
		case pos%97 == 96:
			data = fmt.Sprintf(`{"offset":%d,"detection":{"satellite":"","acq_date":"2026-11-18","acq_time":0,"latitude":0,"longitude":0,"confidence":"n","frp":1}}`, pos)
			malformed++
		case pos%25 == 24 && len(live) > 0 && !live[len(live)/2].retracted:
			kind = "retract"
			t := &live[len(live)/2]
			t.retracted = true
			data = fmt.Sprintf(`{"offset":%d,"key":%q}`, pos, t.key)
		case pos%10 == 9 && len(live) > 0 && !live[len(live)-1].retracted:
			kind = "update"
			data = fmt.Sprintf(`{"offset":%d,"key":%q,"changes":{"frp":%d.5}}`, pos, live[len(live)-1].key, pos%40)
		default:
			hhmm := (pos/60%24)*100 + pos%60
			// Twenty well separated clusters keep fires small, as in a real multi-region feed.
			lat := float64(pos%20) * 1.5
			lon := float64((pos/20)%700) * 0.002
			data = fmt.Sprintf(`{"offset":%d,"detection":{"satellite":"N21","acq_date":"2026-11-18","acq_time":%d,"latitude":%s,"longitude":%s,"confidence":"n","frp":2}}`, pos, hhmm, strconv.FormatFloat(lat, 'f', -1, 64), strconv.FormatFloat(lon, 'f', -1, 64))
			if norm, err := satellite.Normalize("baseline", satellite.Event{ID: strconv.Itoa(pos), Type: "detection", Data: []byte(data)}); err == nil {
				var op wildfire.Operation
				if wildfire.Decode(norm.Operation, &op) == nil && op.Detection != nil {
					live = append(live, target{key: strings.Join([]string{op.Detection.Satellite, op.Detection.AcqDate, op.Detection.AcqTime, op.Detection.Latitude, op.Detection.Longitude}, "|")})
				}
			}
		}
		events = append(events, satellite.Event{ID: strconv.Itoa(pos), Type: kind, Data: []byte(data)})
	}
	return events, malformed
}

// TestM2IngestionBaseline measures the satellite path of one Ingestion/worker pair
// against a fixed input and checks the result against the reference model. It is
// skipped unless M2_BASELINE_EVENTS is set, for example:
//
//	M2_BASELINE_EVENTS=3000 M2_BASELINE_OUT=/tmp/base.json go test ./internal/ingest -run IngestionBaseline -v
//
// Acquisition and preparation are timed separately so each later change can be
// compared with exactly the stage it touches. Canonical delivery needs the owner
// service and is measured on the cluster by scripts/m2/baseline_capture.py.
func TestM2IngestionBaseline(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("M2_BASELINE_EVENTS"))
	if n <= 0 {
		t.Skip("M2_BASELINE_EVENTS not set")
	}
	s := testStore(t)
	ctx := context.Background()
	ns := "baseline:" + uuid.NewString()
	root := filepath.Join("..", "..", "..", "docs", "m2", "fixtures", "wildfire-m5")
	provinces, e := os.ReadFile(filepath.Join(root, "provinces.json"))
	require.NoError(t, e)
	var provinceRows []wildfire.Province
	require.NoError(t, json.Unmarshal(provinces, &provinceRows))
	for _, p := range provinceRows {
		raw, _ := json.Marshal(p)
		_, e = s.Pool.Exec(ctx, `INSERT INTO provinces(namespace,id,payload) VALUES($1,$2,$3)`, ns, p.ID, raw)
		require.NoError(t, e)
	}
	events, malformed := baselineEvents(n)
	inputs := make([]Input, len(events))
	bytes := 0
	for pos, event := range events {
		normalized, normErr := satellite.Normalize(ns, event)
		metadata := map[string]any{"event_id": event.ID, "event_type": event.Type}
		if normErr != nil {
			metadata["rejection"] = normErr.Error()
		}
		entity := ""
		if normErr == nil {
			var op wildfire.Operation
			if json.Unmarshal(normalized.Operation, &op) == nil {
				entity = op.TargetKey
				if entity == "" && op.Detection != nil {
					entity = strings.Join([]string{op.Detection.Satellite, op.Detection.AcqDate, op.Detection.AcqTime, op.Detection.Latitude, op.Detection.Longitude}, "|")
				}
			}
		}
		inputs[pos] = Input{ID: uuid.NewString(), Source: "SATELIT", Namespace: ns, Position: int64(pos), Entity: entity, Payload: event.Data, Metadata: metadata, CorrelationID: event.ID}
		bytes += len(event.Data)
	}

	// M2_BASELINE_BATCH is the acquisition batch size; 1 is the batch-size-one comparison mode.
	batchSize, _ := strconv.Atoi(os.Getenv("M2_BASELINE_BATCH"))
	batchSize = max(batchSize, 1)
	acquireStart := time.Now()
	require.NoError(t, s.Leases.Run(ctx, "acquire:"+ns, func(c context.Context) error {
		for from := 0; from < len(inputs); from += batchSize {
			if e := s.AppendBatch(c, inputs[from:min(from+batchSize, len(inputs))], 0, "raw.satellite"); e != nil {
				return e
			}
		}
		return nil
	}))
	acquire := time.Since(acquireStart)
	if os.Getenv("M2_BASELINE_ACQUIRE_ONLY") != "" {
		var rows int
		require.NoError(t, s.Pool.QueryRow(ctx, "SELECT count(*) FROM raw_outbox o JOIN raw_inputs i USING(message_id) WHERE i.namespace=$1", ns).Scan(&rows))
		require.Equal(t, n, rows)
		t.Logf("acquisition only: %d records, batch %d, %.2fs (%.0f/s)", n, batchSize, acquire.Seconds(), float64(n)/acquire.Seconds())
		return
	}
	t.Logf("acquisition of %d records took %.2fs", n, acquire.Seconds())

	prepareStart := time.Now()
	require.NoError(t, s.Leases.Run(ctx, "worker:"+ns, func(c context.Context) error {
		for _, i := range inputs {
			if e := s.prepareFire(c, i, 30*time.Minute); e != nil {
				return e
			}
		}
		return nil
	}))
	prepare := time.Since(prepareStart)
	t.Logf("preparation of %d records took %.2fs", n, prepare.Seconds())

	// Expected result: the reference model applied to the same valid operations.
	start, lateness := int64(0), int64(1800)
	model, e := wildfire.New(wildfire.Config{Version: 1, AlgorithmVersion: 2, Namespace: ns, InitialOffset: &start, AllowedLatenessSeconds: &lateness, ProvinceJSON: provinces, ProvinceSHA256: Hash(provinces)})
	require.NoError(t, e)
	for _, event := range events {
		normalized, normErr := satellite.Normalize(ns, event)
		var op wildfire.Operation
		if normErr != nil {
			// A parked record still consumes its offset; the model sees it as a heartbeat.
			beat, _ := json.Marshal(map[string]any{"version": 1, "namespace": ns, "offset": normalized.Offset, "kind": "heartbeat", "payload": ""})
			require.NoError(t, wildfire.Decode(beat, &op))
			_, e = model.Apply(op)
			require.NoError(t, e)
			continue
		}
		require.NoError(t, wildfire.Decode(normalized.Operation, &op))
		_, e = model.Apply(op)
		require.NoError(t, e)
	}
	rows, e := s.Pool.Query(ctx, `SELECT payload FROM wildfire_fires WHERE namespace=$1 AND active ORDER BY fire_id`, ns)
	require.NoError(t, e)
	defer rows.Close()
	actual := []wildfire.Fire{}
	for rows.Next() {
		var raw []byte
		var f wildfire.Fire
		require.NoError(t, rows.Scan(&raw))
		require.NoError(t, json.Unmarshal(raw, &f))
		actual = append(actual, f)
	}
	require.NoError(t, rows.Err())
	active := []wildfire.Fire{}
	for _, f := range model.Snapshot().Fires {
		if f.Active {
			active = append(active, f)
		}
	}
	require.Equal(t, active, actual, "durable fires differ from the reference model")
	received, prepared, _, e := s.Progress(ctx, "SATELIT", ns, 0)
	require.NoError(t, e)
	require.EqualValues(t, n-1, received)
	require.EqualValues(t, n-1, prepared, "preparation frontier must reach the last offset")
	var parked, outbox, canonical int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM raw_inputs WHERE namespace=$1 AND state='parked'`, ns).Scan(&parked))
	require.Equal(t, malformed, parked, "every malformed record is parked, none lost")
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM raw_outbox`).Scan(&outbox))
	require.Equal(t, n, outbox, "every acquired record has a raw outbox entry")
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM canonical_outbox WHERE namespace=$1`, ns+":wildfire-output").Scan(&canonical))

	w := httptest.NewRecorder()
	platform.Metrics(w, httptest.NewRequest("GET", "/metrics", nil))
	lines := []string{}
	for _, line := range strings.Split(w.Body.String(), "\n") {
		if strings.HasPrefix(line, "app_ingest_") && !strings.Contains(line, "_bucket") {
			lines = append(lines, line)
		}
	}
	sort.Strings(lines)
	result := map[string]any{
		"events": n, "payload_bytes": bytes, "malformed": malformed, "active_fires": len(active), "canonical_outbox": canonical,
		"acquire_seconds": acquire.Seconds(), "acquire_per_second": float64(n) / acquire.Seconds(),
		"prepare_seconds": prepare.Seconds(), "prepare_per_second": float64(n) / prepare.Seconds(),
		"metrics": lines,
	}
	t.Logf("baseline: %d events (%d malformed), acquisition %.2fs (%.0f/s), preparation %.2fs (%.0f/s), %d active fires, reference match",
		n, malformed, acquire.Seconds(), result["acquire_per_second"], prepare.Seconds(), result["prepare_per_second"], len(active))
	if out := os.Getenv("M2_BASELINE_OUT"); out != "" {
		raw, _ := json.MarshalIndent(result, "", "  ")
		require.NoError(t, os.WriteFile(out, raw, 0o644))
	}
}
