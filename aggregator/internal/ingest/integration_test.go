package ingest

import (
	"aggregator/internal/coastal"
	"aggregator/internal/satellite"
	"aggregator/internal/wildfire"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("M2_TEST_INGESTION_URL")
	if dsn == "" {
		t.Skip("M2_TEST_INGESTION_URL not set")
	}
	ctx := context.Background()
	admin, e := pgx.Connect(ctx, dsn)
	require.NoError(t, e)
	schema := "ingest_test_" + uuid.NewString()[:8]
	_, e = admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize())
	require.NoError(t, e)
	u, e := url.Parse(dsn)
	require.NoError(t, e)
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	s, e := New(ctx, u.String())
	require.NoError(t, e)
	_, e = s.Pool.Exec(ctx, Schema)
	require.NoError(t, e)
	t.Cleanup(func() {
		s.Pool.Close()
		_, e := admin.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		if e != nil {
			t.Error(e)
		}
		admin.Close(context.Background())
	})
	return s
}
func TestM2FieldAcceptanceSurvivesBrokerOutageAndLostResponse(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	body := []byte(`{"report_type":"flood","occurred_at":"2026-11-21T00:00:00Z","latitude":-6.2,"longitude":106.8,"description":"water rising","attributes":{}}`)
	require.NoError(t, ValidateReport(body))
	const count = 12
	var wg sync.WaitGroup
	ids := make(chan string, count)
	errs := make(chan error, count)
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := s.AcceptReport(ctx, "field-demo", "lost-response-key", body, "cid")
			if e != nil {
				errs <- e
				return
			}
			ids <- r["receipt_id"].(string)
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for e := range errs {
		require.NoError(t, e)
	}
	id := ""
	for got := range ids {
		if id == "" {
			id = got
		}
		require.Equal(t, id, got)
	}
	var rows int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM field_reports`).Scan(&rows))
	require.Equal(t, 1, rows)
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM raw_outbox`).Scan(&rows))
	require.Equal(t, 1, rows)
	_, e := s.AcceptReport(ctx, "field-demo", "lost-response-key", append(body, ' '), "cid")
	require.ErrorIs(t, e, ErrConflict)
	input, e := s.Input(ctx, id)
	require.NoError(t, e)
	require.NoError(t, s.ProcessReport(ctx, input))
	require.NoError(t, s.ProcessReport(ctx, input))
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM field_results`).Scan(&rows))
	require.Equal(t, 1, rows)
	receipt, e := s.Report(ctx, id, "field-demo", "field")
	require.NoError(t, e)
	require.Equal(t, "processed", receipt["status"])
}
func TestM2JournalFencesAndPreservesOriginalPositions(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	ns := uuid.NewString()
	i := Input{Source: "SATELIT", Namespace: ns, Position: 41, Payload: []byte("original bytes"), Metadata: map[string]any{}, CorrelationID: "test"}
	require.Error(t, s.Append(ctx, i, 41, "raw.satellite"))
	require.NoError(t, s.Leases.Run(ctx, "acquire:"+ns, func(c context.Context) error {
		if e := s.Append(c, i, 41, "raw.satellite"); e != nil {
			return e
		}
		if e := s.Append(c, i, 41, "raw.satellite"); e != nil {
			return e
		}
		changed := i
		changed.Payload = []byte("different")
		require.ErrorIs(t, s.Append(c, changed, 41, "raw.satellite"), ErrConflict)
		changed = i
		changed.Position = 43
		require.ErrorIs(t, s.Append(c, changed, 41, "raw.satellite"), ErrGap)
		return nil
	}))
	received, prepared, applied, e := s.Progress(ctx, "SATELIT", ns, 41)
	require.NoError(t, e)
	require.Equal(t, int64(41), received)
	require.Equal(t, int64(40), prepared)
	require.Equal(t, int64(40), applied)
}
func TestM2DurableAffectedRegionMatchesReference(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	root := filepath.Join("..", "..", "..", "docs", "m2", "fixtures", "wildfire-m5")
	provinces, e := os.ReadFile(filepath.Join(root, "provinces.json"))
	require.NoError(t, e)
	ns := "fire:" + uuid.NewString()
	var provinceRows []wildfire.Province
	require.NoError(t, json.Unmarshal(provinces, &provinceRows))
	for _, p := range provinceRows {
		raw, _ := json.Marshal(p)
		_, e = s.Pool.Exec(ctx, `INSERT INTO provinces(namespace,id,payload) VALUES($1,$2,$3)`, ns, p.ID, raw)
		require.NoError(t, e)
	}
	start, lateness := int64(1), int64(1800)
	model, e := wildfire.New(wildfire.Config{Version: 1, AlgorithmVersion: 2, Namespace: ns, InitialOffset: &start, AllowedLatenessSeconds: &lateness, ProvinceJSON: provinces, ProvinceSHA256: Hash(provinces)})
	require.NoError(t, e)
	file, e := os.Open(filepath.Join(root, "bridge.jsonl"))
	require.NoError(t, e)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var op wildfire.Operation
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &op))
		op.Namespace = ns
		raw, _ := json.Marshal(op)
		metadata := map[string]any{"operation": json.RawMessage(raw)}
		i := Input{ID: uuid.NewString(), Source: "SATELIT", Namespace: ns, Position: op.Offset, Payload: op.Payload, Metadata: metadata, CorrelationID: fmt.Sprint(op.Offset)}
		require.NoError(t, s.Leases.Run(ctx, "source:"+ns, func(c context.Context) error { return s.Append(c, i, start, "raw.satellite") }))
		require.NoError(t, s.Leases.Run(ctx, "worker:"+ns, func(c context.Context) error { return s.prepareFire(c, i, 30*time.Minute) }))
		_, e = model.Apply(op)
		require.NoError(t, e)
	}
	require.NoError(t, scanner.Err())
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
	expected := model.Snapshot().Fires
	active := []wildfire.Fire{}
	for _, f := range expected {
		if f.Active {
			active = append(active, f)
		}
	}
	require.Equal(t, active, actual)
	var singletons, excluded int64
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT singletons,excluded FROM source_progress WHERE source='SATELIT' AND namespace=$1`, ns).Scan(&singletons, &excluded))
	require.EqualValues(t, model.Snapshot().Singletons, singletons)
	require.EqualValues(t, model.Snapshot().Excluded, excluded)
}

func TestM2LiveSatellitePartialUpdatesAndRetainedReplay(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	ns := "patch:" + uuid.NewString()
	_, err := s.Pool.Exec(ctx, `INSERT INTO provinces(namespace,id,payload) VALUES($1,'fixture','{"id":"fixture","name":"Fixture Province","latitude":0,"longitude":0}')`, ns)
	require.NoError(t, err)
	events := []satellite.Event{
		{ID: "0", Type: "detection", Data: []byte(`{"offset":0,"detection":{"satellite":"N21","acq_date":"2026-11-18","acq_time":2359,"latitude":0,"longitude":0,"confidence":"n","frp":2}}`)},
		{ID: "1", Type: "update", Data: []byte(`{"offset":1,"key":"N21|2026-11-18|2359|0|0","changes":{"frp":8.68,"bright_ti5":272.92}}`)},
		{ID: "2", Type: "update", Data: []byte(`{"offset":2,"key":"N21|2026-11-18|2359|0|0","changes":{"bright_ti4":318.05}}`)},
	}
	for pos, event := range events {
		i := Input{ID: uuid.NewString(), Source: "SATELIT", Namespace: ns, Position: int64(pos), Payload: event.Data, Metadata: map[string]any{"event_id": event.ID, "event_type": event.Type}, CorrelationID: event.ID}
		if pos == 1 {
			i.Metadata["rejection"] = "unreadable satellite event: detection is missing satellite"
		}
		require.NoError(t, s.Leases.Run(ctx, "acquire:"+ns, func(c context.Context) error { return s.Append(c, i, 0, "raw.satellite") }))
		require.NoError(t, s.Leases.Run(ctx, "worker:"+ns, func(c context.Context) error { return s.prepareFire(c, i, 30*time.Minute) }))
		var state string
		require.NoError(t, s.Pool.QueryRow(ctx, `SELECT state FROM raw_inputs WHERE message_id=$1`, i.ID).Scan(&state))
		require.NotEqual(t, "parked", state)
	}
	var payload []byte
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT payload FROM wildfire_observations WHERE namespace=$1`, ns).Scan(&payload))
	var ob wildfire.Observation
	require.NoError(t, json.Unmarshal(payload, &ob))
	require.Equal(t, 8.68, ob.Detection.FRPMW)
	require.Equal(t, "n", ob.Detection.Confidence)
	require.Equal(t, "N21|2026-11-18|2359|0|0", ob.Key)
	require.EqualValues(t, 2, ob.LastOffset)
	// A patch parked by an older reader may be replayed after the source has
	// already retracted its target. It must resolve without resurrecting it.
	var patchID string
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT message_id::text FROM raw_inputs WHERE namespace=$1 AND position=1`, ns).Scan(&patchID))
	_, err = s.Pool.Exec(ctx, `UPDATE raw_inputs SET state='parked' WHERE message_id=$1`, patchID)
	require.NoError(t, err)
	_, err = s.Pool.Exec(ctx, `UPDATE wildfire_observations SET active=false,last_position=3,payload=jsonb_set(jsonb_set(payload,'{active}','false'),'{last_offset}','3') WHERE namespace=$1`, ns)
	require.NoError(t, err)
	_, err = s.RequestReplay(ctx, []string{patchID})
	require.NoError(t, err)
	retained, err := s.Input(ctx, patchID)
	require.NoError(t, err)
	require.NoError(t, s.Leases.Run(ctx, "worker:"+ns, func(c context.Context) error { return s.prepareFire(c, retained, 30*time.Minute) }))
	var state, reason string
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT state,reason FROM raw_inputs WHERE message_id=$1`, patchID).Scan(&state, &reason))
	require.Equal(t, "applied", state)
	var active bool
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT active FROM wildfire_observations WHERE namespace=$1`, ns).Scan(&active))
	require.False(t, active)
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT payload FROM wildfire_observations WHERE namespace=$1`, ns).Scan(&payload))
	require.NoError(t, json.Unmarshal(payload, &ob))
	require.Equal(t, 8.68, ob.Detection.FRPMW)
	require.EqualValues(t, 3, ob.LastOffset, "replay must preserve the later retraction frontier")
}

func TestM2InactiveSatelliteUpdateAfterWorkerRestart(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	ns := "inactive:" + uuid.NewString()
	_, err := s.Pool.Exec(ctx, `INSERT INTO provinces(namespace,id,payload) VALUES($1,'fixture','{"id":"fixture","name":"Fixture Province","latitude":0,"longitude":0}')`, ns)
	require.NoError(t, err)
	events := []satellite.Event{
		{ID: "0", Type: "detection", Data: []byte(`{"detection":{"satellite":"N21","acq_date":"2026-11-18","acq_time":2359,"latitude":0,"longitude":0,"confidence":"n","frp":2}}`)},
		{ID: "1", Type: "detection", Data: []byte(`{"detection":{"satellite":"N21","acq_date":"2026-11-18","acq_time":2359,"latitude":0,"longitude":0.001,"confidence":"n","frp":2}}`)},
		{ID: "2", Type: "retract", Data: []byte(`{"key":"N21|2026-11-18|2359|0|0"}`)},
		{ID: "3", Type: "update", Data: []byte(`{"key":"N21|2026-11-18|2359|0|0","changes":{"frp":200,"confidence":"h"}}`)},
		{ID: "4", Type: "update", Data: []byte(`{"key":"N21|2026-11-18|2359|0|0","detection":{"satellite":"N21","acq_date":"2026-11-18","acq_time":2359,"latitude":0,"longitude":0,"confidence":"h","frp":300}}`)},
	}
	worker := s
	for pos, event := range events {
		if pos == 3 {
			// A replacement process can acquire the scope after the crashed
			// incarnation's lease expires. Advance that condition in the fixture.
			_, err = s.Pool.Exec(ctx, `UPDATE source_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE scope=$1`, "worker:"+ns)
			require.NoError(t, err)
			worker, err = New(ctx, s.Pool.Config().ConnString())
			require.NoError(t, err)
			defer worker.Pool.Close()
		}
		i := Input{ID: uuid.NewString(), Source: "SATELIT", Namespace: ns, Position: int64(pos), Payload: event.Data, Metadata: map[string]any{"event_id": event.ID, "event_type": event.Type}, CorrelationID: event.ID}
		require.NoError(t, s.Leases.Run(ctx, "acquire:"+ns, func(c context.Context) error { return s.Append(c, i, 0, "raw.satellite") }))
		require.NoError(t, worker.Leases.Run(ctx, "worker:"+ns, func(c context.Context) error { return worker.prepareFire(c, i, 30*time.Minute) }))
		require.NoError(t, worker.Leases.Run(ctx, "worker:"+ns, func(c context.Context) error { return worker.prepareFire(c, i, 30*time.Minute) }), "lost-ack retry")
	}
	var payload []byte
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT payload FROM wildfire_observations WHERE namespace=$1 AND key='N21|2026-11-18|2359|0|0'`, ns).Scan(&payload))
	var ob wildfire.Observation
	require.NoError(t, json.Unmarshal(payload, &ob))
	require.False(t, ob.Active)
	require.Equal(t, 300.0, ob.Detection.FRPMW)
	var activeFires, patchOutputs, parked int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM wildfire_fires WHERE namespace=$1 AND active`, ns).Scan(&activeFires))
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM canonical_outbox o JOIN raw_inputs i USING(message_id) WHERE i.namespace=$1 AND i.position>=3`, ns).Scan(&patchOutputs))
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM raw_inputs WHERE namespace=$1 AND state='parked'`, ns).Scan(&parked))
	require.Zero(t, activeFires)
	require.Zero(t, patchOutputs)
	require.Zero(t, parked)
}

type incompleteBody struct{}

func (incompleteBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestM2ResponseQuarantineKeepsFullBytesWithoutAdvancingSource(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	ns := "response:" + uuid.NewString()
	payload := bytes.Repeat([]byte("original\x00bytes\r\n"), 10000)
	require.NoError(t, s.Leases.Run(ctx, "acquire:"+ns, func(owned context.Context) error {
		if e := s.RetainResponse(owned, "SATELIT", ns, "stream:41", bytes.NewReader(payload), "oversized SSE frame"); e != nil {
			return e
		}
		incomplete := io.MultiReader(bytes.NewReader([]byte("partial bytes")), incompleteBody{})
		require.Error(t, s.RetainResponse(owned, "SATELIT", ns, "stream:41", incomplete, "incomplete transport"))
		return nil
	}))
	var id string
	var count int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM response_failures`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT message_id::text FROM response_failures`).Scan(&id))
	var out bytes.Buffer
	size, digest, e := s.WriteResponsePayload(ctx, id, &out)
	require.NoError(t, e)
	require.EqualValues(t, len(payload), size)
	require.Equal(t, Hash(payload), digest)
	require.Equal(t, payload, out.Bytes())
	received, prepared, applied, e := s.Progress(ctx, "SATELIT", ns, 41)
	require.NoError(t, e)
	require.EqualValues(t, 40, received)
	require.EqualValues(t, 40, prepared)
	require.EqualValues(t, 40, applied)
	page, e := s.Failures(ctx, "SATELIT", 10, "")
	require.NoError(t, e)
	require.Len(t, page["items"], 1)
}

func TestM2CoastalRejectedPageRetainsTailAndResolvesOnReacquisition(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	ns := "page:" + uuid.NewString()
	var mode atomic.Int32
	large := bytes.Repeat([]byte("original rejected response\x00"), 180000)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mode.Load() == 0 {
			_, _ = w.Write([]byte(`{"items":null}`))
		} else if mode.Load() == 1 {
			_, _ = w.Write(large)
		} else {
			_, _ = w.Write([]byte(`{"generated_at":"2026-11-12T00:00:00Z","items":[],"next_cursor":null}`))
		}
	}))
	defer server.Close()
	c := &coastalClient{cfg: CoastalConfig{URL: server.URL, Namespace: ns}, http: server.Client(), s: s,
		limiter: coastal.Limiter{Rate: 1000, Burst: 1}, observations: coastal.Breaker{Name: "observations", Threshold: 3}}
	require.NoError(t, s.Leases.Run(ctx, "acquire:"+ns, func(owned context.Context) error {
		require.Error(t, c.acquire(owned))
		mode.Store(1)
		require.Error(t, c.acquire(owned))
		var id string
		require.NoError(t, s.Pool.QueryRow(owned, `SELECT message_id::text FROM response_failures WHERE size=$1`, len(large)).Scan(&id))
		var retained bytes.Buffer
		_, digest, e := s.WriteResponsePayload(owned, id, &retained)
		require.NoError(t, e)
		require.Equal(t, Hash(large), digest)
		require.Equal(t, large, retained.Bytes())
		received, _, _, e := s.Progress(owned, "SENSOR_PANTAI", ns, 0)
		require.NoError(t, e)
		require.EqualValues(t, -1, received)
		mode.Store(2)
		return c.acquire(owned)
	}))
	var unresolved int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM response_failures WHERE NOT resolved`).Scan(&unresolved))
	require.Zero(t, unresolved)
}
