package ingest

import (
	"aggregator/internal/wildfire"
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"net/url"
	"os"
	"path/filepath"
	"sync"
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
