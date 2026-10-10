package store_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"aggregator/internal/clients"
	"aggregator/internal/poller"
	"aggregator/internal/store"
	"aggregator/internal/volcano"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type pollingSource struct {
	mu      sync.Mutex
	rows    map[string][]map[string]any
	queries map[string][]string
	failed  map[string]bool
}

func (s *pollingSource) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.URL.Path == "/history" {
		after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
		json.NewEncoder(w).Encode(clients.HistoryPage{Data: []clients.HistoryItem{}, NextCursor: after})
		return
	}
	raw := r.URL.Query().Get("since")
	s.queries[r.URL.Path] = append(s.queries[r.URL.Path], raw)
	if s.failed[r.URL.Path] {
		w.WriteHeader(503)
		return
	}
	var since time.Time
	if raw != "" {
		var err error
		since, err = time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			w.WriteHeader(400)
			return
		}
	}
	key := "occurred_at"
	if r.URL.Path == "/volcanic-reports" {
		key = "reported_at"
	}
	out := []map[string]any{}
	for _, row := range s.rows[r.URL.Path] {
		at, _ := row[key].(time.Time)
		if r.URL.Path == "/tsunami-warnings" {
			for _, ev := range s.rows["/seismic-events"] {
				if ev["event_id"] == row["related_event_id"] {
					at = ev["occurred_at"].(time.Time)
					break
				}
			}
		}
		if since.IsZero() || !at.Before(since) {
			out = append(out, row)
		}
	}
	json.NewEncoder(w).Encode(out)
}

func pollingFixture(t *testing.T) (*store.Store, *pollingSource, func() *poller.Poller, string) {
	t.Helper()
	s := testStore(t)
	_, err := s.Pool.Exec(context.Background(), `DELETE FROM source_poll_state WHERE endpoint=ANY($1::text[])`, []string{"BMKG", "BMKG_TSUNAMI", "PVMBG"})
	require.NoError(t, err)
	id := "incremental-" + uuid.NewString()
	old := time.Now().UTC().Add(-time.Hour)
	up := &pollingSource{
		queries: map[string][]string{}, failed: map[string]bool{},
		rows: map[string][]map[string]any{
			"/seismic-events":   {{"event_id": id, "magnitude": 6.8, "depth_km": 20, "epicenter_lat": -6, "epicenter_lon": 106, "region_name": "Polling test", "occurred_at": old, "potential_tsunami": true}},
			"/tsunami-warnings": {{"warning_id": "w-" + id, "related_event_id": id, "threat_level": "Awas", "affected_zones": []string{"Coast"}, "estimated_arrival": old}},
			"/volcanic-reports": {{"report_id": "v-" + id, "volcano_id": "merapi", "alert_level": "Waspada", "eruption_count_24h": 1, "ash_column_height_m": 500, "reported_at": old}},
		},
	}
	server := httptest.NewServer(http.HandlerFunc(up.serve))
	t.Cleanup(server.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	newPoller := func() *poller.Poller {
		return &poller.Poller{Store: s, BMKG: clients.NewBMKGClient(server.URL, "key", logger), PVMBG: clients.NewPVMBGClient(server.URL, "token", logger), Lookup: volcano.NewLookup(s.Pool), Interval: time.Second, MaxBackoff: time.Minute, Logger: logger}
	}
	return s, up, newPoller, id
}

func pollAll(p *poller.Poller) {
	p.PollBMKGOnce(context.Background())
	p.PollPVMBGOnce(context.Background())
}

func TestPollingLastSuccessPersistsAndAvoidsReprocessingOldRecords(t *testing.T) {
	s, source, newPoller, id := pollingFixture(t)
	ctx := context.Background()
	pollAll(newPoller())
	statuses, err := s.PollStatuses(ctx, []string{"BMKG", "BMKG_TSUNAMI", "PVMBG"})
	require.NoError(t, err)
	for _, state := range statuses {
		require.Equal(t, "fresh", state.State)
		require.NotNil(t, state.LastSuccess)
	}
	var oldSeen time.Time
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT last_seen_at FROM source_observations WHERE record_id=$1`, id).Scan(&oldSeen))
	// Insert records exactly at the stored boundary. The overlap must include
	// them with inclusive event-time filtering and the polling overlap.
	source.mu.Lock()
	for path, spec := range map[string]struct{ endpoint, idKey, timeKey string }{
		"/seismic-events":   {"BMKG", "event_id", "occurred_at"},
		"/tsunami-warnings": {"BMKG_TSUNAMI", "warning_id", ""},
		"/volcanic-reports": {"PVMBG", "report_id", "reported_at"},
	} {
		copy := map[string]any{}
		for k, v := range source.rows[path][0] {
			copy[k] = v
		}
		copy[spec.idKey] = copy[spec.idKey].(string) + "-boundary"
		if spec.timeKey != "" {
			copy[spec.timeKey] = *statuses[spec.endpoint].LastSuccess
		} else {
			copy["related_event_id"] = id + "-boundary"
		}
		source.rows[path] = append(source.rows[path], copy)
	}
	source.mu.Unlock()
	// Simulate restarting the Aggregator: no in-memory polling state survives.
	pollAll(newPoller())
	source.mu.Lock()
	for path, endpoint := range map[string]string{"/seismic-events": "BMKG", "/tsunami-warnings": "BMKG_TSUNAMI", "/volcanic-reports": "PVMBG"} {
		require.Equal(t, []string{"", statuses[endpoint].LastSuccess.Add(-time.Second).UTC().Format(time.RFC3339Nano)}, source.queries[path])
	}
	source.mu.Unlock()
	var seenAgain time.Time
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT last_seen_at FROM source_observations WHERE record_id=$1`, id).Scan(&seenAgain))
	require.Equal(t, oldSeen, seenAgain, "older records must not be fetched/processed again during an incremental poll")
	for _, recordID := range []string{id + "-boundary", "w-" + id + "-boundary", "v-" + id + "-boundary"} {
		var n int
		require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM source_observations WHERE record_id=$1`, recordID).Scan(&n))
		require.Equal(t, 1, n, recordID)
	}
	var severity string
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT severity FROM hazard_events WHERE source_ref_id=$1`, id).Scan(&severity))
	require.Equal(t, "AWAS", severity, "an old warning still participates in correlation")
	// No new data is still a successful source check.
	pollAll(newPoller())
	after, err := s.PollStatuses(ctx, []string{"BMKG", "BMKG_TSUNAMI", "PVMBG"})
	require.NoError(t, err)
	for endpoint, state := range after {
		require.Equal(t, "fresh", state.State)
		require.True(t, state.LastSuccess.After(*statuses[endpoint].LastSuccess))
	}
}

func TestPollingOutageAndDatabaseRollbackKeepSuccessfulBoundary(t *testing.T) {
	s, source, newPoller, id := pollingFixture(t)
	ctx := context.Background()
	pollAll(newPoller())
	before, err := s.PollStatus(ctx, "PVMBG")
	require.NoError(t, err)
	source.mu.Lock()
	source.failed["/volcanic-reports"] = true
	source.mu.Unlock()
	newPoller().PollPVMBGOnce(ctx)
	during, err := s.PollStatus(ctx, "PVMBG")
	require.NoError(t, err)
	require.Equal(t, before.LastSuccess, during.LastSuccess)
	require.Equal(t, "stale", during.State)
	source.mu.Lock()
	source.failed["/volcanic-reports"] = false
	row := map[string]any{}
	for k, v := range source.rows["/volcanic-reports"][0] {
		row[k] = v
	}
	row["report_id"] = "v-" + id + "-retry"
	row["reported_at"] = time.Now().UTC()
	source.rows["/volcanic-reports"] = append(source.rows["/volcanic-reports"], row)
	source.mu.Unlock()
	// Fail inside the real data transaction, after the observation insert.
	_, err = s.Pool.Exec(ctx, `ALTER TABLE hazard_events RENAME TO hazard_events_polling_test`)
	require.NoError(t, err)
	restore := func() {
		_, err := s.Pool.Exec(ctx, `ALTER TABLE IF EXISTS hazard_events_polling_test RENAME TO hazard_events`)
		require.NoError(t, err)
	}
	t.Cleanup(restore)
	newPoller().PollPVMBGOnce(ctx)
	failed, err := s.PollStatus(ctx, "PVMBG")
	require.NoError(t, err)
	require.Equal(t, before.LastSuccess, failed.LastSuccess)
	var count int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM source_observations WHERE record_id=$1`, row["report_id"]).Scan(&count))
	require.Zero(t, count, "the observation must roll back with the failed hazard commit")
	restore()
	newPoller().PollPVMBGOnce(ctx)
	after, err := s.PollStatus(ctx, "PVMBG")
	require.NoError(t, err)
	require.Equal(t, "fresh", after.State)
	require.True(t, after.LastSuccess.After(*before.LastSuccess))
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM hazard_events WHERE source_ref_id=$1`, row["report_id"]).Scan(&count))
	require.Equal(t, 1, count, "the failed window is ingested on recovery without resetting the cursor")
}

func TestPollingNewReportPreservesHistoricRecord(t *testing.T) {
	s, source, newPoller, id := pollingFixture(t)
	ctx := context.Background()
	pollAll(newPoller())
	var hazardID string
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT hazard_id::text FROM hazard_events WHERE source_ref_id=$1`, "v-"+id).Scan(&hazardID))
	source.mu.Lock()
	row := map[string]any{}
	for k, v := range source.rows["/volcanic-reports"][0] {
		row[k] = v
	}
	row["report_id"] = "v-" + id + "-new"
	row["reported_at"] = time.Now().UTC()
	row["ash_column_height_m"] = 900
	source.rows["/volcanic-reports"] = append(source.rows["/volcanic-reports"], row)
	source.mu.Unlock()
	newPoller().PollPVMBGOnce(ctx)
	h, err := s.GetByID(ctx, hazardID)
	require.NoError(t, err)
	require.Equal(t, float64(500), h.Attributes["ash_column_height_m"])
	var height float64
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT (attributes->>'ash_column_height_m')::float FROM hazard_events WHERE source_ref_id=$1`, row["report_id"]).Scan(&height))
	require.Equal(t, float64(900), height)
}

func TestEmptyIncrementalWarningResponseDoesNotResolvePendingAssessment(t *testing.T) {
	s, _, newPoller, id := pollingFixture(t)
	ctx := context.Background()
	pollAll(newPoller())
	missing := clients.SeismicEvent{EventID: id + "-pending", Magnitude: 6.8, RegionName: "Pending assessment", OccurredAt: time.Now().UTC(), PotentialTsunami: true}
	require.NoError(t, s.CommitSeismic(ctx, []clients.SeismicEvent{missing}, "test", 0))
	newPoller().PollBMKGOnce(ctx)
	var state string
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT state FROM report_notices WHERE source_ref_id=$1`, missing.EventID).Scan(&state))
	require.Equal(t, "pending", state, "a successful empty delta is not a definitive empty warning snapshot")
	var count int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM hazard_events WHERE source_ref_id=$1`, missing.EventID).Scan(&count))
	require.Zero(t, count)
	// An explicit full resync plus the caught-up archive can establish absence.
	_, err := s.Pool.Exec(ctx, `UPDATE source_poll_state SET last_success=NULL WHERE endpoint='BMKG_TSUNAMI'`)
	require.NoError(t, err)
	p := newPoller()
	p.PollBMKGOnce(ctx)
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT state FROM report_notices WHERE source_ref_id=$1`, missing.EventID).Scan(&state))
	require.Equal(t, "resolved", state)
}

func TestRecordAttemptUsesSuccessfulRequestStartAndDoesNotRegress(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	endpoint := "poll-time-" + uuid.NewString()
	start := time.Now().UTC().Add(-3 * time.Second).Truncate(time.Microsecond)
	require.NoError(t, s.RecordAttempt(ctx, endpoint, "test", true, time.Second, time.Minute, start))
	state, err := s.PollStatus(ctx, endpoint)
	require.NoError(t, err)
	require.Equal(t, start, state.LastSuccess.UTC(), "completion time would skip data created while the response was processed")
	require.NoError(t, s.RecordAttempt(ctx, endpoint, "test", false, time.Second, time.Minute, start.Add(time.Second)))
	require.NoError(t, s.RecordAttempt(ctx, endpoint, "test", true, time.Second, time.Minute, start.Add(-time.Second)))
	state, err = s.PollStatus(ctx, endpoint)
	require.NoError(t, err)
	require.Equal(t, "stale", state.State, "an older concurrent attempt must not overwrite newer health")
	require.Equal(t, start, state.LastSuccess.UTC())
	require.Error(t, s.RecordAttempt(ctx, endpoint, "test", true, time.Second, time.Minute, time.Now().Add(time.Hour)))
	var count int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM source_poll_state WHERE endpoint=$1`, endpoint).Scan(&count))
	require.Equal(t, 1, count, "successful attempts replace state instead of appending polling logs")
}
