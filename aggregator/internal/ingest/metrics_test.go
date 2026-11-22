package ingest

import (
	"aggregator/internal/platform"
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func scrape() string {
	w := httptest.NewRecorder()
	platform.Metrics(w, httptest.NewRequest("GET", "/metrics", nil))
	return w.Body.String()
}

func TestM2AppendReportsCommitLockBatchAndBacklogMetrics(t *testing.T) {
	s := testStore(t)
	require.NoError(t, s.Leases.Run(context.Background(), "acquire:SATELIT:metrics", func(c context.Context) error {
		return s.Append(c, Input{Source: "SATELIT", Namespace: "metrics", Position: 1, Payload: []byte(`{"x":1}`), Metadata: map[string]any{}, CorrelationID: "c"}, 1, "raw.satellite")
	}))
	s.sampleMetrics(context.Background())
	out := scrape()
	for _, name := range []string{
		"app_ingest_commit_seconds_append_count",
		"app_ingest_lock_wait_seconds_source_progress_count",
		"app_ingest_lock_wait_seconds_lease_guard_count",
		"app_ingest_pool_acquire_seconds_count",
		"app_ingest_stage_seconds_acquire_append_count",
		"app_ingest_batches_total_append ",
		"app_ingest_batch_bytes_total_append ",
		"app_ingest_backlog_raw_outbox ",
		"app_ingest_backlog_oldest_age_seconds_raw_outbox ",
		"app_ingest_backlog_unprepared ",
		"app_ingest_pool_max_conns ",
		"app_ingest_pg_sessions_waiting_syncrep ",
	} {
		require.Truef(t, strings.Contains(out, name), "metric %q missing from /metrics", name)
	}
	require.Contains(t, out, "app_ingest_backlog_raw_outbox 1\n")
}
