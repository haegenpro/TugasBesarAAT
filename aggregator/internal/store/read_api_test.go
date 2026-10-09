package store_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"aggregator/internal/api"
	"aggregator/internal/domain"
	"aggregator/internal/middleware"
	"aggregator/internal/store"
	"aggregator/internal/timeutil"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type readLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *readLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *readLogBuffer) selects(t *testing.T, cid string) int {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	count := 0
	for _, line := range strings.Split(strings.TrimSpace(b.buffer.String()), "\n") {
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		if entry["correlation_id"] == cid && entry["target_endpoint"] == "query:select" {
			count++
		}
	}
	return count
}

func TestReadAPICombinedQueries(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping read API integration tests")
	}
	var logs readLogBuffer
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := store.New(ctx, dsn, slog.New(slog.NewJSONHandler(&logs, nil)))
	require.NoError(t, err)
	t.Cleanup(s.Close)
	require.NoError(t, s.CheckSchema(ctx))
	_, err = s.Pool.Exec(ctx, `DELETE FROM source_poll_state WHERE endpoint=ANY($1::text[])`, []string{"BMKG", "BMKG_TSUNAMI", "PVMBG"})
	require.NoError(t, err)

	deps := &api.Dependencies{}
	deps.SetStoreReady(true)
	a := api.New(s, nil, deps, time.Minute, 30*time.Minute)
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(middleware.CorrelationID())
	router.GET("/hazards", a.ListHazards)
	router.GET("/hazards/:id", a.GetHazard)
	router.GET("/notices", a.ListNotices)

	request := func(t *testing.T, path string, status, queries int, output any) {
		t.Helper()
		cid := uuid.NewString()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set(middleware.CorrelationIDHeader, cid)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		require.Equal(t, status, response.Code, response.Body.String())
		if output != nil {
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), output))
		}
		require.Equal(t, queries, logs.selects(t, cid), path)
	}
	type sourceState struct {
		State      string `json:"state"`
		StaleSince string `json:"stale_since"`
	}
	type listResponse struct {
		Data         []domain.HazardEvent             `json:"data"`
		Count        int                              `json:"count"`
		AsOf         string                           `json:"as_of"`
		Metadata     map[string]domain.RecordMetadata `json:"metadata"`
		SourceStatus map[string]sourceState           `json:"source_status"`
	}
	area := "combined-read-" + uuid.NewString()
	t.Run("empty results retain missing source defaults", func(t *testing.T) {
		var response listResponse
		request(t, "/hazards?area_name="+area, http.StatusOK, 2, &response)
		require.NotNil(t, response.Data)
		require.Empty(t, response.Data)
		require.Zero(t, response.Count)
		require.NotNil(t, response.Metadata)
		require.Empty(t, response.Metadata)
		require.Len(t, response.SourceStatus, 3)
		for _, state := range response.SourceStatus {
			require.Equal(t, "unavailable", state.State)
		}
	})

	now := time.Now().UTC().Truncate(time.Millisecond)
	older := sampleEvent(domain.SourceBMKG, area+"-older")
	older.AreaName = area
	older.OccurredAt = timeutil.New(now.Add(-2 * time.Hour))
	newer := sampleEvent(domain.SourceBMKG, area+"-newer")
	newer.AreaName = area
	newer.OccurredAt = timeutil.New(now.Add(-5 * time.Second))
	newer.Attributes["potential_tsunami"] = true
	volcanic := sampleEvent(domain.SourcePVMBG, area+"-volcanic")
	volcanic.AreaName = area
	volcanic.HazardType = domain.HazardVolcanic
	volcanic.OccurredAt = timeutil.New(now.Add(-10 * time.Second))
	for _, event := range []domain.HazardEvent{older, newer, volcanic} {
		_, err = s.SaveDedup(ctx, event, "read-setup")
		require.NoError(t, err)
	}
	newer.Severity = domain.SeverityAwas
	_, err = s.SaveDedup(ctx, newer, "read-correction")
	require.NoError(t, err)
	_, err = s.Pool.Exec(ctx, `UPDATE hazard_events SET last_seen_at=NULL WHERE hazard_id=$1`, older.HazardID)
	require.NoError(t, err)
	for _, endpoint := range []string{"BMKG", "BMKG_TSUNAMI", "PVMBG"} {
		require.NoError(t, s.RecordAttempt(ctx, endpoint, "read-setup", true, time.Minute, time.Minute))
	}

	t.Run("filters ordering revision and record freshness", func(t *testing.T) {
		var response listResponse
		request(t, "/hazards?hazard_type=SEISMIC&area_name="+strings.ToUpper(area), http.StatusOK, 2, &response)
		require.Equal(t, 2, response.Count)
		require.Len(t, response.Data, 2)
		require.Equal(t, newer.HazardID, response.Data[0].HazardID)
		require.Equal(t, older.HazardID, response.Data[1].HazardID)
		require.Equal(t, newer.Attributes, response.Data[0].Attributes)
		require.NotEmpty(t, response.AsOf)
		m := response.Metadata[newer.HazardID]
		require.EqualValues(t, 2, m.Revision)
		require.NotNil(t, m.LastSeenAt)
		require.True(t, m.ObservationTime.Equal(newer.OccurredAt.Time))
		require.Equal(t, "fresh", m.State)
		require.Equal(t, "complete", m.AssessmentState)
		require.False(t, m.Historical)
		m = response.Metadata[older.HazardID]
		require.Nil(t, m.LastSeenAt)
		require.Equal(t, "unavailable", m.State)
		require.True(t, m.Historical)
		response = listResponse{}
		request(t, "/hazards?area_name="+area+"&hazard_type=SEISMIC&severity=AWAS&since="+now.Add(-time.Minute).Format(time.RFC3339), http.StatusOK, 2, &response)
		require.Equal(t, 1, response.Count)
		require.Len(t, response.Metadata, 1)
		require.Equal(t, newer.HazardID, response.Data[0].HazardID)
	})
	t.Run("detail accepts uppercase UUID and retains metadata", func(t *testing.T) {
		var response struct {
			Data     domain.HazardEvent    `json:"data"`
			Metadata domain.RecordMetadata `json:"metadata"`
		}
		request(t, "/hazards/"+strings.ToUpper(newer.HazardID), http.StatusOK, 2, &response)
		require.Equal(t, newer.HazardID, response.Data.HazardID)
		require.EqualValues(t, 2, response.Metadata.Revision)
		require.True(t, response.Metadata.ObservationTime.Equal(newer.OccurredAt.Time))
		request(t, "/hazards/"+uuid.NewString(), http.StatusNotFound, 1, nil)
		request(t, "/hazards/invalid", http.StatusBadRequest, 0, nil)
	})
	t.Run("outage and expiry retain canonical evidence", func(t *testing.T) {
		require.NoError(t, s.RecordAttempt(ctx, "PVMBG", "read-outage", false, time.Minute, time.Minute))
		require.NoError(t, s.RecordAttempt(ctx, "BMKG_TSUNAMI", "read-outage", false, time.Minute, time.Minute))
		var response listResponse
		request(t, "/hazards?hazard_type=VOLCANIC&area_name="+area, http.StatusOK, 2, &response)
		require.Equal(t, volcanic.HazardID, response.Data[0].HazardID)
		require.Equal(t, volcanic.Attributes, response.Data[0].Attributes)
		require.Equal(t, "stale", response.SourceStatus["PVMBG"].State)
		require.NotEmpty(t, response.SourceStatus["PVMBG"].StaleSince)
		require.Equal(t, "stale", response.Metadata[volcanic.HazardID].State)
		response = listResponse{}
		request(t, "/hazards?hazard_type=SEISMIC&severity=AWAS&area_name="+area, http.StatusOK, 2, &response)
		require.Equal(t, "stale", response.Metadata[newer.HazardID].AssessmentState)
		_, err = s.Pool.Exec(ctx, `UPDATE source_poll_state SET last_success=$1 WHERE endpoint='PVMBG'`, now.Add(-time.Hour))
		require.NoError(t, err)
		response = listResponse{}
		request(t, "/hazards?hazard_type=VOLCANIC&area_name="+area, http.StatusOK, 2, &response)
		require.Equal(t, "unavailable", response.SourceStatus["PVMBG"].State)
		require.Equal(t, "unavailable", response.Metadata[volcanic.HazardID].State)
		require.Equal(t, volcanic.HazardID, response.Data[0].HazardID)
		_, err = s.Pool.Exec(ctx, `DELETE FROM source_poll_state WHERE endpoint='PVMBG'`)
		require.NoError(t, err)
		response = listResponse{}
		request(t, "/hazards?hazard_type=VOLCANIC&area_name="+area, http.StatusOK, 2, &response)
		require.Equal(t, "unavailable", response.SourceStatus["PVMBG"].State)
	})
	t.Run("notice lists share the batched status query", func(t *testing.T) {
		request(t, "/notices?state=all", http.StatusOK, 2, nil)
	})
	t.Run("concurrent reads have separate result maps", func(t *testing.T) {
		type result struct {
			cid    string
			status int
			count  int
			err    error
		}
		results := make(chan result, 12)
		for i := 0; i < cap(results); i++ {
			go func() {
				cid := uuid.NewString()
				req := httptest.NewRequest(http.MethodGet, "/hazards?area_name="+area, nil)
				req.Header.Set(middleware.CorrelationIDHeader, cid)
				response := httptest.NewRecorder()
				router.ServeHTTP(response, req)
				var body listResponse
				err := json.Unmarshal(response.Body.Bytes(), &body)
				results <- result{cid, response.Code, body.Count, err}
			}()
		}
		for i := 0; i < cap(results); i++ {
			select {
			case got := <-results:
				require.NoError(t, got.err)
				require.Equal(t, http.StatusOK, got.status)
				require.Equal(t, 3, got.count)
				require.Equal(t, 2, logs.selects(t, got.cid))
			case <-time.After(10 * time.Second):
				t.Fatal("concurrent read timed out")
			}
		}
	})
	t.Run("source query failure returns service unavailable", func(t *testing.T) {
		_, err = s.Pool.Exec(ctx, `ALTER TABLE source_poll_state RENAME TO source_poll_state_read_test`)
		require.NoError(t, err)
		defer func() {
			_, restoreErr := s.Pool.Exec(context.Background(), `ALTER TABLE source_poll_state_read_test RENAME TO source_poll_state`)
			require.NoError(t, restoreErr)
		}()
		request(t, fmt.Sprintf("/hazards?area_name=%s", area), http.StatusServiceUnavailable, 2, nil)
	})
}
