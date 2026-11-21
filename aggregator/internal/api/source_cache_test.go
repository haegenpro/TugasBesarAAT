package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStoredFreshnessSeparatesWorkingEndpointFromOldObservations(t *testing.T) {
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer owner", r.Header.Get("Authorization"))
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{
			{"endpoint": "coastal:observations", "state": "fresh", "last_success": now, "last_new": now.Add(-20 * time.Second)},
			{"endpoint": "coastal:stations", "state": "fresh", "last_success": now},
		}})
	}))
	defer server.Close()
	cache := &SourceCache{}
	require.NoError(t, cache.Refresh(context.Background(), server.Client(), server.URL, "owner"))
	states := cache.Statuses(now)
	require.Equal(t, "stale", states["SENSOR_PANTAI"].State)
	require.Equal(t, "fresh", states["SENSOR_PANTAI_STATIONS"].State)
	server.Close()
	require.Error(t, cache.Refresh(context.Background(), server.Client(), server.URL, "owner"))
	// Reads use retained health immediately, then visibly expire the cache.
	require.Equal(t, "stale", cache.Statuses(now)["SENSOR_PANTAI"].State)
	require.Equal(t, "unavailable", cache.Statuses(now.Add(time.Minute))["SENSOR_PANTAI"].State)
}
