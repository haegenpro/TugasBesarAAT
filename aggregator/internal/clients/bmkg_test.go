package clients

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBMKGClient_RenamedRequiredFieldFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"event_code":"evt-1","magnitude":3,"depth_km":10,"epicenter_lat":-6,"epicenter_lon":106,"region_name":"Test Area","occurred_at":"2026-09-01T00:00:00Z","potential_tsunami":false}]`)
	}))
	defer server.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := NewBMKGClient(server.URL, "key", logger)
	_, err := client.FetchSeismicEvents(context.Background(), "cid", time.Time{})
	require.ErrorContains(t, err, `"event_id"`)
}

func TestBMKGClient_PreservesUnmappedField(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"event_id":"evt-1","magnitude":3,"depth_km":10,"epicenter_lat":-6,"epicenter_lon":106,"region_name":"Test Area","occurred_at":"2026-09-01T00:00:00Z","potential_tsunami":false,"sensor_quality":"high"}]`)
	}))
	defer server.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := NewBMKGClient(server.URL, "key", logger)
	events, err := client.FetchSeismicEvents(context.Background(), "cid", time.Time{})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "high", events[0].Attributes["sensor_quality"])
}
