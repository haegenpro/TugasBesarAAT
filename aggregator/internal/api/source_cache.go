package api

import (
	"aggregator/internal/platform"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Source health is refreshed outside read requests. An upstream outage cannot
// turn a stored hazard read into a live sensor request or a long timeout.
type SourceCache struct {
	mu      sync.RWMutex
	checked time.Time
	states  map[string]sourceStatus
}

func (s *SourceCache) Statuses(now time.Time) map[string]sourceStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := map[string]sourceStatus{"SENSOR_PANTAI": {State: "unavailable"}, "SENSOR_PANTAI_STATIONS": {State: "unavailable"}}
	for key, value := range s.states {
		if now.Sub(s.checked) > 10*time.Second {
			value.State = "unavailable"
		}
		out[key] = value
	}
	return out
}
func (s *SourceCache) Refresh(ctx context.Context, client *http.Client, base, token string) error {
	req, e := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(base, "/")+"/internal/ingestion/health", nil)
	if e != nil {
		return e
	}
	req.Header.Set("Authorization", "Bearer "+token)
	platform.Inject(ctx, req.Header)
	response, e := client.Do(req)
	if e != nil {
		return e
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return errors.New("stored source health unavailable")
	}
	raw, e := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if e != nil || len(raw) > 64<<10 {
		return errors.New("invalid health response")
	}
	var page struct {
		Items []struct {
			Endpoint  string     `json:"endpoint"`
			State     string     `json:"state"`
			Success   *time.Time `json:"last_success"`
			Attempt   *time.Time `json:"last_attempt"`
			New       *time.Time `json:"last_new"`
			Observed  *time.Time `json:"latest_observed"`
			Generated *time.Time `json:"source_generated"`
		} `json:"items"`
	}
	if json.Unmarshal(raw, &page) != nil || page.Items == nil {
		return errors.New("invalid health response")
	}
	now := time.Now().UTC()
	states := map[string]sourceStatus{}
	for _, item := range page.Items {
		key := "SENSOR_PANTAI"
		ttl := 15 * time.Second
		if item.Endpoint == "coastal:stations" {
			key = "SENSOR_PANTAI_STATIONS"
			ttl = 60 * time.Second
		} else if item.Endpoint != "coastal:observations" {
			continue
		}
		state := item.State
		if item.Success == nil || now.Sub(*item.Success) > ttl {
			state = "unavailable"
		}
		if key == "SENSOR_PANTAI" && state == "fresh" && (item.New == nil || now.Sub(*item.New) > 10*time.Second) {
			state = "stale"
		}
		value := sourceStatus{State: state, LastSuccess: item.Success, LastAttempt: item.Attempt, LastNew: item.New, LatestObserved: item.Observed, SourceGenerated: item.Generated}
		if state == "stale" && item.New != nil {
			value.StaleSince = item.New.UTC().Format(time.RFC3339Nano)
		}
		states[key] = value
	}
	s.mu.Lock()
	s.checked = now
	s.states = states
	s.mu.Unlock()
	return nil
}
func (s *SourceCache) Run(ctx context.Context, base, token string) {
	client := platform.Client(750 * time.Millisecond)
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	defer client.CloseIdleConnections()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		_ = s.Refresh(ctx, client, base, token)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
