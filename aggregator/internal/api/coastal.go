package api

import (
	"aggregator/internal/timeutil"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type CoastalStatus struct {
	URL      string
	Token    string
	Client   *http.Client
	Interval time.Duration
	MaxAge   time.Duration
	current  atomic.Pointer[observedStatus]
}

type observedStatus struct {
	status sourceStatus
	at     time.Time
}

func (s *CoastalStatus) Get() sourceStatus {
	v := s.current.Load()
	if v == nil || time.Since(v.at) > s.MaxAge {
		return sourceStatus{State: "unavailable"}
	}
	return v.status
}

func (s *CoastalStatus) Run(ctx context.Context) {
	t := time.NewTicker(s.Interval)
	defer t.Stop()
	for {
		s.refresh(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *CoastalStatus) refresh(ctx context.Context) {
	call, cancel := context.WithTimeout(ctx, s.Interval)
	defer cancel()
	req, err := http.NewRequestWithContext(call, http.MethodGet, strings.TrimRight(s.URL, "/")+"/internal/coastal/status", nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+s.Token)
	resp, err := s.Client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var body struct {
		State       string     `json:"state"`
		LastSuccess *time.Time `json:"last_success"`
		LastAttempt *time.Time `json:"last_attempt"`
		StaleSince  *time.Time `json:"stale_since"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(resp.Body, 16<<10)).Decode(&body) != nil {
		return
	}
	status := sourceStatus{State: body.State, LastSuccess: body.LastSuccess, LastAttempt: body.LastAttempt}
	switch body.State {
	case "fresh":
	case "stale", "unavailable":
		since := time.Now().UTC()
		if body.StaleSince != nil {
			since = body.StaleSince.UTC()
		}
		status.StaleSince = timeutil.FormatISOMilli(since)
	default:
		return
	}
	s.current.Store(&observedStatus{status: status, at: time.Now()})
}
