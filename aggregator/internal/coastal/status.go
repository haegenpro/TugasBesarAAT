package coastal

import (
	"aggregator/internal/platform"
	"sync"
	"time"
)

type Snapshot struct {
	State               string     `json:"state"`
	Reason              string     `json:"reason,omitempty"`
	LastSuccess         *time.Time `json:"last_success"`
	LastAttempt         *time.Time `json:"last_attempt"`
	LastNewObservation  *time.Time `json:"last_new_observation"`
	LatestObservedAt    *time.Time `json:"latest_observed_at"`
	SensorClock         *time.Time `json:"sensor_clock"`
	StaleSince          *time.Time `json:"stale_since"`
	ObservationsBreaker string     `json:"observations_breaker"`
	StationsBreaker     string     `json:"stations_breaker"`
}

type Status struct {
	StaleAfter   time.Duration
	Observations *Breaker
	Stations     *Breaker

	mu         sync.RWMutex
	success    time.Time
	attempt    time.Time
	fresh      time.Time
	observed   time.Time
	clock      time.Time
	staleSince time.Time
	reason     string
}

func (s *Status) Attempt(err error) {
	s.AttemptAt(time.Now().UTC(), err)
}
func (s *Status) AttemptAt(now time.Time, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempt = now
	if err == nil {
		s.success, s.reason = now, ""
		return
	}
	s.reason = err.Error()
}

func (s *Status) Page(clock time.Time, latest time.Time, added int) {
	s.PageAt(time.Now().UTC(), clock, latest, added)
}
func (s *Status) PageAt(now, clock, latest time.Time, added int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !clock.IsZero() {
		s.clock = clock
	}
	if added > 0 && latest.After(s.observed) {
		s.fresh, s.observed = now, latest
	}
	if s.fresh.IsZero() {
		s.fresh = now
	}
	if !s.observed.IsZero() {
		platform.Gauge("coastal_observation_lag_seconds", s.clock.Sub(s.observed).Seconds())
	}
}

func (s *Status) Snapshot() Snapshot {
	return s.SnapshotAt(time.Now().UTC())
}
func (s *Status) SnapshotAt(now time.Time) Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	stale := s.reason != "" && now.Sub(s.success) > s.StaleAfter
	if !s.fresh.IsZero() && now.Sub(s.fresh) > s.StaleAfter {
		stale = true
	}
	if !s.clock.IsZero() && !s.observed.IsZero() && s.clock.Sub(s.observed) > s.StaleAfter {
		stale = true
	}
	state := "fresh"
	switch {
	case s.success.IsZero():
		state = "unavailable"
	case stale:
		state = "stale"
	}
	if state == "fresh" {
		s.staleSince = time.Time{}
	} else if s.staleSince.IsZero() {
		s.staleSince = s.attempt
		if s.staleSince.IsZero() {
			s.staleSince = now
		}
		if !s.fresh.IsZero() && now.Sub(s.fresh) > s.StaleAfter {
			s.staleSince = s.fresh.Add(s.StaleAfter)
		}
		if s.reason != "" && !s.success.IsZero() && now.Sub(s.success) > s.StaleAfter && s.success.Add(s.StaleAfter).Before(s.staleSince) {
			s.staleSince = s.success.Add(s.StaleAfter)
		}
	}
	platform.Gauge("coastal_source_stale", map[string]float64{"fresh": 0, "stale": 1, "unavailable": 2}[state])
	return Snapshot{State: state, Reason: s.reason, LastSuccess: ptr(s.success), LastAttempt: ptr(s.attempt), LastNewObservation: ptr(s.fresh), LatestObservedAt: ptr(s.observed), SensorClock: ptr(s.clock), StaleSince: ptr(s.staleSince), ObservationsBreaker: s.Observations.State().String(), StationsBreaker: s.Stations.State().String()}
}

func (s *Status) Restore(v Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value := func(t *time.Time) time.Time {
		if t == nil {
			return time.Time{}
		}
		return *t
	}
	s.success, s.attempt, s.fresh = value(v.LastSuccess), value(v.LastAttempt), value(v.LastNewObservation)
	s.observed, s.clock, s.staleSince = value(v.LatestObservedAt), value(v.SensorClock), value(v.StaleSince)
	s.reason = v.Reason
}

func ptr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
