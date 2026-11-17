package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"time"
)

const maxPages = 5

type Event struct {
	HazardID   string `json:"hazard_id"`
	Source     string `json:"source"`
	HazardType string `json:"hazard_type"`
	Severity   string `json:"severity"`
	AreaName   string `json:"area_name"`
	OccurredAt string `json:"occurred_at"`
}

type SourceState struct {
	State      string `json:"state"`
	StaleSince string `json:"stale_since,omitempty"`
}

type AreaCount struct {
	AreaName string `json:"area_name"`
	Count    int    `json:"count"`
}

type Counts struct {
	Total      int            `json:"total"`
	ByType     map[string]int `json:"by_hazard_type"`
	BySeverity map[string]int `json:"by_severity"`
	TopAreas   []AreaCount    `json:"top_areas"`
}

type Snapshot struct {
	WindowStart time.Time              `json:"window_start"`
	AsOf        time.Time              `json:"as_of"`
	Counts      Counts                 `json:"counts"`
	Notable     []Event                `json:"notable_events"`
	Critical    []Event                `json:"critical_events"`
	Sources     map[string]SourceState `json:"source_status"`
	Partial     bool                   `json:"partial"`
	Fingerprint string                 `json:"-"`
	known       map[string]Event
}

func (s *Snapshot) Has(id string) bool {
	_, ok := s.known[id]
	return ok
}

var severityRank = map[string]int{"AWAS": 4, "SIAGA": 3, "WASPADA": 2, "NORMAL": 1}

func urgent(e Event) bool { return severityRank[e.Severity] >= 3 }

type Aggregator struct {
	BaseURL string
	HTTP    *http.Client
}

type hazardPage struct {
	Data         []Event                `json:"data"`
	NextCursor   *string                `json:"next_cursor"`
	Partial      bool                   `json:"partial"`
	SourceStatus map[string]SourceState `json:"source_status"`
}

func (a *Aggregator) Snapshot(ctx context.Context, window time.Duration, notableLimit int, now time.Time) (*Snapshot, error) {
	start := now.Add(-window).UTC()
	q := url.Values{"since": {start.Format(time.RFC3339)}, "limit": {"1000"}}
	var events []Event
	var sources map[string]SourceState
	partial := false
	for page := 0; page < maxPages; page++ {
		var body hazardPage
		if err := a.get(ctx, "/internal/hazards?"+q.Encode(), &body); err != nil {
			return nil, err
		}
		events = append(events, body.Data...)
		partial = partial || body.Partial
		if sources == nil {
			sources = body.SourceStatus
		}
		if body.NextCursor == nil || *body.NextCursor == "" {
			break
		}
		if page == maxPages-1 {
			partial = true
			break
		}
		q.Set("cursor", *body.NextCursor)
	}
	return build(events, sources, start, now, partial, notableLimit), nil
}

func (a *Aggregator) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.BaseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := a.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("aggregator returned %d", resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(out)
}

func build(events []Event, sources map[string]SourceState, start, now time.Time, partial bool, notableLimit int) *Snapshot {
	s := &Snapshot{WindowStart: start, AsOf: now.UTC(), Sources: sources, Partial: partial, known: make(map[string]Event, len(events))}
	if s.Sources == nil {
		s.Sources = map[string]SourceState{}
	}
	s.Counts = Counts{ByType: map[string]int{}, BySeverity: map[string]int{}}
	areas := map[string]int{}
	unique := make([]Event, 0, len(events))
	for _, e := range events {
		if e.HazardID == "" || s.Has(e.HazardID) {
			continue
		}
		s.known[e.HazardID] = e
		unique = append(unique, e)
		s.Counts.ByType[e.HazardType]++
		s.Counts.BySeverity[e.Severity]++
		if e.AreaName != "" {
			areas[e.AreaName]++
		}
	}
	s.Counts.Total = len(unique)
	for area, n := range areas {
		s.Counts.TopAreas = append(s.Counts.TopAreas, AreaCount{AreaName: area, Count: n})
	}
	sort.Slice(s.Counts.TopAreas, func(i, j int) bool {
		a, b := s.Counts.TopAreas[i], s.Counts.TopAreas[j]
		return a.Count > b.Count || a.Count == b.Count && a.AreaName < b.AreaName
	})
	s.Counts.TopAreas = s.Counts.TopAreas[:min(len(s.Counts.TopAreas), 10)]
	sort.Slice(unique, func(i, j int) bool {
		a, b := unique[i], unique[j]
		if severityRank[a.Severity] != severityRank[b.Severity] {
			return severityRank[a.Severity] > severityRank[b.Severity]
		}
		if a.OccurredAt != b.OccurredAt {
			return a.OccurredAt > b.OccurredAt
		}
		return a.HazardID < b.HazardID
	})
	s.Notable = unique[:min(len(unique), notableLimit*3)]
	for _, e := range unique {
		if e.Severity == "AWAS" && len(s.Critical) < 20 {
			s.Critical = append(s.Critical, e)
		}
	}
	s.Fingerprint = fingerprint(unique, s.Sources)
	return s
}

func fingerprint(events []Event, sources map[string]SourceState) string {
	keys := make([]string, 0, len(events)+len(sources))
	for _, e := range events {
		keys = append(keys, "e\x00"+e.HazardID+"\x00"+e.Severity+"\x00"+e.OccurredAt)
	}
	for name, state := range sources {
		keys = append(keys, "s\x00"+name+"\x00"+state.State)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		_, _ = h.Write([]byte(k + "\n"))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Snapshot) urgentChange(previous *Snapshot) bool {
	if previous == nil {
		return len(s.Critical) > 0
	}
	for id, e := range s.known {
		if old, ok := previous.known[id]; urgent(e) && (!ok || old.Severity != e.Severity) {
			return true
		}
	}
	for name, state := range s.Sources {
		if previous.Sources[name].State != state.State {
			return true
		}
	}
	return false
}

func (s *Snapshot) promptJSON() []byte {
	view := struct {
		Window  string                 `json:"window"`
		Counts  Counts                 `json:"counts"`
		Notable []Event                `json:"notable_events"`
		Sources map[string]SourceState `json:"source_status"`
		Partial bool                   `json:"data_partial"`
	}{Window: s.WindowStart.Format(time.RFC3339) + "/" + s.AsOf.Format(time.RFC3339), Counts: s.Counts, Notable: s.Notable, Sources: s.Sources, Partial: s.Partial}
	b, _ := json.Marshal(view)
	return b
}
