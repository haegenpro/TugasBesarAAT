package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

type Metrics struct {
	mu     sync.Mutex
	values map[string]float64
}

func NewMetrics() *Metrics { return &Metrics{values: map[string]float64{}} }

func (m *Metrics) Add(name string, v float64) {
	m.mu.Lock()
	m.values[name] += v
	m.mu.Unlock()
}

func (m *Metrics) Gauge(name string, v float64) {
	m.mu.Lock()
	m.values[name] = v
	m.mu.Unlock()
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	names := make([]string, 0, len(m.values))
	for name := range m.values {
		names = append(names, name)
	}
	sort.Strings(names)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, name := range names {
		fmt.Fprintf(w, "%s %g\n", name, m.values[name])
	}
	m.mu.Unlock()
}

type Facts struct {
	Window         string                 `json:"window"`
	Counts         Counts                 `json:"counts"`
	CriticalEvents []Event                `json:"critical_events"`
	SourceStatus   map[string]SourceState `json:"source_status"`
	Partial        bool                   `json:"partial"`
}

type Response struct {
	Status      string     `json:"status"`
	Reasons     []string   `json:"reasons,omitempty"`
	GeneratedAt *time.Time `json:"generated_at"`
	DataAsOf    *time.Time `json:"data_as_of"`
	CheckedAt   *time.Time `json:"checked_at"`
	Model       string     `json:"model,omitempty"`
	AI          *Narrative `json:"ai_summary"`
	Facts       *Facts     `json:"facts"`
	Disclaimer  string     `json:"disclaimer"`
}

func factsOf(s *Snapshot) *Facts {
	if s == nil {
		return nil
	}
	critical := s.Critical
	if critical == nil {
		critical = []Event{}
	}
	return &Facts{Window: s.WindowStart.Format(time.RFC3339) + "/" + s.AsOf.Format(time.RFC3339), Counts: s.Counts, CriticalEvents: critical, SourceStatus: s.Sources, Partial: s.Partial}
}

func stamp(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	t = t.UTC()
	return &t
}

func (s *Scheduler) View() Response {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := Response{Status: "unavailable", Disclaimer: disclaimer, CheckedAt: stamp(s.lastCheck), Facts: factsOf(s.latest)}
	reasons := []string{}
	if s.checkErr != "" {
		reasons = append(reasons, "data BNPB belum dapat diperbarui")
	}
	if s.lastErr != "" {
		reasons = append(reasons, "ringkasan AI terakhir gagal diperbarui")
	}
	if s.summary != nil {
		n := s.summary.Narrative
		r.AI, r.Model, r.GeneratedAt, r.DataAsOf = &n, s.summary.Model, stamp(s.summary.GeneratedAt), stamp(s.summary.Snapshot.AsOf)
		r.Status = "fresh"
		if s.latest == nil || s.latest.Fingerprint != s.summary.Snapshot.Fingerprint || s.checkErr != "" {
			r.Status = "stale"
			if s.latest != nil && s.latest.Fingerprint != s.summary.Snapshot.Fingerprint {
				reasons = append(reasons, "data sudah berubah sejak ringkasan dibuat")
			}
		}
	} else {
		reasons = append(reasons, "ringkasan AI belum tersedia")
	}
	if r.Status != "fresh" {
		r.Reasons = reasons
	}
	value := map[string]float64{"fresh": 0, "stale": 1, "unavailable": 2}[r.Status]
	s.metrics.Gauge("summarizer_status", value)
	if s.summary != nil {
		s.metrics.Gauge("summarizer_summary_age_seconds", s.now().Sub(s.summary.GeneratedAt).Seconds())
	}
	return r
}

func (s *Scheduler) Latest() *Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest
}

func (s *Scheduler) Handler(chat http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "situation-summarizer"})
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		ready := s.latest != nil
		s.mu.Unlock()
		code := http.StatusOK
		if !ready {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, map[string]bool{"ready": ready})
	})
	mux.HandleFunc("GET /summary", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, s.View())
	})
	mux.Handle("POST /chat", chat)
	mux.Handle("GET /metrics", s.metrics)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
