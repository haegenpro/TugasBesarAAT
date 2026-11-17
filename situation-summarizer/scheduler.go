package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type Completer interface {
	Complete(context.Context, Prompt) (Completion, error)
}

type Source interface {
	Snapshot(context.Context, time.Duration, int, time.Time) (*Snapshot, error)
}

type Scheduler struct {
	cfg     config
	source  Source
	llm     Completer
	log     *slog.Logger
	now     func() time.Time
	metrics *Metrics

	mu          sync.Mutex
	latest      *Snapshot
	summary     *Summary
	lastCheck   time.Time
	checkErr    string
	lastErr     string
	urgentSince time.Time
	lastCall    time.Time
	retryAt     time.Time
	failures    int
	day         string
	callsToday  int
	inFlight    atomic.Bool
	calls       sync.WaitGroup
}

func NewScheduler(cfg config, source Source, llm Completer, log *slog.Logger) *Scheduler {
	return &Scheduler{cfg: cfg, source: source, llm: llm, log: log, now: time.Now, metrics: NewMetrics()}
}

func (s *Scheduler) Run(ctx context.Context) {
	defer s.calls.Wait()
	t := time.NewTicker(s.cfg.CheckInterval)
	defer t.Stop()
	for {
		s.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Scheduler) Tick(ctx context.Context) {
	now := s.now()
	call, cancel := context.WithTimeout(ctx, 10*time.Second)
	snap, err := s.source.Snapshot(call, s.cfg.Window, s.cfg.MaxHighlights, now)
	cancel()
	s.mu.Lock()
	s.lastCheck = now
	if err != nil {
		s.checkErr = err.Error()
		s.mu.Unlock()
		s.metrics.Add("summarizer_source_failures_total", 1)
		s.log.Warn("situation data unavailable", "error", err)
		return
	}
	previous := s.latest
	s.latest, s.checkErr = snap, ""
	start := s.decide(now, snap, previous)
	s.mu.Unlock()
	if start && s.inFlight.CompareAndSwap(false, true) {
		s.calls.Add(1)
		go func() {
			defer s.calls.Done()
			defer s.inFlight.Store(false)
			s.summarize(ctx, snap)
		}()
	}
}

func (s *Scheduler) decide(now time.Time, snap, previous *Snapshot) bool {
	if s.summary != nil && s.summary.Snapshot.Fingerprint == snap.Fingerprint {
		s.urgentSince = time.Time{}
		return false
	}
	var basis *Snapshot
	if s.summary != nil {
		basis = s.summary.Snapshot
	}
	if snap.urgentChange(basis) && s.urgentSince.IsZero() {
		s.urgentSince = now
	}
	due := s.summary == nil || now.Sub(s.lastCall) >= s.cfg.RefreshInterval
	if !s.urgentSince.IsZero() && now.Sub(s.urgentSince) >= s.cfg.UrgentDelay {
		due = true
	}
	if !due || now.Before(s.retryAt) || (!s.lastCall.IsZero() && now.Sub(s.lastCall) < s.cfg.MinCallInterval) {
		return false
	}
	if day := now.UTC().Format("2006-01-02"); day != s.day {
		s.day, s.callsToday = day, 0
	}
	if s.callsToday >= s.cfg.MaxCallsPerDay {
		s.metrics.Add("summarizer_budget_exhausted_total", 1)
		return false
	}
	s.callsToday++
	s.lastCall = now
	s.metrics.Gauge("summarizer_budget_remaining", float64(s.cfg.MaxCallsPerDay-s.callsToday))
	return true
}

func (s *Scheduler) summarize(ctx context.Context, snap *Snapshot) {
	started := s.now()
	call, cancel := context.WithTimeout(ctx, s.cfg.LLMTimeout)
	defer cancel()
	if s.cfg.Fault == "slow" {
		select {
		case <-call.Done():
		case <-time.After(s.cfg.LLMTimeout + time.Second):
		}
	}
	var completion Completion
	err := call.Err()
	if err == nil {
		completion, err = s.llm.Complete(call, summaryPrompt(snap))
	}
	if err == nil && s.cfg.Fault == "invalid" {
		completion.Content = `{"headline":"Gempa besar di wilayah fiktif","overview":"Ringkasan uji yang sengaja salah.","highlights":[{"ref":"E999","summary":"Kejadian ini tidak ada di data."}],"source_notes":[]}`
	}
	var narrative Narrative
	if err == nil {
		narrative, err = validate(completion.Content, snap, s.cfg.MaxHighlights)
	}
	s.metrics.Gauge("summarizer_llm_last_seconds", s.now().Sub(started).Seconds())
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.failures++
		backoff := min(time.Duration(1<<min(s.failures, 6))*time.Minute, time.Hour)
		var limited *RateLimited
		if errors.As(err, &limited) {
			backoff = max(backoff, limited.RetryAfter)
		}
		s.retryAt = s.now().Add(backoff)
		s.lastErr = err.Error()
		outcome := "error"
		var bad *Invalid
		switch {
		case errors.As(err, &bad):
			outcome = "rejected"
		case errors.Is(err, context.DeadlineExceeded):
			outcome = "timeout"
		case limited != nil:
			outcome = "rate_limited"
		}
		s.metrics.Add("summarizer_llm_calls_"+outcome+"_total", 1)
		s.log.Warn("situation summary not updated", "outcome", outcome, "error", err, "retry_at", s.retryAt)
		return
	}
	s.failures, s.lastErr, s.retryAt, s.urgentSince = 0, "", time.Time{}, time.Time{}
	s.summary = &Summary{Narrative: narrative, Model: completion.Model, GeneratedAt: s.now().UTC(), Snapshot: snap}
	s.metrics.Add("summarizer_llm_calls_ok_total", 1)
	s.log.Info("situation summary updated", "model", completion.Model, "events", snap.Counts.Total)
}
