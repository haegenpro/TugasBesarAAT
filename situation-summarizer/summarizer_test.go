package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var base = time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)

func events(severities ...string) []Event {
	out := []Event{}
	for i, s := range severities {
		out = append(out, Event{HazardID: "hz-" + string(rune('a'+i)), Source: "BMKG", HazardType: "SEISMIC", Severity: s, AreaName: []string{"Sumatera Barat", "Aceh", "Sumatera Barat"}[i%3], OccurredAt: base.Add(time.Duration(i) * time.Minute).Format(time.RFC3339)})
	}
	return out
}

func TestSnapshotCountsRanksAndFingerprints(t *testing.T) {
	list := events("NORMAL", "AWAS", "SIAGA", "NORMAL")
	list = append(list, list[1])
	s := build(list, map[string]SourceState{"BMKG": {State: "fresh"}}, base.Add(-time.Hour), base, false, 8)
	if s.Counts.Total != 4 || s.Counts.BySeverity["AWAS"] != 1 || s.Counts.TopAreas[0].AreaName != "Sumatera Barat" || s.Counts.TopAreas[0].Count != 3 {
		t.Fatalf("unexpected counts %+v", s.Counts)
	}
	if s.Notable[0].Severity != "AWAS" || s.Notable[1].Severity != "SIAGA" || len(s.Critical) != 1 {
		t.Fatalf("unexpected ranking %+v", s.Notable)
	}
	reversed := []Event{list[3], list[2], list[1], list[0]}
	if build(reversed, s.Sources, base, base, false, 8).Fingerprint != s.Fingerprint {
		t.Fatal("fingerprint depends on event order")
	}
	changed := events("NORMAL", "AWAS", "SIAGA", "AWAS")
	if build(changed, s.Sources, base, base, false, 8).Fingerprint == s.Fingerprint {
		t.Fatal("severity change did not change the fingerprint")
	}
	if build(list, map[string]SourceState{"BMKG": {State: "stale"}}, base, base, false, 8).Fingerprint == s.Fingerprint {
		t.Fatal("source state change did not change the fingerprint")
	}
}

const goodSummary = `{"headline":"Gempa AWAS di Aceh","overview":"Satu gempa AWAS dan satu SIAGA tercatat dalam dua jam terakhir.","highlights":[{"ref":"E1","summary":"Gempa AWAS di Aceh."}],"source_notes":[]}`

func TestValidateRejectsWhatTheDataDoesNotSupport(t *testing.T) {
	s := build(events("NORMAL", "AWAS"), nil, base, base, false, 8)
	n, err := validate("```json\n"+goodSummary+"\n```", s, 8)
	if err != nil || n.Highlights[0].HazardID != "hz-b" {
		t.Fatalf("valid summary rejected or not mapped back: %+v %v", n.Highlights, err)
	}
	if prompt := string(s.promptJSON()); strings.Contains(prompt, "hz-") || !strings.Contains(prompt, `"ref":"E1"`) {
		t.Fatalf("prompt should expose short refs instead of hazard ids: %s", prompt)
	}
	cases := map[string]string{
		"unknown id":        strings.Replace(goodSummary, `"E1"`, `"E99"`, 1),
		"unknown field":     strings.Replace(goodSummary, `"source_notes"`, `"x":1,"source_notes"`, 1),
		"empty headline":    strings.Replace(goodSummary, "Gempa AWAS di Aceh\"", "\"", 1),
		"not json":          "Situasi aman terkendali.",
		"extra field":       strings.Replace(goodSummary, `"source_notes":[]`, `"source_notes":[],"casualties":120`, 1),
		"fake beyond limit": `{"headline":"h","overview":"o","highlights":[{"ref":"E2","summary":"s"},{"ref":"E99","summary":"s"}],"source_notes":[]}`,
	}
	for name, content := range cases {
		limit := 8
		if name == "fake beyond limit" {
			limit = 1
		}
		var bad *Invalid
		if _, err := validate(content, s, limit); !errors.As(err, &bad) {
			t.Errorf("%s: expected rejection, got %v", name, err)
		}
	}
	if _, err := validate(`{"headline":"h","overview":"o","highlights":[{"ref":"E2","summary":"s"},{"ref":"E2","summary":"s"}],"source_notes":[]}`, s, 8); err == nil {
		t.Error("duplicate highlight accepted")
	}
	n, err = validate(`{"headline":"h","overview":"o","highlights":[{"ref":"E1","summary":"s"},{"ref":"E2","summary":"s"}],"source_notes":[]}`, s, 1)
	if err != nil || len(n.Highlights) != 1 || n.Highlights[0].HazardID != "hz-b" {
		t.Errorf("extra genuine highlights should be trimmed, got %+v %v", n.Highlights, err)
	}
}

func TestOpenRouterRequestAndErrors(t *testing.T) {
	var got map[string]any
	var status atomic.Int32
	status.Store(200)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" || r.Header.Get("Authorization") != "Bearer key" {
			w.WriteHeader(401)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		switch status.Load() {
		case 429:
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"error":{"code":429,"message":"rate limited"}}`))
		case 502:
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"error":{"code":502,"message":"provider down"}}`))
		case 204:
			_, _ = w.Write([]byte(`{"model":"thinker:free","choices":[{"message":{"content":""},"finish_reason":"length"}]}`))
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{"model": "baidu/ernie-5.5-flash-preview:free", "choices": []any{map[string]any{"message": map[string]string{"content": goodSummary}}}})
		}
	}))
	defer server.Close()
	o := &OpenRouter{BaseURL: server.URL, APIKey: "key", Model: "primary:free", Fallbacks: []string{"backup:free"}, MaxTokens: 16000, ReasoningTokens: 2048, HTTP: server.Client()}
	prompt := Prompt{Name: "situation_summary", Schema: summarySchema, Messages: []Message{{Role: "user", Content: "{}"}}}
	c, err := o.Complete(context.Background(), prompt)
	if err != nil || c.Content != goodSummary || c.Model == "" {
		t.Fatalf("completion %+v %v", c, err)
	}
	if got["model"] != "primary:free" || len(got["models"].([]any)) != 2 || got["response_format"].(map[string]any)["type"] != "json_schema" {
		t.Fatalf("unexpected request %v", got)
	}
	if reasoning := got["reasoning"].(map[string]any); reasoning["max_tokens"].(float64) >= got["max_tokens"].(float64) {
		t.Fatalf("reasoning can consume the whole answer budget: %v", got)
	}
	status.Store(429)
	var limited *RateLimited
	if _, err = o.Complete(context.Background(), prompt); !errors.As(err, &limited) || limited.RetryAfter != 2*time.Minute {
		t.Fatalf("rate limit not reported: %v", err)
	}
	status.Store(502)
	if _, err = o.Complete(context.Background(), prompt); err == nil || !strings.Contains(err.Error(), "provider down") {
		t.Fatalf("provider error not reported: %v", err)
	}
	status.Store(204)
	if _, err = o.Complete(context.Background(), prompt); err == nil || !strings.Contains(err.Error(), "thinker:free") || !strings.Contains(err.Error(), "length") {
		t.Fatalf("empty answer not explained: %v", err)
	}
}

type fakeSource struct {
	mu     sync.Mutex
	events []Event
	err    error
}

func (f *fakeSource) set(e []Event, err error) {
	f.mu.Lock()
	f.events, f.err = e, err
	f.mu.Unlock()
}

func (f *fakeSource) Snapshot(_ context.Context, window time.Duration, n int, now time.Time) (*Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return build(f.events, map[string]SourceState{"BMKG": {State: "fresh"}}, now.Add(-window), now, false, n), nil
}

type fakeLLM struct {
	mu      sync.Mutex
	last    Prompt
	calls   atomic.Int32
	content string
	err     error
	block   chan struct{}
}

func (f *fakeLLM) Complete(ctx context.Context, p Prompt) (Completion, error) {
	f.calls.Add(1)
	f.mu.Lock()
	f.last = p
	f.mu.Unlock()
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return Completion{}, ctx.Err()
		}
	}
	return Completion{Content: f.content, Model: "fake"}, f.err
}

func scheduler(source Source, llm Completer, mutate func(*config)) (*Scheduler, *time.Time) {
	cfg := config{Window: 2 * time.Hour, CheckInterval: time.Minute, RefreshInterval: 30 * time.Minute, UrgentDelay: 30 * time.Second, MinCallInterval: 2 * time.Minute, LLMTimeout: time.Second, MaxCallsPerDay: 45, MaxHighlights: 8, Fault: "off"}
	if mutate != nil {
		mutate(&cfg)
	}
	clock := base
	s := NewScheduler(cfg, source, llm, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.now = func() time.Time { return clock }
	return s, &clock
}

func tick(s *Scheduler) {
	s.Tick(context.Background())
	s.calls.Wait()
}

func TestSchedulerCallsOnlyWhenDataChangesAndRespectsUrgency(t *testing.T) {
	source := &fakeSource{events: events("NORMAL", "AWAS")}
	llm := &fakeLLM{content: goodSummary}
	s, clock := scheduler(source, llm, nil)
	tick(s)
	if llm.calls.Load() != 1 || s.View().Status != "fresh" {
		t.Fatalf("first tick: calls=%d status=%s", llm.calls.Load(), s.View().Status)
	}
	*clock = clock.Add(5 * time.Minute)
	tick(s)
	if llm.calls.Load() != 1 {
		t.Fatal("unchanged data triggered an LLM call")
	}
	source.set(append(events("NORMAL", "AWAS"), Event{HazardID: "hz-new", HazardType: "SEISMIC", Severity: "NORMAL", OccurredAt: base.Format(time.RFC3339)}), nil)
	*clock = clock.Add(5 * time.Minute)
	tick(s)
	if llm.calls.Load() != 1 || s.View().Status != "stale" {
		t.Fatalf("minor change before refresh interval: calls=%d status=%s", llm.calls.Load(), s.View().Status)
	}
	source.set(append(events("NORMAL", "AWAS"), Event{HazardID: "hz-new", HazardType: "VOLCANIC", Severity: "SIAGA", OccurredAt: base.Format(time.RFC3339)}), nil)
	*clock = clock.Add(time.Minute)
	tick(s)
	if llm.calls.Load() != 1 {
		t.Fatal("urgent change did not wait for the debounce delay")
	}
	*clock = clock.Add(31 * time.Second)
	tick(s)
	if llm.calls.Load() != 2 || s.View().Status != "fresh" {
		t.Fatalf("urgent change after debounce: calls=%d status=%s", llm.calls.Load(), s.View().Status)
	}
}

func TestWrongAnswerIsRejectedAndPreviousSummaryKept(t *testing.T) {
	source := &fakeSource{events: events("NORMAL", "AWAS")}
	llm := &fakeLLM{content: goodSummary}
	s, clock := scheduler(source, llm, nil)
	tick(s)
	first := s.View()
	llm.content = strings.Replace(goodSummary, `"E1"`, `"E77"`, 1)
	source.set(events("NORMAL", "AWAS", "SIAGA"), nil)
	*clock = clock.Add(time.Hour)
	tick(s)
	view := s.View()
	if view.Status != "stale" || view.AI.Headline != first.AI.Headline || view.Facts.Counts.Total != 3 || len(view.Reasons) == 0 {
		t.Fatalf("rejected answer leaked or facts not refreshed: %+v", view)
	}
	if s.metrics.values["summarizer_llm_calls_rejected_total"] != 1 {
		t.Fatalf("rejection not counted: %v", s.metrics.values)
	}
}

func TestFaultInjectionModes(t *testing.T) {
	source := &fakeSource{events: events("AWAS")}
	invalidLLM := &fakeLLM{content: goodSummary}
	s, _ := scheduler(source, invalidLLM, func(c *config) { c.Fault = "invalid" })
	tick(s)
	if s.View().Status != "unavailable" || s.metrics.values["summarizer_llm_calls_rejected_total"] != 1 {
		t.Fatalf("invalid fault mode was not rejected: %+v", s.View())
	}
	slowLLM := &fakeLLM{content: goodSummary}
	s, _ = scheduler(source, slowLLM, func(c *config) { c.Fault = "slow"; c.LLMTimeout = 50 * time.Millisecond })
	tick(s)
	if slowLLM.calls.Load() != 0 || s.metrics.values["summarizer_llm_calls_timeout_total"] != 1 || s.View().Facts == nil {
		t.Fatalf("slow fault mode: calls=%d metrics=%v", slowLLM.calls.Load(), s.metrics.values)
	}
}

func TestServingNeverWaitsForTheLLM(t *testing.T) {
	llm := &fakeLLM{content: goodSummary, block: make(chan struct{})}
	s, _ := scheduler(&fakeSource{events: events("NORMAL", "AWAS")}, llm, func(c *config) { c.LLMTimeout = 10 * time.Second })
	server := httptest.NewServer(s.Handler(http.NotFoundHandler()))
	defer server.Close()
	s.Tick(context.Background())
	started := time.Now()
	resp, err := http.Get(server.URL + "/summary")
	if err != nil {
		t.Fatal(err)
	}
	var body Response
	_ = json.NewDecoder(resp.Body).Decode(&body)
	resp.Body.Close()
	if time.Since(started) > 500*time.Millisecond || body.Status != "unavailable" || body.Facts == nil || len(body.Facts.CriticalEvents) != 1 || body.Disclaimer == "" {
		t.Fatalf("summary waited for or depended on the LLM: %+v", body)
	}
	close(llm.block)
	s.calls.Wait()
	if s.View().Status != "fresh" {
		t.Fatal("summary not published after the LLM returned")
	}
}

func TestOutagesBackOffAndBudgetIsEnforced(t *testing.T) {
	source := &fakeSource{events: events("AWAS")}
	llm := &fakeLLM{err: errors.New("connection refused")}
	s, clock := scheduler(source, llm, func(c *config) { c.MaxCallsPerDay = 2 })
	tick(s)
	*clock = clock.Add(time.Minute)
	tick(s)
	if llm.calls.Load() != 1 {
		t.Fatal("failed call retried before its backoff")
	}
	*clock = clock.Add(3 * time.Minute)
	tick(s)
	*clock = clock.Add(time.Hour)
	tick(s)
	if llm.calls.Load() != 2 || s.metrics.values["summarizer_budget_exhausted_total"] == 0 {
		t.Fatalf("daily budget not enforced: calls=%d metrics=%v", llm.calls.Load(), s.metrics.values)
	}
	source.set(nil, errors.New("aggregator down"))
	tick(s)
	view := s.View()
	if view.Status != "unavailable" || view.Facts == nil {
		t.Fatalf("aggregator outage hid the last facts: %+v", view)
	}
}
