package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestContentCorrectionAndPartialCoverageInvalidateSummary(t *testing.T) {
	decode := func(extra string) Event {
		var event Event
		if err := json.Unmarshal([]byte(`{"hazard_id":"one","source":"SATELIT","hazard_type":"WILDFIRE","severity":"SIAGA","area_name":"Aceh","occurred_at":"2026-11-20T00:00:00Z",`+extra+`}`), &event); err != nil {
			t.Fatal(err)
		}
		return event
	}
	a := decode(`"attributes":{"total_frp_mw":40},"latitude":1.5`)
	b := decode(`"attributes":{"total_frp_mw":45},"latitude":1.6`)
	left := build([]Event{a}, nil, base, base, false, 8)
	right := build([]Event{b}, nil, base, base, false, 8)
	if left.Fingerprint == right.Fingerprint {
		t.Fatal("content-only correction was hidden by cache")
	}
	partial := build([]Event{a}, nil, base, base, true, 8)
	if partial.Fingerprint == left.Fingerprint {
		t.Fatal("partial data retained a complete-data fingerprint")
	}
	public, _ := json.Marshal(b)
	for _, field := range []string{"attributes", "latitude", "contentHash", "source_ref_id"} {
		if strings.Contains(string(public), field) {
			t.Fatalf("raw field leaked into summary reference: %s", field)
		}
	}
}

func TestChatRejectsTrailingJSON(t *testing.T) {
	c := NewChat(config{}, func() *Snapshot { return nil }, nil, nil, NewMetrics())
	w := httptest.NewRecorder()
	c.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/chat", strings.NewReader(`{"question":"hello"}{"question":"second"}`)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("trailing JSON accepted: %d", w.Code)
	}
}
