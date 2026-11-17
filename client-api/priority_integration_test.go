package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// Uses the real router and HTTP Auth boundary. Client headers cannot select
// the field reserve, and rejected general requests never reach Aggregator.
func TestPriorityAcrossVerifiedAuthAndSaturatedGeneralReads(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Token string `json:"access_token"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		role := "media"
		scope := "hazards:summary"
		if body.Token == "verified-field" {
			role = "field"
			scope = "hazards:raw"
		}
		_ = json.NewEncoder(w).Encode(principal{ClientID: role, Role: role, Scope: scope})
	}))
	defer auth.Close()
	agg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Correlation-ID") == "hold-media" {
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		io.WriteString(w, `{"data":[],"count":0,"as_of":"2026-11-17T00:00:00Z","source_status":{}}`)
	}))
	defer agg.Close()
	defer once.Do(func() { close(release) })
	a := &api{cfg: config{AuthURL: auth.URL, AggregatorURL: agg.URL, AuthInternalKey: "fixture-internal", MaxInFlight: 3, MaxInFlightPerClient: 3, PriorityEnabled: true, FieldReserved: 1, AuthMaxInFlight: 8}, client: &http.Client{Timeout: 3 * time.Second}, log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	router := a.router()
	request := func(token, cid string) int {
		req := httptest.NewRequest("GET", "/hazards", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Role", "field")
		req.Header.Set("X-Correlation-ID", cid)
		reply := httptest.NewRecorder()
		router.ServeHTTP(reply, req)
		return reply.Code
	}
	done := make(chan int, 2)
	for i := 0; i < 2; i++ {
		go func() { done <- request("verified-media", "hold-media") }()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("media read did not reach upstream")
		}
	}
	for i := 0; i < 20; i++ {
		if status := request("verified-media", "rejected-media"); status != 429 {
			t.Fatalf("media claimed field reserve: %d", status)
		}
		if status := request("verified-field", "field-progress"); status != 200 {
			t.Fatalf("verified field request did not progress: %d", status)
		}
	}
	once.Do(func() { close(release) })
	for i := 0; i < 2; i++ {
		select {
		case status := <-done:
			if status != 200 {
				t.Fatalf("admitted request failed: %d", status)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("admitted request did not finish")
		}
	}
	if status := request("verified-media", "released"); status != 200 {
		t.Fatalf("admission slot leaked: %d", status)
	}
}
