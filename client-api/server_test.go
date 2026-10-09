package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const testHazardID = "11111111-1111-4111-8111-111111111111"

func TestAggregatorRenameReturnsServiceUnavailable(t *testing.T) {
	aggregator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"data":[{"hazard_code":"renamed","source":"PVMBG","source_ref_id":"rep-1","hazard_type":"VOLCANIC","severity":"SIAGA","area_name":"Merapi","latitude":-7.5,"longitude":110.4,"occurred_at":"2026-09-30T00:00:00Z","ingested_at":"2026-09-30T00:00:01Z","attributes":{}}],"count":1,"as_of":"2026-09-30T01:01:00Z","source_status":{"PVMBG":{"state":"fresh"}}}`)
	}))
	defer aggregator.Close()
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"client_id":"field-id","role":"field","scope":"hazards:raw"}`)
	}))
	defer auth.Close()
	a := &api{cfg: config{AuthURL: auth.URL, AggregatorURL: aggregator.URL, AuthInternalKey: "internal-secret"}, log: slog.New(slog.NewJSONHandler(io.Discard, nil)), client: http.DefaultClient}
	server := httptest.NewServer(a.router())
	defer server.Close()
	req, err := http.NewRequest(http.MethodGet, server.URL+"/hazards", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer field-token")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("renamed hazard_id returned %d, want 503", resp.StatusCode)
	}
}

func TestMediaProjectionAndRawAuthorization(t *testing.T) {
	var receivedCorrelation string
	aggregator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedCorrelation = r.Header.Get("X-Correlation-ID")
		w.Header().Set("Content-Type", "application/json")
		item := `{"hazard_id":"` + testHazardID + `","source":"PVMBG","source_ref_id":"private-source-id","hazard_type":"VOLCANIC","severity":"SIAGA","area_name":"Merapi","latitude":-7.5,"longitude":110.4,"occurred_at":"2026-09-30T00:00:00Z","ingested_at":"2026-09-30T00:00:01Z","attributes":{"ash_height":900,"stale":true,"stale_since":"2026-09-30T01:00:00Z"}}`
		if strings.HasPrefix(r.URL.Path, "/internal/hazards/") {
			io.WriteString(w, item)
			return
		}
		io.WriteString(w, `{"data":[`+item+`],"count":1,"as_of":"2026-09-30T01:01:00Z","source_status":{"PVMBG":{"state":"stale","stale_since":"2026-09-30T01:00:00Z"}}}`)
	}))
	defer aggregator.Close()
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Internal-Key") != "internal-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var payload struct {
			AccessToken string `json:"access_token"`
		}
		json.NewDecoder(r.Body).Decode(&payload)
		role := ""
		switch payload.AccessToken {
		case "media-token":
			role = "media"
		case "field-token":
			role = "field"
		}
		if role == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		scope := "hazards:summary"
		if role == "field" {
			scope = "hazards:raw"
		}
		json.NewEncoder(w).Encode(principal{ClientID: role + "-id", Role: role, Scope: scope})
	}))
	defer auth.Close()
	a := &api{cfg: config{AuthURL: auth.URL, AggregatorURL: aggregator.URL, AuthInternalKey: "internal-secret"}, log: slog.New(slog.NewJSONHandler(io.Discard, nil)), client: http.DefaultClient}
	server := httptest.NewServer(a.router())
	defer server.Close()
	request := func(path, token string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, server.URL+path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Correlation-ID", "demo-correlation")
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body map[string]any
		json.NewDecoder(resp.Body).Decode(&body)
		return resp.StatusCode, body
	}
	status, body := request("/hazards", "media-token")
	if status != http.StatusOK {
		t.Fatalf("media list status=%d", status)
	}
	media := body["data"].([]any)[0].(map[string]any)
	for _, key := range []string{"source_ref_id", "latitude", "longitude", "attributes"} {
		if _, exists := media[key]; exists {
			t.Fatalf("media response leaked %s", key)
		}
	}
	if body["source_status"].(map[string]any)["PVMBG"].(map[string]any)["state"] != "stale" {
		t.Fatal("stale status missing from summary")
	}
	if receivedCorrelation != "demo-correlation" {
		t.Fatalf("correlation ID not forwarded: %s", receivedCorrelation)
	}
	status, _ = request("/hazards?view=raw", "media-token")
	if status != http.StatusForbidden {
		t.Fatalf("media raw status=%d", status)
	}
	status, body = request("/hazards/"+testHazardID, "media-token")
	if status != http.StatusOK {
		t.Fatalf("media detail status=%d", status)
	}
	if _, exists := body["data"].(map[string]any)["attributes"]; exists {
		t.Fatal("media detail leaked attributes")
	}
	status, body = request("/hazards", "field-token")
	if status != http.StatusOK || body["data"].([]any)[0].(map[string]any)["attributes"] == nil {
		t.Fatalf("field raw response missing: status=%d", status)
	}
	status, _ = request("/hazards", "unknown-token")
	if status != http.StatusUnauthorized {
		t.Fatalf("invalid token status=%d", status)
	}
}

func TestInFlightLimitRejectsExcessWithoutCallingAuth(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 2)
	var authCalls atomic.Int32
	aggregator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		io.WriteString(w, `{"data":[],"count":0,"as_of":"2026-09-30T01:01:00Z","source_status":{}}`)
	}))
	defer aggregator.Close()
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authCalls.Add(1)
		io.WriteString(w, `{"client_id":"field-id","role":"field","scope":"hazards:raw"}`)
	}))
	defer auth.Close()
	a := &api{cfg: config{AuthURL: auth.URL, AggregatorURL: aggregator.URL, AuthInternalKey: "k", MaxInFlight: 2}, log: slog.New(slog.NewJSONHandler(io.Discard, nil)), client: http.DefaultClient}
	server := httptest.NewServer(a.router())
	defer server.Close()

	get := func(path string) (*http.Response, error) {
		req, _ := http.NewRequest(http.MethodGet, server.URL+path, nil)
		req.Header.Set("Authorization", "Bearer field-token")
		return server.Client().Do(req)
	}
	done := make(chan int, 2)
	for i := 0; i < 2; i++ {
		go func() {
			resp, err := get("/hazards")
			if err != nil {
				done <- -1
				return
			}
			resp.Body.Close()
			done <- resp.StatusCode
		}()
	}
	<-entered
	<-entered // both slots are now held inside the aggregator call

	callsBefore := authCalls.Load()
	resp, err := get("/hazards")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("excess request returned %d, want 429", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("429 response has no Retry-After header")
	}
	if authCalls.Load() != callsBefore {
		t.Fatal("rejected request still called auth-service")
	}

	health, err := get("/health")
	if err != nil {
		t.Fatal(err)
	}
	health.Body.Close()
	if health.StatusCode != http.StatusOK {
		t.Fatalf("/health returned %d while saturated, want 200", health.StatusCode)
	}

	close(release)
	for i := 0; i < 2; i++ {
		if code := <-done; code != http.StatusOK {
			t.Fatalf("held request finished with %d, want 200", code)
		}
	}
	again, err := get("/hazards")
	if err != nil {
		t.Fatal(err)
	}
	again.Body.Close()
	if again.StatusCode != http.StatusOK {
		t.Fatalf("request after release returned %d, want 200", again.StatusCode)
	}
}

func TestInFlightLimitDisabledWhenNonPositive(t *testing.T) {
	a := &api{cfg: config{MaxInFlight: 0}, log: slog.New(slog.NewJSONHandler(io.Discard, nil)), client: http.DefaultClient}
	server := httptest.NewServer(a.router())
	defer server.Close()
	resp, err := server.Client().Get(server.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/health returned %d, want 200", resp.StatusCode)
	}
}

func TestInternalReadScopePreservesRawViews(t *testing.T) {
	aggregator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"data":[],"count":0,"as_of":"2026-10-07T00:00:00Z","source_status":{}}`)
	}))
	defer aggregator.Close()
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"client_id":"internal-id","role":"internal","scope":"hazards:raw"}`)
	}))
	defer auth.Close()
	a := &api{cfg: config{AuthURL: auth.URL, AggregatorURL: aggregator.URL, AuthInternalKey: "internal-secret"}, log: slog.New(slog.NewJSONHandler(io.Discard, nil)), client: http.DefaultClient}
	router := a.router()
	for _, path := range []string{"/hazards", "/hazards?view=raw", "/hazards?view=summary"} {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Authorization", "Bearer internal-token")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("%s status=%d body=%s", path, w.Code, w.Body.String())
		}
	}
	if hasScope("hazards:raw-extra hazards:summary", "hazards:raw") {
		t.Fatal("scope substring grants raw access")
	}
}
