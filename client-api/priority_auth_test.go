package main

import (
	"clientapi/internal/platform"
	"encoding/base64"
	"encoding/json"
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

func TestSignedFieldHintBypassesSaturatedIntrospectionWithoutCachingAuth(t *testing.T) {
	key := strings.Repeat("k", 32)
	nonce := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	field := platform.SignPriorityToken(nonce, "access", "field", key, time.Now().Add(time.Minute).Unix())
	media := platform.SignPriorityToken(nonce, "access", "media", key, time.Now().Add(time.Minute).Unix())
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var revoked atomic.Bool
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Token string `json:"access_token"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		if request.Token == media {
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		if request.Token == field && revoked.Load() {
			w.WriteHeader(401)
			return
		}
		role := "media"
		if request.Token == field {
			role = "field"
		}
		_ = json.NewEncoder(w).Encode(principal{ClientID: role, Role: role, Scope: "hazards:summary"})
	}))
	defer auth.Close()
	agg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"data":[],"count":0,"as_of":"2026-11-17T00:00:00Z","source_status":{}}`)
	}))
	defer agg.Close()
	app := &api{cfg: config{AuthURL: auth.URL, AggregatorURL: agg.URL, AuthInternalKey: "internal", PriorityEnabled: true, MaxInFlight: 4, MaxInFlightPerClient: 4, FieldReserved: 1, AuthMaxInFlight: 2, AuthFieldReserved: 1, AuthPriorityKey: key}, client: &http.Client{Timeout: 3 * time.Second}, log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	router := app.router()
	request := func(token string) int {
		req := httptest.NewRequest("GET", "/hazards", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Role", "field")
		reply := httptest.NewRecorder()
		router.ServeHTTP(reply, req)
		return reply.Code
	}
	done := make(chan int, 1)
	go func() { done <- request(media) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("media introspection did not occupy general lane")
	}
	if status := request(strings.Replace(media, ".media.", ".field.", 1)); status != 429 {
		t.Fatalf("tampered scheduling hint claimed field lane: %d", status)
	}
	if status := request(field); status != 200 {
		t.Fatalf("field could not pass saturated introspection: %d", status)
	}
	revoked.Store(true)
	if status := request(field); status != 401 {
		t.Fatalf("live Auth rejection was bypassed: %d", status)
	}
	once.Do(func() { close(release) })
	if status := <-done; status != 200 {
		t.Fatalf("general request failed after release: %d", status)
	}
}
