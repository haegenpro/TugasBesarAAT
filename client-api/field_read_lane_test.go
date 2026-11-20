package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestVerifiedFieldPrincipalAloneSelectsPrivateReadRoute(t *testing.T) {
	key := strings.Repeat("r", 32)
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Token string `json:"access_token"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		role := "media"
		if body.Token == "field" {
			role = "field"
		}
		_ = json.NewEncoder(w).Encode(principal{ClientID: role, Role: role, Scope: "hazards:summary"})
	}))
	defer auth.Close()
	agg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		field := r.URL.Path == "/internal/field/hazards"
		if field != (r.Header.Get("X-Field-Read-Key") == key) {
			w.WriteHeader(403)
			return
		}
		w.Header().Set("X-Test-Field", r.URL.Path)
		io.WriteString(w, `{"data":[],"count":0,"as_of":"2026-11-17T00:00:00Z","source_status":{}}`)
	}))
	defer agg.Close()
	app := &api{cfg: config{AuthURL: auth.URL, AggregatorURL: agg.URL, AuthInternalKey: "auth-only", FieldReadToken: key}, client: http.DefaultClient, log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	router := app.router()
	for _, role := range []string{"media", "field"} {
		req := httptest.NewRequest("GET", "/hazards", nil)
		req.Header.Set("Authorization", "Bearer "+role)
		req.Header.Set("X-Field-Read-Key", key)
		req.Header.Set("X-Role", "field")
		reply := httptest.NewRecorder()
		router.ServeHTTP(reply, req)
		if reply.Code != 200 {
			t.Fatalf("verified %s routing failed: %d", role, reply.Code)
		}
	}
}
