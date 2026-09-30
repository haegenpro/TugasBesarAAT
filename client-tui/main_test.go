package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProtectedRequestRefreshesAndRetries(t *testing.T) {
	refreshes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/refresh":
			refreshes++
			json.NewEncoder(w).Encode(session{AccessToken: "new-access", RefreshToken: "new-refresh", Role: "field", ClientID: "field-id"})
		case "/hazards":
			if r.Header.Get("Authorization") != "Bearer new-access" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"data": []any{}, "count": 0})
		}
	}))
	defer server.Close()
	a := &app{baseURL: server.URL, client: server.Client(), session: session{AccessToken: "old-access", RefreshToken: "old-refresh"}}
	status, _, err := a.protected(http.MethodGet, "/hazards")
	if err != nil || status != http.StatusOK || refreshes != 1 || a.previousAccess != "old-access" || a.session.RefreshToken != "new-refresh" {
		t.Fatalf("status=%d err=%v refreshes=%d session=%+v", status, err, refreshes, a.session)
	}
	oldStatus, _, err := a.request(http.MethodGet, "/hazards", nil, a.previousAccess)
	if err != nil || oldStatus != http.StatusUnauthorized {
		t.Fatalf("old token status=%d err=%v", oldStatus, err)
	}
}

func TestRefreshOutageKeepsSessionForRetry(t *testing.T) {
	refreshes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/refresh" {
			refreshes++
			if refreshes == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			json.NewEncoder(w).Encode(session{AccessToken: "new-access", RefreshToken: "new-refresh", Role: "field"})
			return
		}
		if r.Header.Get("Authorization") != "Bearer new-access" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	a := &app{baseURL: server.URL, client: server.Client(), session: session{AccessToken: "old-access", RefreshToken: "old-refresh"}}
	if _, _, err := a.protected(http.MethodGet, "/hazards"); err == nil || a.session.RefreshToken != "old-refresh" {
		t.Fatal("temporary auth outage discarded the session")
	}
	status, _, err := a.protected(http.MethodGet, "/hazards")
	if err != nil || status != http.StatusOK || a.session.RefreshToken != "new-refresh" {
		t.Fatalf("retry status=%d err=%v", status, err)
	}
}
