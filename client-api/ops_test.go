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

func TestOpsUsesLiveRoleAndServerCredential(t *testing.T) {
	var internal atomic.Bool
	var calls atomic.Int32
	auth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		role := "media"
		if internal.Load() {
			role = "internal"
		}
		_ = json.NewEncoder(w).Encode(principal{ClientID: "operator", Role: role, Scope: "hazards:raw"})
	}))
	defer auth.Close()
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer owner-only" {
			t.Error("service credential was not selected by gateway")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"items":[],"next_cursor":null}`)
	}))
	defer owner.Close()
	a := &api{cfg: config{AuthURL: auth.URL, AggregatorURL: owner.URL, AuthInternalKey: "auth-only", OpsToken: "owner-only"}, client: owner.Client(), log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	router := a.router()
	for _, want := range []int{403, 200} {
		req := httptest.NewRequest("GET", "/ops/api/failures", nil)
		req.Header.Set("Authorization", "Bearer user-access")
		req.Header.Set("X-Role", "internal")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != want {
			t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
		}
		internal.Store(true)
	}
	if calls.Load() != 1 {
		t.Fatal("unprivileged request reached owner")
	}
}
func TestPartialDetailKeepsNullDataExplicit(t *testing.T) {
	var value aggregatorDetail
	err := json.Unmarshal([]byte(`{"data":null,"partial":true,"unavailable_units":["canonical-2-b"],"source_status":{},"as_of":"2026-11-21T00:00:00Z"}`), &value)
	if err != nil || !value.Partial {
		t.Fatalf("partial detail: %+v %v", value, err)
	}
	if err = json.Unmarshal([]byte(`{"data":null,"partial":true}`), &value); err == nil {
		t.Fatal("accepted partial response without its missing-unit boundary")
	}
	if strings.Contains(value.AsOf, "secret") {
		t.Fatal("unexpected fixture")
	}
}
