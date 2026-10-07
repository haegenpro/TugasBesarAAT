package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func recoveryApp(t *testing.T, auth http.Handler) *app {
	t.Helper()
	db, err := openDB(filepath.Join(t.TempDir(), "consumer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	server := httptest.NewServer(auth)
	t.Cleanup(server.Close)
	a := &app{haltController: &haltController{}, db: db, cfg: config{Role: "dashboard", AuthURL: server.URL, AuthInternalKey: "service-secret"}}
	a.lifecycle = &app{haltController: a.haltController, db: db, cfg: a.cfg}
	a.ready.Store(true)
	a.lifecycle.ready.Store(true)
	a.lifecycle.halt("consumer_incompatible: old model")
	return a
}

func authorizedRecovery(w http.ResponseWriter, r *http.Request) {
	_ = json.NewEncoder(w).Encode(recoveryPrincipal{ClientID: "internal-id", Role: "internal", Scope: "hazards:raw consumer:resume"})
}

func requestResume(a *app, header string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/admin/resume", nil)
	r.Header.Set("Authorization", header)
	r.Header.Set(correlationHeader, "recovery-test")
	w := httptest.NewRecorder()
	a.router().ServeHTTP(w, r)
	return w
}

func TestRecoveryAuthorizationFailsWithoutChangingState(t *testing.T) {
	cases := []struct {
		name, header, response string
		upstream, want         int
	}{
		{"missing", "", "", 200, 401},
		{"malformed", "Bearer a b", "", 200, 401},
		{"wrong scheme", "Basic secret", "", 200, 401},
		{"invalid token", "Bearer invalid", "", 401, 401},
		{"expired", "Bearer expired", "", 401, 401},
		{"revoked", "Bearer revoked", "", 401, 401},
		{"media", "Bearer media", `{"client_id":"media-id","role":"media","scope":"hazards:summary"}`, 200, 403},
		{"field", "Bearer field", `{"client_id":"field-id","role":"field","scope":"hazards:raw"}`, 200, 403},
		{"scope substring", "Bearer other", `{"client_id":"other-id","role":"internal","scope":"consumer:resume-extra"}`, 200, 403},
		{"outage", "Bearer valid", "", 503, 503},
		{"invalid JSON", "Bearer valid", `invalid`, 200, 503},
		{"missing principal", "Bearer valid", `{}`, 200, 503},
		{"trailing JSON", "Bearer valid", `{"client_id":"internal-id","role":"internal","scope":"consumer:resume"}{}`, 200, 503},
		{"redirect", "Bearer valid", "", 302, 503},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			calls := 0
			a := recoveryApp(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(c.upstream)
				io.WriteString(w, c.response)
			}))
			before := a.recoveryGeneration()
			w := requestResume(a, c.header)
			if w.Code != c.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			halted, reason := a.snapshot()
			if !halted || reason != "consumer_incompatible: old model" || a.recoveryGeneration() != before {
				t.Fatal("denied request changed halt")
			}
			var count int
			if err := a.db.QueryRow(`SELECT count(*) FROM consumer_control WHERE key='last_resume'`).Scan(&count); err != nil || count != 0 {
				t.Fatal("denied request changed storage", err)
			}
			if c.header == "" || c.name == "malformed" || c.name == "wrong scheme" {
				if calls != 0 {
					t.Fatal("invalid header called auth")
				}
			}
		})
	}
}

func TestRecoveryClearsBothSubscriptionsAndAudits(t *testing.T) {
	var requestErr string
	a := recoveryApp(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if r.Method != "POST" || r.URL.Path != "/internal/introspect" || r.Header.Get("X-Internal-Key") != "service-secret" || r.Header.Get(correlationHeader) != "recovery-test" || body["access_token"] != "internal-token" {
			requestErr = "incorrect introspection request"
		}
		authorizedRecovery(w, r)
	}))
	_, err := a.db.Exec(`INSERT INTO schema_retry VALUES('HAZARDS',1,17,1,1,2)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.db.Exec(`INSERT INTO processed_messages VALUES('HAZARDS',2,'digest','now')`)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.refreshHalt(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := slog.Default()
	var output bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	defer slog.SetDefault(old)
	w := requestResume(a, "Bearer internal-token")
	if requestErr != "" || w.Code != 200 || !strings.Contains(w.Body.String(), "resume_requested") {
		t.Fatal(requestErr, w.Code, w.Body.String())
	}
	if a.halted.Load() || a.lifecycle.halted.Load() || a.ready.Load() || a.lifecycle.ready.Load() {
		t.Fatal("shared halt or readiness incorrectly cleared")
	}
	var encoded string
	if err := a.db.QueryRow(`SELECT value FROM consumer_control WHERE key='last_resume'`).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	var audit recoveryAudit
	if err := json.Unmarshal([]byte(encoded), &audit); err != nil {
		t.Fatal(err)
	}
	if audit.ClientID != "internal-id" || audit.CorrelationID != "recovery-test" || audit.PreviousReason != "consumer_incompatible: old model" {
		t.Fatalf("bad audit %+v", audit)
	}
	if _, err := time.Parse(time.RFC3339Nano, audit.Timestamp); err != nil {
		t.Fatal(err)
	}
	var count int
	for _, table := range []string{"schema_retry", "processed_messages"} {
		if err := a.db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 1 {
			t.Fatal("existing state changed", table, err)
		}
	}
	a.markReady()
	a.lifecycle.markReady()
	ready := httptest.NewRecorder()
	a.router().ServeHTTP(ready, httptest.NewRequest("GET", "/ready", nil))
	if ready.Code != 200 {
		t.Fatal("did not become ready after loops resumed")
	}
	w = requestResume(a, "Bearer internal-token")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "not_halted") || !a.ready.Load() {
		t.Fatal("repeated resume not idempotent")
	}
	if strings.Contains(output.String(), "internal-token") || strings.Contains(output.String(), "service-secret") {
		t.Fatal("credentials logged")
	}
	for _, field := range []string{`"target_service":"auth-service"`, `"correlation_id":"recovery-test"`, `"latency_ms":`, `"client_id":"internal-id"`} {
		if !strings.Contains(output.String(), field) {
			t.Fatal("missing recovery log field", field)
		}
	}
}

func TestRecoveryStorageFailureRollsBackAudit(t *testing.T) {
	a := recoveryApp(t, http.HandlerFunc(authorizedRecovery))
	if _, err := a.db.Exec(`CREATE TRIGGER deny_clear BEFORE DELETE ON consumer_control WHEN OLD.key='halt_reason' BEGIN SELECT RAISE(ABORT,'cannot clear'); END`); err != nil {
		t.Fatal(err)
	}
	w := requestResume(a, "Bearer valid")
	if w.Code != 503 || !a.halted.Load() {
		t.Fatal("write failure cleared halt", w.Code)
	}
	var count int
	if err := a.db.QueryRow(`SELECT count(*) FROM consumer_control WHERE key='last_resume'`).Scan(&count); err != nil || count != 0 {
		t.Fatal("failed transaction retained audit", err)
	}
	if _, err := a.db.Exec(`DROP TRIGGER deny_clear`); err != nil {
		t.Fatal(err)
	}
	if w := requestResume(a, "Bearer valid"); w.Code != 200 {
		t.Fatal("did not resume after fixing storage", w.Code)
	}
}

func TestRecoveryStickyHaltRequiresWritableStorage(t *testing.T) {
	a := recoveryApp(t, http.HandlerFunc(authorizedRecovery))
	if _, err := a.db.Exec(`DELETE FROM consumer_control WHERE key='halt_reason'`); err != nil {
		t.Fatal(err)
	}
	if _, err := a.db.Exec(`PRAGMA query_only=ON`); err != nil {
		t.Fatal(err)
	}
	a.lifecycle.halt("disk write failed")
	if !a.haltSticky.Load() {
		t.Fatal("halt not sticky after failed persistence")
	}
	if w := requestResume(a, "Bearer valid"); w.Code != 503 || !a.haltSticky.Load() {
		t.Fatal("read-only recovery incorrectly cleared halt", w.Code)
	}
	if _, err := a.db.Exec(`PRAGMA query_only=OFF`); err != nil {
		t.Fatal(err)
	}
	if w := requestResume(a, "Bearer valid"); w.Code != 200 || a.haltSticky.Load() || a.lifecycle.halted.Load() {
		t.Fatal("sticky recovery failed", w.Code)
	}
}

func TestRecoveryCannotClearHaltRaisedDuringAuthorization(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	a := recoveryApp(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release; authorizedRecovery(w, r) }))
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- requestResume(a, "Bearer valid") }()
	<-entered
	a.lifecycle.halt("newer failure")
	close(release)
	w := <-done
	halted, reason := a.snapshot()
	if w.Code != 409 || !halted || reason != "newer failure" {
		t.Fatal("newer halt lost", w.Code, reason)
	}
}

func TestConcurrentControlOperationsKeepMemoryAndStorageConsistent(t *testing.T) {
	a := recoveryApp(t, http.HandlerFunc(authorizedRecovery))
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Go(func() { a.lifecycle.halt("concurrent failure") })
		wg.Go(func() { _, _, _ = a.resume(context.Background(), "internal-id", a.recoveryGeneration()) })
		wg.Go(func() { _ = a.refreshHalt(context.Background()) })
	}
	wg.Wait()
	var count int
	if err := a.db.QueryRow(`SELECT count(*) FROM consumer_control WHERE key='halt_reason'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if (count == 1) != a.halted.Load() || a.halted.Load() != a.lifecycle.halted.Load() {
		t.Fatal("persistent and shared memory halt disagree")
	}
}

func TestRecoveryConnectionFailureAndGeneratedCorrelation(t *testing.T) {
	a := recoveryApp(t, http.HandlerFunc(authorizedRecovery))
	a.cfg.AuthURL = "http://127.0.0.1:1"
	r := httptest.NewRequest("POST", "/admin/resume", nil)
	r.Header.Set("Authorization", "Bearer valid")
	w := httptest.NewRecorder()
	a.router().ServeHTTP(w, r)
	if w.Code != 503 || !a.halted.Load() || w.Header().Get(correlationHeader) == "" {
		t.Fatal("outage did not fail closed with correlation ID", w.Code)
	}
}

func TestRecoveryAuthorizationDeadlineLeavesHaltUnchanged(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	a := recoveryApp(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release }))
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest("POST", "/admin/resume", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer valid")
	w := httptest.NewRecorder()
	a.router().ServeHTTP(w, req)
	<-entered
	if w.Code != 503 || !a.halted.Load() {
		t.Fatal("authorization deadline changed halt", w.Code)
	}
}
