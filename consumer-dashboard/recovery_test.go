package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func recoveryApp(t *testing.T) *app {
	t.Helper()
	// Keep Unix socket paths below the platform's length limit.
	dir, err := os.MkdirTemp("", "consumer-recovery-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "consumer.db")
	db, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	a := &app{haltController: &haltController{}, db: db, cfg: config{Role: "dashboard", DBPath: path}}
	a.lifecycle = &app{haltController: a.haltController, db: db, cfg: a.cfg}
	a.ready.Store(true)
	a.lifecycle.ready.Store(true)
	a.lifecycle.halt("consumer_incompatible: old model")
	return a
}

func startRecoveryAdmin(t *testing.T, a *app) {
	t.Helper()
	closeAdmin, err := a.startLocalAdmin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeAdmin)
}

func localResume(a *app) (localResumeResponse, error) {
	return requestLocalResume(context.Background(), adminSocketPath(a.cfg.DBPath), localResumeRequest{
		Operator: "operator-test", Reason: "updated consumer model", CorrelationID: "recovery-test",
	})
}

func TestResumeHTTPRouteRemoved(t *testing.T) {
	a := recoveryApp(t)
	before := a.recoveryGeneration()
	for _, token := range []string{"", "media-token", "field-token", "internal-token"} {
		req := httptest.NewRequest("POST", "/admin/resume", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		a.router().ServeHTTP(w, req)
		if w.Code != 404 {
			t.Fatalf("removed route returned %d", w.Code)
		}
	}
	if !a.halted.Load() || a.recoveryGeneration() != before {
		t.Fatal("HTTP request changed halt")
	}
}

func TestLocalRecoveryClearsBothSubscriptionsAndAudits(t *testing.T) {
	a := recoveryApp(t)
	for _, sql := range []string{
		`INSERT INTO schema_retry VALUES('HAZARDS',1,17,1,1,2)`,
		`INSERT INTO constraint_retry VALUES('HAZARDS',3,1,2)`,
		`INSERT INTO corrupt_messages VALUES('HAZARDS',4,1)`,
		`INSERT INTO processed_messages VALUES('HAZARDS',2,'digest','now')`,
		`INSERT INTO schema_cache VALUES('registry',17,'cached schema')`,
		`INSERT INTO records VALUES('hazard-id','{}','original-correlation','now')`,
	} {
		if _, err := a.db.Exec(sql); err != nil {
			t.Fatal(err)
		}
	}
	startRecoveryAdmin(t, a)
	t.Setenv("CONSUMER_DB_PATH", a.cfg.DBPath)
	t.Setenv("AUTH_BASE_URL", "")
	t.Setenv("AUTH_INTERNAL_KEY", "")
	old := slog.Default()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(old)
	var output bytes.Buffer
	err := runAdminCommand(context.Background(), []string{"resume", "--operator", "operator-test", "--reason", "updated consumer model", "--correlation-id", "recovery-test"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	var response localResumeResponse
	if err := json.Unmarshal(output.Bytes(), &response); err != nil || response.Status != "resume_requested" {
		t.Fatal(response, err)
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
	if audit.Operator != "operator-test" || audit.Reason != "updated consumer model" || audit.CorrelationID != "recovery-test" || audit.PreviousReason != "consumer_incompatible: old model" {
		t.Fatalf("bad audit %+v", audit)
	}
	if _, err := time.Parse(time.RFC3339Nano, audit.Timestamp); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"schema_retry", "constraint_retry", "corrupt_messages", "processed_messages", "schema_cache", "records"} {
		var count int
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
	response, err = localResume(a)
	if err != nil || response.Status != "not_halted" || !a.ready.Load() {
		t.Fatal("repeated resume not idempotent", response, err)
	}
	var after string
	if err := a.db.QueryRow(`SELECT value FROM consumer_control WHERE key='last_resume'`).Scan(&after); err != nil || after != encoded {
		t.Fatal("no-op changed audit", err)
	}
	// A response can arrive just before the serving goroutine finishes; the log
	// write precedes that response, and the no-op request has also completed.
	for _, field := range []string{`"operator":"operator-test"`, `"correlation_id":"recovery-test"`, `"latency_ms":`, `"msg":"local consumer resume"`} {
		if !strings.Contains(logs.String(), field) {
			t.Fatal("missing recovery log field", field)
		}
	}
	if strings.Contains(logs.String(), "auth-service") {
		t.Fatal("recovery contacted Auth")
	}
}

func TestLocalRecoveryStorageFailureRollsBackAudit(t *testing.T) {
	a := recoveryApp(t)
	startRecoveryAdmin(t, a)
	if _, err := a.db.Exec(`CREATE TRIGGER deny_clear BEFORE DELETE ON consumer_control WHEN OLD.key='halt_reason' BEGIN SELECT RAISE(ABORT,'cannot clear'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := localResume(a); err == nil || !a.halted.Load() {
		t.Fatal("write failure cleared halt", err)
	}
	var count int
	if err := a.db.QueryRow(`SELECT count(*) FROM consumer_control WHERE key='last_resume'`).Scan(&count); err != nil || count != 0 {
		t.Fatal("failed transaction retained audit", err)
	}
	if _, err := a.db.Exec(`DROP TRIGGER deny_clear`); err != nil {
		t.Fatal(err)
	}
	if response, err := localResume(a); err != nil || response.Status != "resume_requested" {
		t.Fatal(response, err)
	}
}

func TestLocalRecoveryStickyHaltRequiresWritableStorage(t *testing.T) {
	a := recoveryApp(t)
	startRecoveryAdmin(t, a)
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
	if _, err := localResume(a); err == nil || !a.haltSticky.Load() {
		t.Fatal("read-only recovery cleared halt", err)
	}
	if _, err := a.db.Exec(`PRAGMA query_only=OFF`); err != nil {
		t.Fatal(err)
	}
	if _, err := localResume(a); err != nil || a.haltSticky.Load() || a.lifecycle.halted.Load() {
		t.Fatal("sticky recovery failed", err)
	}
}

func TestLocalRecoveryCannotClearNewerHalt(t *testing.T) {
	a := recoveryApp(t)
	before := a.recoveryGeneration()
	a.lifecycle.halt("newer failure")
	_, _, err := a.resume(context.Background(), "operator-test", "fixed old failure", before)
	halted, reason := a.snapshot()
	if !errors.Is(err, errHaltChanged) || !halted || reason != "newer failure" {
		t.Fatal("newer halt lost", err, reason)
	}
	var count int
	if err := a.db.QueryRow(`SELECT count(*) FROM consumer_control WHERE key='last_resume'`).Scan(&count); err != nil || count != 0 {
		t.Fatal("stale recovery changed audit", err)
	}
}

func TestConcurrentControlOperationsKeepMemoryAndStorageConsistent(t *testing.T) {
	a := recoveryApp(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Go(func() { a.lifecycle.halt("concurrent failure") })
		wg.Go(func() {
			_, _, _ = a.resume(context.Background(), "operator-test", "fixed failure", a.recoveryGeneration())
		})
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

func TestLocalAdminSocketPermissionsAndActiveListener(t *testing.T) {
	a := recoveryApp(t)
	startRecoveryAdmin(t, a)
	path := adminSocketPath(a.cfg.DBPath)
	for name, want := range map[string]os.FileMode{path: 0600, filepath.Dir(path): 0700} {
		info, err := os.Stat(name)
		if err != nil || info.Mode().Perm() != want {
			t.Fatal("incorrect admin permissions", name, info, err)
		}
	}
	if closeAdmin, err := a.startLocalAdmin(context.Background()); err == nil {
		closeAdmin()
		t.Fatal("second listener replaced active socket")
	}
	if _, err := localResume(a); err != nil {
		t.Fatal("original listener broken", err)
	}
}

func TestLocalAdminRecoversStaleSocketAndCleansUp(t *testing.T) {
	a := recoveryApp(t)
	path := adminSocketPath(a.cfg.DBPath)
	listener, err := listenLocalAdmin(path)
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	listener.Close()
	closeAdmin, err := a.startLocalAdmin(context.Background())
	if err != nil {
		t.Fatal("did not recover crash socket", err)
	}
	if _, err := localResume(a); err != nil {
		closeAdmin()
		t.Fatal(err)
	}
	closeAdmin()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("socket not cleaned up", err)
	}
}

func TestLocalAdminDoesNotRemoveRegularFile(t *testing.T) {
	a := recoveryApp(t)
	path := adminSocketPath(a.cfg.DBPath)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if closeAdmin, err := a.startLocalAdmin(context.Background()); err == nil {
		closeAdmin()
		t.Fatal("regular file replaced")
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "keep" {
		t.Fatal("regular file changed", err)
	}
}

func TestLocalResumeRejectsInvalidRequests(t *testing.T) {
	a := recoveryApp(t)
	startRecoveryAdmin(t, a)
	for _, raw := range []string{
		`invalid`, `{}`, `{"operator":"op","reason":" "}`,
		`{"operator":"op","reason":"fixed","unknown":true}`,
		`{"operator":"op","reason":"fixed"}{}`,
		`{"operator":"op","reason":"` + strings.Repeat("x", adminMessageLimit) + `"}`,
	} {
		conn, err := net.Dial("unix", adminSocketPath(a.cfg.DBPath))
		if err != nil {
			t.Fatal(err)
		}
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		io.WriteString(conn, raw)
		conn.(*net.UnixConn).CloseWrite()
		var response localResumeResponse
		err = json.NewDecoder(conn).Decode(&response)
		conn.Close()
		if err != nil || response.Error == "" {
			t.Fatal("invalid request accepted", response, err)
		}
		if !a.halted.Load() {
			t.Fatal("invalid request cleared halt")
		}
	}
}

func TestLocalRecoveryCLIValidation(t *testing.T) {
	t.Setenv("CONSUMER_DB_PATH", "")
	for _, args := range [][]string{
		{}, {"unknown"}, {"resume"}, {"resume", "--operator", "op"},
		{"resume", "--operator", "op", "--reason", "fixed", "extra"},
		{"resume", "--operator", "op", "--reason", "fixed"},
	} {
		if err := runAdminCommand(context.Background(), args, io.Discard); err == nil {
			t.Fatal("invalid CLI accepted", args)
		}
	}
	var output bytes.Buffer
	if err := runAdminCommand(context.Background(), []string{"--help"}, &output); err != nil || !strings.Contains(output.String(), "consumer resume") {
		t.Fatal("missing help", err)
	}
}
