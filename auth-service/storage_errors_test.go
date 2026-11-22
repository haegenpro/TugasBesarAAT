package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type failingSessions struct{ err error }

func (f failingSessions) issue(context.Context, identity) (tokenPair, error) {
	return tokenPair{}, f.err
}
func (f failingSessions) verify(context.Context, string) (principal, error) {
	return principal{}, f.err
}
func (f failingSessions) rotate(context.Context, string) (tokenPair, error) {
	return tokenPair{}, f.err
}
func (f failingSessions) logout(context.Context, string) error { return f.err }
func (f failingSessions) Ping(context.Context) error           { return nil }

// A saturated auth database is a controlled rejection (429 + Retry-After) that callers back off
// from; a broken one is still 503.
func TestStorageSaturationIsAControlledRejection(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		status int
	}{
		{"too many connections", &pgconn.PgError{Code: "53300"}, http.StatusTooManyRequests},
		{"statement timeout", &pgconn.PgError{Code: "57014"}, http.StatusTooManyRequests},
		{"lock timeout", &pgconn.PgError{Code: "55P03"}, http.StatusTooManyRequests},
		{"deadline", context.DeadlineExceeded, http.StatusTooManyRequests},
		{"broken", errors.New("connection refused"), http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			app := &authServer{store: failingSessions{test.err}, config: config{InternalKey: "internal-key", Identities: map[string]identity{
				"media-id": {ID: "media-id", Secret: "media-secret", Role: "media"}}}, log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
			body, _ := json.Marshal(loginRequest{ClientID: "media-id", ClientSecret: "media-secret"})
			req := httptest.NewRequest(http.MethodPost, "/internal/login", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Internal-Key", "internal-key")
			w := httptest.NewRecorder()
			app.router().ServeHTTP(w, req)
			if w.Code != test.status {
				t.Fatalf("status %d, want %d: %s", w.Code, test.status, w.Body.String())
			}
			if test.status == http.StatusTooManyRequests && w.Header().Get("Retry-After") != "1" {
				t.Fatal("controlled rejection without Retry-After")
			}
		})
	}
}

// blockingSessions waits like a saturated pool: until its context ends.
type blockingSessions struct{ failingSessions }

func (blockingSessions) rotate(ctx context.Context, _ string) (tokenPair, error) {
	<-ctx.Done()
	return tokenPair{}, ctx.Err()
}

// A storage wait that outlasts Auth's own deadline is answered as a controlled 429 within that
// deadline, instead of holding the request until the caller gives up.
func TestStorageWaitBeyondDeadlineIsAControlledRejection(t *testing.T) {
	t.Setenv("AUTH_STORAGE_TIMEOUT", "200ms")
	app := &authServer{store: blockingSessions{}, config: config{InternalKey: "internal-key"}, log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	body, _ := json.Marshal(refreshRequest{RefreshToken: "some-refresh-token"})
	req := httptest.NewRequest(http.MethodPost, "/internal/refresh", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Key", "internal-key")
	w := httptest.NewRecorder()
	started := time.Now()
	app.router().ServeHTTP(w, req)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("status %d (Retry-After %q), want 429 with Retry-After", w.Code, w.Header().Get("Retry-After"))
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("answered after %s, want within the storage deadline", elapsed)
	}
}
