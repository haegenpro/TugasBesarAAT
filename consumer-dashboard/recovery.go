package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Both subscriptions share this controller because they share consumer_control.
// The mutex orders persistent and in-memory transitions, including failed writes.
type haltController struct {
	mu         sync.Mutex
	halted     atomic.Bool
	haltSticky atomic.Bool
	reason     string
	generation uint64
}

var errHaltChanged = errors.New("halt changed during recovery authorization")

func (h *haltController) recoveryGeneration() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.generation
}

func (h *haltController) snapshot() (bool, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.halted.Load(), h.reason
}

type recoveryPrincipal struct {
	ClientID string `json:"client_id"`
	Role     string `json:"role"`
	Scope    string `json:"scope"`
}

type recoveryAudit struct {
	ClientID       string `json:"client_id"`
	Timestamp      string `json:"timestamp"`
	CorrelationID  string `json:"correlation_id"`
	PreviousReason string `json:"previous_reason"`
}

var recoveryHTTP = &http.Client{
	Timeout: 5 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

func (a *app) authorizeRecovery(ctx context.Context, token string) (p recoveryPrincipal, status int, err error) {
	started := time.Now()
	upstreamStatus := 0
	defer func() { logOutbound(ctx, "auth_introspect", "auth-service", upstreamStatus, started, err) }()
	body, _ := json.Marshal(map[string]string{"access_token": token})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(a.cfg.AuthURL, "/")+"/internal/introspect", bytes.NewReader(body))
	if err != nil {
		return p, http.StatusServiceUnavailable, errors.New("auth service request unavailable")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Key", a.cfg.AuthInternalKey)
	req.Header.Set(correlationHeader, correlationFromContext(ctx))
	resp, err := recoveryHTTP.Do(req)
	if err != nil {
		return p, http.StatusServiceUnavailable, errors.New("auth service unavailable")
	}
	defer resp.Body.Close()
	upstreamStatus = resp.StatusCode
	if resp.StatusCode == http.StatusUnauthorized {
		return p, http.StatusUnauthorized, errors.New("invalid access token")
	}
	if resp.StatusCode != http.StatusOK {
		return p, http.StatusServiceUnavailable, fmt.Errorf("auth introspection returned status %d", resp.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 1<<20))
	if err := decoder.Decode(&p); err != nil || p.ClientID == "" || p.Role == "" || p.Scope == "" {
		return recoveryPrincipal{}, http.StatusServiceUnavailable, errors.New("invalid auth introspection response")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return recoveryPrincipal{}, http.StatusServiceUnavailable, errors.New("invalid auth introspection response")
	}
	for _, scope := range strings.Fields(p.Scope) {
		if scope == "consumer:resume" {
			return p, http.StatusOK, nil
		}
	}
	return p, http.StatusForbidden, errors.New("consumer:resume permission required")
}

// resume clears only halt state. A failed transaction leaves the old state intact.
func (a *app) resume(ctx context.Context, clientID string, generation uint64) (bool, string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback()
	var reason string
	err = tx.QueryRowContext(ctx, `SELECT value FROM consumer_control WHERE key='halt_reason'`).Scan(&reason)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, "", err
	}
	if errors.Is(err, sql.ErrNoRows) && !a.halted.Load() && !a.haltSticky.Load() {
		return false, "", nil
	}
	if generation != a.generation {
		return false, "", errHaltChanged
	}
	if reason == "" {
		reason = a.reason
	}
	audit, err := json.Marshal(recoveryAudit{ClientID: clientID, Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		CorrelationID: correlationFromContext(ctx), PreviousReason: reason})
	if err != nil {
		return false, "", err
	}
	// This write verifies persistence even for an in-memory halt whose original
	// write failed and therefore has no halt_reason row to delete.
	if _, err = tx.ExecContext(ctx, `INSERT INTO consumer_control(key,value) VALUES('last_resume',?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, string(audit)); err != nil {
		return false, "", err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM consumer_control WHERE key='halt_reason'`); err != nil {
		return false, "", err
	}
	if err = tx.Commit(); err != nil {
		return false, "", err
	}
	a.ready.Store(false)
	if a.lifecycle != nil {
		a.lifecycle.ready.Store(false)
	}
	a.reason = ""
	a.generation++
	a.haltSticky.Store(false)
	a.halted.Store(false)
	return true, reason, nil
}

func (a *app) markReady() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ready.Store(!a.halted.Load())
}

func (a *app) resumeHandler(w http.ResponseWriter, r *http.Request) {
	cid := strings.TrimSpace(r.Header.Get(correlationHeader))
	if cid == "" {
		cid = uuid.NewString()
	}
	w.Header().Set(correlationHeader, cid)
	w.Header().Set("Content-Type", "application/json")
	ctx := context.WithValue(r.Context(), traceKey{}, messageTrace{Service: "consumer-" + a.cfg.Role, CorrelationID: cid})
	started := time.Now()
	status := http.StatusOK
	defer func() {
		logEvent(ctx, slog.LevelInfo, "http request", "method", r.Method, "path", r.URL.Path, "status_code", status, "latency_ms", time.Since(started).Milliseconds())
	}()
	fail := func(code int, message string) {
		status = code
		w.WriteHeader(code)
		writeJSON(w, map[string]string{"error": message})
	}
	parts := strings.Fields(r.Header.Get("Authorization"))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		fail(http.StatusUnauthorized, "bearer token required")
		return
	}
	generation := a.recoveryGeneration()
	p, code, err := a.authorizeRecovery(ctx, parts[1])
	if err != nil {
		logEvent(ctx, slog.LevelWarn, "consumer resume denied", "status_code", code, "client_id", p.ClientID)
		message := "auth service unavailable"
		if code == http.StatusUnauthorized {
			message = "invalid access token"
		} else if code == http.StatusForbidden {
			message = "consumer:resume permission required"
		}
		fail(code, message)
		return
	}
	resumed, previous, err := a.resume(ctx, p.ClientID, generation)
	if errors.Is(err, errHaltChanged) {
		logEvent(ctx, slog.LevelWarn, "consumer resume deferred", "client_id", p.ClientID)
		fail(http.StatusConflict, "halt changed; inspect readiness and retry")
		return
	}
	if err != nil {
		logEvent(ctx, slog.LevelError, "consumer resume failed", "client_id", p.ClientID, "error", err)
		fail(http.StatusServiceUnavailable, "recovery storage unavailable")
		return
	}
	result := "not_halted"
	if resumed {
		result = "resume_requested"
	}
	logEvent(ctx, slog.LevelInfo, "consumer resume", "client_id", p.ClientID, "status", result, "previous_reason", previous)
	writeJSON(w, map[string]string{"status": result})
}
