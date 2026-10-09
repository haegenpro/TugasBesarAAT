package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"
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

var errHaltChanged = errors.New("halt changed during recovery")

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

type recoveryAudit struct {
	Operator       string `json:"operator"`
	Reason         string `json:"reason"`
	Timestamp      string `json:"timestamp"`
	CorrelationID  string `json:"correlation_id"`
	PreviousReason string `json:"previous_reason"`
}

// resume clears only halt state. A failed transaction leaves the old state intact.
func (a *app) resume(ctx context.Context, operator, recoveryReason string, generation uint64) (bool, string, error) {
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
	audit, err := json.Marshal(recoveryAudit{Operator: operator, Reason: recoveryReason, Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
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
