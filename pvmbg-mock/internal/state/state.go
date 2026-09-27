// Package state holds the in-memory, mutex-guarded toggle state for PVMBG's
// admin endpoints: schema-version (confidence_level visibility) and outage
// mode. Both toggle immediately, in memory, with no restart/redeploy.
package state

import "sync"

type Toggles struct {
	mu             sync.RWMutex
	showConfidence bool
	outage         bool
}

func New() *Toggles {
	return &Toggles{}
}

func (t *Toggles) ShowConfidence() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.showConfidence
}

// ToggleSchema flips confidence_level visibility and returns the new state.
func (t *Toggles) ToggleSchema() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.showConfidence = !t.showConfidence
	return t.showConfidence
}

func (t *Toggles) Outage() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.outage
}

// SetOutage sets outage mode explicitly (on/off) and returns the new state.
func (t *Toggles) SetOutage(on bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.outage = on
	return t.outage
}

// ToggleOutage flips outage mode and returns the new state.
func (t *Toggles) ToggleOutage() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.outage = !t.outage
	return t.outage
}
