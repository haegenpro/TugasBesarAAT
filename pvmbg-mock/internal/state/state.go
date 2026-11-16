// Package state holds the in-memory, mutex-guarded toggle state for PVMBG's
// admin endpoints: schema-version mode and outage mode. Both change
// immediately, in memory, with no restart/redeploy.
package state

import "sync"

type Mode string

const (
	ModeV1              Mode = "v1"
	ModeV2              Mode = "v2"
	ModeRename          Mode = "rename"
	ModeTypeChange      Mode = "type_change"
	ModeMissingRequired Mode = "missing_required"
)

func (m Mode) Valid() bool {
	switch m {
	case ModeV1, ModeV2, ModeRename, ModeTypeChange, ModeMissingRequired:
		return true
	}
	return false
}

func (m Mode) Incompatible() bool {
	return m == ModeRename || m == ModeTypeChange || m == ModeMissingRequired
}

type Toggles struct {
	mu     sync.RWMutex
	mode   Mode
	outage bool
}

func New() *Toggles {
	return &Toggles{mode: ModeV1}
}

func (t *Toggles) Mode() Mode {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mode
}

func (t *Toggles) SetMode(m Mode) bool {
	if !m.Valid() {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mode = m
	return true
}

func (t *Toggles) ShowConfidence() bool {
	return t.Mode() == ModeV2
}

// ToggleSchema flips confidence_level visibility and returns the new state.
func (t *Toggles) ToggleSchema() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mode == ModeV2 {
		t.mode = ModeV1
	} else {
		t.mode = ModeV2
	}
	return t.mode == ModeV2
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
