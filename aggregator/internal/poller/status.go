package poller

import (
	"sync"
	"time"
)

// PVMBGStatus tracks whether PVMBG is currently reachable, and when it was
// last seen going unreachable, so the read API can attach stale/stale_since
// metadata to volcanic data served from last-known store state.
type PVMBGStatus struct {
	mu         sync.RWMutex
	healthy    bool
	staleSince time.Time
}

func NewPVMBGStatus() *PVMBGStatus {
	return &PVMBGStatus{healthy: true}
}

func (s *PVMBGStatus) MarkHealthy() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthy = true
	s.staleSince = time.Time{}
}

func (s *PVMBGStatus) MarkUnhealthy(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.healthy {
		s.staleSince = now
	}
	s.healthy = false
}

func (s *PVMBGStatus) IsHealthy() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.healthy
}

func (s *PVMBGStatus) StaleSince() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.staleSince
}

func (s *PVMBGStatus) Snapshot() (bool, time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.healthy, s.staleSince
}
