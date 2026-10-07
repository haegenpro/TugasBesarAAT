package api

import (
	"aggregator/internal/domain"
	"testing"
	"time"
)

func TestFreshnessUsesSuccessfulCheckAndExpiresWithoutLosingEvidence(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	if got := statusAt(domain.EndpointStatus{State: "fresh"}, now, time.Minute); got.State != "unavailable" {
		t.Fatalf("unverified startup was fresh: %#v", got)
	}
	last := now.Add(-time.Minute)
	failed := now.Add(-30 * time.Second)
	p := domain.EndpointStatus{State: "stale", LastSuccess: &last, StaleSince: &failed}
	before := statusAt(p, now, time.Minute)
	if before.State != "stale" || before.StaleSince == "" {
		t.Fatalf("lost dated fallback at boundary: %#v", before)
	}
	after := statusAt(p, now.Add(time.Millisecond), time.Minute)
	if after.State != "unavailable" || after.LastSuccess == nil || after.StaleSince == "" {
		t.Fatalf("expiry erased evidence or stayed fresh: %#v", after)
	}
}
func TestStoppedPollingBecomesStaleEvenIfLastStoredStateWasFresh(t *testing.T) {
	now := time.Now().UTC()
	last := now.Add(-20 * time.Second)
	due := now.Add(-10 * time.Second)
	got := statusAt(domain.EndpointStatus{State: "fresh", LastSuccess: &last, NextAttempt: &due}, now, time.Minute)
	if got.State != "stale" || got.StaleSince == "" {
		t.Fatalf("persisted success hid stopped polling: %#v", got)
	}
}
