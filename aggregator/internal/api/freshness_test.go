package api

import (
	"aggregator/internal/domain"
	"aggregator/internal/timeutil"
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

func TestUnchangedRecordsFollowSourceChecksRatherThanLastSeenExpiry(t *testing.T) {
	now := time.Now().UTC()
	old := now.Add(-24 * time.Hour)
	a := &API{BMKGFreshness: time.Minute, PVMBGFreshness: time.Minute}
	events := []domain.HazardEvent{
		{HazardID: "unchanged", Source: domain.SourceBMKG, OccurredAt: timeutil.New(old)},
		{HazardID: "history-only", Source: domain.SourceBMKG, OccurredAt: timeutil.New(old)},
		{HazardID: "cached", Source: domain.SourcePVMBG, OccurredAt: timeutil.New(old)},
	}
	metadata := map[string]domain.RecordMetadata{"unchanged": {LastSeenAt: &old}, "history-only": {}, "cached": {LastSeenAt: &old}}
	statuses := map[string]sourceStatus{"BMKG": {State: "fresh"}, "PVMBG": {State: "stale"}}
	out := a.recordMetadata(events, statuses, metadata)
	if out["unchanged"].State != "fresh" || out["history-only"].State != "fresh" || out["cached"].State != "stale" {
		t.Fatalf("unchanged record timestamps overrode source availability: %#v", out)
	}
	if !out["unchanged"].Historical || out["unchanged"].LastSeenAt != &old || out["history-only"].LastSeenAt != nil {
		t.Fatalf("historical/observation evidence was lost: %#v", out)
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
