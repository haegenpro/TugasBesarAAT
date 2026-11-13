package config

import (
	"testing"
	"time"
)

func TestOwnershipSettings(t *testing.T) {
	t.Setenv("CANONICAL_OWNERSHIP_ENABLED", "true")
	t.Setenv("OWNERSHIP_LEASE_TTL", "15s")
	cfg, err := ownershipSettings(true)
	if err != nil || !cfg.enabled || cfg.ttl != 15*time.Second {
		t.Fatalf("valid settings: %+v %v", cfg, err)
	}
	if _, err := ownershipSettings(false); err == nil {
		t.Fatal("accepted ownership without private adapter")
	}
	for _, ttl := range []string{"", "5s", "61s", "invalid"} {
		t.Setenv("OWNERSHIP_LEASE_TTL", ttl)
		if _, err := ownershipSettings(true); err == nil {
			t.Fatalf("accepted TTL %q", ttl)
		}
	}
	t.Setenv("CANONICAL_OWNERSHIP_ENABLED", "invalid")
	if _, err := ownershipSettings(true); err == nil {
		t.Fatal("accepted malformed switch")
	}
	t.Setenv("CANONICAL_OWNERSHIP_ENABLED", "false")
	if _, err := ownershipSettings(false); err != nil {
		t.Fatal(err)
	}
}
