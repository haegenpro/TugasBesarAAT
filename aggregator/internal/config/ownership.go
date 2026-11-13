package config

import (
	"aggregator/internal/canonical"
	"fmt"
	"os"
	"strconv"
	"time"
)

type ownershipConfig struct {
	enabled bool
	ttl     time.Duration
}

func ownershipSettings(canonicalEnabled bool) (ownershipConfig, error) {
	var cfg ownershipConfig
	if raw := os.Getenv("CANONICAL_OWNERSHIP_ENABLED"); raw != "" {
		var err error
		cfg.enabled, err = strconv.ParseBool(raw)
		if err != nil {
			return cfg, fmt.Errorf("invalid CANONICAL_OWNERSHIP_ENABLED")
		}
	}
	if !cfg.enabled {
		return cfg, nil
	}
	if !canonicalEnabled {
		return cfg, fmt.Errorf("CANONICAL_OWNERSHIP_ENABLED requires CANONICAL_ADAPTER_ENABLED")
	}
	var err error
	cfg.ttl, err = time.ParseDuration(os.Getenv("OWNERSHIP_LEASE_TTL"))
	if err != nil || cfg.ttl < 3*canonical.Deadline || cfg.ttl > time.Minute {
		return cfg, fmt.Errorf("OWNERSHIP_LEASE_TTL must be between 6s and 1m")
	}
	return cfg, nil
}
