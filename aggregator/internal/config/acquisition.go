package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

func acquisitionSettings() (bool, time.Duration, error) {
	enabled := false
	var err error
	if raw := os.Getenv("ACQUISITION_ENABLED"); raw != "" {
		enabled, err = strconv.ParseBool(raw)
		if err != nil {
			return false, 0, errors.New("invalid ACQUISITION_ENABLED")
		}
	}
	if !enabled {
		return false, 0, nil
	}
	if os.Getenv("CANONICAL_DURABILITY_CONFIG") == "" {
		return false, 0, errors.New("ACQUISITION_ENABLED requires CANONICAL_DURABILITY_CONFIG")
	}
	ttl, err := time.ParseDuration(os.Getenv("ACQUISITION_LEASE_TTL"))
	if err != nil || ttl < 12*time.Second || ttl > 30*time.Second || ttl%time.Millisecond != 0 {
		return false, 0, errors.New("ACQUISITION_LEASE_TTL must be 12s..30s in whole milliseconds")
	}
	return true, ttl, nil
}
