package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAcquisitionSettings(t *testing.T) {
	t.Setenv("ACQUISITION_ENABLED", "false")
	t.Setenv("CANONICAL_DURABILITY_CONFIG", "")
	on, _, err := acquisitionSettings()
	require.NoError(t, err)
	require.False(t, on)
	t.Setenv("ACQUISITION_ENABLED", "true")
	t.Setenv("ACQUISITION_LEASE_TTL", "15s")
	_, _, err = acquisitionSettings()
	require.Error(t, err, "multiple pollers need the configured replicated authority")
	t.Setenv("CANONICAL_DURABILITY_CONFIG", "/example/durability.json")
	on, ttl, err := acquisitionSettings()
	require.NoError(t, err)
	require.True(t, on)
	require.Equal(t, 15*time.Second, ttl)
	for _, value := range []string{"", "11s", "31s", "12s1ns", "invalid"} {
		t.Setenv("ACQUISITION_LEASE_TTL", value)
		_, _, err := acquisitionSettings()
		require.Error(t, err, value)
	}
	t.Setenv("ACQUISITION_ENABLED", "invalid")
	_, _, err = acquisitionSettings()
	require.Error(t, err)
}
