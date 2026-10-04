package seed

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNextEventUniqueIDsAndWarningOnlyWithTsunamiPotential(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	seen := map[string]bool{}
	warned := 0
	for n := 1; n <= 500; n++ {
		event, warning := NextEvent(1700000000, n, now, rng)
		require.False(t, seen[event.EventID], "duplicate event ID %s", event.EventID)
		seen[event.EventID] = true
		require.True(t, event.OccurredAt.Time.Equal(now))
		require.Equal(t, event.PotentialTsunami, warning != nil)
		if warning != nil {
			warned++
			require.Equal(t, event.EventID, warning.RelatedEventID)
			require.GreaterOrEqual(t, event.Magnitude, 6.0)
		}
	}
	require.Positive(t, warned)
}

func TestWarningsOnlyReferenceTsunamiPotentialEvents(t *testing.T) {
	events, warnings := Build()
	require.GreaterOrEqual(t, len(events), 20)
	byID := make(map[string]bool, len(events))
	for _, event := range events {
		byID[event.EventID] = event.PotentialTsunami
	}
	for _, warning := range warnings {
		potential, exists := byID[warning.RelatedEventID]
		require.True(t, exists, "warning %s has no seismic event", warning.WarningID)
		require.True(t, potential, "warning %s references an event without tsunami potential", warning.WarningID)
	}
}
