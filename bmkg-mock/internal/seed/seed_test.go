package seed

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
