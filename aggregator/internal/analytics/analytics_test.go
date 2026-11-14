package analytics

import (
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
	"time"
)

func TestFilters(t *testing.T) {
	now := time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)
	q, e := Parse(url.Values{}, now)
	require.NoError(t, e)
	require.Equal(t, now.Add(-7*24*time.Hour), q.From)
	for _, raw := range []string{"source=unknown", "interval=week", "from=2026-01-01T00:00:00%2B07:00", "to=2026-01-01T00:00:00Z&from=2026-01-02T00:00:00Z", "interval=hour&from=2025-01-01T00:00:00Z", "source=BMKG&source=SATELIT", "snapshot_id=bad", "extra=x"} {
		v, _ := url.ParseQuery(raw)
		_, e = Parse(v, now)
		require.Error(t, e, raw)
	}
}
