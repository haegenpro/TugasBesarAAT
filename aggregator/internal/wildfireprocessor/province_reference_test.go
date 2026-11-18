package wildfireprocessor

import (
	"aggregator/internal/wildfire"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIntegratedProvinceReferenceMapping(t *testing.T) {
	// Exercise the checked-in deployment config, including relative file loading
	// and its exact-byte hash, rather than the one-province synthetic fixture.
	c, err := LoadConfig(filepath.Join("..", "..", "..", "deployment", "role1", "wildfire.integrated.example.json"))
	require.NoError(t, err)
	provinces, _, err := c.validate()
	require.NoError(t, err)
	require.Len(t, provinces, 38)

	for _, tc := range []struct {
		name     string
		lat, lon float64
		province string
	}{
		{"Jakarta", -6.200000, 106.816666, "DKI Jakarta"},
		{"Bandung", -6.917500, 107.619100, "Jawa Barat"},
		{"Merauke", -8.490000, 140.400000, "Papua Selatan"},
		{"Sorong", -0.876000, 131.255000, "Papua Barat Daya"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			at := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
			var a wildfire.Aggregate
			a.Add("first", at, tc.lat, tc.lon, 10, "n")
			a.Add("second", at.Add(time.Minute), tc.lat, tc.lon, 10, "n")
			event, err := a.Event([]string{"first", "second"}, []string{"N20"}, provinces)
			require.NoError(t, err)
			require.Equal(t, tc.province, event.AreaName)
			// Province assignment must not replace the fire's coordinates.
			require.InDelta(t, tc.lat, event.Latitude, 1e-10)
			require.InDelta(t, tc.lon, event.Longitude, 1e-10)
		})
	}
}
