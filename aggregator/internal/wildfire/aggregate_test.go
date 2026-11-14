package wildfire

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestAggregateCheckpointPreservesReferenceMapping(t *testing.T) {
	obs := map[string]Observation{}
	keys := []string{"a", "b", "c"}
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var a Aggregate
	for i, key := range keys {
		o := Observation{Key: key, Active: true, EventTime: &at, Latitude: 0, Longitude: 179.999 + float64(i)*.001, Detection: &Detection{Satellite: "single", Confidence: "h", FRPMW: []float64{1e16, 1, 1}[i]}}
		obs[key] = o
		a.Add(key, at, o.Latitude, o.Longitude, o.Detection.FRPMW, "h")
		b, e := json.Marshal(a)
		require.NoError(t, e)
		a = Aggregate{}
		require.NoError(t, json.Unmarshal(b, &a))
	}
	provinces := []Province{{ID: "p", Name: "Synthetic", Latitude: 0, Longitude: 180}}
	expected, e := mapEvent(keys, obs, provinces)
	require.NoError(t, e)
	actual, e := a.Event(keys, []string{"single"}, provinces)
	require.NoError(t, e)
	require.Equal(t, expected, actual)
}
