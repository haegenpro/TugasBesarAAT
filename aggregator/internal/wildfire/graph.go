package wildfire

import (
	"github.com/pymaxion/geographiclib-go/v2/geodesic"
	"math"
	"sort"
	"time"
)

func distance(a, b Observation) float64 {
	return geodesic.WGS84.Inverse(a.Latitude, a.Longitude, b.Latitude, b.Longitude).S12
}
func edge(metres float64, delta time.Duration) bool {
	if delta == time.Duration(math.MinInt64) {
		return false
	}
	if delta < 0 {
		delta = -delta
	}
	return finite(metres) && metres >= 0 && metres <= 1000 && delta <= 360*time.Minute
}
func earlier(a, b Observation) bool {
	if a.EventTime.Equal(*b.EventTime) {
		return a.Key < b.Key
	}
	return a.EventTime.Before(*b.EventTime)
}
func earliest(keys []string, obs map[string]Observation) string {
	key := keys[0]
	for _, k := range keys[1:] {
		if earlier(obs[k], obs[key]) {
			key = k
		}
	}
	return key
}
func graph(obs map[string]Observation) ([][]string, int, int) {
	keys := []string{}
	excluded := 0
	for key, o := range obs {
		if !o.Active {
			continue
		}
		if o.Detection.Confidence == "l" {
			excluded++
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parents := make([]int, len(keys))
	for i := range parents {
		parents[i] = i
	}
	var find func(int) int
	find = func(i int) int {
		if parents[i] != i {
			parents[i] = find(parents[i])
		}
		return parents[i]
	}
	for i, k := range keys {
		for j := i + 1; j < len(keys); j++ {
			a, b := obs[k], obs[keys[j]]
			delta := a.EventTime.Sub(*b.EventTime)
			if delta == time.Duration(math.MinInt64) {
				continue
			}
			if delta < 0 {
				delta = -delta
			}
			if delta > 360*time.Minute {
				continue
			}
			if edge(distance(a, b), delta) {
				parents[find(j)] = find(i)
			}
		}
	}
	groups := map[int][]string{}
	for i, k := range keys {
		r := find(i)
		groups[r] = append(groups[r], k)
	}
	components := [][]string{}
	singletons := 0
	for _, group := range groups {
		if len(group) < 2 {
			singletons++
			continue
		}
		components = append(components, group)
	}
	sort.Slice(components, func(i, j int) bool {
		return earlier(obs[earliest(components[i], obs)], obs[earliest(components[j], obs)])
	})
	return components, singletons, excluded
}
