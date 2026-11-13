package wildfire

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"
)

// Assignment definition regression: this independent BFS uses analytical WGS84
// equatorial arc lengths, rather than the model's graph/distance/union-find.
// The seed is test reproducibility only; production has no profile/seed cases.
func TestAssignmentDefinitionAcrossOrderedMutations(t *testing.T) {
	m := newModel(t)
	rng := rand.New(rand.NewSource(4031))
	detections := make([]Detection, 12)
	for i := range detections {
		d := detection(fmt.Sprintf("%.3f", float64(i)*0.004), time.Date(2026, 11, 12, 0, i*31, 0, 0, time.UTC).Format("1504"))
		if i%3 == 0 {
			d.Satellite = "N21"
		}
		detections[i] = d
	}
	retained := map[int]Detection{}
	for step := 1; step <= 100; step++ {
		i := rng.Intn(len(detections))
		d := detections[i]
		op := operation(int64(step), d)
		if rng.Intn(4) == 0 {
			op.Kind = "retract"
			op.Detection = nil
			op.TargetKey = key(d)
			delete(retained, i)
		} else {
			d.FRPMW = float64(rng.Intn(40))
			d.Confidence = []string{"l", "n", "h"}[rng.Intn(3)]
			op.Detection = &d
			retained[i] = d
		}
		apply(t, m, op)
		state := m.Snapshot()
		require.Empty(t, state.Diagnostics)
		included := []int{}
		for j, d := range retained {
			if d.Confidence != "l" {
				included = append(included, j)
			}
		}
		sort.Ints(included)
		visited := map[int]bool{}
		expected := map[string][]int{}
		singletons := 0
		for _, start := range included {
			if visited[start] {
				continue
			}
			visited[start] = true
			queue := []int{start}
			component := []int{}
			for len(queue) > 0 {
				current := queue[0]
				queue = queue[1:]
				component = append(component, current)
				for _, other := range included {
					if visited[other] {
						continue
					}
					metres := math.Abs(float64(current-other)*0.004) * math.Pi / 180 * 6378137
					minutes := math.Abs(float64(current-other) * 31)
					if metres <= 1000 && minutes <= 360 {
						visited[other] = true
						queue = append(queue, other)
					}
				}
			}
			if len(component) == 1 {
				singletons++
				continue
			}
			keys := []string{}
			for _, j := range component {
				keys = append(keys, key(detections[j]))
			}
			sort.Strings(keys)
			expected[strings.Join(keys, "\n")] = component
		}
		require.Equal(t, singletons, state.Singletons, "offset %d", step)
		require.Len(t, active(m), len(expected), "offset %d", step)
		for _, f := range active(m) {
			members, ok := expected[strings.Join(f.Members, "\n")]
			require.True(t, ok, "fire membership differs at offset %d", step)
			require.Equal(t, int64(len(members)), f.Event.Attributes.DetectionCount)
			require.Equal(t, f.Members, f.Event.Attributes.DetectionKeys)
			total := 0.0
			confidence := "n"
			satellites := map[string]bool{}
			earliestIndex := len(detections)
			for _, j := range members {
				v := retained[j]
				total += v.FRPMW
				satellites[v.Satellite] = true
				if v.Confidence == "h" {
					confidence = "h"
				}
				if j < earliestIndex {
					earliestIndex = j
				}
			}
			require.Equal(t, total, f.Event.Attributes.TotalFRPMW)
			require.Equal(t, confidence, f.Event.Attributes.MaxConfidence)
			require.Equal(t, len(satellites), len(f.Event.Attributes.Satellites))
			require.Equal(t, "SATELIT", string(f.Source))
			require.Equal(t, "WILDFIRE", string(f.Event.HazardType))
			require.Equal(t, "Fixture Province", f.Event.AreaName)
			wantSeverity := "WASPADA"
			if len(members) >= 6 || total >= 30 {
				wantSeverity = "SIAGA"
			}
			if len(members) >= 20 || total >= 100 {
				wantSeverity = "AWAS"
			}
			require.Equal(t, wantSeverity, string(f.Event.Severity))
			wantTime := time.Date(2026, 11, 12, 0, earliestIndex*31, 0, 0, time.UTC)
			require.True(t, f.Event.OccurredAt.Equal(wantTime))
		}
	}
}
