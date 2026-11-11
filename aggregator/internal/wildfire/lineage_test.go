package wildfire

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

type historical struct {
	Identity            string   `json:"identity"`
	Membership          []string `json:"membership"`
	DeactivatedPosition int64    `json:"deactivated_position"`
	BirthOffset         int64    `json:"birth_offset"`
}

func seedObservation(label string) Observation {
	d := detection("0", "0000")
	o, _ := parse(d, 1, nil)
	at := time.Date(2026, 11, 12, 0, int(label[0]-'A'), 0, 0, time.UTC)
	o.Key = label
	o.EventTime = &at
	return o
}
func seedFire(id string, members []string, active bool, at, birth int64) Fire {
	keys := append([]string{}, members...)
	sort.Strings(keys)
	f := Fire{ID: id, Members: keys, Active: active, Derivation: Derivation{config().Namespace, id, birth}, LogicalRevision: 2, Related: []string{}}
	if !active {
		f.DeactivatedAt = &at
	}
	return f
}
func TestSharedRestorationFixtures(t *testing.T) {
	raw, e := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "m2-contract-examples.json"))
	require.NoError(t, e)
	var fixtures struct {
		Cases []struct {
			Name   string `json:"name"`
			Before struct {
				ActiveParent    string       `json:"active_parent"`
				Membership      []string     `json:"membership"`
				InactiveHistory []historical `json:"inactive_history"`
			} `json:"before"`
			SplitChildren      [][]string     `json:"split_children"`
			InactiveHistory    []historical   `json:"inactive_history"`
			RestoredMembership []string       `json:"restored_membership"`
			Expected           map[string]any `json:"expected"`
			ExpectedIdentity   string         `json:"expected_identity"`
		} `json:"wildfire_lifecycle_cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixtures))
	for _, tc := range fixtures.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Name == "bridge_merge_then_retract" {
				testSharedBridge(t, raw)
				return
			}
			old := map[string]Fire{}
			obs := map[string]Observation{}
			components := [][]string{}
			all := []string{}
			for _, c := range tc.SplitChildren {
				for _, k := range c {
					obs[k] = seedObservation(k)
					all = append(all, k)
				}
				if len(c) >= 2 {
					components = append(components, c)
				}
			}
			if len(tc.RestoredMembership) > 0 {
				components = append(components, tc.RestoredMembership)
				for _, k := range tc.RestoredMembership {
					obs[k] = seedObservation(k)
				}
			}
			parent := tc.Before.ActiveParent
			if parent == "" && tc.Name == "singleton_does_not_keep_active_identity" {
				parent = "current-parent"
				tc.Before.Membership = all
			}
			if parent != "" {
				old[parent] = seedFire(parent, tc.Before.Membership, true, 0, 1)
			}
			histories := append(tc.Before.InactiveHistory, tc.InactiveHistory...)
			for _, h := range histories {
				f := seedFire(h.Identity, h.Membership, false, h.DeactivatedPosition, h.BirthOffset)
				if parent != "" {
					f.Related = []string{parent}
					p := old[parent]
					p.Related = append(p.Related, h.Identity)
					old[parent] = p
				}
				old[h.Identity] = f
			}
			next, _, diagnostics := resolve(config().Namespace, 20, components, obs, old, nil, []Province{{ID: "fixture", Name: "Fixture"}})
			require.Empty(t, diagnostics)
			if tc.ExpectedIdentity != "" {
				require.True(t, next[tc.ExpectedIdentity].Active)
			}
			for name, expected := range tc.Expected {
				switch name {
				case "inactive":
					for _, id := range expected.([]any) {
						require.False(t, next[id.(string)].Active)
					}
				case "extra_active_parent", "singletons", "active_fire_count":
					continue
				default:
					id, ok := expected.(string)
					if !ok {
						continue
					}
					require.True(t, next[id].Active, id)
					require.Equal(t, strings.Split(name, ""), next[id].Members)
				}
			}
			assigned := map[string]bool{}
			for id, f := range next {
				if f.Active {
					require.False(t, assigned[id])
					assigned[id] = true
				}
			}
		})
	}
}
func testSharedBridge(t *testing.T, raw []byte) {
	var fixture struct {
		Cases []struct {
			Name  string `json:"name"`
			Nodes []struct {
				Key       string  `json:"key"`
				Longitude float64 `json:"longitude"`
				Minute    int     `json:"minute"`
			} `json:"nodes"`
		} `json:"wildfire_lifecycle_cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	m := newModel(t)
	ids := []string{}
	var target string
	for i, node := range fixture.Cases[0].Nodes {
		d := detection("0", "0000")
		d.Longitude = jsonNumber(node.Longitude)
		d.AcqTime = time.Date(2026, 1, 1, 0, node.Minute, 0, 0, time.UTC).Format("1504")
		apply(t, m, operation(int64(i+1), d))
		if i == 1 {
			ids = append(ids, active(m)[0].ID)
		}
		if i == 3 {
			for _, f := range active(m) {
				if f.ID != ids[0] {
					ids = append(ids, f.ID)
				}
			}
		}
		if i == 4 {
			target = key(d)
			require.Len(t, active(m), 1)
			require.Equal(t, ids[0], active(m)[0].ID)
		}
	}
	apply(t, m, Operation{Version: 1, Namespace: config().Namespace, Offset: 6, Kind: "retract", TargetKey: target})
	got := []string{}
	for _, f := range active(m) {
		got = append(got, f.ID)
	}
	require.ElementsMatch(t, ids, got)
}
func jsonNumber(v float64) string { raw, _ := json.Marshal(v); return string(raw) }
func TestRestorationTiesAndUndefinedCenterIsolation(t *testing.T) {
	obs := map[string]Observation{"A": seedObservation("A"), "B": seedObservation("B")}
	a, b := seedFire("a", []string{"A", "B"}, false, 7, 2), seedFire("b", []string{"A", "B"}, false, 7, 4)
	next, _, diag := resolve(config().Namespace, 20, [][]string{{"A", "B"}}, obs, map[string]Fire{"a": a, "b": b}, nil, []Province{{ID: "fixture", Name: "Fixture"}})
	require.Empty(t, diag)
	require.True(t, next["a"].Active)
	b.Derivation.BirthOffset = 2
	b.Derivation.EarliestKey = "Z"
	a.Derivation.EarliestKey = "A"
	next, _, _ = resolve(config().Namespace, 20, [][]string{{"A", "B"}}, obs, map[string]Fire{"a": a, "b": b}, nil, []Province{{ID: "fixture", Name: "Fixture"}})
	require.True(t, next["a"].Active)
	a.Derivation.EarliestKey = "\""
	b.Derivation.EarliestKey = "A"
	next, _, _ = resolve(config().Namespace, 20, [][]string{{"A", "B"}}, obs, map[string]Fire{"a": a, "b": b}, nil, []Province{{ID: "fixture", Name: "Fixture"}})
	require.True(t, next["a"].Active, "tie uses original component UTF-8 bytes, not escaped JSON ordering")
	// Direct mapper/lineage unit fixture: antipodal members cannot form a small
	// two-point graph, but the mapping stage must still reject degenerate means.
	antipode := obs["B"]
	antipode.Longitude = 180
	obs["B"] = antipode
	obs["C"] = seedObservation("C")
	obs["D"] = seedObservation("D")
	next, intents, diag := resolve(config().Namespace, 20, [][]string{{"A", "B"}, {"C", "D"}}, obs, map[string]Fire{}, nil, []Province{{ID: "fixture", Name: "Fixture"}})
	require.Len(t, diag, 1)
	require.Equal(t, "undefined_center", diag[0].Code)
	require.Len(t, intents, 1)
	require.Len(t, next, 1)
}
func TestMixedMergeSplitReproduction(t *testing.T) {
	raw, e := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "wildfire-m5", "mixed-merge-split.jsonl"))
	require.NoError(t, e)
	m := newModel(t)
	var prior Snapshot
	for i, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var op Operation
		require.NoError(t, Decode([]byte(line), &op))
		if i == 7 {
			prior = m.Snapshot()
		}
		result := apply(t, m, op)
		if i == 7 {
			require.Equal(t, "blocked_output", result.Outcome)
			require.Nil(t, result.Batch)
			require.Equal(t, "ambiguous_merge_split", result.State.Diagnostics[0].Code)
			require.Equal(t, prior.Fires, result.State.Fires)
		}
	}
	apply(t, m, operation(9, detection("1", "0100")))
	result := apply(t, m, operation(10, detection("1.001", "0101")))
	require.Equal(t, "blocked_output", result.Outcome)
	require.Len(t, result.Batch.Intents, 1)
	require.Len(t, active(m), 3)
}
