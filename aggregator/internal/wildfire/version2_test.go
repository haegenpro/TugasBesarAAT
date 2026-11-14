package wildfire

import (
	"bufio"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestVersionTwoMixedTransitionPreservesBothSurvivors(t *testing.T) {
	c := config()
	c.AlgorithmVersion = 2
	m, err := New(c)
	require.NoError(t, err)
	f, err := os.Open(filepath.Join("..", "..", "..", "docs", "fixtures", "wildfire-m5", "mixed-merge-split.jsonl"))
	require.NoError(t, err)
	defer f.Close()
	scan := bufio.NewScanner(f)
	var left, right string
	var result Result
	for scan.Scan() {
		var op Operation
		require.NoError(t, Decode(scan.Bytes(), &op))
		result, err = m.Apply(op)
		require.NoError(t, err)
		if op.Offset == 2 {
			left = active(m)[0].ID
		}
		if op.Offset == 7 {
			for _, fire := range active(m) {
				if fire.ID != left {
					right = fire.ID
				}
			}
		}
	}
	require.NoError(t, scan.Err())
	require.Empty(t, result.State.Diagnostics)
	require.Equal(t, "wildfire-reference-v2", result.State.Algorithm)
	require.Len(t, active(m), 2)
	require.Len(t, result.Batch.Intents, 2)
	for _, fire := range active(m) {
		if fire.ID == left {
			require.Len(t, fire.Members, 2)
		} else {
			require.Equal(t, right, fire.ID)
			require.Len(t, fire.Members, 5)
		}
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "wildfire-m6", "mixed.expected.json"))
	require.NoError(t, err)
	var golden struct {
		Fires []struct {
			ID                  string  `json:"source_ref_id"`
			Revision            int64   `json:"logical_revision"`
			Count               int64   `json:"detection_count"`
			FRP                 float64 `json:"total_frp_mw"`
			Severity            string  `json:"severity"`
			Latitude, Longitude float64
		}
	}
	require.NoError(t, json.Unmarshal(raw, &golden))
	got := map[string]Fire{}
	for _, f := range active(m) {
		got[f.ID] = f
	}
	require.Len(t, got, len(golden.Fires))
	for _, want := range golden.Fires {
		fire, ok := got[want.ID]
		require.True(t, ok)
		require.Equal(t, want.Revision, fire.LogicalRevision)
		require.Equal(t, want.Count, fire.Event.Attributes.DetectionCount)
		require.Equal(t, want.FRP, fire.Event.Attributes.TotalFRPMW)
		require.Equal(t, want.Severity, string(fire.Event.Severity))
		require.InDelta(t, want.Latitude, fire.Event.Latitude, 1e-10)
		require.InDelta(t, want.Longitude, fire.Event.Longitude, 1e-10)
	}
}

func TestVersionTwoOneClaimRetiresLoser(t *testing.T) {
	obs := map[string]Observation{}
	for _, k := range []string{"A", "B", "C", "D"} {
		obs[k] = seedObservation(k)
	}
	old := map[string]Fire{"p": seedFire("p", []string{"A", "C"}, true, 0, 1), "q": seedFire("q", []string{"B", "D"}, true, 0, 2)}
	next, intents, diag := resolveVersion(2, config().Namespace, 20, [][]string{{"A", "B"}, {"C", "D"}}, obs, old, nil, []Province{{ID: "fixture", Name: "Fixture"}})
	require.Empty(t, diag)
	require.True(t, next["p"].Active)
	require.Equal(t, []string{"A", "B"}, next["p"].Members)
	require.False(t, next["q"].Active)
	child, err := Identity(Derivation{config().Namespace, "C", 20})
	require.NoError(t, err)
	require.True(t, next[child].Active)
	require.Len(t, intents, 3)
	old["r"] = seedFire("r", []string{"A", "B"}, false, 10, 0)
	next, _, diag = resolveVersion(2, config().Namespace, 20, [][]string{{"A", "B"}, {"C", "D"}}, obs, old, nil, []Province{{ID: "fixture", Name: "Fixture"}})
	require.Empty(t, diag)
	require.True(t, next["r"].Active)
	require.Equal(t, []string{"C", "D"}, next["p"].Members)
	require.False(t, next["q"].Active)
}
