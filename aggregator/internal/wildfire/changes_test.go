package wildfire

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartialUpdatePreservesIdentityAndHistory(t *testing.T) {
	m := newModel(t)
	d := detection("0.000", "0000")
	apply(t, m, operation(1, d))
	op := Operation{Version: 1, Namespace: config().Namespace, Offset: 2, Kind: "update", TargetKey: key(d), Changes: json.RawMessage(`{"frp":8.68,"bright_ti5":272.92}`)}
	apply(t, m, op)
	state := m.Snapshot()
	require.Equal(t, key(d), state.Observations[0].Key)
	require.Equal(t, d.Confidence, state.Observations[0].Detection.Confidence)
	require.Equal(t, 8.68, state.Observations[0].Detection.FRPMW)
	restored := newModel(t)
	for _, retained := range state.History {
		apply(t, restored, retained)
	}
	require.Equal(t, state, restored.Snapshot())
	_, err := m.Apply(op)
	require.NoError(t, err, "lost-response redelivery preserves the original patch identity")
	for _, raw := range []string{`{"confidence":null}`, `{"frp":null}`, `{"frp":"NaN"}`, `{"frp":1,"frp_mw":2}`} {
		_, err := MergeDetection(d, json.RawMessage(raw))
		require.Error(t, err)
	}
}
