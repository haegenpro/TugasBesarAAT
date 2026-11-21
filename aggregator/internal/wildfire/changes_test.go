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

func TestUpdatesPreserveRetractionAndExplicitDetectionRestores(t *testing.T) {
	m := newModel(t)
	a, b := detection("0.000", "0000"), detection("0.001", "0001")
	apply(t, m, operation(1, a))
	apply(t, m, operation(2, b))
	require.Len(t, active(m), 1)
	apply(t, m, Operation{Version: 1, Namespace: config().Namespace, Offset: 3, Kind: "retract", TargetKey: key(a)})
	require.Empty(t, active(m))
	op := Operation{Version: 1, Namespace: config().Namespace, Offset: 4, Kind: "update", TargetKey: key(a), Changes: json.RawMessage(`{"frp":200,"confidence":"h"}`)}
	r := apply(t, m, op)
	require.Nil(t, r.Batch, "an inactive detection must not recreate a wildfire")
	require.Empty(t, active(m))
	for _, ob := range m.Snapshot().Observations {
		if ob.Key == key(a) {
			require.False(t, ob.Active)
			require.Equal(t, 200.0, ob.Detection.FRPMW)
		}
	}
	_, err := m.Apply(op)
	require.NoError(t, err)
	replacement := a
	replacement.FRPMW = 300
	apply(t, m, Operation{Version: 1, Namespace: config().Namespace, Offset: 5, Kind: "update", TargetKey: key(a), Detection: &replacement})
	require.Empty(t, active(m))
	restored := newModel(t)
	for _, retained := range m.Snapshot().History {
		apply(t, restored, retained)
	}
	require.Equal(t, m.Snapshot(), restored.Snapshot())
	apply(t, m, operation(6, a))
	require.Len(t, active(m), 1)
}
