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

func TestLaterUpdatesReactivateAndOlderReplaysCannotUndoRetraction(t *testing.T) {
	m := newModel(t)
	a, b := detection("0.000", "0000"), detection("0.001", "0001")
	apply(t, m, operation(1, a))
	apply(t, m, operation(2, b))
	require.Len(t, active(m), 1)
	originalID := active(m)[0].ID
	apply(t, m, Operation{Version: 1, Namespace: config().Namespace, Offset: 3, Kind: "retract", TargetKey: key(a)})
	require.Empty(t, active(m))
	op := Operation{Version: 1, Namespace: config().Namespace, Offset: 4, Kind: "update", TargetKey: key(a), Changes: json.RawMessage(`{"frp":200,"confidence":"h"}`)}
	r := apply(t, m, op)
	require.NotNil(t, r.Batch)
	require.Len(t, active(m), 1)
	require.Equal(t, originalID, active(m)[0].ID, "restore the original logical wildfire")
	for _, ob := range m.Snapshot().Observations {
		if ob.Key == key(a) {
			require.True(t, ob.Active)
			require.Equal(t, 200.0, ob.Detection.FRPMW)
		}
	}
	_, err := m.Apply(op)
	require.NoError(t, err)
	replacement := a
	replacement.FRPMW = 300
	apply(t, m, Operation{Version: 1, Namespace: config().Namespace, Offset: 5, Kind: "update", TargetKey: key(a), Detection: &replacement})
	require.Len(t, active(m), 1)
	require.Equal(t, originalID, active(m)[0].ID)
	apply(t, m, Operation{Version: 1, Namespace: config().Namespace, Offset: 6, Kind: "retract", TargetKey: key(a)})
	closed := m.Snapshot()
	_, err = m.Apply(op)
	require.NoError(t, err)
	require.Equal(t, closed, m.Snapshot(), "an older update replay must not undo a later retraction")
	_, err = m.Apply(operation(1, a))
	require.NoError(t, err)
	require.Equal(t, closed, m.Snapshot(), "an older detection replay must not restore the target either")
	_, err = m.Apply(Operation{Version: 1, Namespace: config().Namespace, Offset: 7, Kind: "update", TargetKey: "unknown", Changes: json.RawMessage(`{"frp":2}`)})
	require.Error(t, err)
	apply(t, m, Operation{Version: 1, Namespace: config().Namespace, Offset: 7, Kind: "update", TargetKey: key(a), Changes: json.RawMessage(`{"confidence":"l"}`)})
	require.Empty(t, active(m), "low confidence stays excluded after reactivation")
	apply(t, m, Operation{Version: 1, Namespace: config().Namespace, Offset: 8, Kind: "update", TargetKey: key(a), Changes: json.RawMessage(`{"confidence":"h"}`)})
	require.Equal(t, originalID, active(m)[0].ID)
	current := m.Snapshot()
	_, err = m.Apply(Operation{Version: 1, Namespace: config().Namespace, Offset: 3, Kind: "retract", TargetKey: key(a)})
	require.NoError(t, err)
	require.Equal(t, current, m.Snapshot(), "an older retraction cannot undo a later restoration")
	restored := newModel(t)
	for _, retained := range m.Snapshot().History {
		apply(t, restored, retained)
	}
	require.Equal(t, m.Snapshot(), restored.Snapshot())
}
