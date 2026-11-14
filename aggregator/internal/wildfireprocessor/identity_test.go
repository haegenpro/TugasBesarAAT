package wildfireprocessor

import (
	"aggregator/internal/rawinput"
	"aggregator/internal/wildfire"
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestM6NormalizedHistoryGapsAndLexicalIdentity(t *testing.T) {
	l := databaseLab(t, true)
	m := oracle(t, l.e.Config)
	d := *fixtureOperations(t, "bridge")[0].Detection
	d.Satellite, d.Latitude, d.Longitude = "同じ|satellite", "0", "0.000"
	op := wildfire.Operation{Version: 1, Namespace: l.e.Config.Namespace, Offset: 1, Kind: "detection", Detection: &d, Payload: []byte{0, 255}}
	applyBoth(t, l, m, op)
	gap := envelope(t, wildfire.Operation{Version: 1, Namespace: op.Namespace, Offset: 3, Kind: "heartbeat"})
	_, err := l.s.AppendHistory(context.Background(), rawinput.AppendRequest{Version: 1, Ownership: l.p, Messages: []rawinput.Envelope{gap}})
	require.Error(t, err)
	var received int64
	require.NoError(t, l.c.QueryRow(context.Background(), `SELECT received_through FROM input_sessions`).Scan(&received))
	require.Equal(t, int64(1), received)
	// Equal coordinates with distinct original lexical forms are distinct keys.
	d.Longitude = "0.0"
	op.Offset, op.Detection = 2, &d
	applyBoth(t, l, m, op)
	var identities int
	require.NoError(t, l.c.QueryRow(context.Background(), `SELECT count(DISTINCT entity_key) FROM wildfire_state WHERE kind='observation'`).Scan(&identities))
	require.Equal(t, 2, identities)
	for _, fire := range l.fires(t) {
		if fire.Active {
			require.Len(t, fire.Event.Attributes.Satellites, 1)
			require.Len(t, fire.Members, 2)
		}
	}
	_, err = l.c.Exec(context.Background(), `INSERT INTO wildfire_state(route_key,namespace,kind,entity_key,position,payload,sha256) VALUES('wrong',$1,'observation','fixture',99,$2,$3)`, op.Namespace, []byte("{}"), rawinput.Hash([]byte("{}")))
	var constraint *pgconn.PgError
	require.ErrorAs(t, err, &constraint)
	require.Equal(t, "23514", constraint.Code)
	unknown := wildfire.Operation{Version: 1, Namespace: op.Namespace, Offset: 3, Kind: "update", TargetKey: "unknown", Detection: &d}
	l.append(t, unknown)
	_, err = l.e.Step(context.Background(), l.p)
	require.ErrorContains(t, err, "unknown update target")
	var prepared, applied int64
	require.NoError(t, l.c.QueryRow(context.Background(), `SELECT prepared_through,applied_through FROM input_sessions`).Scan(&prepared, &applied))
	require.Equal(t, int64(2), prepared)
	require.Equal(t, int64(2), applied)
}
