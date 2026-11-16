package store

import (
	"aggregator/internal/clients"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLegacyReplayLifecycle(t *testing.T) {
	dsn, c := m2Database(t, false)
	m2Schema(t, c, "postgres", false)
	s := m2Store(t, dsn)
	ctx := context.Background()
	for _, payload := range []string{`[{"report_id":"one"}]`, `[{"report_id":"two"}]`} {
		require.NoError(t, s.RecordFailure(ctx, clients.Failure{Source: "PVMBG", Entity: "/volcanic-reports", Payload: []byte(payload), Reason: "schema_incompatible", CorrelationID: "fixture"}))
	}
	require.NoError(t, s.RecordFailure(ctx, clients.Failure{Source: "BMKG", Entity: "/seismic-events", Payload: []byte(`{}`), Reason: "bmkg", Retryable: true, CorrelationID: "fixture"}))
	var ids []string
	rows, err := s.Pool.Query(ctx, `SELECT message_id::text FROM failed_ingestions WHERE source='PVMBG' ORDER BY received_at,message_id`)
	require.NoError(t, err)
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	require.Len(t, ids, 2)

	_, err = s.RequestLegacyReplay(ctx, append(ids, "00000000-0000-4000-8000-00000000ffff"), "pvmbg-adapter-v2")
	require.Error(t, err)
	selected, err := s.RequestLegacyReplay(ctx, ids, "pvmbg-adapter-v2")
	require.NoError(t, err)
	require.Equal(t, int64(2), selected)
	counts, err := s.LegacyReplayStatus(ctx, ids)
	require.NoError(t, err)
	require.Equal(t, 2, counts["retrying"])

	next, err := s.NextLegacyReplay(ctx)
	require.NoError(t, err)
	require.Equal(t, ids[0], next.ID)
	require.Equal(t, `[{"report_id":"one"}]`, string(next.Payload))
	require.NoError(t, s.FinishLegacyReplay(ctx, next.ID, nil))
	next, err = s.NextLegacyReplay(ctx)
	require.NoError(t, err)
	require.Equal(t, ids[1], next.ID)
	require.NoError(t, s.FinishLegacyReplay(ctx, next.ID, errors.New("still unreadable")))
	next, err = s.NextLegacyReplay(ctx)
	require.NoError(t, err)
	require.Nil(t, next)

	counts, err = s.LegacyReplayStatus(ctx, ids)
	require.NoError(t, err)
	require.Equal(t, map[string]int{"selected": 2, "recovered": 1, "retrying": 0, "parked": 1}, counts)
	var visible int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM failed_ingestions WHERE status<>'recovered'`).Scan(&visible))
	require.Equal(t, 2, visible)
}
