package store_test

import (
	"aggregator/internal/domain"
	"aggregator/internal/ownership"
	"aggregator/internal/store"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestM2ReadBarrierSeesPublicationAfterWaitingForWriter(t *testing.T) {
	s := m2Store(t)
	ctx := context.Background()
	h := sampleEvent(domain.SourceBMKG, uuid.NewString())
	require.NoError(t, ownership.New(s.Pool).Run(ctx, "test:barrier", func(c context.Context) error { _, err := s.SaveDedup(c, h, "barrier"); return err }))
	writer, err := s.Pool.Begin(ctx)
	require.NoError(t, err)
	defer writer.Rollback(ctx)
	_, err = writer.Exec(ctx, `SELECT pg_advisory_xact_lock(4031002)`)
	require.NoError(t, err)
	_, err = writer.Exec(ctx, `UPDATE canonical_visibility SET published=false`)
	require.NoError(t, err)
	type result struct {
		page store.Page
		err  error
	}
	done := make(chan result, 1)
	go func() { p, e := s.QueryPage(ctx, store.Filters{}, store.PageRequest{Limit: 50}); done <- result{p, e} }()
	require.Eventually(t, func() bool {
		var n int
		err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objid=4031002 AND NOT granted`).Scan(&n)
		return err == nil && n > 0
	}, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, writer.Commit(ctx))
	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.True(t, got.page.Partial)
		require.Empty(t, got.page.Events)
	case <-time.After(3 * time.Second):
		t.Fatal("reader did not finish after writer released lock")
	}
	_, err = s.Pool.Exec(ctx, `UPDATE canonical_visibility SET published=true`)
	require.NoError(t, err)
	p, err := s.QueryPage(ctx, store.Filters{}, store.PageRequest{Limit: 50})
	require.NoError(t, err)
	require.False(t, p.Partial)
	require.Len(t, p.Events, 1)
}

func TestM2SummaryProjectionMatchesRawOrderingAndMetadata(t *testing.T) {
	s := m2Store(t)
	ctx := context.Background()
	authority := ownership.New(s.Pool)
	for i := 0; i < 4; i++ {
		h := sampleEvent(domain.SourceBMKG, uuid.NewString())
		h.Attributes["potential_tsunami"] = true
		require.NoError(t, authority.Run(ctx, "test:summary", func(c context.Context) error { _, err := s.SaveDedup(c, h, "summary"); return err }))
	}
	raw, err := s.QueryPage(ctx, store.Filters{}, store.PageRequest{Limit: 2})
	require.NoError(t, err)
	summary, err := s.QueryPage(ctx, store.Filters{SummaryOnly: true}, store.PageRequest{Limit: 2})
	require.NoError(t, err)
	require.False(t, summary.Partial)
	require.Equal(t, raw.NextCursor, summary.NextCursor)
	require.Equal(t, raw.Metadata, summary.Metadata)
	for i, h := range summary.Events {
		require.Equal(t, raw.Events[i].HazardID, h.HazardID)
		require.Equal(t, raw.Events[i].OccurredAt, h.OccurredAt)
		require.Empty(t, h.SourceRefID)
		require.Zero(t, h.Latitude)
		require.Zero(t, h.Longitude)
		require.Equal(t, map[string]any{"potential_tsunami": true}, h.Attributes)
	}
	next, err := s.QueryPage(ctx, store.Filters{SummaryOnly: true}, store.PageRequest{Limit: 2, Cursor: *summary.NextCursor})
	require.NoError(t, err)
	require.Len(t, next.Events, 2)
	require.NotEqual(t, summary.Events[0].HazardID, next.Events[0].HazardID)
}
